package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.nats.client.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.util.ReflectionTestUtils;

class BatchRecoveryTest {

    @Test
    void retriesConsumerBootstrapWithoutWaitingForReconnect() {
        ApplicationContext context = mock(ApplicationContext.class);
        NatsStreamBootstrap bootstrap = mock(NatsStreamBootstrap.class);
        Connection connection = mock(Connection.class);
        when(connection.getStatus()).thenReturn(Connection.Status.CONNECTED);
        when(context.getBean(NatsStreamBootstrap.class)).thenReturn(bootstrap);
        NatsConnectionHolder holder = new NatsConnectionHolder(context);
        ReflectionTestUtils.setField(holder, "connection", connection);

        holder.retryBootstrap();

        verify(bootstrap).bootstrap(connection);
    }

    @Test
    void cancelledScheduleNeverCreatesAnotherReminder() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), any(RowMapper.class), any(byte[].class))).thenReturn(List.of());
        ScheduleReconcileService reconcile = new ScheduleReconcileService(jdbc);
        Instant now = Instant.parse("2026-09-24T00:00:00Z");
        ScheduleEventPayload cancelled = new ScheduleEventPayload(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), "private title",
                now.plusSeconds(3600), null, false, "manual", "cancelled", 1L,
                List.of(new ReminderPayload(30, "push")), now);

        reconcile.upsert(cancelled);

        verify(jdbc).update(anyString(), any(byte[].class));
        assertThat(org.mockito.Mockito.mockingDetails(jdbc).getInvocations())
                .noneMatch(call -> call.getMethod().getName().equals("update")
                        && call.getArgument(0).toString().startsWith("INSERT INTO reminder_dispatch"));
    }

    @Test
    void unknownSourceCannotBeCountedAsManual() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        ScheduleReconcileService reconcile = new ScheduleReconcileService(jdbc);
        Instant now = Instant.parse("2026-09-24T00:00:00Z");

        assertThatThrownBy(() -> reconcile.upsert(new ScheduleEventPayload(
                UUID.randomUUID().toString(), UUID.randomUUID().toString(), "meeting",
                now.plusSeconds(3600), null, false, "unexpected", "confirmed", 1L,
                List.of(), now)))
                .isInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(jdbc);
    }

    @Test
    void revisionOrdersDifferentUpdatesInTheSameMillisecond() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        Instant now = Instant.parse("2026-09-24T00:00:00Z");
        ResultSet row = mock(ResultSet.class);
        when(row.getTimestamp("last_event_at")).thenReturn(Timestamp.from(now));
        when(row.getObject("last_revision", Long.class)).thenReturn(2L);
        when(row.getBoolean("is_deleted")).thenReturn(false);
        when(jdbc.query(anyString(), any(RowMapper.class), any(byte[].class)))
                .thenAnswer(call -> List.of(((RowMapper<?>) call.getArgument(1)).mapRow(row, 0)));
        ScheduleReconcileService reconcile = new ScheduleReconcileService(jdbc);
        String scheduleId = UUID.randomUUID().toString();
        String userId = UUID.randomUUID().toString();

        reconcile.upsert(new ScheduleEventPayload(scheduleId, userId, "old", now.plusSeconds(3600),
                null, false, "manual", "confirmed", 1L, List.of(), now));
        assertThat(org.mockito.Mockito.mockingDetails(jdbc).getInvocations())
                .noneMatch(call -> call.getMethod().getName().equals("update"));

        reconcile.upsert(new ScheduleEventPayload(scheduleId, userId, "new", now.plusSeconds(3600),
                null, false, "manual", "confirmed", 3L, List.of(), now));
        assertThat(org.mockito.Mockito.mockingDetails(jdbc).getInvocations())
                .anyMatch(call -> call.getMethod().getName().equals("update")
                        && call.getArgument(0).toString().contains("last_revision = ?"));
    }

    @Test
    void readinessDoesNotCallAnUnstartedConsumerHealthy() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject("SELECT 1", Integer.class)).thenReturn(1);
        RedisConnectionFactory redis = mock(RedisConnectionFactory.class);
        RedisConnection connection = mock(RedisConnection.class);
        when(redis.getConnection()).thenReturn(connection);
        NatsConnectionHolder holder = mock(NatsConnectionHolder.class);
        NatsStreamBootstrap bootstrap = mock(NatsStreamBootstrap.class);
        when(holder.isConnected()).thenReturn(true);
        when(bootstrap.isStarted()).thenReturn(false);

        var response = new HealthController(jdbc, redis, holder, bootstrap).readyz();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("events", "unavailable");
        verify(connection).close();
    }

    @Test
    void readinessRejectsMissingBatchTable() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.queryForObject("SELECT 1", Integer.class)).thenReturn(1);
        doThrow(new IllegalStateException("table missing"))
                .when(jdbc).execute("SELECT 1 FROM reminder_dispatch LIMIT 0");
        RedisConnectionFactory redis = mock(RedisConnectionFactory.class);
        RedisConnection connection = mock(RedisConnection.class);
        when(redis.getConnection()).thenReturn(connection);
        NatsConnectionHolder holder = mock(NatsConnectionHolder.class);
        NatsStreamBootstrap bootstrap = mock(NatsStreamBootstrap.class);
        when(holder.isConnected()).thenReturn(true);
        when(bootstrap.isStarted()).thenReturn(true);

        var response = new HealthController(jdbc, redis, holder, bootstrap).readyz();

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(response.getBody()).containsEntry("database", "ready")
                .containsEntry("schema", "unavailable");
    }
}
