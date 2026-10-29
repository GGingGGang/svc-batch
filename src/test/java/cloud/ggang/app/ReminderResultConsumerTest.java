package cloud.ggang.app;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

class ReminderResultConsumerTest {
    @Test void acceptedResultIsAppliedAndCountedExactlyOnce() throws Exception {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        PlatformTransactionManager manager=manager();
        ResultSet attempt=mock(ResultSet.class);
        when(attempt.getBoolean(1)).thenReturn(false);
        ResultSet current=mock(ResultSet.class);
        when(current.getString("status")).thenReturn("dispatching");
        when(current.getInt("attempt_count")).thenReturn(1);
        when(current.getTimestamp("remind_at")).thenReturn(Timestamp.from(Instant.now()));
        when(jdbc.query(anyString(),any(RowMapper.class),any(Object[].class))).thenAnswer(call -> {
            String sql=call.getArgument(0);
            RowMapper<?> mapper=call.getArgument(1);
            return sql.contains("reminder_attempt")?List.of(mapper.mapRow(attempt,0)):List.of(mapper.mapRow(current,0));
        });
        UUID id=UUID.randomUUID();
        MessageFactory.deliver(new ReminderResultConsumer(new ObjectMapper().findAndRegisterModules(),jdbc,manager),
                new ReminderDeliveryResult(id.toString(),1,"accepted",null,"receipt-1",Instant.now()));
        verify(jdbc).update(anyString(),org.mockito.ArgumentMatchers.eq("accepted"),any(),any(),any(byte[].class),org.mockito.ArgumentMatchers.eq(1));
        verify(jdbc).update(org.mockito.ArgumentMatchers.contains("reminders_sent"),
                org.mockito.ArgumentMatchers.any(java.time.LocalDate.class));
    }

    @Test void duplicateResultDoesNotIncrementAgain() throws Exception {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        PlatformTransactionManager manager=manager();
        ResultSet attempt=mock(ResultSet.class);
        when(attempt.getBoolean(1)).thenReturn(true);
        when(jdbc.query(anyString(),any(RowMapper.class),any(Object[].class))).thenAnswer(call -> {
            RowMapper<?> mapper=call.getArgument(1);
            return List.of(mapper.mapRow(attempt,0));
        });
        UUID id=UUID.randomUUID();
        MessageFactory.deliver(new ReminderResultConsumer(new ObjectMapper().findAndRegisterModules(),jdbc,manager),
                new ReminderDeliveryResult(id.toString(),1,"accepted",null,null,Instant.now()));
        verify(jdbc,never()).update(anyString(),any(Object[].class));
    }

    private PlatformTransactionManager manager() {
        PlatformTransactionManager manager=mock(PlatformTransactionManager.class);
        when(manager.getTransaction(any(TransactionDefinition.class))).thenReturn(new SimpleTransactionStatus());
        return manager;
    }

    private static final class MessageFactory {
        static void deliver(ReminderResultConsumer consumer,ReminderDeliveryResult result) throws Exception {
            io.nats.client.Message msg=mock(io.nats.client.Message.class);
            when(msg.getData()).thenReturn(new ObjectMapper().findAndRegisterModules().writeValueAsBytes(result));
            consumer.onMessage(msg);
            verify(msg).ack();
        }
    }
}
