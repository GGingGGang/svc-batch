package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;

class ReminderScanJobTest {
    @Test
    void dueReminderIsStagedForDurableDispatchAndNeverCountedSent() {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        byte[] id=UuidBytes.toBytes(Uuid7Generator.generate());
        byte[] schedule=UuidBytes.toBytes(Uuid7Generator.generate());
        byte[] user=UuidBytes.toBytes(Uuid7Generator.generate());
        Instant now=Instant.now();
        when(jdbc.query(anyString(),org.mockito.ArgumentMatchers.<RowMapper<byte[]>>any(),eq(ReminderScanJob.SCAN_LIMIT)))
                .thenReturn(List.of(id));
        when(jdbc.queryForObject(anyString(),any(RowMapper.class),eq(id))).thenReturn(
                new ReminderScanJob.DeliveryRow(id,schedule,user,"title",10,"push",now.plusSeconds(3600),
                        now.minusSeconds(300),"UTC",false,null,null,1L,0));
        SimpleMeterRegistry meters=new SimpleMeterRegistry();
        try {
            ReminderScanJob job=new ReminderScanJob(jdbc,meters,mock(PlatformTransactionManager.class));
            assertThat(job.scanOnce()).isEqualTo(1);
            verify(jdbc).update(org.mockito.ArgumentMatchers.contains("status='dispatching'"),eq(1),eq(id));
            verify(jdbc).update(org.mockito.ArgumentMatchers.contains("INSERT INTO reminder_attempt"),
                    any(byte[].class),eq(1),any(byte[].class),any(byte[].class),eq("push"),anyString());
            assertThat(meters.find("reminders_sent_total").counter()).isNull();
        } finally { meters.close(); }
    }

    @Test
    void failedCommitDoesNotPublishOrCount() {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        when(jdbc.query(anyString(),org.mockito.ArgumentMatchers.<RowMapper<byte[]>>any(),eq(ReminderScanJob.SCAN_LIMIT)))
                .thenReturn(List.of());
        PlatformTransactionManager manager=mock(PlatformTransactionManager.class);
        when(manager.getTransaction(any(TransactionDefinition.class))).thenReturn(new SimpleTransactionStatus());
        org.mockito.Mockito.doThrow(new IllegalStateException("commit failed"))
                .when(manager).commit(any(TransactionStatus.class));
        SimpleMeterRegistry meters=new SimpleMeterRegistry();
        try {
            ReminderScanJob job=new ReminderScanJob(jdbc,meters,manager);
            org.assertj.core.api.Assertions.assertThatThrownBy(job::scan).isInstanceOf(IllegalStateException.class);
            assertThat(meters.counter("reminder_scan_runs_total","result","error").count()).isEqualTo(1);
        } finally { meters.close(); }
    }
}
