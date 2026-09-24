package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.LocalDate;
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
    void disabledDeliveryNeverCountsAsSent() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), org.mockito.ArgumentMatchers.<RowMapper<byte[]>>any(),
                        eq(ReminderScanJob.SCAN_LIMIT)))
                .thenReturn(List.of(new byte[] {1}));
        when(jdbc.queryForObject("SELECT UTC_DATE()", LocalDate.class)).thenReturn(LocalDate.of(2026, 9, 23));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            assertThat(new ReminderScanJob(jdbc, meters, mock(PlatformTransactionManager.class)).scanOnce()).isEqualTo(1);
            verify(jdbc).batchUpdate(eq("UPDATE reminder_dispatch SET status = 'skipped' WHERE id = ?"), anyList());
            assertThat(meters.find("reminders_skipped_total").counter()).isNull();
            assertThat(meters.find("reminders_sent_total").counter()).isNull();
        } finally {
            meters.close();
        }
    }

    @Test
    void failedCommitDoesNotCountSkippedReminder() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        when(jdbc.query(anyString(), org.mockito.ArgumentMatchers.<RowMapper<byte[]>>any(),
                        eq(ReminderScanJob.SCAN_LIMIT)))
                .thenReturn(List.of(new byte[] {1}));
        when(jdbc.queryForObject("SELECT UTC_DATE()", LocalDate.class)).thenReturn(LocalDate.of(2026, 9, 23));
        PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
        when(transactions.getTransaction(any(TransactionDefinition.class))).thenReturn(new SimpleTransactionStatus());
        doThrow(new IllegalStateException("commit failed")).when(transactions).commit(any(TransactionStatus.class));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            ReminderScanJob job = new ReminderScanJob(jdbc, meters, transactions);
            assertThatThrownBy(job::scan).isInstanceOf(IllegalStateException.class);
            assertThat(meters.find("reminders_skipped_total").counter()).isNull();
            assertThat(meters.counter("reminder_scan_runs_total", "result", "error").count()).isEqualTo(1);
        } finally {
            meters.close();
        }
    }
}
