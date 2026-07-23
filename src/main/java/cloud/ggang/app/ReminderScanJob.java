package cloud.ggang.app;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

// FOR UPDATE SKIP LOCKED 로 같은 배치 사이클 안에서 행 단위 경쟁을 피하고, ShedLock(SchedulingConfig)
// 으로 replica 간 잡 자체의 중복 실행을 막는다(이중 방어).
// 실제 알림 발송은 별도 서비스(svc-notify) 몫이라 여기서는 채널 dispatch 를 호출하지 않는다
// ("발송" = pending → sent 상태 전이가 전부).
@Component
public class ReminderScanJob {

    static final int SCAN_LIMIT = 100;

    private static final Logger log = LoggerFactory.getLogger(ReminderScanJob.class);

    private static final String SELECT_DUE_SQL =
            "SELECT id, remind_at < UTC_TIMESTAMP(3) - INTERVAL 30 MINUTE AS grace_expired "
                    + "FROM reminder_dispatch "
                    + "WHERE status = 'pending' AND remind_at <= UTC_TIMESTAMP(3) "
                    + "ORDER BY remind_at LIMIT ? "
                    + "FOR UPDATE SKIP LOCKED";

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    public ReminderScanJob(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(
            initialDelayString = "${app.reminder-scan.initial-delay-ms:0}",
            fixedDelayString = "${app.reminder-scan.interval-ms:60000}")
    @SchedulerLock(name = "batch:lock:reminder-scan", lockAtMostFor = "PT2M", lockAtLeastFor = "PT5S")
    public void scan() {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            int scanned = scanOnce();
            sample.stop(meterRegistry.timer("reminder_scan_duration_seconds"));
            meterRegistry.counter("reminder_scan_runs_total", "result", "success").increment();
            if (scanned > 0) {
                log.info("reminder scan processed rows={}", scanned);
            }
        } catch (RuntimeException ex) {
            sample.stop(meterRegistry.timer("reminder_scan_duration_seconds"));
            meterRegistry.counter("reminder_scan_runs_total", "result", "error").increment();
            throw ex;
        }
    }

    @Transactional
    public int scanOnce() {
        List<DueReminder> due =
                jdbcTemplate.query(
                        SELECT_DUE_SQL,
                        (rs, rowNum) -> new DueReminder(rs.getBytes("id"), rs.getBoolean("grace_expired")),
                        SCAN_LIMIT);
        if (due.isEmpty()) {
            return 0;
        }
        meterRegistry.counter("reminders_scanned_total").increment(due.size());

        List<byte[]> toSend = new ArrayList<>();
        List<byte[]> toSkip = new ArrayList<>();
        for (DueReminder reminder : due) {
            if (reminder.graceExpired()) {
                toSkip.add(reminder.id());
            } else {
                toSend.add(reminder.id());
            }
        }

        LocalDate statDate = jdbcTemplate.queryForObject("SELECT UTC_DATE()", LocalDate.class);

        if (!toSend.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "UPDATE reminder_dispatch SET status = 'sent', sent_at = UTC_TIMESTAMP(3) WHERE id = ?",
                    idBatchArgs(toSend));
            incrementStat(statDate, "reminders_sent", toSend.size());
            meterRegistry.counter("reminders_sent_total").increment(toSend.size());
        }
        if (!toSkip.isEmpty()) {
            jdbcTemplate.batchUpdate(
                    "UPDATE reminder_dispatch SET status = 'skipped' WHERE id = ?", idBatchArgs(toSkip));
            incrementStat(statDate, "reminders_skipped", toSkip.size());
            meterRegistry.counter("reminders_skipped_total").increment(toSkip.size());
        }

        return due.size();
    }

    private List<Object[]> idBatchArgs(List<byte[]> ids) {
        List<Object[]> args = new ArrayList<>(ids.size());
        for (byte[] id : ids) {
            args.add(new Object[] {id});
        }
        return args;
    }

    private void incrementStat(LocalDate statDate, String column, int amount) {
        String sql =
                "INSERT INTO daily_schedule_stats (stat_date, "
                        + column
                        + ") VALUES (?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + column
                        + " = daily_schedule_stats."
                        + column
                        + " + new."
                        + column;
        jdbcTemplate.update(sql, statDate, amount);
    }

    private record DueReminder(byte[] id, boolean graceExpired) {}
}
