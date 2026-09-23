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
// 실제 알림 발송이 비활성인 데모에서는 due 작업을 skipped 로 마감하고 성공으로 집계하지 않는다.
@Component
public class ReminderScanJob {

    static final int SCAN_LIMIT = 100;

    private static final Logger log = LoggerFactory.getLogger(ReminderScanJob.class);

    private static final String SELECT_DUE_SQL =
            "SELECT id "
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
        List<byte[]> due =
                jdbcTemplate.query(
                        SELECT_DUE_SQL,
                        (rs, rowNum) -> rs.getBytes("id"),
                        SCAN_LIMIT);
        if (due.isEmpty()) {
            return 0;
        }
        meterRegistry.counter("reminders_scanned_total").increment(due.size());

        LocalDate statDate = jdbcTemplate.queryForObject("SELECT UTC_DATE()", LocalDate.class);
        jdbcTemplate.batchUpdate(
                "UPDATE reminder_dispatch SET status = 'skipped' WHERE id = ?", idBatchArgs(due));
        incrementStat(statDate, "reminders_skipped", due.size());
        meterRegistry.counter("reminders_skipped_total").increment(due.size());

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

}
