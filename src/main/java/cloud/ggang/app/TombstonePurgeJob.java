package cloud.ggang.app;

import io.micrometer.core.instrument.MeterRegistry;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

// 주 1회 오래된 tombstone(is_deleted=1) 행을 정리한다(PLAN.md §6) — NATS stream 의
// max_age(7d, 전체문서 §7.2)보다 길게(14d) 유지한 뒤 정리해, 그 사이 재전달된 오래된 이벤트가
// 여전히 §4.2 의 terminal 가드(is_deleted=1 행이 남아있으면 무시)를 받도록 보장한다.
// ReminderScanJob 과 동일하게 ShedLock 으로 replica 간 중복 실행을 막는다 — 삭제는 멱등이라
// FOR UPDATE SKIP LOCKED 같은 행 단위 경쟁 방어는 불필요(단일 DELETE 문).
@Component
public class TombstonePurgeJob {

    static final int RETENTION_DAYS = 14;

    private static final Logger log = LoggerFactory.getLogger(TombstonePurgeJob.class);

    // RETENTION_DAYS is baked in at compile time (matching the literal-INTERVAL style the rest of
    // this codebase uses, e.g. ReminderScanJob's "- INTERVAL 30 MINUTE") rather than bound as a
    // JDBC parameter — MySQL's INTERVAL grammar takes ? placeholders fine, but there's no existing
    // precedent for it here to follow.
    private static final String DELETE_SQL =
            "DELETE FROM schedule_event_state "
                    + "WHERE is_deleted = 1 AND updated_at < UTC_TIMESTAMP() - INTERVAL "
                    + RETENTION_DAYS
                    + " DAY";

    private final JdbcTemplate jdbcTemplate;
    private final MeterRegistry meterRegistry;

    public TombstonePurgeJob(JdbcTemplate jdbcTemplate, MeterRegistry meterRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(
            initialDelayString = "${app.tombstone-purge.initial-delay-ms:0}",
            fixedDelayString = "${app.tombstone-purge.interval-ms:604800000}")
    @SchedulerLock(name = "batch:lock:tombstone-purge", lockAtMostFor = "PT10M", lockAtLeastFor = "PT5S")
    public void purge() {
        int deleted = purgeOnce();
        meterRegistry.counter("tombstone_purge_runs_total").increment();
        meterRegistry.counter("tombstone_purged_total").increment(deleted);
        if (deleted > 0) {
            log.info("tombstone purge deleted rows={}", deleted);
        }
    }

    int purgeOnce() {
        return jdbcTemplate.update(DELETE_SQL);
    }
}
