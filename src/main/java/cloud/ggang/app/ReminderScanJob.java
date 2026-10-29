package cloud.ggang.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.nats.client.JetStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class ReminderScanJob {

    static final int SCAN_LIMIT = 100;
    private static final Logger log = LoggerFactory.getLogger(ReminderScanJob.class);
    private final JdbcTemplate jdbc;
    private final MeterRegistry meters;
    private final TransactionTemplate transactions;
    private final NatsConnectionHolder nats;
    private final ObjectMapper mapper;

    @Autowired
    public ReminderScanJob(JdbcTemplate jdbc, MeterRegistry meters,
            PlatformTransactionManager manager, NatsConnectionHolder nats, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.meters = meters;
        this.transactions = new TransactionTemplate(manager);
        this.nats = nats;
        this.mapper = mapper;
    }

    ReminderScanJob(JdbcTemplate jdbc, MeterRegistry meters, PlatformTransactionManager manager) {
        this(jdbc, meters, manager, null, new ObjectMapper().findAndRegisterModules());
    }

    public int scanOnce() {
        return transactions.execute(status -> stageDue());
    }

    @Scheduled(initialDelayString = "${app.reminder-scan.initial-delay-ms:0}",
            fixedDelayString = "${app.reminder-scan.interval-ms:60000}")
    @SchedulerLock(name = "batch:lock:reminder-scan", lockAtMostFor = "PT2M", lockAtLeastFor = "PT5S")
    public void scan() {
        Timer.Sample sample = Timer.start(meters);
        try {
            int staged = scanOnce();
            int published = publishQueued();
            sample.stop(meters.timer("reminder_scan_duration_seconds"));
            meters.counter("reminder_scan_runs_total", "result", "success").increment();
            if (staged + published > 0) {
                meters.counter("reminders_scanned_total").increment(staged);
                log.info("reminder scan staged={} published={}", staged, published);
            }
        } catch (RuntimeException ex) {
            sample.stop(meters.timer("reminder_scan_duration_seconds"));
            meters.counter("reminder_scan_runs_total", "result", "error").increment();
            throw ex;
        }
    }

    int stageDue() {
        List<byte[]> ids = jdbc.query(
                "SELECT id FROM reminder_dispatch WHERE status = 'pending' "
                        + "AND COALESCE(next_attempt_at, remind_at) <= UTC_TIMESTAMP(3) "
                        + "ORDER BY COALESCE(next_attempt_at, remind_at) LIMIT ? FOR UPDATE SKIP LOCKED",
                (rs, row) -> rs.getBytes(1), SCAN_LIMIT);
        for (byte[] id : ids) {
            DeliveryRow row = jdbc.queryForObject(
                    "SELECT id,schedule_id,user_id,title,minutes_before,channel,start_at,remind_at,"
                            + "timezone,all_day,start_date,end_date,schedule_revision,attempt_count "
                            + "FROM reminder_dispatch WHERE id=?",
                    (rs, n) -> new DeliveryRow(rs.getBytes("id"), rs.getBytes("schedule_id"),
                            rs.getBytes("user_id"), rs.getString("title"), rs.getInt("minutes_before"),
                            rs.getString("channel"), rs.getTimestamp("start_at").toInstant(),
                            rs.getTimestamp("remind_at").toInstant(), rs.getString("timezone"),
                            rs.getBoolean("all_day"), rs.getDate("start_date") == null ? null
                                    : rs.getDate("start_date").toLocalDate(),
                            rs.getDate("end_date") == null ? null : rs.getDate("end_date").toLocalDate(),
                            rs.getObject("schedule_revision", Long.class), rs.getInt("attempt_count")), id);
            if (Instant.now().isAfter(row.remindAt().plusSeconds(30 * 60L))) {
                jdbc.update("UPDATE reminder_dispatch SET status='skipped',last_error='grace_expired' "
                        + "WHERE id=? AND status='pending'", id);
                incrementStat(LocalDate.now(ZoneOffset.UTC), "reminders_skipped", 1);
                continue;
            }
            int attempt = row.attemptCount() + 1;
            ReminderDeliveryPayload payload = new ReminderDeliveryPayload(
                    UuidBytes.fromBytes(row.id()).toString(), attempt,
                    UuidBytes.fromBytes(row.userId()).toString(), UuidBytes.fromBytes(row.scheduleId()).toString(),
                    row.title(), row.startAt(), row.remindAt(), row.minutesBefore(), row.channel(), row.timezone(),
                    row.allDay(), row.startDate(), row.endDate(), row.revision());
            try {
                jdbc.update("UPDATE reminder_dispatch SET status='dispatching', attempt_count=?, next_attempt_at=NULL "
                        + "WHERE id=? AND status='pending'", attempt, id);
                jdbc.update("INSERT INTO reminder_attempt(dispatch_id,attempt,user_id,schedule_id,channel,status,payload) "
                                + "VALUES (?,?,?,?,?,'queued',CAST(? AS JSON))",
                        row.id(), attempt, row.userId(), row.scheduleId(), row.channel(), mapper.writeValueAsString(payload));
            } catch (Exception ex) {
                throw new IllegalStateException("unable to stage reminder", ex);
            }
        }
        return ids.size();
    }

    private int publishQueued() {
        JetStream stream;
        if (nats == null) return 0;
        try {
            stream = nats.jetStreamOrNull();
        } catch (Exception ex) {
            return 0;
        }
        if (stream == null) return 0;
        List<QueuedAttempt> queued = jdbc.query(
                "SELECT dispatch_id,attempt,payload FROM reminder_attempt WHERE status='queued' "
                        + "ORDER BY created_at LIMIT ?", (rs, row) -> new QueuedAttempt(
                                rs.getBytes("dispatch_id"), rs.getInt("attempt"), rs.getString("payload")), SCAN_LIMIT);
        int count = 0;
        for (QueuedAttempt attempt : queued) {
            try {
                stream.publish(NatsSubjects.REMINDER_DUE, mapper.writeValueAsBytes(
                        mapper.readTree(attempt.payload())));
                jdbc.update("UPDATE reminder_attempt SET status='published' WHERE dispatch_id=? AND attempt=? "
                        + "AND status='queued'", attempt.dispatchId(), attempt.attempt());
                count++;
            } catch (Exception ex) {
                log.warn("reminder outbox publish failed dispatch_id={} attempt={} error={}",
                        UuidBytes.fromBytes(attempt.dispatchId()), attempt.attempt(), ex.getClass().getSimpleName());
            }
        }
        return count;
    }

    private void incrementStat(LocalDate date, String column, int amount) {
        jdbc.update("INSERT INTO daily_schedule_stats(stat_date," + column + ") VALUES(?,?) AS new "
                + "ON DUPLICATE KEY UPDATE " + column + "=daily_schedule_stats." + column + "+new." + column,
                date, amount);
    }

    record DeliveryRow(byte[] id, byte[] scheduleId, byte[] userId, String title, int minutesBefore,
            String channel, Instant startAt, Instant remindAt, String timezone, boolean allDay,
            LocalDate startDate, LocalDate endDate, Long revision, int attemptCount) {}

    private record QueuedAttempt(byte[] dispatchId, int attempt, String payload) {}
}
