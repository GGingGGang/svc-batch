package cloud.ggang.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class ReminderResultConsumer implements MessageHandler {

    private static final Set<String> STATUSES = Set.of("accepted", "retryable_failure", "permanent_failure",
            "unknown", "cancelled", "skipped");
    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public ReminderResultConsumer(ObjectMapper mapper, JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.mapper = mapper;
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(manager);
    }

    @Override
    public void onMessage(Message msg) {
        try {
            ReminderDeliveryResult result = mapper.readValue(msg.getData(), ReminderDeliveryResult.class);
            validate(result);
            tx.executeWithoutResult(status -> apply(result));
            msg.ack();
        } catch (RuntimeException ex) {
            msg.nakWithDelay(java.time.Duration.ofSeconds(30));
        } catch (Exception ex) {
            msg.nakWithDelay(java.time.Duration.ofSeconds(30));
        }
    }

    private void apply(ReminderDeliveryResult result) {
        byte[] dispatchId = UuidBytes.toBytes(UUID.fromString(result.dispatchId()));
        List<Boolean> done = jdbc.query("SELECT result_at IS NOT NULL FROM reminder_attempt "
                        + "WHERE dispatch_id=? AND attempt=? FOR UPDATE",
                (rs, row) -> rs.getBoolean(1), dispatchId, result.attempt());
        if (done.isEmpty() || done.get(0)) return;
        jdbc.update("UPDATE reminder_attempt SET status=?,reason_code=?,provider_receipt_id=?,result_at=UTC_TIMESTAMP(3) "
                        + "WHERE dispatch_id=? AND attempt=? AND result_at IS NULL",
                result.status(), safeReason(result.reasonCode()), safeReceipt(result.providerReceiptId()),
                dispatchId, result.attempt());
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        if ("accepted".equals(result.status())) increment(today, "reminders_sent");
        if ("permanent_failure".equals(result.status())
                || ("retryable_failure".equals(result.status()) && result.attempt() == 3))
            increment(today, "reminders_failed");
        if ("unknown".equals(result.status())) increment(today, "reminders_unknown");
        if ("cancelled".equals(result.status()) || "skipped".equals(result.status()))
            increment(today, "reminders_skipped");
        List<CurrentRow> rows = jdbc.query("SELECT status,attempt_count,remind_at FROM reminder_dispatch "
                        + "WHERE id=? FOR UPDATE", (rs, row) -> new CurrentRow(rs.getString("status"),
                                rs.getInt("attempt_count"), rs.getTimestamp("remind_at").toInstant()), dispatchId);
        if (rows.isEmpty() || !"dispatching".equals(rows.get(0).status())
                || rows.get(0).attemptCount() != result.attempt()) return;
        CurrentRow row = rows.get(0);
        switch (result.status()) {
            case "accepted" -> {
                jdbc.update("UPDATE reminder_dispatch SET status='sent',sent_at=UTC_TIMESTAMP(3) WHERE id=?", dispatchId);
            }
            case "retryable_failure" -> {
                if (result.attempt() < 3) {
                    int delayMinutes = result.attempt() == 1 ? 1 : 5;
                    jdbc.update("UPDATE reminder_dispatch SET status='pending',next_attempt_at=DATE_ADD(UTC_TIMESTAMP(3), INTERVAL ? MINUTE) "
                            + "WHERE id=?", delayMinutes, dispatchId);
                } else {
                    jdbc.update("UPDATE reminder_dispatch SET status='failed',last_error=? WHERE id=?",
                            safeReason(result.reasonCode()), dispatchId);
                }
            }
            case "permanent_failure" -> {
                jdbc.update("UPDATE reminder_dispatch SET status='failed',last_error=? WHERE id=?",
                        safeReason(result.reasonCode()), dispatchId);
            }
            case "unknown" -> {
                jdbc.update("UPDATE reminder_dispatch SET status='unknown' WHERE id=?", dispatchId);
            }
            case "cancelled", "skipped" -> {
                jdbc.update("UPDATE reminder_dispatch SET status='skipped' WHERE id=?", dispatchId);
            }
            default -> throw new IllegalStateException("unsupported result");
        }
    }

    private void validate(ReminderDeliveryResult result) {
        UUID.fromString(result.dispatchId());
        if (result.attempt() < 1 || result.attempt() > 3 || !STATUSES.contains(result.status())
                || result.occurredAt() == null) throw new IllegalArgumentException("invalid reminder result");
    }

    private String safeReason(String value) {
        if (value == null) return null;
        return value.matches("[A-Za-z0-9_.-]{1,64}") ? value : "provider_error";
    }

    private String safeReceipt(String value) {
        if (value == null) return null;
        return value.matches("[A-Za-z0-9_.:-]{1,255}") ? value : null;
    }

    private void increment(LocalDate date, String column) {
        jdbc.update("INSERT INTO daily_schedule_stats(stat_date," + column + ") VALUES(?,1) AS new "
                + "ON DUPLICATE KEY UPDATE " + column + "=daily_schedule_stats." + column + "+1", date);
    }

    private record CurrentRow(String status, int attemptCount, Instant remindAt) {}
}
