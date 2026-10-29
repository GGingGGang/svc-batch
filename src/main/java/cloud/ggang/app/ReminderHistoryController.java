package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/internal/users")
public class ReminderHistoryController {

    private final JdbcTemplate jdbc;

    @Value("${app.internal.token:}")
    private String internalToken;

    public ReminderHistoryController(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @GetMapping("/{userId}/reminders")
    public ReminderHistoryResponse history(@PathVariable String userId,
            @RequestParam Instant from, @RequestParam Instant to,
            @RequestParam(defaultValue = "100") int limit,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        requireToken(authorization);
        UUID user;
        try {
            user = UUID.fromString(userId);
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        if (from == null || to == null || from.isAfter(to) || Duration.between(from, to).compareTo(Duration.ofDays(30)) > 0
                || limit < 1 || limit > 100) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        List<ReminderHistory> reminders = jdbc.query("SELECT d.id,d.schedule_id,d.channel,d.status,d.last_error,d.attempt_count,d.remind_at,d.created_at,"
                        + "(SELECT a.reason_code FROM reminder_attempt a WHERE a.dispatch_id=d.id "
                        + "ORDER BY a.attempt DESC LIMIT 1) AS attempt_reason "
                        + "FROM reminder_dispatch d WHERE d.user_id=? AND d.remind_at>=? AND d.remind_at<=? "
                        + "ORDER BY d.remind_at DESC,d.id DESC LIMIT ?",
                (rs, row) -> new ReminderHistory(
                        UuidBytes.fromBytes(rs.getBytes("id")).toString(),
                        UuidBytes.fromBytes(rs.getBytes("schedule_id")).toString(), rs.getString("channel"),
                        rs.getString("status"), safeReason(rs.getString("last_error"), rs.getString("attempt_reason"),
                                rs.getString("status")), rs.getInt("attempt_count"),
                        rs.getTimestamp("remind_at").toInstant(), rs.getTimestamp("created_at").toInstant()),
                UuidBytes.toBytes(user), Timestamp.from(from), Timestamp.from(to), limit);
        return new ReminderHistoryResponse(reminders);
    }

    private void requireToken(String authorization) {
        if (internalToken == null || internalToken.isBlank())
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        byte[] expected = ("Bearer " + internalToken).getBytes(StandardCharsets.UTF_8);
        byte[] supplied = authorization == null ? new byte[0] : authorization.getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, supplied)) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
    }

    private String safeReason(String dispatchReason, String attemptReason, String status) {
        String value = dispatchReason != null ? dispatchReason : attemptReason;
        if (value != null && value.matches("[A-Za-z0-9_.-]{1,64}")) return value;
        return switch (status) {
            case "pending", "dispatching" -> "pending";
            case "sent" -> "accepted";
            case "failed" -> "permanent_failure";
            case "unknown" -> "unknown";
            case "skipped" -> "skipped";
            default -> "unspecified";
        };
    }

    public record ReminderHistory(@JsonProperty("dispatch_id") String dispatchId,
            @JsonProperty("schedule_id") String scheduleId, String channel, String status,
            @JsonProperty("reason_code") String reasonCode,
            @JsonProperty("attempt_count") int attemptCount,
            @JsonProperty("scheduled_at") Instant scheduledAt,
            @JsonProperty("created_at") Instant createdAt) {}

    public record ReminderHistoryResponse(List<ReminderHistory> reminders) {}
}
