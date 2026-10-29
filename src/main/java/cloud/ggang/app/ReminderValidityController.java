package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.security.MessageDigest;
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
@RequestMapping("/internal/reminders")
public class ReminderValidityController {

    private final JdbcTemplate jdbc;
    @Value("${app.internal.token:}")
    private String internalToken;

    public ReminderValidityController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping("/{dispatchId}/validity")
    public Validity validity(@PathVariable String dispatchId, @RequestParam int attempt,
            @RequestHeader(value = "Authorization", required = false) String authorization) {
        requireToken(authorization);
        UUID id;
        try { id = UUID.fromString(dispatchId); } catch (IllegalArgumentException ex) {
            return Validity.invalid();
        }
        if (attempt < 1 || attempt > 3) return Validity.invalid();
        List<Validity> matches = jdbc.query("SELECT d.schedule_id,d.user_id,d.schedule_revision,d.start_at,d.remind_at,"
                        + "d.minutes_before,d.channel,d.timezone,d.all_day,d.start_date,d.end_date,d.title "
                        + "FROM reminder_dispatch d JOIN schedule_event_state s ON s.schedule_id=d.schedule_id "
                        + "JOIN account_lifecycle_guard a ON a.user_hash=UNHEX(SHA2(d.user_id,256)) "
                        + "WHERE d.id=? AND d.status='dispatching' AND d.attempt_count=? AND s.is_deleted=0 "
                        + "AND a.is_deleted=0 AND EXISTS(SELECT 1 FROM reminder_attempt x "
                        + "WHERE x.dispatch_id=d.id AND x.attempt=? AND x.status IN ('queued','published'))",
                (rs, row) -> new Validity(true, UuidBytes.fromBytes(rs.getBytes("schedule_id")).toString(),
                        UuidBytes.fromBytes(rs.getBytes("user_id")).toString(), rs.getObject("schedule_revision", Long.class),
                        rs.getTimestamp("start_at").toInstant(), rs.getTimestamp("remind_at").toInstant(),
                        rs.getInt("minutes_before"), rs.getString("channel"), rs.getString("timezone"),
                        rs.getBoolean("all_day"), rs.getDate("start_date") == null ? null : rs.getDate("start_date").toLocalDate(),
                        rs.getDate("end_date") == null ? null : rs.getDate("end_date").toLocalDate(), rs.getString("title")),
                UuidBytes.toBytes(id), attempt, attempt);
        return matches.isEmpty() ? Validity.invalid() : matches.get(0);
    }

    private void requireToken(String authorization) {
        if (internalToken == null || internalToken.isBlank())
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        byte[] expected = ("Bearer " + internalToken).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] supplied = authorization == null ? new byte[0]
                : authorization.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, supplied)) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
    }

    public record Validity(boolean valid, @JsonProperty("schedule_id") String scheduleId,
            @JsonProperty("user_id") String userId, Long revision,
            @JsonProperty("start_at") Instant startAt, @JsonProperty("remind_at") Instant remindAt,
            @JsonProperty("minutes_before") Integer minutesBefore, String channel, String timezone,
            @JsonProperty("all_day") Boolean allDay, @JsonProperty("start_date") java.time.LocalDate startDate,
            @JsonProperty("end_date") java.time.LocalDate endDate, String title) {
        static Validity invalid() { return new Validity(false, null, null, null, null, null, null, null,
                null, null, null, null, null); }
    }
}
