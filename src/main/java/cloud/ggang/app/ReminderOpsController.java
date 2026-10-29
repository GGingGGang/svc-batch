package cloud.ggang.app;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.web.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/internal/ops/reminders")
public class ReminderOpsController {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    private final TransactionTemplate tx;
    @Value("${app.ops.operator-tokens-json:}")
    private String tokenMapJson;
    @Value("${app.internal.token:}")
    private String internalToken;
    @Value("${app.core.internal-url:}")
    private String coreUrl;
    @Value("${app.auth.account-status-url:}")
    private String accountStatusUrl;

    public ReminderOpsController(JdbcTemplate jdbc, ObjectMapper mapper, PlatformTransactionManager manager) {
        this.jdbc = jdbc;
        this.mapper = mapper;
        this.tx = new TransactionTemplate(manager);
    }

    @GetMapping
    public List<Map<String, Object>> list(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @RequestParam(defaultValue = "failed") String status,
            @RequestParam(defaultValue = "100") int limit) {
        actor(authorization);
        if (!List.of("failed", "unknown").contains(status) || limit < 1 || limit > 500) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        return jdbc.query("SELECT LOWER(CONCAT(SUBSTR(HEX(id),1,8),'-',SUBSTR(HEX(id),9,4),'-',"
                        + "SUBSTR(HEX(id),13,4),'-',SUBSTR(HEX(id),17,4),'-',SUBSTR(HEX(id),21))) dispatch_id,status,attempt_count,minutes_before,channel,"
                        + "remind_at,created_at,last_error FROM reminder_dispatch WHERE status=? "
                        + "ORDER BY updated_at DESC LIMIT ?",
                (rs, row) -> Map.of("dispatch_id", rs.getString("dispatch_id"), "status", rs.getString("status"),
                        "attempt_count", rs.getInt("attempt_count"), "minutes_before", rs.getInt("minutes_before"),
                        "channel", rs.getString("channel"), "remind_at", rs.getTimestamp("remind_at").toInstant(),
                        "created_at", rs.getTimestamp("created_at").toInstant(), "reason_code",
                        safeReason(rs.getString("last_error"))), status, limit);
    }

    @PostMapping("/{dispatchId}/reprocess")
    public Map<String, String> reprocess(
            @RequestHeader(value = "Authorization", required = false) String authorization,
            @PathVariable String dispatchId, @RequestBody ReprocessRequest request) {
        String operator = actor(authorization);
        UUID oldId;
        try { oldId = UUID.fromString(dispatchId); } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
        if (request.reason() == null || request.reason().isBlank() || request.reason().length() > 500) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "reason is required");
        }
        List<OpsRow> found = jdbc.query("SELECT user_id,schedule_id,status,remind_at,minutes_before,channel,schedule_revision "
                        + "FROM reminder_dispatch WHERE id=?", (rs, row) -> new OpsRow(rs.getBytes("user_id"),
                            rs.getBytes("schedule_id"), rs.getString("status"), rs.getTimestamp("remind_at").toInstant(),
                            rs.getInt("minutes_before"), rs.getString("channel"), rs.getObject("schedule_revision", Long.class)),
                UuidBytes.toBytes(oldId));
        if (found.isEmpty() || !"failed".equals(found.get(0).status())) {
            audit(UuidBytes.toBytes(oldId), operator, request.reason(), "not_reprocessable");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "not_reprocessable");
        }
        OpsRow row = found.get(0);
        if (row.remindAt().isBefore(Instant.now().minusSeconds(1800))) {
            audit(UuidBytes.toBytes(oldId), operator, request.reason(), "outside_grace");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "outside_grace");
        }
        boolean current;
        try {
            current = isCurrentlyValid(row);
        } catch (ResponseStatusException ex) {
            audit(UuidBytes.toBytes(oldId), operator, request.reason(), "validation_unavailable");
            throw ex;
        }
        if (!current) {
            audit(UuidBytes.toBytes(oldId), operator, request.reason(), "stale_or_inactive");
            throw new ResponseStatusException(HttpStatus.CONFLICT, "stale_or_inactive");
        }
        String status = tx.execute(statusTx -> {
            List<OpsRow> locked = jdbc.query("SELECT user_id,schedule_id,status,remind_at,minutes_before,channel,schedule_revision "
                            + "FROM reminder_dispatch WHERE id=? FOR UPDATE", (rs, n) -> new OpsRow(rs.getBytes("user_id"),
                            rs.getBytes("schedule_id"), rs.getString("status"), rs.getTimestamp("remind_at").toInstant(),
                            rs.getInt("minutes_before"), rs.getString("channel"), rs.getObject("schedule_revision", Long.class)),
                    UuidBytes.toBytes(oldId));
            if (locked.isEmpty() || !"failed".equals(locked.get(0).status()) || !sameSnapshot(row, locked.get(0)))
                return "changed_during_validation";
            byte[] newId = UuidBytes.toBytes(Uuid7Generator.generate());
            jdbc.update("UPDATE reminder_dispatch SET id=?,status='pending',attempt_count=0,last_error=NULL,sent_at=NULL,"
                            + "next_attempt_at=NULL WHERE id=?", newId, UuidBytes.toBytes(oldId));
            jdbc.update("INSERT INTO reminder_ops_audit(id,dispatch_id,actor,action,reason,result) VALUES(?,?,?,?,?,?)",
                    UuidBytes.toBytes(Uuid7Generator.generate()), newId, operator, "reprocess",
                    request.reason().trim(), "queued");
            return "queued";
        });
        if (!"queued".equals(status)) {
            audit(UuidBytes.toBytes(oldId), operator, request.reason(), status);
            throw new ResponseStatusException(HttpStatus.CONFLICT, status);
        }
        return Map.of("status", "queued");
    }

    private boolean isCurrentlyValid(OpsRow row) {
        if (internalToken == null || internalToken.isBlank() || coreUrl == null || coreUrl.isBlank()
                || accountStatusUrl == null || accountStatusUrl.isBlank()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
        try {
            String user = UuidBytes.fromBytes(row.userId()).toString();
            String schedule = UuidBytes.fromBytes(row.scheduleId()).toString();
            CoreValidity validity = RestClient.create(coreUrl).get()
                    .uri(uri -> uri.path("/internal/reminders/{scheduleId}/validity")
                            .queryParam("user_id", user).queryParam("revision", row.revision())
                            .queryParam("minutes_before", row.minutesBefore()).queryParam("channel", row.channel())
                            .queryParam("remind_at", row.remindAt())
                            .build(schedule))
                    .header("Authorization", "Bearer " + internalToken).retrieve().body(CoreValidity.class);
            if (validity == null || !validity.valid()) return false;
            String accountUrl = accountStatusUrl.contains("{userId}")
                    ? accountStatusUrl.replace("{userId}", user)
                    : accountStatusUrl.replaceAll("/$", "") + "/internal/accounts/" + user + "/status";
            AccountStatus account = RestClient.create().get().uri(accountUrl)
                    .header("Authorization", "Bearer " + internalToken).retrieve().body(AccountStatus.class);
            return account != null && account.active();
        } catch (ResponseStatusException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private boolean sameSnapshot(OpsRow a, OpsRow b) {
        return java.util.Arrays.equals(a.userId(), b.userId())
                && java.util.Arrays.equals(a.scheduleId(), b.scheduleId())
                && a.remindAt().equals(b.remindAt()) && a.minutesBefore() == b.minutesBefore()
                && a.channel().equals(b.channel()) && java.util.Objects.equals(a.revision(), b.revision());
    }

    private String actor(String authorization) {
        if (tokenMapJson == null || tokenMapJson.isBlank()) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
        }
        try {
            Map<String, String> actors = mapper.readValue(tokenMapJson, new TypeReference<>() {});
            byte[] supplied = authorization.substring(7).getBytes(StandardCharsets.UTF_8);
            String match = null;
            for (var entry : actors.entrySet()) {
                byte[] expected = entry.getValue().getBytes(StandardCharsets.UTF_8);
                if (expected.length < 32 || entry.getKey().isBlank())
                    throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
                if (MessageDigest.isEqual(supplied, expected)) match = entry.getKey();
            }
            if (match == null || match.isBlank()) throw new ResponseStatusException(HttpStatus.UNAUTHORIZED);
            return match;
        } catch (ResponseStatusException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    private String safeReason(String value) {
        return value != null && value.matches("[A-Za-z0-9_.-]{1,64}") ? value : "unspecified";
    }

    private void audit(byte[] dispatchId, String actor, String reason, String result) {
        tx.executeWithoutResult(status -> jdbc.update(
                "INSERT INTO reminder_ops_audit(id,dispatch_id,actor,action,reason,result) VALUES(?,?,?,?,?,?)",
                UuidBytes.toBytes(Uuid7Generator.generate()), dispatchId, actor, "reprocess", reason.trim(), result));
    }

    public record ReprocessRequest(String reason) {}
    private record CoreValidity(boolean valid) {}
    private record AccountStatus(boolean active) {}
    private record OpsRow(byte[] userId, byte[] scheduleId, String status, Instant remindAt,
            int minutesBefore, String channel, Long revision) {}
}
