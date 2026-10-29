package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class AccountDeletionConsumer implements MessageHandler {

    private final ObjectMapper mapper;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final NatsConnectionHolder nats;

    public AccountDeletionConsumer(ObjectMapper mapper, JdbcTemplate jdbc,
            PlatformTransactionManager manager, NatsConnectionHolder nats) {
        this.mapper = mapper;
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(manager);
        this.nats = nats;
    }

    @Override
    public void onMessage(Message msg) {
        try {
            Request request = mapper.readValue(msg.getData(), Request.class);
            if (request.occurredAt() == null || request.credentialVersion() == null)
                throw new IllegalArgumentException("invalid account deletion event");
            UUID eventId = UUID.fromString(request.eventId());
            UUID requestId = UUID.fromString(request.requestId());
            UUID userId = UUID.fromString(request.userId());
            byte[] event = UuidBytes.toBytes(eventId);
            byte[] user = UuidBytes.toBytes(userId);
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(user);
            tx.executeWithoutResult(s -> delete(event, requestId, user, hash));
            JetStream js = nats.jetStreamOrNull();
            if (js == null) throw new IllegalStateException("NATS unavailable");
            var result = new Result(eventId.toString(), requestId.toString(), "batch", "completed", Instant.now());
            js.publish(NatsSubjects.ACCOUNT_DELETION_RESULT, mapper.writeValueAsBytes(result));
            msg.ack();
        } catch (Exception ex) {
            msg.nakWithDelay(java.time.Duration.ofSeconds(30));
        }
    }

    private void delete(byte[] event, UUID requestId, byte[] user, byte[] hash) {
        Integer done = jdbc.queryForObject("SELECT COUNT(*) FROM account_deletion_processed WHERE event_id=?",
                Integer.class, event);
        if (done != null && done > 0) return;
        jdbc.update("INSERT IGNORE INTO account_lifecycle_guard(user_hash,is_deleted) VALUES(?,0)",hash);
        jdbc.queryForObject("SELECT is_deleted FROM account_lifecycle_guard WHERE user_hash=? FOR UPDATE",
                Boolean.class,hash);
        jdbc.update("UPDATE account_lifecycle_guard SET is_deleted=1 WHERE user_hash=?",hash);
        jdbc.update("INSERT INTO deleted_account_tombstone(user_hash,event_id,deleted_at) VALUES(?,?,UTC_TIMESTAMP(3)) "
                + "ON DUPLICATE KEY UPDATE event_id=VALUES(event_id),deleted_at=VALUES(deleted_at)", hash, event);
        jdbc.update("DELETE FROM reminder_attempt WHERE user_id=?", user);
        jdbc.update("DELETE FROM reminder_dispatch WHERE user_id=?", user);
        jdbc.update("DELETE FROM schedule_event_state WHERE user_id=?", user);
        jdbc.update("INSERT INTO account_deletion_processed(event_id,request_id,user_hash) VALUES(?,?,?)",
                event, UuidBytes.toBytes(requestId), hash);
    }

    public record Request(@JsonProperty("event_id") String eventId,
            @JsonProperty("request_id") String requestId, @JsonProperty("user_id") String userId,
            @JsonProperty("credential_version") Long credentialVersion,
            @JsonProperty("occurred_at") Instant occurredAt) {}

    public record Result(@JsonProperty("event_id") String eventId,
            @JsonProperty("request_id") String requestId, @JsonProperty("service") String service,
            @JsonProperty("status") String status, @JsonProperty("occurred_at") Instant occurredAt) {}
}
