package cloud.ggang.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// durable consumer batch-reminders (NatsStreamBootstrap), AckPolicy.Explicit —
// DB 트랜잭션 커밋 후에만 msg.ack() 호출.
// 역직렬화 실패는 재시도 없이 즉시 dlq, 그 외 처리 실패는 3회 재시도 후 dlq — 각 경우 발행 후 ack.
@Component
public class ScheduleEventConsumer implements MessageHandler {

    private static final int MAX_ATTEMPTS = 3;
    private static final Pattern TRACEPARENT = Pattern.compile("(?i)^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$");
    private static final Logger log = LoggerFactory.getLogger(ScheduleEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final ScheduleReconcileService reconcileService;
    private final DlqPublisher dlqPublisher;
    private final MeterRegistry meterRegistry;
    private final AtomicLong lastDlqFailureEpochSeconds = new AtomicLong();

    public ScheduleEventConsumer(
            ObjectMapper objectMapper,
            ScheduleReconcileService reconcileService,
            DlqPublisher dlqPublisher,
            MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.reconcileService = reconcileService;
        this.dlqPublisher = dlqPublisher;
        this.meterRegistry = meterRegistry;
        meterRegistry.gauge("schedule_events_dlq_last_failure_epoch_seconds", lastDlqFailureEpochSeconds);
    }

    @Override
    public void onMessage(Message msg) {
        String subject = msg.getSubject();
        meterRegistry.counter("schedule_events_consumed_total", "subject", subject).increment();
        if (NatsSubjects.SCHEDULE_CREATED.equals(subject) || NatsSubjects.SCHEDULE_UPDATED.equals(subject)) {
            process(msg, ScheduleEventPayload.class, reconcileService::upsert);
        } else if (NatsSubjects.SCHEDULE_DELETED.equals(subject)) {
            process(msg, ScheduleDeletedPayload.class, reconcileService::delete);
        } else {
            log.warn("unexpected subject={}", subject);
            msg.ack();
        }
    }

    private <T> void process(Message msg, Class<T> payloadType, Consumer<T> handler) {
        String errorId = errorId(msg);
        T payload;
        try {
            payload = objectMapper.readValue(msg.getData(), payloadType);
        } catch (Exception ex) {
            log.warn("deserialize failed error_id={} subject={} error={}", errorId, msg.getSubject(), ex.getClass().getSimpleName());
            sendToDlq(msg, "deserialize: " + ex.getClass().getSimpleName(), errorId);
            return;
        }

        Exception lastFailure = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                handler.accept(payload);
                msg.ack();
                return;
            } catch (Exception ex) {
                lastFailure = ex;
                log.warn("process failed error_id={} attempt={} subject={} error={}",
                        errorId, attempt, msg.getSubject(), ex.getClass().getSimpleName());
            }
        }
        sendToDlq(msg, "processing failed after " + MAX_ATTEMPTS
                + " attempts: " + lastFailure.getClass().getSimpleName(), errorId);
    }

    private void sendToDlq(Message msg, String reason, String errorId) {
        if (dlqPublisher.publish(msg, reason, errorId)) {
            meterRegistry.counter("schedule_events_dlq_total", "subject", msg.getSubject()).increment();
            msg.ack();
        } else {
            meterRegistry.counter("schedule_events_dlq_failures_total", "subject", msg.getSubject())
                    .increment();
            lastDlqFailureEpochSeconds.set(Instant.now().getEpochSecond());
            // Leave unacked: the durable consumer redelivers after its 30-second ack wait.
        }
    }

    private String errorId(Message msg) {
        if (msg.getHeaders() == null) {
            return UUID.randomUUID().toString();
        }
        String requestId = msg.getHeaders().getFirst("x-request-id");
        if (requestId != null) {
            try {
                UUID parsed = UUID.fromString(requestId);
                if (parsed.toString().equalsIgnoreCase(requestId)) {
                    return parsed.toString();
                }
            } catch (IllegalArgumentException ignored) {
                // Untrusted headers are never copied into logs or DLQ identifiers.
            }
        }
        String traceparent = msg.getHeaders().getFirst("traceparent");
        Matcher match = traceparent == null ? null : TRACEPARENT.matcher(traceparent);
        return match != null && match.matches() ? match.group(1).toLowerCase() : UUID.randomUUID().toString();
    }
}
