package cloud.ggang.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// durable consumer batch-reminders (NatsStreamBootstrap), AckPolicy.Explicit —
// DB 트랜잭션 커밋 후에만 msg.ack() 호출.
// 역직렬화 실패는 재시도 없이 즉시 dlq, 그 외 처리 실패는 3회 재시도 후 dlq — 각 경우 발행 후 ack.
@Component
public class ScheduleEventConsumer implements MessageHandler {

    private static final int MAX_ATTEMPTS = 3;
    private static final Logger log = LoggerFactory.getLogger(ScheduleEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final ScheduleReconcileService reconcileService;
    private final DlqPublisher dlqPublisher;
    private final MeterRegistry meterRegistry;

    public ScheduleEventConsumer(
            ObjectMapper objectMapper,
            ScheduleReconcileService reconcileService,
            DlqPublisher dlqPublisher,
            MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.reconcileService = reconcileService;
        this.dlqPublisher = dlqPublisher;
        this.meterRegistry = meterRegistry;
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
        T payload;
        try {
            payload = objectMapper.readValue(msg.getData(), payloadType);
        } catch (Exception ex) {
            log.warn("deserialize failed subject={}", msg.getSubject(), ex);
            dlqPublisher.publish(msg, "deserialize: " + ex.getMessage());
            meterRegistry.counter("schedule_events_dlq_total", "subject", msg.getSubject()).increment();
            msg.ack();
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
                log.warn("process failed attempt={} subject={}", attempt, msg.getSubject(), ex);
            }
        }
        dlqPublisher.publish(
                msg, "processing failed after " + MAX_ATTEMPTS + " attempts: " + lastFailure);
        meterRegistry.counter("schedule_events_dlq_total", "subject", msg.getSubject()).increment();
        msg.ack();
    }
}
