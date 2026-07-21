package cloud.ggang.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

// group batch-reminders (application.yml spring.kafka.consumer.group-id), manual ack —
// DB 트랜잭션 커밋 후에만 offset 을 commit 한다 (ack-mode: manual_immediate).
// 역직렬화 실패는 재시도 없이 즉시 dlq, 그 외 처리 실패는 3회 재시도 후 dlq — 각 경우 발행 후 ack.
@Component
public class ScheduleEventConsumer {

    private static final int MAX_ATTEMPTS = 3;
    private static final Logger log = LoggerFactory.getLogger(ScheduleEventConsumer.class);

    private final ObjectMapper objectMapper;
    private final ScheduleReconcileService reconcileService;
    private final DlqPublisher dlqPublisher;

    public ScheduleEventConsumer(
            ObjectMapper objectMapper,
            ScheduleReconcileService reconcileService,
            DlqPublisher dlqPublisher) {
        this.objectMapper = objectMapper;
        this.reconcileService = reconcileService;
        this.dlqPublisher = dlqPublisher;
    }

    @KafkaListener(topics = {KafkaTopics.SCHEDULE_CREATED, KafkaTopics.SCHEDULE_UPDATED})
    public void onScheduleUpserted(ConsumerRecord<String, String> record, Acknowledgment ack) {
        process(record, ack, ScheduleEventPayload.class, reconcileService::upsert);
    }

    @KafkaListener(topics = KafkaTopics.SCHEDULE_DELETED)
    public void onScheduleDeleted(ConsumerRecord<String, String> record, Acknowledgment ack) {
        process(record, ack, ScheduleDeletedPayload.class, reconcileService::delete);
    }

    private <T> void process(
            ConsumerRecord<String, String> record,
            Acknowledgment ack,
            Class<T> payloadType,
            Consumer<T> handler) {
        T payload;
        try {
            payload = objectMapper.readValue(record.value(), payloadType);
        } catch (Exception ex) {
            log.warn(
                    "deserialize failed topic={} partition={} offset={}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    ex);
            dlqPublisher.publish(record, "deserialize: " + ex.getMessage());
            ack.acknowledge();
            return;
        }

        Exception lastFailure = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                handler.accept(payload);
                ack.acknowledge();
                return;
            } catch (Exception ex) {
                lastFailure = ex;
                log.warn(
                        "process failed attempt={} topic={} partition={} offset={}",
                        attempt,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        ex);
            }
        }
        dlqPublisher.publish(
                record, "processing failed after " + MAX_ATTEMPTS + " attempts: " + lastFailure);
        ack.acknowledge();
    }
}
