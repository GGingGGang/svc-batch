package cloud.ggang.app;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

// 실패 레코드를 <topic>.dlq 로 원문 그대로 전달 (key/value 재직렬화 없음).
// header: x-original-topic / x-failure-reason / x-failure-ts. 전체문서 PLAN.md §7.5, svc-batch/PLAN.md §4.3.
@Component
public class DlqPublisher {

    private static final int MAX_REASON_LENGTH = 500;

    private final KafkaTemplate<Object, Object> kafkaTemplate;

    public DlqPublisher(KafkaTemplate<Object, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(ConsumerRecord<String, String> original, String failureReason) {
        ProducerRecord<Object, Object> dlqRecord =
                new ProducerRecord<>(
                        KafkaTopics.dlqTopic(original.topic()), original.key(), original.value());
        dlqRecord
                .headers()
                .add("x-original-topic", original.topic().getBytes(StandardCharsets.UTF_8))
                .add("x-failure-reason", truncate(failureReason).getBytes(StandardCharsets.UTF_8))
                .add("x-failure-ts", Instant.now().toString().getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.send(dlqRecord);
    }

    private String truncate(String reason) {
        if (reason == null) {
            return "unknown";
        }
        return reason.length() > MAX_REASON_LENGTH ? reason.substring(0, MAX_REASON_LENGTH) : reason;
    }
}
