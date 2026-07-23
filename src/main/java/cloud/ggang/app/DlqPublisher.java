package cloud.ggang.app;

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.time.Instant;
import org.springframework.stereotype.Component;

// 실패 메시지를 app.schedules.dlq.<event> 로 원문 그대로 전달(payload 재직렬화 없음).
// header: x-original-subject / x-failure-reason / x-failure-ts. 전체문서 PLAN.md §7.5, svc-batch/PLAN.md §4.3.
@Component
public class DlqPublisher {

    private static final int MAX_REASON_LENGTH = 500;

    private final JetStream jetStream;

    public DlqPublisher(JetStream jetStream) {
        this.jetStream = jetStream;
    }

    public void publish(Message original, String failureReason) {
        Headers headers =
                new Headers()
                        .add("x-original-subject", original.getSubject())
                        .add("x-failure-reason", truncate(failureReason))
                        .add("x-failure-ts", Instant.now().toString());
        Message dlqMessage =
                NatsMessage.builder()
                        .subject(NatsSubjects.dlqSubject(original.getSubject()))
                        .headers(headers)
                        .data(original.getData())
                        .build();
        jetStream.publishAsync(dlqMessage);
    }

    private String truncate(String reason) {
        if (reason == null) {
            return "unknown";
        }
        return reason.length() > MAX_REASON_LENGTH ? reason.substring(0, MAX_REASON_LENGTH) : reason;
    }
}
