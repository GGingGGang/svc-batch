package cloud.ggang.app;

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.io.IOException;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// 실패 메시지를 app.schedules.dlq.<event> 로 원문 그대로 전달(payload 재직렬화 없음).
// header: x-original-subject / x-failure-reason / x-failure-ts. 전체문서 PLAN.md §7.5, svc-batch/PLAN.md §4.3.
@Component
public class DlqPublisher {

    private static final int MAX_REASON_LENGTH = 500;
    private static final Logger log = LoggerFactory.getLogger(DlqPublisher.class);

    private final NatsConnectionHolder connectionHolder;

    public DlqPublisher(NatsConnectionHolder connectionHolder) {
        this.connectionHolder = connectionHolder;
    }

    public void publish(Message original, String failureReason) {
        JetStream jetStream;
        try {
            jetStream = connectionHolder.jetStreamOrNull();
        } catch (IOException ex) {
            log.error("dlq publish failed to obtain jetstream subject={}", original.getSubject(), ex);
            return;
        }
        if (jetStream == null) {
            // consumer 자체가 연결 성립 후에만 메시지를 받으므로 실제로는 발생하지 않아야 하는 경로.
            log.error("nats not connected, dropping dlq message subject={}", original.getSubject());
            return;
        }
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
