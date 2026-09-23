package cloud.ggang.app;

import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
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

    // 서버의 발행 확인을 받은 경우에만 true. 실패하면 원본을 ack하지 않고 재전달받는다.
    public boolean publish(Message original, String failureReason) {
        try {
            doPublish(original, failureReason);
            return true;
        } catch (Exception ex) {
            log.error("dlq publish failed subject={} error={}",
                    original.getSubject(), ex.getClass().getSimpleName());
            return false;
        }
    }

    private void doPublish(Message original, String failureReason)
            throws IOException, JetStreamApiException {
        JetStream jetStream = connectionHolder.jetStreamOrNull();
        if (jetStream == null) {
            // consumer 자체가 연결 성립 후에만 메시지를 받으므로 실제로는 발생하지 않아야 하는 경로.
            throw new IOException("nats not connected");
        }
        Headers headers =
                new Headers()
                        .add("x-original-subject", original.getSubject())
                        .add("x-failure-reason", sanitize(failureReason))
                        .add("x-failure-ts", Instant.now().toString());
        Message dlqMessage =
                NatsMessage.builder()
                        .subject(NatsSubjects.dlqSubject(original.getSubject()))
                        .headers(headers)
                        .data(original.getData())
                        .build();
        jetStream.publish(dlqMessage);
    }

    // 헤더 값은 개행/제어문자를 허용하지 않는다 — Jackson 예외 메시지처럼 "at [Source: ...]" 를
    // 개행으로 덧붙이는 원인 문자열이 그대로 들어오면 nats.java 가 즉시 IllegalArgumentException 을 던진다.
    private String sanitize(String reason) {
        if (reason == null) {
            return "unknown";
        }
        String flattened = reason.replaceAll("[\\r\\n\\t]+", " ").replaceAll("\\p{Cntrl}", "");
        return flattened.length() > MAX_REASON_LENGTH ? flattened.substring(0, MAX_REASON_LENGTH) : flattened;
    }
}
