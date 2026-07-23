package cloud.ggang.app;

import io.nats.client.Connection;
import io.nats.client.ConsumerContext;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.MessageConsumer;
import io.nats.client.StreamContext;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

// NATS 연결(CONNECTED/RECONNECTED) 시점에 NatsConfig 가 호출 — APP_SCHEDULES_DLQ stream(batch 소유,
// 전체문서 PLAN.md §7.2)을 idempotent create-or-update 하고 APP_SCHEDULES 위에 durable pull consumer
// batch-reminders 를 붙여 소비를 시작한다. 실패 시(예: core 가 아직 APP_SCHEDULES 를 안 만듦) 다음
// 연결 이벤트에서 재시도하도록 시작 플래그를 되돌린다.
@Component
public class NatsStreamBootstrap {

    private static final int ERR_STREAM_NAME_IN_USE = 10058;
    private static final Logger log = LoggerFactory.getLogger(NatsStreamBootstrap.class);

    private final ScheduleEventConsumer handler;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile MessageConsumer messageConsumer;

    public NatsStreamBootstrap(ScheduleEventConsumer handler) {
        this.handler = handler;
    }

    public void bootstrap(Connection connection) {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        try {
            JetStreamManagement jsm = connection.jetStreamManagement();
            createOrUpdateStream(jsm, dlqStreamConfig());

            StreamContext scheduleStream = connection.getStreamContext(NatsSubjects.STREAM_SCHEDULES);
            ConsumerConfiguration consumerConfig =
                    ConsumerConfiguration.builder()
                            .durable(NatsSubjects.CONSUMER_NAME)
                            .ackPolicy(AckPolicy.Explicit)
                            .ackWait(Duration.ofSeconds(30))
                            .build();
            ConsumerContext consumerContext = scheduleStream.createOrUpdateConsumer(consumerConfig);

            messageConsumer = consumerContext.consume(handler);
            log.info("nats consumer started durable={}", NatsSubjects.CONSUMER_NAME);
        } catch (Exception ex) {
            started.set(false);
            log.error("nats stream/consumer bootstrap failed, will retry on next reconnect", ex);
        }
    }

    @PreDestroy
    public void shutdown() throws Exception {
        if (messageConsumer != null) {
            messageConsumer.close();
        }
    }

    private void createOrUpdateStream(JetStreamManagement jsm, StreamConfiguration config)
            throws Exception {
        try {
            jsm.addStream(config);
        } catch (JetStreamApiException ex) {
            if (ex.getApiErrorCode() == ERR_STREAM_NAME_IN_USE) {
                jsm.updateStream(config);
            } else {
                throw ex;
            }
        }
    }

    private StreamConfiguration dlqStreamConfig() {
        return StreamConfiguration.builder()
                .name(NatsSubjects.STREAM_SCHEDULES_DLQ)
                .subjects("app.schedules.dlq.>")
                .storageType(StorageType.File)
                .maxAge(Duration.ofDays(30))
                .maxBytes(512L * 1024 * 1024)
                .discardPolicy(DiscardPolicy.Old)
                .build();
    }
}
