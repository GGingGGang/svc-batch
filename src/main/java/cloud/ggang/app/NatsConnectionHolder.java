package cloud.ggang.app;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

// nats.java 는 동기 connect() 가 초기 접속 실패 시 그대로 던져버려(연결 재시도 옵션이 없음 —
// nats-io/nats.java#406), 기동 시점에 NATS 가 없으면 컨텍스트 자체가 안 뜨는 회귀가 생긴다.
// Nats.connectAsynchronously(..., true) 로 접속을 별도 스레드에 위임하고 ConnectionListener 의
// CONNECTED/RECONNECTED 콜백에서 Connection 을 받는다 — 기존 Redis/ShedLock 의 lazy 연결 관례와 정합
// (BatchMetaSchemaIntegrationTest 는 NATS 컨테이너 없이도 계속 통과).
// ApplicationContext 지연 조회로 Connection → Bootstrap → ScheduleEventConsumer → DlqPublisher →
// Connection 순환 의존을 피한다.
@Component
public class NatsConnectionHolder {

    @Value("${app.nats.url}")
    private String natsUrl;

    private final ApplicationContext applicationContext;

    private volatile Connection connection;

    public NatsConnectionHolder(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void connect() throws InterruptedException {
        Options options =
                new Options.Builder()
                        .server(natsUrl)
                        .connectionName("svc-batch")
                        .maxReconnects(-1)
                        .connectionListener(
                                (conn, type) -> {
                                    if (type == ConnectionListener.Events.CONNECTED
                                            || type == ConnectionListener.Events.RECONNECTED) {
                                        connection = conn;
                                        applicationContext.getBean(NatsStreamBootstrap.class).bootstrap(conn);
                                    }
                                })
                        .build();
        Nats.connectAsynchronously(options, true);
    }

    // 아직 연결 전이면 null.
    public JetStream jetStreamOrNull() throws IOException {
        Connection conn = connection;
        return conn == null ? null : conn.jetStream();
    }

    @PreDestroy
    public void shutdown() throws InterruptedException {
        Connection conn = connection;
        if (conn != null) {
            conn.close();
        }
    }
}
