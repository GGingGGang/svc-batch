package cloud.ggang.app;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// retryOnFailedConnect 로 기동 시점에 NATS 가 없어도 컨텍스트는 정상 기동 — 연결 성공(CONNECTED/
// RECONNECTED) 시점에 NatsStreamBootstrap 이 stream/durable consumer 를 기동한다. ApplicationContext 를
// 통한 지연 조회로 Connection → NatsStreamBootstrap → ScheduleEventConsumer → DlqPublisher → JetStream →
// Connection 순환 의존을 피한다.
@Configuration
public class NatsConfig {

    @Bean(destroyMethod = "close")
    public Connection natsConnection(
            @Value("${app.nats.url}") String natsUrl, ApplicationContext applicationContext)
            throws IOException, InterruptedException {
        Options options =
                new Options.Builder()
                        .server(natsUrl)
                        .connectionName("svc-batch")
                        .maxReconnects(-1)
                        .retryOnFailedConnect(true)
                        .connectionListener(
                                (conn, type) -> {
                                    if (type == ConnectionListener.Events.CONNECTED
                                            || type == ConnectionListener.Events.RECONNECTED) {
                                        applicationContext.getBean(NatsStreamBootstrap.class).bootstrap(conn);
                                    }
                                })
                        .build();
        return Nats.connect(options);
    }

    // Connection.jetStream() 은 네트워크 왕복 없이 컨텍스트만 만든다 — 재연결 대기 중에도 안전하게 생성 가능.
    @Bean
    public JetStream jetStream(Connection natsConnection) throws IOException {
        return natsConnection.jetStream();
    }
}
