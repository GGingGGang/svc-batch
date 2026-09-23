package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.LoggerFactory;

class ScheduleEventConsumerTest {

    @Test
    void failureDiagnosticsDoNotExposePayloadOrExceptionMessage() {
        String secret = "private-schedule-title";
        Message message = mock(Message.class);
        when(message.getSubject()).thenReturn(NatsSubjects.SCHEDULE_CREATED);
        when(message.getData()).thenReturn(
                ("{\"title\":\"" + secret + "\",").getBytes(StandardCharsets.UTF_8),
                ("{\"title\":\"" + secret + "\"}").getBytes(StandardCharsets.UTF_8));
        ScheduleReconcileService reconcile = mock(ScheduleReconcileService.class);
        doThrow(new IllegalStateException(secret)).when(reconcile).upsert(any());
        DlqPublisher dlq = mock(DlqPublisher.class);
        Logger logger = (Logger) LoggerFactory.getLogger(ScheduleEventConsumer.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try (SimpleMeterRegistry meters = new SimpleMeterRegistry()) {
            ScheduleEventConsumer consumer = new ScheduleEventConsumer(
                    new ObjectMapper(), reconcile, dlq, meters);
            consumer.onMessage(message);
            consumer.onMessage(message);

            ArgumentCaptor<String> reasons = ArgumentCaptor.forClass(String.class);
            verify(dlq, times(2)).publish(eq(message), reasons.capture());
            assertThat(reasons.getAllValues()).allSatisfy(reason -> assertThat(reason)
                    .doesNotContain(secret).containsAnyOf("Json", "IllegalStateException"));
            assertThat(logs.list).allSatisfy(event -> {
                assertThat(event.getFormattedMessage()).doesNotContain(secret);
                assertThat(event.getThrowableProxy()).isNull();
            });
            verify(message, times(2)).ack();
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }
}
