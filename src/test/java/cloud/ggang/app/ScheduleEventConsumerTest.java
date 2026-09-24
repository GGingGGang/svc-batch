package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
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
        when(message.getHeaders()).thenReturn(new Headers()
                .add("x-request-id", "123e4567-e89b-12d3-a456-426614174000")
                .add("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"));
        when(message.getData()).thenReturn(
                ("{\"title\":\"" + secret + "\",").getBytes(StandardCharsets.UTF_8),
                ("{\"title\":\"" + secret + "\"}").getBytes(StandardCharsets.UTF_8));
        ScheduleReconcileService reconcile = mock(ScheduleReconcileService.class);
        doThrow(new IllegalStateException(secret)).when(reconcile).upsert(any());
        DlqPublisher dlq = mock(DlqPublisher.class);
        when(dlq.publish(any(), any(), any())).thenReturn(true);
        Logger logger = (Logger) LoggerFactory.getLogger(ScheduleEventConsumer.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            ScheduleEventConsumer consumer = new ScheduleEventConsumer(
                    new ObjectMapper(), reconcile, dlq, meters);
            consumer.onMessage(message);
            consumer.onMessage(message);

            ArgumentCaptor<String> reasons = ArgumentCaptor.forClass(String.class);
            verify(dlq, times(2)).publish(eq(message), reasons.capture(), eq("123e4567-e89b-12d3-a456-426614174000"));
            assertThat(reasons.getAllValues()).allSatisfy(reason -> assertThat(reason)
                    .doesNotContain(secret).containsAnyOf("Json", "IllegalStateException"));
            assertThat(logs.list).allSatisfy(event -> {
                assertThat(event.getFormattedMessage()).doesNotContain(secret);
                assertThat(event.getFormattedMessage()).contains("error_id=123e4567-e89b-12d3-a456-426614174000");
                assertThat(event.getThrowableProxy()).isNull();
            });
            verify(message, times(2)).ack();
        } finally {
            meters.close();
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    @Test
    void failedDlqPublishLeavesOriginalUnackedForRedelivery() {
        Message message = mock(Message.class);
        when(message.getSubject()).thenReturn(NatsSubjects.SCHEDULE_CREATED);
        when(message.getData()).thenReturn("{".getBytes(StandardCharsets.UTF_8));
        DlqPublisher dlq = mock(DlqPublisher.class);
        when(dlq.publish(any(), any(), any())).thenReturn(false);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            ScheduleEventConsumer consumer = new ScheduleEventConsumer(
                    new ObjectMapper(), mock(ScheduleReconcileService.class), dlq, meters);
            consumer.onMessage(message);

            verify(message, never()).ack();
            assertThat(meters.counter("schedule_events_dlq_failures_total", "subject", NatsSubjects.SCHEDULE_CREATED)
                            .count())
                    .isEqualTo(1);
            assertThat(meters.counter("schedule_events_dlq_total", "subject", NatsSubjects.SCHEDULE_CREATED)
                            .count())
                    .isZero();
            assertThat(meters.get("schedule_events_dlq_last_failure_epoch_seconds").gauge().value())
                    .isPositive();
        } finally {
            meters.close();
        }
    }

    @Test
    void untrustedRequestIdIsNotCopiedIntoErrorIdentifier() {
        Message message = mock(Message.class);
        when(message.getSubject()).thenReturn(NatsSubjects.SCHEDULE_CREATED);
        when(message.getData()).thenReturn("{".getBytes(StandardCharsets.UTF_8));
        when(message.getHeaders()).thenReturn(new Headers().add("x-request-id", "private-schedule-title"));
        DlqPublisher dlq = mock(DlqPublisher.class);
        when(dlq.publish(any(), any(), any())).thenReturn(true);
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        try {
            new ScheduleEventConsumer(new ObjectMapper(), mock(ScheduleReconcileService.class), dlq, meters)
                    .onMessage(message);
            ArgumentCaptor<String> ids = ArgumentCaptor.forClass(String.class);
            verify(dlq).publish(eq(message), any(), ids.capture());
            assertThat(ids.getValue()).matches("[0-9a-f-]{36}").doesNotContain("private");
        } finally {
            meters.close();
        }
    }
}
