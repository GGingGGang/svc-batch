package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.nats.client.JetStream;
import io.nats.client.Message;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class DlqPublisherTest {

    @Test
    void onlyReportsSuccessAfterSynchronousPublish() throws Exception {
        NatsConnectionHolder holder = mock(NatsConnectionHolder.class);
        JetStream jetStream = mock(JetStream.class);
        Message original = mock(Message.class);
        when(holder.jetStreamOrNull()).thenReturn(jetStream);
        when(original.getSubject()).thenReturn(NatsSubjects.SCHEDULE_CREATED);
        when(original.getData()).thenReturn("{".getBytes(StandardCharsets.UTF_8));
        DlqPublisher publisher = new DlqPublisher(holder);

        assertThat(publisher.publish(original, "bad payload")).isTrue();
        verify(jetStream).publish(any(Message.class));

        when(jetStream.publish(any(Message.class))).thenThrow(new IOException("unavailable"));
        assertThat(publisher.publish(original, "bad payload")).isFalse();
    }
}
