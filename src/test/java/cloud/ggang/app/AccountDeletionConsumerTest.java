package cloud.ggang.app;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.JetStream;
import io.nats.client.Message;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.SimpleTransactionStatus;

class AccountDeletionConsumerTest {
    @Test void deletionPurgesUserRowsBeforePublishingCompletionAndAcking() throws Exception {
        JdbcTemplate jdbc=mock(JdbcTemplate.class);
        PlatformTransactionManager manager=mock(PlatformTransactionManager.class);
        when(manager.getTransaction(any(TransactionDefinition.class))).thenReturn(new SimpleTransactionStatus());
        NatsConnectionHolder nats=mock(NatsConnectionHolder.class);
        JetStream js=mock(JetStream.class);
        when(nats.jetStreamOrNull()).thenReturn(js);
        UUID user=UUID.randomUUID(), event=UUID.randomUUID(), request=UUID.randomUUID();
        String payload="{\"event_id\":\""+event+"\",\"request_id\":\""+request
                +"\",\"user_id\":\""+user+"\",\"credential_version\":1,\"occurred_at\":\""
                +Instant.now()+"\"}";
        Message msg=mock(Message.class);
        when(msg.getData()).thenReturn(payload.getBytes(StandardCharsets.UTF_8));

        new AccountDeletionConsumer(new ObjectMapper().findAndRegisterModules(),jdbc,manager,nats).onMessage(msg);

        verify(jdbc).update(org.mockito.ArgumentMatchers.contains("DELETE FROM reminder_attempt"),any(byte[].class));
        verify(jdbc).update(org.mockito.ArgumentMatchers.contains("DELETE FROM reminder_dispatch"),any(byte[].class));
        verify(jdbc).update(org.mockito.ArgumentMatchers.contains("DELETE FROM schedule_event_state"),any(byte[].class));
        verify(js).publish(eq(NatsSubjects.ACCOUNT_DELETION_RESULT),any(byte[].class));
        verify(msg).ack();
    }
}
