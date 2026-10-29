package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public record ReminderDeliveryResult(
        @JsonProperty("dispatch_id") String dispatchId,
        @JsonProperty("attempt") int attempt,
        @JsonProperty("status") String status,
        @JsonProperty("reason_code") String reasonCode,
        @JsonProperty("provider_receipt_id") String providerReceiptId,
        @JsonProperty("occurred_at") Instant occurredAt) {}
