package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

// schedules.deleted.v1 value schema. 전체문서 PLAN.md §7.4.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScheduleDeletedPayload(
        @JsonProperty("schedule_id") String scheduleId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("occurred_at") Instant occurredAt) {}
