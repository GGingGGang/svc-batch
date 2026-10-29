package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.time.LocalDate;

// NATS app.reminders.due.v1 — 민감한 설명/원문/목적지는 포함하지 않는다.
public record ReminderDeliveryPayload(
        @JsonProperty("dispatch_id") String dispatchId,
        @JsonProperty("attempt") int attempt,
        @JsonProperty("user_id") String userId,
        @JsonProperty("schedule_id") String scheduleId,
        @JsonProperty("title") String title,
        @JsonProperty("start_at") Instant startAt,
        @JsonProperty("remind_at") Instant remindAt,
        @JsonProperty("minutes_before") int minutesBefore,
        @JsonProperty("channel") String channel,
        @JsonProperty("timezone") String timezone,
        @JsonProperty("all_day") boolean allDay,
        @JsonProperty("start_date") LocalDate startDate,
        @JsonProperty("end_date") LocalDate endDate,
        @JsonProperty("schedule_revision") Long scheduleRevision) {}
