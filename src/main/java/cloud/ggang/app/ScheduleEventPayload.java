package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.List;

// schedules.created.v1 / schedules.updated.v1 공통 value schema. 전체문서 PLAN.md §7.3.
// reminders 는 해당 시점의 전체 스냅샷 — consumer 는 이 payload 기준으로 재조립한다.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ScheduleEventPayload(
        @JsonProperty("schedule_id") String scheduleId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("title") String title,
        @JsonProperty("start_at") Instant startAt,
        @JsonProperty("end_at") Instant endAt,
        @JsonProperty("all_day") boolean allDay,
        @JsonProperty("source") String source,
        @JsonProperty("reminders") List<ReminderPayload> reminders,
        @JsonProperty("occurred_at") Instant occurredAt) {

    public ScheduleEventPayload {
        reminders = reminders == null ? List.of() : List.copyOf(reminders);
    }
}
