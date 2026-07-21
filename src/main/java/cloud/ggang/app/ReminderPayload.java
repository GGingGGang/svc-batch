package cloud.ggang.app;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// schedules.created.v1 / updated.v1 payload 의 reminders[] 원소. 전체문서 PLAN.md §7.3.
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReminderPayload(
        @JsonProperty("minutes_before") int minutesBefore, @JsonProperty("channel") String channel) {}
