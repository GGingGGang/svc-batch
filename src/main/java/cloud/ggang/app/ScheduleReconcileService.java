package cloud.ggang.app;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

// consumer reconcile 알고리즘 — 전체문서 PLAN.md §6.5, svc-batch/PLAN.md §4 그대로.
// 이벤트당 1 트랜잭션. schedule_event_state FOR UPDATE 가드로 at-least-once 중복/역순 도착을 방어한다.
@Service
public class ScheduleReconcileService {

    private static final String REMINDER_UPSERT_SQL =
            "INSERT INTO reminder_dispatch "
                    + "(id, schedule_id, user_id, title, minutes_before, channel, start_at, remind_at, status) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, "
                    + "IF(? > UTC_TIMESTAMP(3), 'pending', 'skipped')) AS new "
                    + "ON DUPLICATE KEY UPDATE "
                    + "title = new.title, "
                    + "start_at = new.start_at, "
                    + "remind_at = new.remind_at, "
                    + "status = IF(new.remind_at = reminder_dispatch.remind_at, "
                    + "reminder_dispatch.status, "
                    + "IF(new.remind_at > UTC_TIMESTAMP(3), 'pending', 'skipped')), "
                    + "attempt_count = IF(new.remind_at = reminder_dispatch.remind_at, "
                    + "reminder_dispatch.attempt_count, 0), "
                    + "last_error = IF(new.remind_at = reminder_dispatch.remind_at, "
                    + "reminder_dispatch.last_error, NULL), "
                    + "sent_at = IF(new.remind_at = reminder_dispatch.remind_at, reminder_dispatch.sent_at, NULL)";

    private final JdbcTemplate jdbcTemplate;

    public ScheduleReconcileService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void upsert(ScheduleEventPayload payload) {
        byte[] scheduleId = UuidBytes.toBytes(UUID.fromString(payload.scheduleId()));
        byte[] userId = UuidBytes.toBytes(UUID.fromString(payload.userId()));
        Instant occurredAt = payload.occurredAt();

        EventState existing = selectStateForUpdate(scheduleId);
        if (existing != null && (existing.deleted() || !occurredAt.isAfter(existing.lastEventAt()))) {
            // 이미 처리됐거나 더 최신 이벤트면 no-op — 변경 없이 커밋.
            return;
        }

        if (existing == null) {
            jdbcTemplate.update(
                    "INSERT INTO schedule_event_state (schedule_id, user_id, last_event_at, is_deleted) "
                            + "VALUES (?, ?, ?, 0)",
                    scheduleId,
                    userId,
                    toLocalDateTime(occurredAt));
            // 최초 관측 (updated 선착도 포함) — schedules_created 증분.
            incrementCreatedStats(occurredAt, payload.source());
        } else {
            jdbcTemplate.update(
                    "UPDATE schedule_event_state SET last_event_at = ? WHERE schedule_id = ?",
                    toLocalDateTime(occurredAt),
                    scheduleId);
        }

        // 구 pending 리마인더 정리 (sent/failed/skipped 이력은 보존).
        jdbcTemplate.update(
                "DELETE FROM reminder_dispatch WHERE schedule_id = ? AND status = 'pending'", scheduleId);

        for (ReminderPayload reminder : payload.reminders()) {
            if ("none".equals(reminder.channel())) {
                continue;
            }
            upsertReminder(scheduleId, userId, payload.title(), payload.startAt(), reminder);
        }
    }

    @Transactional
    public void delete(ScheduleDeletedPayload payload) {
        byte[] scheduleId = UuidBytes.toBytes(UUID.fromString(payload.scheduleId()));
        byte[] userId = UuidBytes.toBytes(UUID.fromString(payload.userId()));
        Instant occurredAt = payload.occurredAt();

        EventState existing = selectStateForUpdate(scheduleId);
        boolean newlyDeleted;
        if (existing == null) {
            jdbcTemplate.update(
                    "INSERT INTO schedule_event_state (schedule_id, user_id, last_event_at, is_deleted) "
                            + "VALUES (?, ?, ?, 1)",
                    scheduleId,
                    userId,
                    toLocalDateTime(occurredAt));
            newlyDeleted = true;
        } else {
            newlyDeleted = !existing.deleted();
            jdbcTemplate.update(
                    "UPDATE schedule_event_state SET is_deleted = 1, "
                            + "last_event_at = GREATEST(last_event_at, ?) WHERE schedule_id = ?",
                    toLocalDateTime(occurredAt),
                    scheduleId);
        }

        if (newlyDeleted) {
            incrementStat(occurredAt, "schedules_deleted", 1);
        }

        // 항상 적용 (occurred_at 무관) — hard delete 는 terminal.
        int skipped =
                jdbcTemplate.update(
                        "UPDATE reminder_dispatch SET status = 'skipped' "
                                + "WHERE schedule_id = ? AND status = 'pending'",
                        scheduleId);
        if (skipped > 0) {
            incrementStat(occurredAt, "reminders_skipped", skipped);
        }
    }

    private void upsertReminder(
            byte[] scheduleId, byte[] userId, String title, Instant startAt, ReminderPayload reminder) {
        Instant remindAt = startAt.minus(reminder.minutesBefore(), ChronoUnit.MINUTES);
        LocalDateTime remindAtValue = toLocalDateTime(remindAt);
        jdbcTemplate.update(
                REMINDER_UPSERT_SQL,
                UuidBytes.toBytes(Uuid7Generator.generate()),
                scheduleId,
                userId,
                title,
                reminder.minutesBefore(),
                reminder.channel(),
                toLocalDateTime(startAt),
                remindAtValue,
                remindAtValue);
    }

    private EventState selectStateForUpdate(byte[] scheduleId) {
        List<EventState> rows =
                jdbcTemplate.query(
                        "SELECT last_event_at, is_deleted FROM schedule_event_state "
                                + "WHERE schedule_id = ? FOR UPDATE",
                        (rs, rowNum) ->
                                new EventState(
                                        rs.getTimestamp("last_event_at").toInstant(),
                                        rs.getBoolean("is_deleted")),
                        scheduleId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private void incrementCreatedStats(Instant occurredAt, String source) {
        int aiCount = "ai".equals(source) ? 1 : 0;
        int manualCount = "ai".equals(source) ? 0 : 1;
        jdbcTemplate.update(
                "INSERT INTO daily_schedule_stats "
                        + "(stat_date, schedules_created, schedules_created_ai, schedules_created_manual) "
                        + "VALUES (?, 1, ?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + "schedules_created = daily_schedule_stats.schedules_created + 1, "
                        + "schedules_created_ai = "
                        + "daily_schedule_stats.schedules_created_ai + new.schedules_created_ai, "
                        + "schedules_created_manual = "
                        + "daily_schedule_stats.schedules_created_manual + new.schedules_created_manual",
                toUtcDate(occurredAt),
                aiCount,
                manualCount);
    }

    private void incrementStat(Instant occurredAt, String column, int amount) {
        String sql =
                "INSERT INTO daily_schedule_stats (stat_date, "
                        + column
                        + ") VALUES (?, ?) AS new "
                        + "ON DUPLICATE KEY UPDATE "
                        + column
                        + " = daily_schedule_stats."
                        + column
                        + " + new."
                        + column;
        jdbcTemplate.update(sql, toUtcDate(occurredAt), amount);
    }

    private static LocalDateTime toLocalDateTime(Instant instant) {
        return LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
    }

    private static LocalDate toUtcDate(Instant instant) {
        return instant.atZone(ZoneOffset.UTC).toLocalDate();
    }

    private record EventState(Instant lastEventAt, boolean deleted) {}
}
