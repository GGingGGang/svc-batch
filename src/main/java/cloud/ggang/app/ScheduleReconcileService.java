package cloud.ggang.app;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ScheduleReconcileService {
    private final JdbcTemplate jdbc;

    public ScheduleReconcileService(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @Transactional
    public void upsert(ScheduleEventPayload payload) { upsert(payload, true); }

    @Transactional
    public void upsert(ScheduleEventPayload payload, boolean createdEvent) {
        if (!"manual".equals(payload.source()) && !"ai".equals(payload.source()))
            throw new IllegalArgumentException("invalid schedule source");
        if (payload.status() != null && !List.of("confirmed", "tentative", "cancelled").contains(payload.status()))
            throw new IllegalArgumentException("invalid schedule status");
        if (payload.reminders().size() > 5) throw new IllegalArgumentException("too many reminders");
        byte[] scheduleId = UuidBytes.toBytes(UUID.fromString(payload.scheduleId()));
        byte[] userId = UuidBytes.toBytes(UUID.fromString(payload.userId()));
        if (isDeletedAccount(userId)) return;
        Instant occurredAt = payload.occurredAt();
        EventState state = selectStateForUpdate(scheduleId);
        boolean first = state == null;
        if (first) {
            jdbc.update("INSERT INTO schedule_event_state(schedule_id,user_id,last_event_at,last_revision,is_deleted,creation_accounted) "
                            + "VALUES(?,?,?,?,0,0)", scheduleId,userId,toLocalDateTime(occurredAt),payload.revision());
            state = new EventState(occurredAt,payload.revision(),false);
        }
        if (createdEvent && jdbc.update("UPDATE schedule_event_state SET creation_accounted=1 "
                + "WHERE schedule_id=? AND creation_accounted=0", scheduleId) == 1)
            incrementCreatedStats(occurredAt,payload.source());
        if (state.deleted() || (!first && isStale(state,payload.revision(),occurredAt))) return;
        jdbc.update("UPDATE schedule_event_state SET last_event_at = ?, last_revision = ? WHERE schedule_id=?",
                toLocalDateTime(occurredAt),payload.revision(),scheduleId);
        if ("cancelled".equals(payload.status()) || "tentative".equals(payload.status())) {
            if ("tentative".equals(payload.status())) {
                Instant startAt = reminderStartAt(payload);
                for (ReminderPayload reminder : payload.reminders()) {
                    if ("none".equals(reminder.channel())) continue;
                    validateReminder(reminder, payload.reminders());
                    upsertReminder(scheduleId, userId, payload, startAt, reminder, occurredAt);
                }
            }
            int skipped=jdbc.update("UPDATE reminder_dispatch SET status = 'skipped',last_error='schedule_inactive' WHERE schedule_id=? "
                    + "AND status IN ('pending','dispatching')",scheduleId);
            if (skipped>0) incrementStat(occurredAt,"reminders_skipped",skipped);
            return;
        }
        Set<String> desired=new HashSet<>();
        for (ReminderPayload reminder:payload.reminders()) {
            if ("none".equals(reminder.channel())) continue;
            validateReminder(reminder,payload.reminders());
            Instant startAt=reminderStartAt(payload);
            desired.add(reminder.minutesBefore()+":"+reminder.channel());
            upsertReminder(scheduleId,userId,payload,startAt,reminder,occurredAt);
        }
        List<ReminderKey> old=jdbc.query("SELECT minutes_before,channel FROM reminder_dispatch WHERE schedule_id=? "
                        + "AND status IN ('pending','dispatching') FOR UPDATE",
                (rs,n)->new ReminderKey(rs.getInt(1),rs.getString(2)),scheduleId);
        int skipped=0;
        for (ReminderKey key:old) if (!desired.contains(key.minutesBefore()+":"+key.channel()))
            skipped+=jdbc.update("UPDATE reminder_dispatch SET status='skipped',last_error='reminder_removed' WHERE schedule_id=? AND minutes_before=? "
                    + "AND channel=? AND status IN ('pending','dispatching')",scheduleId,key.minutesBefore(),key.channel());
        if (skipped>0) incrementStat(occurredAt,"reminders_skipped",skipped);
    }

    @Transactional
    public void delete(ScheduleDeletedPayload payload) {
        byte[] scheduleId=UuidBytes.toBytes(UUID.fromString(payload.scheduleId()));
        byte[] userId=UuidBytes.toBytes(UUID.fromString(payload.userId()));
        if (isDeletedAccount(userId)) return;
        Instant occurredAt=payload.occurredAt();
        EventState state=selectStateForUpdate(scheduleId);
        boolean newlyDeleted;
        if (state==null) {
            jdbc.update("INSERT INTO schedule_event_state(schedule_id,user_id,last_event_at,last_revision,is_deleted,creation_accounted) "
                    + "VALUES(?,?,?,?,1,0)",scheduleId,userId,toLocalDateTime(occurredAt),payload.revision());
            newlyDeleted=true;
        } else {
            newlyDeleted=!state.deleted();
            jdbc.update("UPDATE schedule_event_state SET is_deleted=1,last_event_at=GREATEST(last_event_at,?), "
                    + "last_revision=COALESCE(?,last_revision) WHERE schedule_id=?",
                    toLocalDateTime(occurredAt),payload.revision(),scheduleId);
        }
        if (newlyDeleted) incrementStat(occurredAt,"schedules_deleted",1);
        int skipped=jdbc.update("UPDATE reminder_dispatch SET status = 'skipped',last_error='schedule_deleted' WHERE schedule_id=? "
                + "AND status IN ('pending','dispatching')",scheduleId);
        if (skipped>0) incrementStat(occurredAt,"reminders_skipped",skipped);
    }

    private void upsertReminder(byte[] scheduleId,byte[] userId,ScheduleEventPayload payload,Instant startAt,
            ReminderPayload reminder,Instant occurredAt) {
        Instant remindAt=startAt.minus(reminder.minutesBefore(),ChronoUnit.MINUTES);
        LocalDateTime remindValue=toLocalDateTime(remindAt);
        List<Snapshot> previous=jdbc.query("SELECT id,status,remind_at,schedule_revision,last_error FROM reminder_dispatch "
                        + "WHERE schedule_id=? AND minutes_before=? AND channel=? FOR UPDATE",
                (rs,n)->new Snapshot(rs.getBytes("id"),rs.getString("status"),rs.getTimestamp("remind_at").toInstant(),
                        rs.getObject("schedule_revision",Long.class),rs.getString("last_error")),scheduleId,reminder.minutesBefore(),reminder.channel());
        boolean lifecycleSkipped=!previous.isEmpty() && "skipped".equals(previous.get(0).status())
                && List.of("schedule_inactive","reminder_removed","schedule_deleted").contains(previous.get(0).lastError());
        boolean same=!previous.isEmpty() && previous.get(0).remindAt().equals(remindAt) && !lifecycleSkipped;
        byte[] id=same?previous.get(0).id():UuidBytes.toBytes(Uuid7Generator.generate());
        String status=same?previous.get(0).status():(remindAt.isAfter(Instant.now())?"pending":"skipped");
        if(previous.isEmpty()) {
            jdbc.update("INSERT INTO reminder_dispatch(id,schedule_id,user_id,title,minutes_before,channel,start_at,remind_at,status,"
                            + "timezone,all_day,start_date,end_date,schedule_revision) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    id,scheduleId,userId,payload.title(),reminder.minutesBefore(),reminder.channel(),toLocalDateTime(startAt),
                    remindValue,status,payload.timezone(),payload.allDay(),payload.startDate(),payload.endDate(),payload.revision());
        } else if(same) {
            jdbc.update("UPDATE reminder_dispatch SET user_id=?,title=?,start_at=?,timezone=?,all_day=?,start_date=?,end_date=?,"
                            + "schedule_revision=? WHERE schedule_id=? AND minutes_before=? AND channel=?",
                    userId,payload.title(),toLocalDateTime(startAt),payload.timezone(),payload.allDay(),payload.startDate(),
                    payload.endDate(),payload.revision(),scheduleId,reminder.minutesBefore(),reminder.channel());
        } else {
            jdbc.update("UPDATE reminder_dispatch SET id=?,user_id=?,title=?,start_at=?,remind_at=?,status=?,timezone=?,all_day=?,"
                            + "start_date=?,end_date=?,schedule_revision=?,attempt_count=0,last_error=NULL,sent_at=NULL,next_attempt_at=NULL "
                            + "WHERE schedule_id=? AND minutes_before=? AND channel=?",
                    id,userId,payload.title(),toLocalDateTime(startAt),remindValue,status,payload.timezone(),payload.allDay(),
                    payload.startDate(),payload.endDate(),payload.revision(),scheduleId,reminder.minutesBefore(),reminder.channel());
        }
        if("skipped".equals(status) && (previous.isEmpty() || !"skipped".equals(previous.get(0).status())))
            incrementStat(occurredAt,"reminders_skipped",1);
    }

    private void validateReminder(ReminderPayload reminder,List<ReminderPayload> all) {
        if(reminder.minutesBefore()<0 || reminder.minutesBefore()>10080 || !List.of("push","email").contains(reminder.channel())
                || all.stream().filter(r->r.minutesBefore()==reminder.minutesBefore() && reminder.channel().equals(r.channel())).count()>1)
            throw new IllegalArgumentException("invalid reminder configuration");
    }

    private Instant reminderStartAt(ScheduleEventPayload payload) {
        ZoneId zone;
        try { zone=ZoneId.of(payload.timezone()); } catch(RuntimeException ex) { throw new IllegalArgumentException("invalid timezone",ex); }
        if(payload.allDay()) {
            if(payload.startDate()==null || payload.endDate()==null || !payload.endDate().isAfter(payload.startDate()))
                throw new IllegalArgumentException("all-day dates are required");
            return payload.startDate().atTime(9,0).atZone(zone).toInstant();
        }
        if(payload.startAt()==null) throw new IllegalArgumentException("start_at is required");
        return payload.startAt();
    }

    private boolean isDeletedAccount(byte[] userId) {
        jdbc.update("INSERT IGNORE INTO account_lifecycle_guard(user_hash,is_deleted) VALUES(UNHEX(SHA2(?,256)),0)",userId);
        return Boolean.TRUE.equals(jdbc.queryForObject("SELECT is_deleted FROM account_lifecycle_guard "
                + "WHERE user_hash=UNHEX(SHA2(?,256)) FOR UPDATE",Boolean.class,userId));
    }

    private EventState selectStateForUpdate(byte[] id) {
        List<EventState> rows=jdbc.query("SELECT last_event_at,last_revision,is_deleted FROM schedule_event_state "
                        + "WHERE schedule_id=? FOR UPDATE",(rs,n)->new EventState(rs.getTimestamp("last_event_at").toInstant(),
                        rs.getObject("last_revision",Long.class),rs.getBoolean("is_deleted")),id);
        return rows.isEmpty()?null:rows.get(0);
    }
    private void incrementCreatedStats(Instant at,String source) {
        int ai="ai".equals(source)?1:0,manual=ai==1?0:1;
        jdbc.update("INSERT INTO daily_schedule_stats(stat_date,schedules_created,schedules_created_ai,schedules_created_manual) "
                        + "VALUES(?,1,?,?) AS new ON DUPLICATE KEY UPDATE schedules_created=daily_schedule_stats.schedules_created+1, "
                        + "schedules_created_ai=daily_schedule_stats.schedules_created_ai+new.schedules_created_ai, "
                        + "schedules_created_manual=daily_schedule_stats.schedules_created_manual+new.schedules_created_manual",
                toUtcDate(at),ai,manual);
    }
    private void incrementStat(Instant at,String col,int amount) {
        jdbc.update("INSERT INTO daily_schedule_stats(stat_date,"+col+") VALUES(?,?) AS new ON DUPLICATE KEY UPDATE "
                + col+"=daily_schedule_stats."+col+"+new."+col,toUtcDate(at),amount);
    }
    private boolean isStale(EventState state,Long revision,Instant occurredAt) {
        if(revision!=null && state.revision()!=null) return revision<=state.revision();
        return state.revision()!=null || !occurredAt.isAfter(state.lastEventAt());
    }
    private static LocalDateTime toLocalDateTime(Instant value) { return LocalDateTime.ofInstant(value,ZoneOffset.UTC); }
    private static LocalDate toUtcDate(Instant value) { return value.atZone(ZoneOffset.UTC).toLocalDate(); }
    private record EventState(Instant lastEventAt,Long revision,boolean deleted) {}
    private record Snapshot(byte[] id,String status,Instant remindAt,Long revision,String lastError) {}
    private record ReminderKey(int minutesBefore,String channel) {}
}
