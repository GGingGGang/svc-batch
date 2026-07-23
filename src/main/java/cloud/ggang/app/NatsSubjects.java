package cloud.ggang.app;

// subject/stream 명명은 전체문서 PLAN.md §7.2 컨벤션 — APP_SCHEDULES 는 core 소유,
// APP_SCHEDULES_DLQ + durable consumer batch-reminders 는 batch 소유.
public final class NatsSubjects {

    public static final String STREAM_SCHEDULES = "APP_SCHEDULES";
    public static final String STREAM_SCHEDULES_DLQ = "APP_SCHEDULES_DLQ";
    public static final String CONSUMER_NAME = "batch-reminders";

    public static final String SCHEDULE_CREATED = "app.schedules.created.v1";
    public static final String SCHEDULE_UPDATED = "app.schedules.updated.v1";
    public static final String SCHEDULE_DELETED = "app.schedules.deleted.v1";

    private static final String DLQ_SUBJECT_PREFIX = "app.schedules.dlq.";

    private NatsSubjects() {}

    // app.schedules.<event>.v1 -> app.schedules.dlq.<event>
    public static String dlqSubject(String sourceSubject) {
        String[] parts = sourceSubject.split("\\.");
        return DLQ_SUBJECT_PREFIX + parts[2];
    }
}
