package cloud.ggang.app;

// topic 명명은 전체문서 PLAN.md §7.2 컨벤션 — <domain>.<event>.v<major>, DLQ 는 <topic>.dlq.
public final class KafkaTopics {

    public static final String SCHEDULE_CREATED = "schedules.created.v1";
    public static final String SCHEDULE_UPDATED = "schedules.updated.v1";
    public static final String SCHEDULE_DELETED = "schedules.deleted.v1";

    private KafkaTopics() {}

    public static String dlqTopic(String sourceTopic) {
        return sourceTopic + ".dlq";
    }
}
