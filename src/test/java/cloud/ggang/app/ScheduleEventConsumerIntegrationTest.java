package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

// Kafka consumer reconcile 검증 — 실토픽 불요 (testcontainers Kafka, 운영에서는 auto-create 금지지만
// 여기서는 테스트 설정으로 허용). svc-batch/PLAN.md §4 / §7 DoD: 중복/역순/삭제-후-갱신 시나리오.
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ScheduleEventConsumerIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("batch")
                    .withUsername("app_batch")
                    .withPassword("app_batch_pw")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("db/testcontainers-init.sql"),
                            "/docker-entrypoint-initdb.d/01-batch-meta.sql");

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
                    .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "spring.datasource.url",
                () -> MYSQL.getJdbcUrl() + "?sslMode=REQUIRED&serverTimezone=UTC&characterEncoding=utf8");
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.url", () -> "redis://localhost:6379/2");
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private ObjectMapper objectMapper;

    private KafkaProducer<String, String> producer;

    @BeforeEach
    void setUpProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @AfterEach
    void tearDownProducer() {
        producer.close();
    }

    @Test
    void duplicateCreatedEventIsIdempotent() throws Exception {
        UUID scheduleId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Instant startAt = Instant.parse("2026-08-01T03:00:00Z");
        Instant occurredAt = Instant.parse("2026-07-21T00:00:00Z");
        String json =
                upsertJson(
                        scheduleId,
                        userId,
                        "회의",
                        startAt,
                        "manual",
                        List.of(Map.of("minutes_before", 30, "channel", "push")),
                        occurredAt);

        send(KafkaTopics.SCHEDULE_CREATED, scheduleId.toString(), json);
        send(KafkaTopics.SCHEDULE_CREATED, scheduleId.toString(), json);

        awaitReminderCount(scheduleId, 1);

        // 두 번째 전달이 반영될 시간을 더 준 뒤에도 결과가 그대로인지 재확인.
        Thread.sleep(2000);
        assertThat(reminderCount(scheduleId)).isEqualTo(1);
        Integer created =
                jdbcTemplate.queryForObject(
                        "SELECT schedules_created FROM daily_schedule_stats WHERE stat_date = ?",
                        Integer.class,
                        occurredAt.atZone(ZoneOffset.UTC).toLocalDate());
        assertThat(created).isEqualTo(1);
    }

    @Test
    void outOfOrderUpdatedThenCreatedIsHarmless() throws Exception {
        UUID scheduleId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Instant startAtOld = Instant.parse("2026-08-01T03:00:00Z");
        Instant startAtNew = Instant.parse("2026-08-02T03:00:00Z");
        Instant occurredOld = Instant.parse("2026-07-21T00:00:00Z");
        Instant occurredNew = Instant.parse("2026-07-21T00:05:00Z");

        String updatedJson =
                upsertJson(
                        scheduleId,
                        userId,
                        "회의(변경)",
                        startAtNew,
                        "manual",
                        List.of(Map.of("minutes_before", 10, "channel", "push")),
                        occurredNew);
        String createdJson =
                upsertJson(
                        scheduleId,
                        userId,
                        "회의",
                        startAtOld,
                        "manual",
                        List.of(Map.of("minutes_before", 30, "channel", "push")),
                        occurredOld);

        // updated(최신) 먼저, created(과거) 나중 도착 — 역순.
        send(KafkaTopics.SCHEDULE_UPDATED, scheduleId.toString(), updatedJson);
        awaitReminderCount(scheduleId, 1);
        send(KafkaTopics.SCHEDULE_CREATED, scheduleId.toString(), createdJson);

        // 과거 이벤트가 나중에 와도 무해해야 하므로, 처리 유예 시간 후에도 최신 상태 유지를 확인.
        Thread.sleep(2000);
        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(
                        () -> {
                            assertThat(reminderCount(scheduleId)).isEqualTo(1);
                            assertThat(reminderTitle(scheduleId)).isEqualTo("회의(변경)");
                            assertThat(reminderMinutesBefore(scheduleId)).isEqualTo(10);
                        });
        assertThat(lastEventAt(scheduleId)).isEqualTo(occurredNew.atZone(ZoneOffset.UTC).toLocalDateTime());
    }

    @Test
    void deletedThenUpdatedIsIgnored() throws Exception {
        UUID scheduleId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Instant startAt = Instant.parse("2026-08-01T03:00:00Z");
        Instant occurredCreated = Instant.parse("2026-07-21T00:00:00Z");
        Instant occurredDeleted = Instant.parse("2026-07-21T00:05:00Z");
        Instant occurredLateUpdate = Instant.parse("2026-07-21T00:10:00Z");

        String createdJson =
                upsertJson(
                        scheduleId,
                        userId,
                        "회의",
                        startAt,
                        "manual",
                        List.of(Map.of("minutes_before", 30, "channel", "push")),
                        occurredCreated);
        send(KafkaTopics.SCHEDULE_CREATED, scheduleId.toString(), createdJson);
        awaitReminderCount(scheduleId, 1);

        String deleteJsonBody = deletedJson(scheduleId, userId, occurredDeleted);
        send(KafkaTopics.SCHEDULE_DELETED, scheduleId.toString(), deleteJsonBody);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(reminderStatus(scheduleId)).isEqualTo("skipped"));

        // occurred_at 이 delete 보다 더 최신인 updated 가 나중에 와도 delete 는 terminal — 무시되어야 함.
        String lateUpdateJson =
                upsertJson(
                        scheduleId,
                        userId,
                        "부활 시도",
                        startAt,
                        "manual",
                        List.of(Map.of("minutes_before", 15, "channel", "push")),
                        occurredLateUpdate);
        send(KafkaTopics.SCHEDULE_UPDATED, scheduleId.toString(), lateUpdateJson);

        Thread.sleep(3000);
        assertThat(reminderCount(scheduleId)).isEqualTo(1);
        assertThat(reminderStatus(scheduleId)).isEqualTo("skipped");
        assertThat(reminderTitle(scheduleId)).isEqualTo("회의");
        Boolean deletedFlag =
                jdbcTemplate.queryForObject(
                        "SELECT is_deleted FROM schedule_event_state WHERE schedule_id = ?",
                        Boolean.class,
                        toBytes(scheduleId));
        assertThat(deletedFlag).isTrue();
    }

    @Test
    void malformedPayloadGoesToDlqWithHeaders() throws Exception {
        String badJson = "{not-valid-json";
        send(KafkaTopics.SCHEDULE_CREATED, "bad-key", badJson);

        ConsumerRecord<String, String> dlqRecord =
                awaitDlqRecord(KafkaTopics.dlqTopic(KafkaTopics.SCHEDULE_CREATED));

        assertThat(dlqRecord.value()).isEqualTo(badJson);
        assertThat(headerValue(dlqRecord, "x-original-topic")).isEqualTo(KafkaTopics.SCHEDULE_CREATED);
        assertThat(headerValue(dlqRecord, "x-failure-reason")).isNotBlank();
        assertThat(headerValue(dlqRecord, "x-failure-ts")).isNotBlank();
    }

    private ConsumerRecord<String, String> awaitDlqRecord(String dlqTopic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KAFKA.getBootstrapServers());
        props.put("group.id", "dlq-test-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(dlqTopic));
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    return record;
                }
            }
        }
        throw new AssertionError("no record arrived on " + dlqTopic + " within timeout");
    }

    private String headerValue(ConsumerRecord<String, String> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        assertThat(header).as("header " + headerName).isNotNull();
        return new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private void send(String topic, String key, String json) throws Exception {
        producer.send(new ProducerRecord<>(topic, key, json)).get();
    }

    private String upsertJson(
            UUID scheduleId,
            UUID userId,
            String title,
            Instant startAt,
            String source,
            List<Map<String, Object>> reminders,
            Instant occurredAt)
            throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schedule_id", scheduleId.toString());
        payload.put("user_id", userId.toString());
        payload.put("title", title);
        payload.put("start_at", startAt.toString());
        payload.put("end_at", null);
        payload.put("all_day", false);
        payload.put("source", source);
        payload.put("reminders", reminders);
        payload.put("occurred_at", occurredAt.toString());
        return objectMapper.writeValueAsString(payload);
    }

    private String deletedJson(UUID scheduleId, UUID userId, Instant occurredAt) throws Exception {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("schedule_id", scheduleId.toString());
        payload.put("user_id", userId.toString());
        payload.put("occurred_at", occurredAt.toString());
        return objectMapper.writeValueAsString(payload);
    }

    private void awaitReminderCount(UUID scheduleId, int expected) {
        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(reminderCount(scheduleId)).isEqualTo(expected));
    }

    private int reminderCount(UUID scheduleId) {
        Integer count =
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM reminder_dispatch WHERE schedule_id = ?",
                        Integer.class,
                        toBytes(scheduleId));
        return count == null ? 0 : count;
    }

    private String reminderTitle(UUID scheduleId) {
        return jdbcTemplate.queryForObject(
                "SELECT title FROM reminder_dispatch WHERE schedule_id = ?",
                String.class,
                toBytes(scheduleId));
    }

    private String reminderStatus(UUID scheduleId) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM reminder_dispatch WHERE schedule_id = ?",
                String.class,
                toBytes(scheduleId));
    }

    private Integer reminderMinutesBefore(UUID scheduleId) {
        return jdbcTemplate.queryForObject(
                "SELECT minutes_before FROM reminder_dispatch WHERE schedule_id = ?",
                Integer.class,
                toBytes(scheduleId));
    }

    private java.time.LocalDateTime lastEventAt(UUID scheduleId) {
        return jdbcTemplate.queryForObject(
                "SELECT last_event_at FROM schedule_event_state WHERE schedule_id = ?",
                java.time.LocalDateTime.class,
                toBytes(scheduleId));
    }

    private byte[] toBytes(UUID uuid) {
        return UuidBytes.toBytes(uuid);
    }
}
