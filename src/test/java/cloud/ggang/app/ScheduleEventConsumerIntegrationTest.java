package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Connection;
import io.nats.client.ConsumerContext;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.StreamContext;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DiscardPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

// NATS consumer reconcile 검증 — 실스트림 불요(testcontainers 공식 nats 이미지, org.testcontainers 전용
// 모듈 부재라 GenericContainer 로 직접 기동). svc-batch/PLAN.md §4 / 전체문서 §7 DoD: 중복/역순/삭제-후-갱신.
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Tag("integration")
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
    static final GenericContainer<?> NATS =
            new GenericContainer<>(DockerImageName.parse("nats:2.14.2-alpine"))
                    .withExposedPorts(4222)
                    .withCommand("-js")
                    .waitingFor(Wait.forLogMessage(".*Server is ready.*\\n", 1));

    // 테스트 전용 producer/inspector 커넥션 — 앱 내부의 NatsConnectionHolder(비동기 접속) 배선과는
    // 무관하게 독립적으로 발행/조회한다(구 Kafka 테스트의 별도 KafkaProducer/KafkaConsumer 와 동형).
    private static Connection testConnection;
    private static JetStream testJetStream;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) throws Exception {
        registry.add(
                "spring.datasource.url",
                () -> MYSQL.getJdbcUrl() + "?sslMode=REQUIRED&serverTimezone=UTC&characterEncoding=utf8");
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.data.redis.url", () -> "redis://localhost:6379/2");

        String natsUrl = "nats://" + NATS.getHost() + ":" + NATS.getMappedPort(4222);
        registry.add("app.nats.url", () -> natsUrl);
        registry.add("app.nats.bootstrap-retry-ms", () -> "3600000");

        testConnection = Nats.connect(natsUrl);
        testJetStream = testConnection.jetStream();
        JetStreamManagement jsm = testConnection.jetStreamManagement();

        // APP_SCHEDULES 는 실 운영에서 core 소유(전체문서 PLAN.md §7.2) — batch 단독 테스트라
        // core 역할을 대신해 컨텍스트 기동 전에 미리 선언해둔다.
        jsm.addStream(
                StreamConfiguration.builder()
                        .name(NatsSubjects.STREAM_SCHEDULES)
                        .subjects(
                                NatsSubjects.SCHEDULE_CREATED,
                                NatsSubjects.SCHEDULE_UPDATED,
                                NatsSubjects.SCHEDULE_DELETED)
                        .storageType(StorageType.File)
                        .maxAge(Duration.ofDays(7))
                        .maxBytes(1024L * 1024 * 1024)
                        .discardPolicy(DiscardPolicy.Old)
                        .build());
    }

    @AfterAll
    static void closeTestConnection() throws Exception {
        if (testConnection != null) {
            testConnection.close();
        }
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private ScheduleReconcileService reconcileService;
    @Autowired private NatsStreamBootstrap natsStreamBootstrap;

    @Test
    void pastReminderStartsSkippedWithoutDelivery() {
        UUID scheduleId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), UUID.randomUUID().toString(),
                "past meeting", now.minus(Duration.ofDays(1)), null, false, "manual", "confirmed",
                1L, List.of(new ReminderPayload(30, "push")), now));

        assertThat(reminderStatus(scheduleId)).isEqualTo("skipped");
        assertThat(jdbcTemplate.queryForObject("SELECT sent_at FROM reminder_dispatch WHERE schedule_id = ?",
                java.time.LocalDateTime.class, toBytes(scheduleId))).isNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reminders_sent FROM daily_schedule_stats WHERE stat_date = ?",
                Integer.class, now.atZone(ZoneOffset.UTC).toLocalDate())).isZero();
    }

    @Test
    void aiAndManualCreationsHaveSeparateCounts() {
        Instant occurredAt = Instant.parse("2026-03-02T00:00:00Z");
        for (String source : List.of("ai", "manual")) {
            reconcileService.upsert(new ScheduleEventPayload(UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), "meeting", occurredAt.plus(Duration.ofDays(1)),
                    null, false, source, "confirmed", 1L, List.of(), occurredAt));
        }

        Map<String, Object> stats = jdbcTemplate.queryForMap(
                "SELECT schedules_created, schedules_created_ai, schedules_created_manual "
                        + "FROM daily_schedule_stats WHERE stat_date = '2026-03-02'");
        assertThat(stats).containsEntry("schedules_created", 2)
                .containsEntry("schedules_created_ai", 1)
                .containsEntry("schedules_created_manual", 1);
    }

    @Test
    void queuedEventIsConsumedAfterWorkerRestarts() throws Exception {
        UUID scheduleId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        await().atMost(Duration.ofSeconds(20)).until(natsStreamBootstrap::isStarted);
        natsStreamBootstrap.shutdown();
        assertThat(natsStreamBootstrap.isStarted()).isFalse();

        send(NatsSubjects.SCHEDULE_CREATED, upsertJson(scheduleId, userId, "meeting",
                now.plus(Duration.ofDays(2)), "manual",
                List.of(Map.of("minutes_before", 30, "channel", "push")), now));
        assertThat(reminderCount(scheduleId)).isZero();

        natsStreamBootstrap.bootstrap(testConnection);
        awaitReminderCount(scheduleId, 1);
        assertThat(reminderStatus(scheduleId)).isEqualTo("pending");
    }

    @Test
    void cancellingAndReconfirmingReopensOnlyFutureReminder() {
        UUID scheduleId = UUID.randomUUID();
        UUID userId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Instant startAt = now.plus(Duration.ofDays(2));
        List<ReminderPayload> reminders = List.of(new ReminderPayload(30, "push"));

        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), userId.toString(),
                "meeting", startAt, null, false, "manual", "confirmed", 1L, reminders, now));
        assertThat(reminderStatus(scheduleId)).isEqualTo("pending");

        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), userId.toString(),
                "meeting", startAt, null, false, "manual", "cancelled", 2L, reminders, now));
        assertThat(reminderStatus(scheduleId)).isEqualTo("skipped");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT reminders_skipped FROM daily_schedule_stats WHERE stat_date = UTC_DATE()",
                Integer.class)).isGreaterThanOrEqualTo(1);

        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), userId.toString(),
                "meeting", startAt, null, false, "manual", "confirmed", 3L, reminders, now));
        assertThat(reminderCount(scheduleId)).isEqualTo(1);
        assertThat(reminderStatus(scheduleId)).isEqualTo("pending");

        jdbcTemplate.update("UPDATE reminder_dispatch SET attempt_count = 2, last_error = 'old' "
                + "WHERE schedule_id = ?", toBytes(scheduleId));
        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), userId.toString(),
                "meeting", startAt, null, false, "manual", "cancelled", 4L, reminders, now));
        reconcileService.upsert(new ScheduleEventPayload(scheduleId.toString(), userId.toString(),
                "meeting", startAt.plus(Duration.ofHours(1)), null, false, "manual", "confirmed", 5L,
                reminders, now));
        assertThat(reminderStatus(scheduleId)).isEqualTo("pending");
        assertThat(jdbcTemplate.queryForObject(
                "SELECT attempt_count FROM reminder_dispatch WHERE schedule_id = ?",
                Integer.class, toBytes(scheduleId))).isZero();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT last_error FROM reminder_dispatch WHERE schedule_id = ?",
                String.class, toBytes(scheduleId))).isNull();
        assertThat(jdbcTemplate.queryForObject(
                "SELECT remind_at FROM reminder_dispatch WHERE schedule_id = ?",
                java.time.LocalDateTime.class, toBytes(scheduleId)))
                .isEqualTo(startAt.plus(Duration.ofMinutes(30)).atZone(ZoneOffset.UTC).toLocalDateTime());
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

        send(NatsSubjects.SCHEDULE_CREATED, json);
        send(NatsSubjects.SCHEDULE_CREATED, json);

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
        send(NatsSubjects.SCHEDULE_UPDATED, updatedJson);
        awaitReminderCount(scheduleId, 1);
        send(NatsSubjects.SCHEDULE_CREATED, createdJson);

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
        send(NatsSubjects.SCHEDULE_CREATED, createdJson);
        awaitReminderCount(scheduleId, 1);

        String deleteJsonBody = deletedJson(scheduleId, userId, occurredDeleted);
        send(NatsSubjects.SCHEDULE_DELETED, deleteJsonBody);

        await()
                .atMost(Duration.ofSeconds(20))
                .untilAsserted(() -> assertThat(reminderStatus(scheduleId)).isEqualTo("skipped"));

        // Core outbox는 무기한 재시도하므로 오래된 삭제 tombstone도 재처리 방어에 필요하다.
        jdbcTemplate.update(
                "UPDATE schedule_event_state SET updated_at = UTC_TIMESTAMP(3) - INTERVAL 46 DAY WHERE schedule_id = ?",
                toBytes(scheduleId));

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
        send(NatsSubjects.SCHEDULE_UPDATED, lateUpdateJson);

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
        String requestId = UUID.randomUUID().toString();
        Logger logger = (Logger) LoggerFactory.getLogger(ScheduleEventConsumer.class);
        ListAppender<ILoggingEvent> logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);
        try {
            testJetStream.publish(NatsMessage.builder()
                    .subject(NatsSubjects.SCHEDULE_CREATED)
                    .headers(new Headers().add("x-request-id", requestId))
                    .data(badJson.getBytes(StandardCharsets.UTF_8))
                    .build());

            Message dlqMessage = awaitDlqMessage(NatsSubjects.dlqSubject(NatsSubjects.SCHEDULE_CREATED));

            assertThat(new String(dlqMessage.getData(), StandardCharsets.UTF_8)).isEqualTo(badJson);
            assertThat(headerValue(dlqMessage, "x-original-subject")).isEqualTo(NatsSubjects.SCHEDULE_CREATED);
            assertThat(headerValue(dlqMessage, "x-error-id")).isEqualTo(requestId);
            assertThat(headerValue(dlqMessage, "x-failure-reason")).isNotBlank();
            assertThat(headerValue(dlqMessage, "x-failure-ts")).isNotBlank();
            assertThat(logs.list).anySatisfy(event -> assertThat(event.getFormattedMessage())
                    .contains("error_id=" + requestId).doesNotContain(badJson));
        } finally {
            logger.detachAppender(logs);
            logs.stop();
        }
    }

    // durable consumer(batch-reminders) 기동이 CONNECTED 이벤트 콜백에서 비동기로 이뤄지고,
    // APP_SCHEDULES_DLQ stream 도 그 기동 절차의 일부라 아직 안 만들어졌을 수 있어 전체를 재시도로 감싼다.
    private Message awaitDlqMessage(String dlqSubject) {
        AtomicReference<Message> found = new AtomicReference<>();
        await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .ignoreExceptions()
                .untilAsserted(
                        () -> {
                            StreamContext dlqStream =
                                    testConnection.getStreamContext(NatsSubjects.STREAM_SCHEDULES_DLQ);
                            ConsumerContext consumerContext =
                                    dlqStream.createOrUpdateConsumer(
                                            ConsumerConfiguration.builder()
                                                    .filterSubject(dlqSubject)
                                                    .ackPolicy(AckPolicy.Explicit)
                                                    .build());
                            Message msg = consumerContext.next(Duration.ofSeconds(2));
                            assertThat(msg).as("dlq message on " + dlqSubject).isNotNull();
                            found.set(msg);
                        });
        found.get().ack();
        return found.get();
    }

    private String headerValue(Message message, String headerName) {
        String value = message.getHeaders().getFirst(headerName);
        assertThat(value).as("header " + headerName).isNotNull();
        return value;
    }

    private void send(String subject, String json) throws Exception {
        testJetStream.publish(subject, json.getBytes(StandardCharsets.UTF_8));
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
