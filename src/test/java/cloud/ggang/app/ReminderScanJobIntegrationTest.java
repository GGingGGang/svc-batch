package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

// 리마인더 스캔 잡 검증 — grace window 상태 전이, FOR UPDATE SKIP LOCKED 행 단위 경쟁 회피,
// ShedLock 의 replica 간 중복 실행 방지("replica 2 로 중복 발송 0"). dry run 이므로 상태 전이
// (pending -> sent/skipped)까지만 검증하고 실제 알림 채널은 다루지 않는다.
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class ReminderScanJobIntegrationTest {

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
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "spring.datasource.url",
                () -> MYSQL.getJdbcUrl() + "?sslMode=REQUIRED&serverTimezone=UTC&characterEncoding=utf8");
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add(
                "spring.data.redis.url",
                () -> "redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379) + "/2");
        // @Scheduled 기본 동작(기동 시 1회 즉시 발화)이 테스트의 수동 scan()/scanOnce() 호출과
        // 경합하지 않도록 초기 지연 + 반복 주기를 테스트 시간보다 넉넉히 늦춘다.
        registry.add("app.reminder-scan.initial-delay-ms", () -> "3600000");
        registry.add("app.reminder-scan.interval-ms", () -> "3600000");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private DataSource dataSource;
    @Autowired private ReminderScanJob reminderScanJob;
    @Autowired private LockProvider lockProvider;

    // 정적 testcontainers(MySQL) 는 테스트 메서드 간 공유되므로 매 테스트 전 관련 테이블을 비워
    // 순서 무관하게 독립적으로 검증되도록 한다.
    @BeforeEach
    void cleanState() {
        jdbcTemplate.update("DELETE FROM reminder_dispatch");
        jdbcTemplate.update("DELETE FROM daily_schedule_stats");
    }

    @Test
    void dueReminderWithinGraceIsMarkedSentAndStatsIncremented() {
        byte[] id = insertDueReminder(UUID.randomUUID(), UUID.randomUUID(), "5분 지난 리마인더", "-5");

        int processed = reminderScanJob.scanOnce();

        assertThat(processed).isEqualTo(1);
        assertThat(statusOf(id)).isEqualTo("sent");
        assertThat(sentAtOf(id)).isNotNull();
        assertThat(todayStat("reminders_sent")).isGreaterThanOrEqualTo(1);
    }

    @Test
    void dueReminderPastGraceIsMarkedSkippedAndStatsIncremented() {
        byte[] id = insertDueReminder(UUID.randomUUID(), UUID.randomUUID(), "40분 지난 리마인더", "-40");

        int processed = reminderScanJob.scanOnce();

        assertThat(processed).isEqualTo(1);
        assertThat(statusOf(id)).isEqualTo("skipped");
        assertThat(sentAtOf(id)).isNull();
        assertThat(todayStat("reminders_skipped")).isGreaterThanOrEqualTo(1);
    }

    @Test
    void futureReminderIsUntouched() {
        byte[] id = insertReminderWithOffset(UUID.randomUUID(), UUID.randomUUID(), "미래 리마인더", "+10");

        int processed = reminderScanJob.scanOnce();

        assertThat(processed).isEqualTo(0);
        assertThat(statusOf(id)).isEqualTo("pending");
    }

    @Test
    void skipLockedAvoidsLockedRowAndProcessesOthers() throws Exception {
        byte[] lockedId = insertDueReminder(UUID.randomUUID(), UUID.randomUUID(), "잠긴 리마인더", "-5");
        byte[] freeId = insertDueReminder(UUID.randomUUID(), UUID.randomUUID(), "자유 리마인더", "-5");

        try (Connection holder = dataSource.getConnection()) {
            holder.setAutoCommit(false);
            try (PreparedStatement ps =
                    holder.prepareStatement("SELECT id FROM reminder_dispatch WHERE id = ? FOR UPDATE")) {
                ps.setBytes(1, lockedId);
                ps.executeQuery();
            }

            int processed = reminderScanJob.scanOnce();

            assertThat(processed).isEqualTo(1);
            assertThat(statusOf(freeId)).isEqualTo("sent");
            assertThat(statusOf(lockedId)).isEqualTo("pending");

            holder.rollback();
        }

        int processedAfterRelease = reminderScanJob.scanOnce();
        assertThat(processedAfterRelease).isEqualTo(1);
        assertThat(statusOf(lockedId)).isEqualTo("sent");
    }

    @Test
    void shedLockBlocksConcurrentScanExecution() {
        byte[] id = insertDueReminder(UUID.randomUUID(), UUID.randomUUID(), "락 경쟁 리마인더", "-5");

        // lockAtLeastFor=ZERO — unlock() 호출 즉시 실제로 풀려야 두 번째 scan() 호출이 막히지 않는다
        // (lockAtLeastFor > 0 이면 unlock() 이후에도 그 시간까지는 락이 유지되는 ShedLock 표준 동작).
        Optional<SimpleLock> heldByOtherReplica =
                lockProvider.lock(
                        new LockConfiguration(
                                Instant.now(),
                                "batch:lock:reminder-scan",
                                Duration.ofMinutes(2),
                                Duration.ZERO));
        assertThat(heldByOtherReplica).isPresent();

        // 다른 replica 가 락을 쥔 상태 — 이번 scan() 호출은 본문이 실행되지 않아야 한다.
        reminderScanJob.scan();
        assertThat(statusOf(id)).isEqualTo("pending");

        heldByOtherReplica.get().unlock();

        reminderScanJob.scan();
        assertThat(statusOf(id)).isEqualTo("sent");
    }

    private byte[] insertDueReminder(UUID scheduleId, UUID userId, String title, String minutesOffset) {
        return insertReminderWithOffset(scheduleId, userId, title, minutesOffset);
    }

    private byte[] insertReminderWithOffset(
            UUID scheduleId, UUID userId, String title, String minutesOffset) {
        byte[] id = UuidBytes.toBytes(Uuid7Generator.generate());
        jdbcTemplate.update(
                "INSERT INTO reminder_dispatch "
                        + "(id, schedule_id, user_id, title, minutes_before, channel, start_at, remind_at, status) "
                        + "VALUES (?, ?, ?, ?, 10, 'push', "
                        + "UTC_TIMESTAMP(3) + INTERVAL 1 HOUR, "
                        + "UTC_TIMESTAMP(3) + INTERVAL "
                        + minutesOffset
                        + " MINUTE, 'pending')",
                id,
                UuidBytes.toBytes(scheduleId),
                UuidBytes.toBytes(userId),
                title);
        return id;
    }

    private String statusOf(byte[] id) {
        return jdbcTemplate.queryForObject(
                "SELECT status FROM reminder_dispatch WHERE id = ?", String.class, id);
    }

    private Object sentAtOf(byte[] id) {
        return jdbcTemplate.queryForObject(
                "SELECT sent_at FROM reminder_dispatch WHERE id = ?", Object.class, id);
    }

    private int todayStat(String column) {
        Integer value =
                jdbcTemplate.queryForObject(
                        "SELECT " + column + " FROM daily_schedule_stats WHERE stat_date = UTC_DATE()",
                        Integer.class);
        return value == null ? 0 : value;
    }
}
