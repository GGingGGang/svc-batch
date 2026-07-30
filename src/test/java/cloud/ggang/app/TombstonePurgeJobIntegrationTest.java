package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
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

// tombstone purge 검증(PLAN.md §6, 5M DoD "purge 동작") — retention(14d) 경계와 is_deleted=0 행
// 보호, ShedLock 의 replica 간 중복 실행 방지를 ReminderScanJobIntegrationTest 와 동일한 패턴으로 확인.
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Tag("integration")
class TombstonePurgeJobIntegrationTest {

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
        // @Scheduled 의 기동 시 즉시 1회 발화가 테스트의 수동 purge()/purgeOnce() 호출과 경합하지
        // 않도록 초기 지연 + 반복 주기를 테스트 시간보다 넉넉히 늦춘다 (ReminderScanJobIntegrationTest 동형).
        registry.add("app.tombstone-purge.initial-delay-ms", () -> "3600000");
        registry.add("app.tombstone-purge.interval-ms", () -> "3600000");
    }

    @Autowired private JdbcTemplate jdbcTemplate;
    @Autowired private TombstonePurgeJob tombstonePurgeJob;
    @Autowired private LockProvider lockProvider;

    @BeforeEach
    void cleanState() {
        jdbcTemplate.update("DELETE FROM schedule_event_state");
    }

    @Test
    void oldTombstoneIsPurged() {
        byte[] scheduleId = insertState(true, "-20");

        int deleted = tombstonePurgeJob.purgeOnce();

        assertThat(deleted).isEqualTo(1);
        assertThat(exists(scheduleId)).isFalse();
    }

    @Test
    void tombstoneWithinRetentionIsKept() {
        byte[] scheduleId = insertState(true, "-5");

        int deleted = tombstonePurgeJob.purgeOnce();

        assertThat(deleted).isEqualTo(0);
        assertThat(exists(scheduleId)).isTrue();
    }

    @Test
    void nonDeletedRowIsNeverPurgedRegardlessOfAge() {
        byte[] scheduleId = insertState(false, "-30");

        int deleted = tombstonePurgeJob.purgeOnce();

        assertThat(deleted).isEqualTo(0);
        assertThat(exists(scheduleId)).isTrue();
    }

    @Test
    void shedLockBlocksConcurrentPurgeExecution() {
        byte[] scheduleId = insertState(true, "-20");

        // lockAtLeastFor=ZERO — unlock() 이 즉시 실제로 풀려야 두 번째 purge() 호출이 막히지 않는다.
        Optional<SimpleLock> heldByOtherReplica =
                lockProvider.lock(
                        new LockConfiguration(
                                Instant.now(),
                                "batch:lock:tombstone-purge",
                                Duration.ofMinutes(10),
                                Duration.ZERO));
        assertThat(heldByOtherReplica).isPresent();

        tombstonePurgeJob.purge();
        assertThat(exists(scheduleId)).isTrue();

        heldByOtherReplica.get().unlock();

        tombstonePurgeJob.purge();
        assertThat(exists(scheduleId)).isFalse();
    }

    private byte[] insertState(boolean isDeleted, String daysOffset) {
        byte[] scheduleId = UuidBytes.toBytes(UUID.randomUUID());
        jdbcTemplate.update(
                "INSERT INTO schedule_event_state (schedule_id, user_id, last_event_at, is_deleted, updated_at) "
                        + "VALUES (?, ?, UTC_TIMESTAMP(3), ?, UTC_TIMESTAMP(3) + INTERVAL "
                        + daysOffset
                        + " DAY)",
                scheduleId,
                UuidBytes.toBytes(UUID.randomUUID()),
                isDeleted);
        return scheduleId;
    }

    private boolean exists(byte[] scheduleId) {
        Integer count =
                jdbcTemplate.queryForObject(
                        "SELECT COUNT(*) FROM schedule_event_state WHERE schedule_id = ?",
                        Integer.class,
                        scheduleId);
        return count != null && count > 0;
    }
}
