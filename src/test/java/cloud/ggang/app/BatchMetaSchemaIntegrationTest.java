package cloud.ggang.app;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

// batch.jdbc.table-prefix=batch_meta.BATCH_ 로 스키마를 분리하는 배선이 실제로 동작하는지 검증.
// 앱 유저는 batch 스키마 권한만 갖고, batch_meta 는 별도 부여 — 운영 부트스트랩과 동일 모델.
// withInitScript() 는 앱 유저(권한 제한) 커넥션으로 실행되어 CREATE DATABASE 가 막히므로,
// MySQL 이미지가 root 권한으로 실행해주는 docker-entrypoint-initdb.d 컨벤션을 사용한다.
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class BatchMetaSchemaIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                    .withDatabaseName("batch")
                    .withUsername("app_batch")
                    .withPassword("app_batch_pw")
                    .withCopyFileToContainer(
                            MountableFile.forClasspathResource("db/testcontainers-init.sql"),
                            "/docker-entrypoint-initdb.d/01-batch-meta.sql");

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add(
                "spring.datasource.url",
                () -> MYSQL.getJdbcUrl() + "?sslMode=REQUIRED&serverTimezone=UTC&characterEncoding=utf8");
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        // 실 Redis 없이 컨텍스트만 검증 — 이 테스트는 아무 커맨드도 실행하지 않으므로 lazy 연결로 충분.
        registry.add("spring.data.redis.url", () -> "redis://localhost:6379/2");
    }

    @Autowired private DataSource dataSource;

    @Test
    void flywayMigratesBatchSchemaAndSpringBatchMetaTablesExistUnderBatchMeta() throws Exception {
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            assertTableExists(st, "batch", "reminder_dispatch");
            assertTableExists(st, "batch", "schedule_event_state");
            assertTableExists(st, "batch", "daily_schedule_stats");
            assertTableExists(st, "batch_meta", "BATCH_JOB_INSTANCE");
            assertTableExists(st, "batch_meta", "BATCH_STEP_EXECUTION");
            // 회귀 가드: Spring Batch 메타 테이블이 batch 스키마로 잘못 새어나가지 않는지 확인.
            assertTableCount(st, "batch", "BATCH_JOB_INSTANCE", 0);
        }
    }

    private void assertTableCount(Statement st, String schema, String table, int expected)
            throws Exception {
        String sql =
                "SELECT COUNT(*) FROM information_schema.tables "
                        + "WHERE table_schema='"
                        + schema
                        + "' AND table_name='"
                        + table
                        + "'";
        try (ResultSet rs = st.executeQuery(sql)) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getInt(1)).isEqualTo(expected);
        }
    }

    private void assertTableExists(Statement st, String schema, String table) throws Exception {
        assertTableCount(st, schema, table, 1);
    }
}
