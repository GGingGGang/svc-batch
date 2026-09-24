package cloud.ggang.app;

import java.util.Map;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// hello 서버까지 — 도메인 라우트는 생성된 서비스가 직접 추가.
@RestController
public class HealthController {

    private final JdbcTemplate jdbcTemplate;
    private final RedisConnectionFactory redisConnectionFactory;
    private final NatsConnectionHolder natsConnectionHolder;
    private final NatsStreamBootstrap natsStreamBootstrap;

    public HealthController(JdbcTemplate jdbcTemplate, RedisConnectionFactory redisConnectionFactory,
            NatsConnectionHolder natsConnectionHolder, NatsStreamBootstrap natsStreamBootstrap) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisConnectionFactory = redisConnectionFactory;
        this.natsConnectionHolder = natsConnectionHolder;
        this.natsStreamBootstrap = natsStreamBootstrap;
    }

    @GetMapping("/healthz")
    public Map<String, String> healthz() {
        return Map.of("status", "ok");
    }

    @GetMapping("/readyz")
    public ResponseEntity<Map<String, String>> readyz() {
        boolean database = available(() -> jdbcTemplate.queryForObject("SELECT 1", Integer.class));
        boolean redis = available(() -> {
            try (var connection = redisConnectionFactory.getConnection()) {
                connection.ping();
            }
        });
        boolean events = natsConnectionHolder.isConnected() && natsStreamBootstrap.isStarted();
        Map<String, String> body = Map.of(
                "status", database && redis && events ? "ready" : "not_ready",
                "database", database ? "ready" : "unavailable",
                "redis", redis ? "ready" : "unavailable",
                "events", events ? "ready" : "unavailable");
        return database && redis && events ? ResponseEntity.ok(body) : ResponseEntity.status(503).body(body);
    }

    private boolean available(Runnable check) {
        try {
            check.run();
            return true;
        } catch (RuntimeException ex) {
            return false;
        }
    }
}
