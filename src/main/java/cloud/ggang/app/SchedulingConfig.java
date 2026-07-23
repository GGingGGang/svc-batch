package cloud.ggang.app;

import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.redis.spring.RedisLockProvider;
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.scheduling.annotation.EnableScheduling;

// 리마인더 스캔 잡의 replica 간 중복 실행을 Redis 기반 분산 락으로 방지한다.
// lockAtMostFor/lockAtLeastFor 는 개별 @SchedulerLock(ReminderScanJob) 에서 지정.
@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT2M")
public class SchedulingConfig {

    @Bean
    public LockProvider lockProvider(RedisConnectionFactory connectionFactory) {
        return new RedisLockProvider(connectionFactory, "svc-batch");
    }
}
