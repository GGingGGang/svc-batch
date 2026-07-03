package cloud.ggang.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    private static final Logger log = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        // APP_VERSION 은 Dockerfile 이 GIT_SHA 로 주입 (기본 dev)
        String version = System.getenv().getOrDefault("APP_VERSION", "dev");
        log.info("svc-batch {} starting", version);
        SpringApplication.run(Application.class, args);
    }
}
