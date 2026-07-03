package cloud.ggang.app;

import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

// hello 서버까지 — 도메인 라우트는 생성된 서비스가 직접 추가.
@RestController
public class HealthController {

    @GetMapping("/healthz")
    public Map<String, String> healthz() {
        return Map.of("status", "ok");
    }

    @GetMapping("/readyz")
    public Map<String, String> readyz() {
        return Map.of("status", "ready");
    }
}
