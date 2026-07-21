> 이 애플리케이션 레포지토리는 AI 코드 에이전트가 구현했습니다.

# svc-batch

MSA 의 비동기 백본 — 일정 이벤트 consume / 리마인더 판정 / 통계 집계.
Java 21 / Spring Boot / Gradle. k8s 매니페스트는 [k8s-gitops](https://github.com/GGingGGang/k8s-gitops) 레포의 `manifests/batch/` 소유 (본 레포는 코드 + Dockerfile + Jenkinsfile).
Kafka consumer / 리마인더 스캔 잡 로직은 아직 미구현 — 현재는 probe/metrics + Flyway 스키마(`V1__init.sql`) 단계.

## Ports

| Port | Purpose |
|------|---------|
| `8080` | probe + `/metrics` (외부 HTTP 노출 없음) |

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/healthz` | Liveness probe → `{"status":"ok"}` |
| GET | `/readyz` | Readiness probe → `{"status":"ready"}` |
| GET | `/metrics` | Prometheus 스크랩 엔드포인트 |

## Environment Variables

```bash
HTTP_PORT=8080                              # listen port (기본 8080)
JAVA_TOOL_OPTIONS=-XX:MaxRAMPercentage=75.0 # 컨테이너 힙 비율 (deployment 에서 주입)
APP_VERSION=<GIT_SHA>                       # Dockerfile 이 주입 (기본 dev)

DB_HOST=              # required
DB_USER=              # required
DB_PASSWORD=          # required, no default — never commit
DB_NAME=batch         # default batch

REDIS_ADDR=           # required, host:port
REDIS_DB=2            # default 2 (ShedLock)
```

## Database

Flyway (`src/main/resources/db/migration/V1__init.sql`) 가 기동 시 자동으로 schema `batch` 에 적용됨.
Spring Batch 자체 메타데이터 테이블은 `spring.batch.jdbc.table-prefix=batch_meta.BATCH_` 로 같은 커넥션/유저(`app_batch`, 양 스키마 GRANT 보유)를 통해 `batch_meta` 스키마로 분리 — 별도 datasource 없음.

## Build

```bash
gradle -q bootJar                    # build/libs/svc-batch.jar
java -jar build/libs/svc-batch.jar   # :8080 — 기동에 위 DB_* / REDIS_ADDR env 필요 (Flyway 가 시작 시 실행)
```

CI: Jenkins(`services` org folder) → Kaniko → GHCR → deployBump → ArgoCD.
