> 이 애플리케이션 레포지토리는 AI 코드 에이전트가 구현했습니다.

# svc-batch

MSA 의 비동기 백본 — 일정 이벤트 consume / 리마인더 판정 / 통계 집계.
Java 21 / Spring Boot / Gradle. k8s 매니페스트는 [k8s-gitops](https://github.com/GGingGGang/k8s-gitops) 레포의 `manifests/batch/` 소유 (본 레포는 코드 + Dockerfile + Jenkinsfile).
Kafka consumer / 리마인더 스캔 잡 로직은 아직 미구현 — 현재는 probe/metrics + Flyway 스키마 + Spring Batch 메타 배선 단계 (testcontainers 통합 테스트로 검증).

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

Flyway (`src/main/resources/db/migration/`) 가 기동 시 자동으로 적용됨.
- `V1__init.sql` — schema `batch` (`reminder_dispatch`, `schedule_event_state`, `daily_schedule_stats`).
- `V2__batch_meta_schema.sql` — schema `batch_meta` (Spring Batch 메타 테이블, spring-batch-core 5.2.1 `schema-mysql.sql` 원본에 스키마 한정자만 추가).

실제 datasource 는 여전히 `batch` 하나 — 같은 커넥션/유저(`app_batch`, `batch`/`batch_meta` 양쪽 GRANT 보유)로 cross-schema DDL/DML 을 태운다. `spring.batch.jdbc.table-prefix=batch_meta.BATCH_` 로 런타임 JobRepository 쿼리는 `batch_meta` 를 정상적으로 찾아가지만, Boot 의 `spring.batch.jdbc.initialize-schema` 자동 초기화는 DDL 단계에서 prefix 의 스키마 한정자를 반영하지 못하고 `BATCH_*` 테이블을 `batch` 스키마에 그대로 만들어버리는 문제가 있어(testcontainers 통합 테스트로 재현 확인) `initialize-schema: never` 로 끄고 `V2__batch_meta_schema.sql` 로 대체했다.

`BatchMetaSchemaIntegrationTest`(testcontainers MySQL)가 컨텍스트 기동 + 두 스키마 마이그레이션 성공 + `BATCH_*` 테이블이 `batch`로 새지 않는지를 검증한다.

## Build

```bash
gradle -q bootJar                    # build/libs/svc-batch.jar
java -jar build/libs/svc-batch.jar   # :8080 — 기동에 위 DB_* / REDIS_ADDR env 필요 (Flyway 가 시작 시 실행)
```

CI: Jenkins(`services` org folder) → Kaniko → GHCR → deployBump → ArgoCD.
