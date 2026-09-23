> 이 애플리케이션 레포지토리는 AI 코드 에이전트가 구현했습니다.

# svc-batch

MSA 의 비동기 백본 — 일정 이벤트 consume / 리마인더 판정 / 통계 집계.
Java 21 / Spring Boot / Gradle. k8s 매니페스트는 [k8s-gitops](https://github.com/GGingGGang/k8s-gitops) 레포의 `manifests/batch/` 소유 (본 레포는 코드 + Dockerfile + Jenkinsfile).
NATS consumer(reconcile + stats 증분) + 리마인더 스캔 잡(ShedLock, dry run) + tombstone purge 잡(ShedLock) + OTel 최소까지 구현 완료 — testcontainers(공식 nats 이미지 / MySQL / Redis)로 중복·역순·삭제·스캔 락·purge 락 시나리오 검증.

## NATS Consumer

`spring-kafka` 급 Spring 통합이 없어 `io.nats:jnats` 를 직접 사용(consumer 루프·재시도·DLQ 라우팅 자작) — 인프라 결정은 `Private-docs/decision/2026-07-10.md`.

- durable pull consumer: `batch-reminders`(stream `APP_SCHEDULES`), manual ack — DB 트랜잭션 커밋 후에만 `msg.ack()`.
- subject: `app.schedules.created.v1` / `app.schedules.updated.v1`(단일 upsert 핸들러) / `app.schedules.deleted.v1`.
- 연결: `NATS_URL` 이 닿지 않아도 앱은 정상 기동(`Nats.connectAsynchronously` 로 접속을 백그라운드 스레드에 위임) — 연결 성공(CONNECTED/RECONNECTED) 시점에 stream/durable consumer 를 기동.
- reconcile 알고리즘(중복/역순 방어, tombstone terminal, stats 증분)은 `PLAN.md` §4 참조.
- 실패 처리: 역직렬화 실패는 즉시, 그 외 처리 실패는 3회 재시도 후 `app.schedules.dlq.<event>`(stream `APP_SCHEDULES_DLQ`, batch 소유)로 원본 그대로 전달
  (header `x-original-subject` / `x-failure-reason` / `x-failure-ts`) 하고 서버 확인 후 ack. DLQ 발행 실패 시 원본을 ack하지 않아 30초 뒤 재전달받는다. 헤더 값은 개행/제어문자를 제거한다.
- 실제 알림 발송은 비활성이다. 실클러스터의 core→NATS→batch 연결은 별도 확인이 필요하다.

## 리마인더 스캔 잡

- `ReminderScanJob` — `@Scheduled`(기본 60초 주기, `REMINDER_SCAN_INTERVAL_MS`) + ShedLock(`@SchedulerLock("batch:lock:reminder-scan")`, Redis DB2, `SchedulingConfig`) 로 replica 간 중복 실행을 막는다.
- 1 트랜잭션에서 `SELECT ... WHERE status='pending' AND remind_at <= UTC_TIMESTAMP(3) ORDER BY remind_at LIMIT 100 FOR UPDATE SKIP LOCKED` 로 최대 100건을 잠근다.
- 발송 비활성 데모에서는 도래한 작업을 `skipped`로 마감하고 `reminders_skipped`만 증분한다. `sent_at`과 `reminders_sent`는 실제 발송 성공 전까지 변경하지 않는다.

## Tombstone Purge

- `TombstonePurgeJob` — `@Scheduled`(기본 7일 주기, `TOMBSTONE_PURGE_INTERVAL_MS`) + ShedLock(`@SchedulerLock("batch:lock:tombstone-purge")`, Redis DB2, `SchedulingConfig`)로 replica 간 중복 실행을 막는다 — 리마인더 스캔 잡과 동일한 락 메커니즘.
- `DELETE FROM schedule_event_state WHERE is_deleted=1 AND updated_at < UTC_TIMESTAMP() - INTERVAL 14 DAY` 단일 문(PLAN.md §6). retention 14일은 core 가 선언하는 NATS stream 의 `max_age`(7일, 전체문서 §7.2)보다 길게 잡아, 그 사이 재전달된 오래된 이벤트도 여전히 `is_deleted=1` tombstone 행을 만나 §4.2 의 terminal 가드로 무시되도록 보장한다.
- `is_deleted=0` 행은 나이와 무관하게 절대 삭제되지 않는다 — 삭제 대상은 tombstone(이미 삭제 처리된 일정의 상태 행)뿐이며, 살아있는 일정의 이력 행이 아니다.

## Observability

- `/metrics` 는 앱 단일 포트(actuator `management.endpoints.web.path-mapping.prometheus=metrics`) — 별도 포트 없음.
- RED: `/healthz`·`/readyz` 는 actuator 자동계측(`http_server_requests_seconds_*`)으로 커버.
- 도메인 카운터: `reminders_scanned_total` / `reminders_skipped_total`, `reminder_scan_duration_seconds`(Timer), `reminder_scan_runs_total{result}`, `schedule_events_consumed_total{subject}` / `schedule_events_dlq_total{subject}` / `schedule_events_dlq_failures_total{subject}` / `schedule_events_dlq_last_failure_epoch_seconds`, `tombstone_purge_runs_total` / `tombstone_purged_total`.

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

NATS_URL=             # default nats://localhost:4222 (local dev) — 운영은 항상 명시 주입

REMINDER_SCAN_INTERVAL_MS=   # default 60000 — 리마인더 스캔 잡 주기(ms)
TOMBSTONE_PURGE_INTERVAL_MS= # default 604800000(7d) — tombstone purge 잡 주기(ms)
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

## Test

`../test-contract.md` §3 의 java 계약 — 유닛(Docker 없음, Jenkins 게이트)과 통합(testcontainers, 이 레포의 GHA)을 클래스 단위 `@Tag("integration")` 로 분리:

```bash
gradle test             # 유닛만(태그 없음) — Jenkins 가 gradle --no-daemon test 로 실행
gradle integrationTest  # 통합만(@Tag("integration")) — Docker 필요, testcontainers MySQL/Redis/NATS 자동 기동
```

4개 통합 테스트 클래스(`BatchMetaSchemaIntegrationTest`, `ReminderScanJobIntegrationTest`, `ScheduleEventConsumerIntegrationTest`, `TombstonePurgeJobIntegrationTest`)는 `@Tag("integration")`로 분리되어 있다. DLQ 확인·재전달과 발송 비활성 동작은 단위 테스트에서도 검증한다.

`TombstonePurgeJobIntegrationTest`는 retention(14일) 경계(오래된 tombstone 삭제·최근 tombstone 보존)와 `is_deleted=0` 행이 나이와 무관하게 보호되는지, ShedLock 이 다른 replica 점유 시 실행을 막는지를 `ReminderScanJobIntegrationTest`와 동일한 패턴(수동 락 선점)으로 검증한다.

`.github/workflows/test.yml` 이 push(main)/PR 마다 `gradle test` + `gradle integrationTest` 풀 스위트를 실행한다.

CI: Jenkins(`services` org folder, 유닛 게이트) → Kaniko → GHCR → deployBump → ArgoCD. 통합은 이 레포 GHA(병렬, 게이트 아님 — 상세는 `test-contract.md` §4).
