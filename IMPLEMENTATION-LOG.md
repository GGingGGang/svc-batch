# svc-batch 구현 기록

## 구현 범위

- REM-004/005/006/007/008/009/011/012: 과거 새 알림은 생성 시 건너뛰고, 기존 알림은 예정 시각 후 30분까지만 재개한다. 설정 5개 이하와 채널·분 단위 값을 검증한다. 일정 시간이 바뀌어 `remind_at` 세대가 바뀌면 새 `dispatch_id`를 만들고, 제목만 바뀌어 예정 시각이 같으면 기존 성공 상태를 유지해 중복 발송하지 않는다. 취소·삭제·리마인더 제거는 pending/dispatching 작업을 취소한다. 종일 일정은 IANA 시간대의 시작 날짜 09:00을 알림 기준으로 계산한다.
- REM-003/NTF-003~007/POL-23: 배치가 APP_REMINDERS JetStream durable due/result 흐름을 소유한다. due 이벤트에 민감한 설명/원문/주소를 넣지 않는다. `reminder_attempt` outbox가 `(dispatch_id, attempt)`를 저장하고 JetStream PubAck 뒤 게시 완료를 기록한다. 최대 3회, 1분/5분 대기 재시도만 허용하고, `unknown`은 자동 재전송하지 않는다. notify가 채널 동의·제공자 응답·실제 접수를 관리하며 provider 설정은 기본 비활성이다.
- REM-007/OPS-005: notify용 batch 내부 preflight `GET /internal/reminders/{dispatchId}/validity?attempt=N`를 추가했다. 공유 `INTERNAL_SERVICE_TOKEN`을 상수시간 비교하고, 현재 dispatch generation/attempt와 schedule/account tombstone 상태를 확인한다. false 응답은 상세 데이터 없이 반환한다. Core authoritative validity를 함께 확인하는 흐름은 notify와 합의했다.
- REL-015/계정 삭제: `ACCOUNT_LIFECYCLE` durable consumer가 user의 알림·시도·projection을 지우고 익명 해시 tombstone을 남긴 뒤 결과를 발행한다. `account_lifecycle_guard` 행 잠금으로 삭제와 지연 schedule event가 경쟁해 데이터를 재생성하지 못하게 한다. 기존 사용자용 guard는 V6에서 초기화한다. 글로벌 익명 통계는 사용자 식별 정보가 없어 보존한다.
- OPS-004/005/006: `/internal/ops/reminders`는 실패/unknown 작업의 비민감 필드만 조회한다. 재처리는 이유, 고정 설정 actor/token, 현재 Core/Auth 유효성, 30분 기한을 요구하고 audit에 actor/action/reason/result를 남긴다. secret 미설정이면 503으로 닫힌다. 계정 정지/해제는 auth 소유 API다.
- V5는 reminder delivery/attempt/outbox/account/audit schema를 추가한다. V6는 계정 삭제와 schedule event 재생성 경쟁을 차단하는 guard를 추가한다. V4의 creation marker와 레거시 집계 원칙은 유지한다. 과거 잘못된 created 통계는 이벤트 원인이 보존되지 않아 자동 보정하지 않는다.
- 이번 CI 회귀 검토에서 lifecycle로 취소된 동일 `remind_at` 행을 그대로 재사용하던 결함을 찾았다. 취소/보류/리마인더 제거/일정 삭제 표식을 `last_error`에 구분해 남기고, 같은 설정이 다시 활성화될 때 새 `dispatch_id`와 pending 상태로 시작하도록 고쳤다. 일반 notify `skipped`나 이미 지난 알림 결과를 제목 변경만으로 다시 열지는 않는다. 기존 통합 테스트에 cancel→confirm 세대 ID 변경 단언을 추가했다.
- tentative 첫 이벤트에서 projection은 생성하지 않고 바로 반환해 기존 integration assertion이 행을 조회하다 `EmptyResult`를 냈다. tentative reminders를 skipped projection으로 저장한 뒤 confirmed 전환 시 새 generation으로 다시 여는 방식으로 수정했다. scan에서 30분 grace 만료도 `grace_expired` reason code로 남긴다.
- 배포 로그로는 기존 integration 7/17 실패의 원인을 확정할 수 없었다. 이전 bootstrap은 실패 원인을 예외 종류만 남겨 숨겼으므로, NATS stream/consumer bootstrap 실패 시 예외 stack을 남겨 다음 CI에서 실제 server/consumer 오류를 확인할 수 있게 했다. 통합 테스트 시간 초과를 늘리거나 건너뛰지 않았다.
- CI run `40834284313` artifact/XML 확인 결과 integration 17개 전부가 테스트 로직 진입 전에 동일하게 실패했다. Spring이 `ReminderScanJob`의 운영 5-인자 생성자와 테스트 3-인자 생성자 중 주입 대상을 고르지 못해 `No default constructor found`를 냈다. 운영 생성자에 `@Autowired`를 추가해 원인을 수정했다. 따라서 이 run은 NATS consumer/DLQ 실행 결과를 검증하지 못한다.
- 후속 CI `40834707652`에서는 컨텍스트와 NATS durable startup이 성공했다. 남은 schedule create/event 처리 테스트는 MySQL의 upsert 구문에서 `schedules_created` target column이 `VALUES ... AS new` alias와 겹쳐 ambiguous가 되는 구체 오류로 실패했다. target table을 qualify했다. ReminderScanJob grace 테스트는 예상대로 dispatching으로 바뀐 뒤, 존재하지 않는 daily stats row를 0으로 읽는 helper가 `EmptyResultDataAccessException`을 냈으므로 helper를 scalar `COALESCE`로 바꿨다. 해당 CI에서는 malformed DLQ timeout도 남았으므로 SQL 수정 이후 재실행으로 검증해야 한다.
- Core/notify 계약 대조: due는 `app.reminders.due.v1`이며 `dispatch_id`, `attempt`, `user_id`, `schedule_id`, `title`, `start_at`, `remind_at`, `minutes_before`, `channel`, `timezone`, `all_day`, `start_date`, `end_date`, `schedule_revision`을 담는다. Core validity는 schedule/user/revision/minutes/channel/remind_at을 받으며, batch validity는 dispatch ID와 attempt로 현재 generation 및 활성 계정을 재확인한다. 이메일/설명 원문/주소는 이벤트에 넣지 않는다. 30분 grace와 최대 3회(+1m/+5m) 정책을 유지한다.

## 요구사항 연계 및 남은 작업

- Batch 소유 구현: REM-004/005/006/007/008/009/011/012, POL-22/23, REL-015 삭제 후처리, OPS-004/005/006 재처리 경로.
- NTF-008 batch ledger API: `GET /internal/users/{userId}/reminders?from=<RFC3339>&to=<RFC3339>&limit=100`을 추가했다. 기간 30일 이하, limit 1..100, 내부 bearer 인증, user_id 및 scheduled_at 범위로 DB를 제한하고 `reminders` wrapper에 비민감 필드만 반환한다. notify는 이 ledger와 provider 결과를 merge한다.
- notify/core 연계가 있어야 완성: REM-003, NTF-003/005/006/007, schedule/user 최신상태 검증. Core·notify 구현 후 서비스 간 계약을 실제로 붙여야 한다. SMTP/Telegram 자격증명이 없어 실제 발송은 비활성으로 유지한다.
- 다른 소유 영역: NTF-001/002/004/008/009/010 중 목적지·동의·테스트 알림·DND와 history API merge는 notify/web 소유다. Batch는 NTF-008의 durable scheduled ledger endpoint만 제공하며 수신 주소나 채널 자격증명을 저장하지 않는다.
- 운영환경에서 확인할 것: `OPERATOR_TOKENS_JSON`, `INTERNAL_SERVICE_TOKEN`, `CORE_INTERNAL_URL`, `AUTH_ACCOUNT_STATUS_URL` 설정과 내부 네트워크 ACL, NATS stream 권한 및 consumer startup. 재처리 route는 모두 유효성 API 호출에 fail-closed다.
- 복구 시험(백업 복원/장애 주입)은 실행하지 않았다. Docker가 없어 Testcontainers 통합 테스트도 실행하지 못했다. 사용자 과거 통계 보정은 하지 않는다.

## 검증

- JDK 21 `C:/Users/kgh_4/.tools/jdk-21.0.11+10`, Gradle 8.12 `C:/Users/kgh_4/.tools/gradle-8.12`.
- JDK 21 + Gradle 8.12 `--no-daemon test`: 수정 후 성공. Docker 기반 `integrationTest`는 미실행(로컬에 Docker 실행 파일/daemon 없음); cancel→confirm 신규 단언은 테스트 소스에 추가했으나 실행되지 않았다.
- `.github/workflows/test.yml`에 항상 Gradle XML/HTML report를 업로드하도록 추가해 CI 통합 실패 자료를 보존한다.
- 외부 발송, 배포, 원격 호출, commit/push는 하지 않았다.
