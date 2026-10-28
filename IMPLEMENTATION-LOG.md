# svc-batch 구현 기록

## 이번 변경

- `REM-008`: tentative 상태 일정의 pending 리마인더를 `skipped` 처리하고 skipped 통계에 반영한다. confirmed 이벤트가 오면 미래 예정 알림을 다시 활성화한다. 실제 외부 발송은 계속 비활성이다.
- `graphify-out/MSA-AUDIT.md` #4 (P1): consumer가 NATS `created`/`updated` subject를 reconcile 내부에 전달한다. `schedule_event_state.creation_accounted` 마커와 `created` subject를 사용해 projection stale/deleted 가드와 생성 사실 통계를 분리했다. deleted-first 뒤 created도 통계만 한 번 기록하고 tombstone은 유지한다. 준비 검사는 새 컬럼도 확인한다.
- V4 migration은 기존 state를 `DEFAULT 1`로 legacy-accounted 처리하고 새 state insert는 0으로 시작한다. 과거 오집계의 원인을 복원할 정보가 없으므로 기존 일별 통계를 재계산하거나 교정하지 않는다.
- 통합 테스트에 tentative → confirmed와 created→updated, updated(day 2)→created(day 1), deleted→created, 중복 created, migration 기존 행 보존 시나리오를 추가했다. NATS 통합 경로의 subject 전달도 검증하도록 했다.

## 확인하지 못한 항목

- **정확한 과거 생성 통계 복구:** 새 이벤트부터 created subject의 `occurred_at` 기준으로 집계한다. 이미 누적된 날짜별 수치는 과거 이벤트 원인과 원 생성일을 알 수 없어 보정하지 않았다.
- **리마인더 외부 발송 재시도 (`REM-003`, `REM-007`, `REM-009`, `POL-23`):** 현재 발송 연결 자체가 비활성이라 채널 동의 확인, 발송 직전 최신 상태 확인, 외부 실패 최대 3회 재시도는 구현할 실행 경로가 없다. 실제 전송을 성공으로 가장하지 않았다.
- **재처리 운영도구 (`OPS-005/006`):** DLQ 실패 보존과 durable 재시작은 이미 구현되어 있지만 권한·감사기록이 있는 선택 재처리/백필 도구는 별도 운영 계약이 필요하다. 이번 변경에서 추가하지 않았다.

## 검증

- JDK `C:/Users/kgh_4/.tools/jdk-21.0.11+10` 및 Gradle `C:/Users/kgh_4/.tools/gradle-8.12`로 `test`, `checkstyleMain`, `checkstyleTest`, `spotlessCheck` 통과(유닛 15개).
- 첫 검증에서 수정한 회귀: 기존 Mockito 테스트가 새 상태/마커 DB 갱신 수를 1회로 가정했으며, pending→skipped SQL을 직접 검증하도록 수정했다. 기존 OTel 테스트는 라이브러리 description 구분자를 `=`로 기대해 실패해 실제 `:` 형식으로 고쳤다.
- `git diff --check` 통과.
- Docker 실행 파일이 없어 Testcontainers 통합 테스트는 미실행이다.
- 원격 호출·배포·commit·push는 하지 않았다.
