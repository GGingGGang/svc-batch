-- batch schema. user_id = JWT sub — cross-schema FK 없음. 모든 시각 UTC (UTC_TIMESTAMP 비교).

CREATE TABLE reminder_dispatch (
  id             BINARY(16)   NOT NULL,             -- UUIDv7, consumer 생성
  schedule_id    BINARY(16)   NOT NULL,
  user_id        BINARY(16)   NOT NULL,
  title          VARCHAR(255) NOT NULL,             -- 이벤트에서 비정규화
  minutes_before INT          NOT NULL,
  channel        ENUM('push','email') NOT NULL DEFAULT 'push',   -- 'none' 은 consumer 가 필터
  start_at       DATETIME(3)  NOT NULL,
  remind_at      DATETIME(3)  NOT NULL,             -- start_at - minutes_before
  status         ENUM('pending','sent','skipped','failed') NOT NULL DEFAULT 'pending',
  attempt_count  INT          NOT NULL DEFAULT 0,
  last_error     VARCHAR(512) NULL,
  sent_at        DATETIME(3)  NULL,
  created_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_reminder_identity (schedule_id, minutes_before, channel),
  KEY idx_scan (status, remind_at),
  KEY idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 이벤트 중복/역순 가드 + hard-delete tombstone. consumer 트랜잭션 안에서 FOR UPDATE.
CREATE TABLE schedule_event_state (
  schedule_id   BINARY(16)   NOT NULL,
  user_id       BINARY(16)   NOT NULL,
  last_event_at DATETIME(3)  NOT NULL,              -- payload occurred_at
  is_deleted    TINYINT(1)   NOT NULL DEFAULT 0,    -- set 후 해제 없음 (terminal)
  updated_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (schedule_id),
  KEY idx_tombstone (is_deleted, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE daily_schedule_stats (
  stat_date                DATE NOT NULL,           -- UTC 기준
  schedules_created        INT  NOT NULL DEFAULT 0,
  schedules_created_ai     INT  NOT NULL DEFAULT 0,
  schedules_created_manual INT  NOT NULL DEFAULT 0,
  schedules_deleted        INT  NOT NULL DEFAULT 0,
  reminders_sent           INT  NOT NULL DEFAULT 0,
  reminders_skipped        INT  NOT NULL DEFAULT 0,
  reminders_failed         INT  NOT NULL DEFAULT 0,
  updated_at               TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
