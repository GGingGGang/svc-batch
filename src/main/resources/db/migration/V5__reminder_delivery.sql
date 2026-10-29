ALTER TABLE reminder_dispatch
  ADD COLUMN timezone VARCHAR(64) NOT NULL DEFAULT 'UTC',
  ADD COLUMN all_day TINYINT(1) NOT NULL DEFAULT 0,
  ADD COLUMN start_date DATE NULL,
  ADD COLUMN end_date DATE NULL,
  ADD COLUMN schedule_revision BIGINT NULL,
  ADD COLUMN next_attempt_at DATETIME(3) NULL,
  MODIFY COLUMN status ENUM('pending','dispatching','sent','unknown','skipped','failed') NOT NULL DEFAULT 'pending';
ALTER TABLE daily_schedule_stats ADD COLUMN reminders_unknown INT NOT NULL DEFAULT 0;

CREATE TABLE reminder_attempt (
  dispatch_id BINARY(16) NOT NULL,
  attempt INT NOT NULL,
  user_id BINARY(16) NOT NULL,
  schedule_id BINARY(16) NOT NULL,
  channel ENUM('push','email') NOT NULL,
  status ENUM('queued','published','accepted','retryable_failure','permanent_failure',
              'unknown','cancelled','skipped') NOT NULL,
  payload JSON NOT NULL,
  reason_code VARCHAR(64) NULL,
  provider_receipt_id VARCHAR(255) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  result_at DATETIME(3) NULL,
  PRIMARY KEY (dispatch_id, attempt),
  KEY idx_attempt_user_created (user_id, created_at),
  KEY idx_attempt_outbox (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE deleted_account_tombstone (
  user_hash BINARY(32) NOT NULL,
  event_id BINARY(16) NOT NULL,
  deleted_at DATETIME(3) NOT NULL,
  PRIMARY KEY (user_hash),
  UNIQUE KEY uq_deletion_event (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE account_deletion_processed (
  event_id BINARY(16) NOT NULL,
  request_id BINARY(16) NOT NULL,
  user_hash BINARY(32) NOT NULL,
  processed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (event_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE reminder_ops_audit (
  id BINARY(16) NOT NULL,
  dispatch_id BINARY(16) NOT NULL,
  actor VARCHAR(128) NOT NULL,
  action VARCHAR(32) NOT NULL,
  reason VARCHAR(500) NOT NULL,
  result VARCHAR(32) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_ops_dispatch_created (dispatch_id, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
