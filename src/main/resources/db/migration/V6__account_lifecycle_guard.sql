CREATE TABLE account_lifecycle_guard (
  user_hash BINARY(32) NOT NULL,
  is_deleted TINYINT(1) NOT NULL DEFAULT 0,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (user_hash)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO account_lifecycle_guard(user_hash,is_deleted)
SELECT UNHEX(SHA2(user_id,256)),0 FROM schedule_event_state
UNION
SELECT UNHEX(SHA2(user_id,256)),0 FROM reminder_dispatch;

INSERT INTO account_lifecycle_guard(user_hash,is_deleted)
SELECT user_hash,1 FROM deleted_account_tombstone
ON DUPLICATE KEY UPDATE is_deleted=1;
