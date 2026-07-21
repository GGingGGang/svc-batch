-- Testcontainers MySQL 은 withDatabaseName 으로 지정한 스키마(batch)만 만들고
-- 그 스키마에 대해서만 앱 유저 권한을 부여한다. batch_meta 는 실 운영 부트스트랩과
-- 동일하게 이 init 스크립트가 root 권한으로 별도 생성 + GRANT 한다.
CREATE DATABASE IF NOT EXISTS batch_meta DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
GRANT ALL PRIVILEGES ON batch_meta.* TO 'app_batch'@'%';
FLUSH PRIVILEGES;
