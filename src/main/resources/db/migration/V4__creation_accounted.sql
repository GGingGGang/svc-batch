-- Existing projection rows may already have contributed to the legacy first-observed statistic.
-- Keep them accounted to avoid duplicating history; new rows explicitly start at 0.
ALTER TABLE schedule_event_state
  ADD COLUMN creation_accounted TINYINT(1) NOT NULL DEFAULT 1;
