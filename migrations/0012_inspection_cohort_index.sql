-- Issue #143: the `/usage` inspection cohort is a bounded time window over
-- parent task turns. Give that predicate an index so the cohort stays a range
-- scan rather than a whole-table scan with a JSON decode per matched row.
CREATE INDEX idx_usage_turns_inspection_cohort
  ON usage_turns(created_at_ms)
  WHERE inspection_batch_json IS NOT NULL AND lane_kind = 'parent';
