-- Issue #146: a task run's elapsed time is not one number. Model execution,
-- active tool processes, build queues, permission prompts, user questions, and
-- child agents are accounted separately, so a budget can consume work without
-- charging the run for time it spent waiting.
ALTER TABLE task_runs ADD COLUMN wall_ms INTEGER
  CHECK (wall_ms IS NULL OR wall_ms >= 0);
ALTER TABLE task_runs ADD COLUMN critical_path_ms INTEGER
  CHECK (critical_path_ms IS NULL OR critical_path_ms >= 0);
ALTER TABLE task_runs ADD COLUMN active_compute_ms INTEGER
  CHECK (active_compute_ms IS NULL OR active_compute_ms >= 0);
ALTER TABLE task_runs ADD COLUMN wait_ms INTEGER
  CHECK (wait_ms IS NULL OR wait_ms >= 0);
ALTER TABLE task_runs ADD COLUMN timing_policy TEXT
  CHECK (timing_policy IS NULL OR timing_policy IN ('active_compute', 'wall_clock'));

-- Per-category totals are additive: a checkpoint adds to the row instead of
-- replacing it, so a task run resumed in a new process after a crash keeps the
-- interrupted segment's evidence. `open_count` is the live number of intervals
-- that had not closed at the last checkpoint; `recovered_count` records
-- intervals that restart reconciliation had to abandon, whose duration is
-- unknown and deliberately not invented.
CREATE TABLE task_run_timings (
  task_run_id INTEGER NOT NULL REFERENCES task_runs(id) ON DELETE CASCADE,
  category TEXT NOT NULL CHECK (
    category IN (
      'model_request',
      'tool_active',
      'queue',
      'permission_wait',
      'user_wait',
      'child_active',
      'child_wait'
    )
  ),
  lane TEXT NOT NULL CHECK (lane IN ('parent', 'child')),
  summed_ms INTEGER NOT NULL DEFAULT 0 CHECK (summed_ms >= 0),
  interval_count INTEGER NOT NULL DEFAULT 0 CHECK (interval_count >= 0),
  open_count INTEGER NOT NULL DEFAULT 0 CHECK (open_count >= 0),
  recovered_count INTEGER NOT NULL DEFAULT 0 CHECK (recovered_count >= 0),
  latest_owner TEXT CHECK (
    latest_owner IS NULL OR length(latest_owner) <= 200
  ),
  latest_reason TEXT CHECK (
    latest_reason IS NULL OR length(latest_reason) <= 512
  ),
  PRIMARY KEY (task_run_id, category, lane)
);

CREATE INDEX idx_task_run_timings_category ON task_run_timings(category);
