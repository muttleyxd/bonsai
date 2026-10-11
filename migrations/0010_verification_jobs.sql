-- Cross-process verification scheduler (#142).
--
-- One row per verification attempt. `job_key` is the exact identity of the
-- requested check (workspace identity + normalized command/cwd + toolchain +
-- effective environment + resolved target directory + execution policy);
-- `lane_key` is the weaker compatibility key that serializes Cargo jobs
-- sharing one target directory even when their result keys differ.

CREATE TABLE verification_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_key TEXT NOT NULL,
  lane_key TEXT NOT NULL,
  project_id INTEGER NOT NULL,
  requested_by_session_id INTEGER NOT NULL,
  producer_session_id INTEGER,
  producer_pid INTEGER,
  state TEXT NOT NULL,
  command TEXT NOT NULL,
  command_cwd TEXT NOT NULL,
  timeout_secs INTEGER NOT NULL,
  identity_json TEXT NOT NULL,
  policy_json TEXT NOT NULL,
  lease_expires_at_ms INTEGER,
  queued_at_ms INTEGER NOT NULL,
  started_at_ms INTEGER,
  finished_at_ms INTEGER,
  exit_code INTEGER,
  timed_out INTEGER NOT NULL DEFAULT 0,
  confined INTEGER NOT NULL DEFAULT 0,
  output_body TEXT,
  output_truncated INTEGER NOT NULL DEFAULT 0,
  terminal_reason TEXT
);

-- At most one live attempt per exact identity: a second identical request
-- joins the running job instead of executing a duplicate.
CREATE UNIQUE INDEX verification_jobs_active_identity
  ON verification_jobs (job_key)
  WHERE state IN ('queued', 'running');

CREATE INDEX verification_jobs_lane_order
  ON verification_jobs (lane_key, queued_at_ms)
  WHERE state IN ('queued', 'running');

CREATE INDEX verification_jobs_retention
  ON verification_jobs (state, finished_at_ms);

CREATE TABLE verification_job_subscribers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  session_id INTEGER NOT NULL,
  tool_call_id TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  joined_at_ms INTEGER NOT NULL,
  queue_ms INTEGER NOT NULL DEFAULT 0,
  compute_ms INTEGER NOT NULL DEFAULT 0,
  finished_at_ms INTEGER
);

CREATE UNIQUE INDEX verification_job_subscribers_identity
  ON verification_job_subscribers (job_id, session_id, tool_call_id);

CREATE INDEX verification_job_subscribers_job
  ON verification_job_subscribers (job_id, state);

-- Per-session link between a shared attempt and the verification evidence it
-- authorized. A linked row is never expired by retention and is exempt from
-- accidental cross-session duplication quarantine.
CREATE TABLE verification_shared_job_links (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER NOT NULL,
  session_id INTEGER NOT NULL,
  tool_call_id TEXT NOT NULL DEFAULT '',
  shared_job_key TEXT NOT NULL,
  evidence_digest TEXT NOT NULL,
  created_at_ms INTEGER NOT NULL
);

CREATE INDEX verification_shared_job_links_job
  ON verification_shared_job_links (job_id);

CREATE UNIQUE INDEX verification_shared_job_links_identity
  ON verification_shared_job_links (job_id, session_id, tool_call_id);

ALTER TABLE verification_runs ADD COLUMN shared_job_key TEXT;
ALTER TABLE verification_checks ADD COLUMN shared_job_key TEXT;

-- Per-database key for domain-separated keyed fingerprints of secret-bearing
-- inputs. Sessions never store the raw values, and two workspaces that differ
-- only in credentials still get distinct keys.
CREATE TABLE verification_scheduler_keys (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  secret_key BLOB NOT NULL,
  created_at_ms INTEGER NOT NULL
);
