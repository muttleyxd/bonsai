# Issue 142: cross-process verification scheduler qualification

Qualification date: 2026-10-11

## Scope

One shared, leased verification attempt per exact identity, coordinated across
concurrently running Bonsai processes through the project's SQLite store:

- `src/verification_scheduler/` — admission, lanes, lease heartbeat, fencing,
  retention sweep, and per-session evidence linking.
- `src/verification_scheduler/identity.rs` — conservative command recognition
  and the exact/compatible key derivation.
- `src/storage/verification.rs`, `src/storage/quality_evidence.rs`,
  `src/tool/bash/{bash.rs,process.rs}`, `src/tool/registry_assembly.rs`,
  `src/agent/verification.rs`.
- Additive migration `migrations/0010_verification_jobs.sql`.

The feature is a *deduplication and serialization* layer in front of the Bash
tool. It never replaces permission flow, sandbox confinement, or read-before-write
tracking: those run exactly as before, and a scheduled attempt produces the same
`CommandResult` an unscheduled one would.

## Coordination contract

A command is coordinated only when the scheduler can describe it completely.
The admission gate at the Bash boundary is the conjunction of:

```text
verification_scheduler configured  (a disabled context short-circuits to None)
planning_command.is_none()
canonical_check.is_none()
direct_verification.is_some()
!interactive
!run_in_background
```

and, inside the scheduler, `identity::recognize` returning `Some`:

- first token is exactly `cargo`;
- subcommand is one of `bench`, `build`, `check`, `clippy`, `doc`, `test`, or
  `fmt` **only** in its read-only `--check` form (plain `cargo fmt` rewrites the
  workspace and is never shared);
- no shell metacharacter is present (`| ; & newline < > $ backtick * ? [ ] { } ~ ( ) ! #`),
  so the executed program cannot differ from the literal command text;
- no `--target-dir`, `--message-format`, or `-Z` token, which would either move
  the artifact directory or change how the Bash tool frames the stream;
- the artifact directory resolves to one canonical path.

A redundant trailing `2>&1` is stripped rather than rejected, so producer and
subscriber agree on one command string byte for byte. Everything else — compound
commands, pipelines, redirects, expansion, background tasks, interactive runs —
keeps ordinary per-process execution. This is the conservative default: an
unrecognized command is never queued.

## Identity

Identity is a pair of domain-separated BLAKE3 digests (`digest()` in
`identity.rs`), never a raw value:

```text
lane_key = H(lane)
job_key  = H(identity_digest, lane, reuse_signature, lane_key)
lane     = {worktree_root, project_root, target_directory,
            policy_fingerprint, secret_fingerprint}
```

- **Exact identity (`job_key`)** — at most one live attempt per key
  (`CREATE UNIQUE INDEX ... WHERE state IN ('queued','running')`); a second
  identical request joins the running attempt instead of executing a duplicate.
- **Compatibility (`lane_key`)** — the weaker key that serializes jobs sharing
  one artifact directory even when their result keys differ, so two different
  cargo invocations cannot corrupt one target directory.
- **Execution policy** participates in *both* keys, so a sandbox-escaped run can
  never supply evidence to a confined subscriber, and vice versa.
- **Secrets** feed a per-database keyed hash
  (`bonsai:verification-secret-environment:v1`, key stored once in
  `verification_scheduler_keys`) over credential-shaped environment names.
  Only the digest is persisted — no secret value reaches a job row, a log line,
  or a diagnostic dump. Two workspaces that differ only in credentials still key
  differently.
- **Target directory** resolves from `CARGO_TARGET_DIR`, else `build.target-dir`
  in the nearest `.cargo/config.toml` at or above the cwd up to the worktree
  root (absolute only), else `<worktree>/target`. A relative config override is
  ambiguous, so it *disables* coordination rather than risking a mis-scoped
  build lock. One target directory per worktree; the shared Cargo build cache is
  reused as-is, with no quota, eviction, or cleanup (explicit product decision).

## Schema

`0010_verification_jobs.sql` is additive: `verification_jobs`,
`verification_job_subscribers`, `verification_shared_job_links`,
`verification_scheduler_keys`, plus `shared_job_key` columns on
`verification_runs` and `verification_checks`. `0001` remains the frozen 1.0
baseline. `verification_shared_job_links` is the per-session record of which
shared attempt authorized which evidence: linked rows are exempt from retention
expiry and from cross-session duplication quarantine, so evidence a session
actually cited is never collected out from under it. Unreferenced terminal
results expire after 7 days.

Because migration `0010` landed with the feature, the schema-baseline
expectation in `storage::tests::fresh_database_uses_one_current_schema_baseline`
was still at its pre-migration count. It is now `10`, per that test's own
instruction to bump alongside each new `migrations/*.sql` file.

## Results

| Gate | Command | Result |
| --- | --- | --- |
| Formatting | `cargo fmt --all --check` | clean |
| Lints | `cargo clippy --all-targets --all-features -- -D warnings` | clean |
| Tests | `cargo test --locked` | 4251 passed, 0 failed, 4 ignored; integration suites 9 passed / 1 ignored and 10 passed |

Focused scheduler and identity coverage — 14 tests, all passing:

| Invariant | Test |
| --- | --- |
| Recognition is conservative; `--target-dir`, `--message-format`, `-Z`, compound and piped commands are refused | `identity::tests::recognizes_single_cargo_verification_commands_only` |
| One target directory per worktree, including from a nested member | `identity::tests::default_target_directory_is_one_per_worktree` |
| Only an absolute config override is honored; a relative one disables coordination | `identity::tests::only_absolute_config_target_dir_is_honored` |
| Policy participates in both keys | `identity::tests::policy_participates_in_both_keys` |
| Secret fingerprint is keyed, and is neither the name nor the value | `identity::tests::secret_fingerprint_is_keyed_and_never_the_value` |
| Credential-shaped names are covered, non-secrets are not | `identity::tests::secret_names_cover_the_common_credential_shapes` |
| Identical requests run one process and share its result | `tests::identical_requests_run_one_process_and_share_the_result` |
| A privileged producer never supplies evidence to a confined subscriber | `tests::a_privileged_producer_never_supplies_evidence_to_a_confined_subscriber` |
| Incompatible Cargo jobs sharing a target directory serialize | `tests::incompatible_cargo_jobs_sharing_a_target_directory_serialize` |
| A workspace that moved while queued declines shared evidence | `tests::a_workspace_that_moved_while_queued_declines_shared_evidence` |
| Every subscriber detaching cancels the producer | `tests::every_subscriber_detaching_cancels_the_producer` |
| A dead producer interrupts its followers instead of racing them | `tests::a_dead_producer_interrupts_its_followers_instead_of_racing_them` |
| Retention expires unreferenced attempts and keeps referenced ones | `tests::retention_expires_unreferenced_attempts_and_keeps_referenced_ones` |
| Secret environment values never reach the job row | `tests::secret_environment_values_never_reach_the_job_row` |

The reclaim policy itself is a pure function, covered at the storage layer by
`storage::verification_jobs::tests::only_a_confirmed_dead_producer_frees_its_lane_immediately`.

## Qualification findings

1. **Disk exhaustion, not a code defect.** The first full-suite run aborted
   with `/System/Volumes/Data` at 100% (104 Mi free) and linker warnings for
   `aws_lc_sys` objects. `target/debug/incremental` held 9.9 GB of regenerable
   incremental-compilation state; removing it freed 7.3 GiB and the suite then
   completed. Nothing in the scheduler was implicated.
2. **Stale schema-baseline expectation.** The only genuine failure was
   `fresh_database_uses_one_current_schema_baseline` asserting 9 applied
   migrations against the then-current 10. Repaired by bumping the constant, as
   the test documents; a fresh database now verifies the migration-`0010`
   schema, including the `builtin_subagent_settings` invariants the same test
   checks.
3. **Reclaim raced a possibly-live producer off unix.** The liveness probe's
   non-unix fallback returned "not alive" unconditionally, so on any platform
   without the probe an expired lease was reclaimed immediately — contradicting
   the module's stated rule that reclaim waits until the producer is confirmed
   gone — and a second process could promote into the lane while the old one was
   still running. Liveness is now tri-state: a confirmed-live producer always
   keeps its lane, a confirmed-dead one is reclaimed at once, and an
   *unconfirmable* one is reclaimed only after it has stopped refreshing for a
   further full lease period. The policy is a pure function with its own test, so
   all three arms are asserted on every platform, not just unix.
4. **The per-database key barely had entropy.** Key generation mixed in
   `Instant::now().elapsed()` measured immediately after construction, which is
   effectively zero, leaving the "keyed, domain-separated" fingerprint resting on
   the database path, the pid, and the wall clock. UUIDv7 bytes — drawn from the
   OS CSPRNG — now supply the random material. This remains defence in depth:
   the key is stored in the same database as the digests it produces, and no
   secret value is ever persisted.
5. **A link's `evidence_digest` was not a digest of anything.**
   `record_shared_evidence` passed the attempt's own `job_key` for both columns,
   so the column retention logic keys off held a duplicate rather than the
   evidence the session bound. It now records the subscriber's own capture digest
   — the workspace identity that authorized reusing the attempt — which is the
   value the reuse decision was actually made on.
6. **The lane key was derived from the wrong root.** `admit` resolved the
   artifact directory from the process's own launch directory while the lane
   recorded the *captured* worktree, so a session started inside a member crate of
   a Cargo workspace computed a `target/` directory Cargo never writes to, and two
   sessions at different depths derived different lane keys for one real artifact
   directory. The directory is now resolved from the captured worktree, and
   recognition no longer resolves it at all — it is the caller's job once the
   worktree is known, with an unresolvable strategy still declining coordination.
7. **The shared path polluted `stdout` with merged streams plus the marker.**
   `with_shared_provenance` replaced the producer's own `stdout` with the combined
   body (stdout+stderr+marker) and the follower path fabricated its `stdout` the
   same way. Read-path extraction reads that field as the command's own output, so
   a shared run marked files read that only appeared on stderr. The marker is
   scheduler-authored text and now stays in the model-visible body: the producer's
   real `stdout` is preserved, and a follower's stand-in stops before the marker
   line.

## Residual risk

- **Shared-attempt provenance is still read out of command output.** The run
  loop recovers `shared_job_key` by scanning the rendered tool output for the
  `[scheduled verification] job=…` line, and that text lives in the same body the
  command writes to. A repository whose output contains a matching line can
  therefore attach an arbitrary key to its own evidence, which then exempts it
  from the cross-session duplicate quarantine — an integrity control, not a
  permission or sandbox boundary, so the effect is a suppressed warning rather
  than an escape. This is **not fixed here**: the correct repair is to carry the
  key out of band, either as a structured field on `ToolOutput::Command` or by
  resolving the `verification_shared_job_links` row for the session and tool call
  when the evidence is recorded. Both touch the 38 construction sites of that
  variant or move the sync evidence recorder onto storage, so it is recorded as
  follow-up rather than patched with a weaker check — position-based or
  format-based parsing cannot rule out a command printing exactly one plausible
  marker on the unshared path.
- **No true multi-process qualification was executed.** The scheduler is
  exercised end to end over a real SQLite store, but its producers and
  subscribers are driven by an in-process fake executor rather than three
  concurrently running `bonsai` processes. Cross-process lease timing, heartbeat
  loss under real scheduling jitter, and process-group reaping after a lease
  fence therefore rest on the unit-level assertions above plus the design
  invariant that an expired lease never permits concurrent execution — the old
  producer is fenced and its process group confirmed reaped before a new attempt
  starts. A three-session manual qualification on the same worktree remains the
  outstanding evidence gap.
- **Eval suites keep ordinary execution.** The eval runner constructs the Bash
  tool with `VerificationSchedulerContext::disabled()`, so eval task timing and
  provider-visible behavior are unchanged by this feature and the eval suites do
  not cover the shared path.
- **A recycled pid can pin a lane.** Producer liveness is `kill(pid, 0)`, which
  reports a reused pid as alive. A crashed producer whose pid was recycled is
  therefore never reclaimed, and every queued attempt in that lane waits out the
  full 30-minute limit before abandoning. This fails in the safe direction — no
  second producer ever runs concurrently — but it wastes the wait. A fix needs a
  process-identity probe beside the pid (start time or boot id), which no current
  dependency exposes portably.
- **Linked attempts are retained forever.** `verification_shared_job_links` rows
  are never expired, and one link permanently exempts its job from retention, so
  each shared attempt pins up to 200 000 chars of `output_body` in the database.
  Retention deliberately protects linked evidence, so bounding this means giving
  links their own window and letting the job expire behind them.
- **The lane's artifact directory has no scheduler-level regression test.** The
  directory is now resolved from the captured worktree rather than the process's
  launch directory, which is what makes two sessions started at different depths
  of one Cargo workspace share a lane. The nested-cwd property itself is covered
  by `identity::tests::default_target_directory_is_one_per_worktree`, but the
  wiring has no end-to-end test because the storage fixture is not a git
  repository and the capture needs one to observe a worktree above the launch
  directory.
- **Build-cache growth is unmanaged by design.** Reusing one target directory
  per worktree means a full disk still degrades builds; the scheduler detects
  nothing about disk pressure and reports the underlying command failure
  verbatim.

## Reproduce

```sh
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --locked
cargo test --locked verification_scheduler   # 14 focused tests
```

The three-session qualification gap needs three concurrently running `bonsai`
sessions on one worktree, each asked to run the same `cargo test --locked`, then
`SELECT state, producer_session_id FROM verification_jobs` and
`SELECT COUNT(*) FROM verification_job_subscribers` to confirm one execution and
three subscribers.
