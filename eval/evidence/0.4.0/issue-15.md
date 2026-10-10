# Issue 15: four-target adversarial release qualification

Qualification date: 2026-10-10

Result: **qualified on all four native release targets.** Each target's packed
archive was extracted on its own native runner, hashed against its sidecars, and
executed by the qualification runner with native sandbox enforcement required and
no skipped probe. No P0/P1 qualification defect is outstanding.

## Qualification contract

- Source revision: `36e01ea52b7fca9e7dc9b2db0c3c1eb0c9d2e7a4`-equivalent commit
  `36e01ea` (`fix(misc): require the approved-escape probe only on seatbelt`) on
  `master`, the tree the archives were packed from
- Workflow: manual **Qualify native candidate** (`.github/workflows/qualify-candidate.yml`),
  run [38080470677](https://github.com/strozynskiw/bonsai/actions/runs/38080470677) —
  same four-target matrix and runner as `release.yml`, publishing nothing
- Runner invocation per target:
  `python3 scripts/qualify_artifact.py --target <target> --revision 36e01ea --archive
  dist/bonsai-candidate-<target>.tar.gz --evidence qualification/<target>`
- Acceptance set on the implementation tree: `cargo fmt --all --check`,
  `cargo clippy --locked --all-targets --all-features -- -D warnings`,
  `cargo test --locked`, warning-free `cargo build --release --locked`

The runner extracts the archive into a private temporary directory, hashes the
archive and the extracted binary against the packed `.sha256` /
`.binary-sha256` sidecars, addresses the extracted binary through
`BONSAI_SURFACE_BINARY` for the surface and mock-eval checks, runs the
release-profile inline suite and the native sandbox probes sourced from the same
revision, and re-hashes archive and binary after the last check. `cargo run` is
never used and no rebuilt executable is substituted.

## Per-target results

| Target | Backend | Archive SHA-256 | Binary SHA-256 | Surface | Inline | Sandbox probes | release-eval | continuity-eval | Status |
| --- | --- | --- | --- | ---: | ---: | ---: | --- | --- | --- |
| x86_64-unknown-linux-gnu | bubblewrap | `9f194dae…c211a` | `14aab6ab…34769` | 10 passed | 4,231 passed | 23 passed | pass | pass | **qualified** |
| aarch64-unknown-linux-gnu | bubblewrap | `04d1b523…4c8c9` | `df051363…40f48` | 10 passed | 4,231 passed | 23 passed | pass | pass | **qualified** |
| aarch64-apple-darwin | seatbelt | `23eb16dc…12e04` | `1d268c37…b17da` | 10 passed | 4,234 passed | 24 passed | pass | pass | **qualified** |
| x86_64-apple-darwin | seatbelt | `5127323a…c2412` | `a5d311c1…6709a` | 10 passed | 4,234 passed | 24 passed | pass | pass | **qualified** |

Toolchains and runners were those of each native job: Linux `rustc 1.99.0
(b940084d7 2026-09-28)` on `Linux-6.8.0-1064-azure` (glibc 2.35), Darwin
`rustc 1.98.1`/`1.98.0` on macOS 15.7.9. The Linux rows execute one fewer probe
by design (see the per-platform probe set below). `sandbox-probes` counts differ
for the same reason.

## Coverage map

- `artifact-surface` — `tests/surface_smoke.rs` executed against the extracted
  binary: successful tool execution and completion contract; denied
  approval-required mutation in noninteractive mode; project confinement of
  outside writes and symlink escapes; untrusted tool-result framing; malformed
  and truncated SSE without false success; bounded tool-loop exhaustion; strict
  binary-override rejection; production startup against a supported older store
  with sentinel preservation and against unsupported/corrupt stores with the
  original bytes preserved.
- `artifact_confines_native_writes_and_network` — the artifact itself, not the
  test-compiled in-process wrappers, must confine an out-of-project write, a
  symlink escape, a child-process write and loopback network egress, each with an
  unsandboxed positive control proving the target path/listener is reachable
  without confinement, and each requiring a reported non-zero exit status. The
  forbidden sentinel lives outside every sandbox writable root (project, private
  temp, OS temp).
- `inline` — full non-ignored inline suite for `--bin bonsai` in release
  profile: `src/redact.rs`, credential persistence in `src/storage/tests.rs`,
  `src/agent/tests/{web_injection,mcp_injection}.rs`, permissions/effect gates,
  `src/tool/secure_fs.rs` and path evidence, `src/provider/sse.rs`, and the
  cancellation/resume regressions.
- `sandbox-probes` — native enforcement of the real backend, run with
  `BONSAI_REQUIRE_NATIVE_SANDBOX=1`. Both platforms must execute
  `blocks_write_outside_project_root` (out-of-root, symlink and child-process
  denial plus an allowed-root positive control) and
  `denies_network_when_configured` (deterministic loopback listener with an
  unsandboxed positive control); Darwin additionally must execute
  `escape_runs_what_confinement_blocks`, which covers `command_unconfined` and
  exists only in the macOS `seatbelt` module. A missing backend, a skipped
  probe, or a probe that never executed fails the target instead of passing it.
- `release-eval` / `continuity-eval` — the extracted binary runs
  `eval --mode mock --suite eval/suites/release_gating.toml --baseline
  eval/baselines/release-v1.toml --fail-on-task-failure` and
  `intent_continuity.toml --fail-on-task-failure` (cancellation, resumption,
  permission denial, shared-workspace persistence).
- The existing `--qualification` language/model matrix and the benchmark/soak
  validators are unchanged and are not part of this gate.

## Qualification findings and repairs

Every finding below was found by running the gate, is repaired with a
regression, and was re-verified by the four-target run above.

1. **P1 — artifact identity could pass without native enforcement.** The first
   gate revision ran native confinement only through the source-level wrappers
   in `src/sandbox/tests.rs`, so a release-only regression that stopped wiring
   the sandbox into the shipped binary would have qualified. Repaired in
   `dfe47bc` by adding `artifact_confines_native_writes_and_network`, which drives
   the *extracted* binary, and by making the runner require named tests to appear
   as executed (`missing_required_tests`).
2. **P1 — the gate broke ordinary CI, and would have hidden real enforcement.**
   `artifact_confines_native_writes_and_network` failed Linux CI (run
   38076104247) because that runner ships `bwrap` while restricting unprivileged
   user namespaces, so the sandbox was inactive and the probes asserted against
   nothing. Repaired in `3768f73`: availability is now *probed* the same way
   `sandbox::linux` probes it rather than inferred from a present binary;
   ordinary runs skip with `surface-qualification: skipping native confinement
   probe`, and `BONSAI_REQUIRE_NATIVE_SANDBOX=1` (set by the runner) turns that
   skip into a hard failure. The runner treats the skip notice as a failed check
   on every target.
3. **P1 — pre-existing: every release build was broken on the current stable.**
   `cc7a576`'s predecessor failed on both Linux jobs with
   `error: use of deprecated method std::sync::atomic::Atomic::<usize>::fetch_update`
   because `release.yml` builds with `RUSTFLAGS=-D warnings` and rustc 1.99
   deprecates that method — the same failure a real Linux release build would
   have hit. Repaired in `cc7a576` with compare-exchange loops in
   `src/agent/retry.rs` (stream-admission ceiling) and
   `src/storage/workspace_locks.rs` (injected-refresh-failure counter), which are
   stable on every supported compiler and preserve both semantics.
4. **P2 — a target-independent probe set failed a healthy target.**
   `escape_runs_what_confinement_blocks` is seatbelt-only, so requiring it on
   Linux failed qualification of a correctly confined artifact (run
   38078102015, `missing_required_tests`). Repaired in `36e01ea` with per-platform
   required sets, so neither platform can qualify on the other's evidence.
5. **P2 — interleaved child output broke provenance parsing.** `curl` progress
   output split a test's `... ok` status line, so a required native test looked
   absent. Repaired by silencing the probes (`curl -s`) and by parsing test names
   independently of the trailing status token.
6. **P2 — silence could have read as success.** A required check that ran zero
   tests, printed a skip marker, or omitted a required test name now fails the
   check instead of passing it (`test_missing_or_skipped_native_results_fail`,
   `test_missing_required_native_tests_fail`, `test_required_probe_sets_are_per_platform`).

## Negative controls

| Control | Expectation | Observed |
| --- | --- | --- |
| Stub non-functional artifact (`exit 3`) through the real runner | runner fails, exit 1 | exit 1; `artifact-surface`, `release-eval`, `continuity-eval` failed |
| Archive/binary identity mismatch | runner fails, no checks run | `failure_reason` = `<archive\|binary> identity mismatch`, exit 1 |
| Missing required native test name / skipped backend / zero tests | check fails | status `failed`, `missing_required_tests` populated |
| Child timeout | check killed and reaped, group signalled | `reason` = `timeout`, status `failed` |
| Invalid or missing `BONSAI_SURFACE_BINARY` override | surface test fails, no fallback to the rebuilt binary | `invalid_surface_binary_override_never_falls_back` passed |
| Uploaded evidence | only `qualification.json` | fixed counters, hashes, revision, toolchain; no logs, databases, transcripts or environment dumps |

Synthetic credential-shaped values used by the fixtures never reach evidence: the
fixtures pass a clearly-fake key through an isolated child environment, and
`test_identity_failures_and_evidence_allowlist` packs a credential-shaped payload
into a candidate archive and asserts the report contains neither the payload nor
any log, and that `qualification.json` is the only retained file.

## Residual risk

- The gate has been exercised through the manual candidate workflow, not yet
  inside a tagged `release.yml` run; that path runs the same runner with the same
  matrix and keeps `publish` dependent on a successful `build` per target.
- Linux qualification depends on a working Bubblewrap on the runner (installed by
  the workflow). If runner policy prevents the native backend from enforcing, the
  gate fails rather than skipping — a runner qualification failure, never a pass.
- The earlier local aarch64-Darwin pass (`target/qualification/local`, run against
  a tree that also carried unrelated uncommitted work) is superseded by the
  Darwin rows above, which were packed from the committed revision.
- `dist/` archives retained locally are the candidate archives, not signed
  release assets.

## Artifacts

| Artifact | Reference |
| --- | --- |
| Candidate workflow run (four targets) | `gh run view 38080470677` |
| Per-target reports | `qualification-<target>/qualification.json` artifacts of that run |
| Runner | `scripts/qualify_artifact.py` (failure controls: `scripts/test_qualify_artifact.py`) |
| Release wiring | `.github/workflows/release.yml` — qualify after **Pack binary**, before upload |
| Gates | `BONSAI_SURFACE_BINARY`, `BONSAI_REQUIRE_NATIVE_SANDBOX=1` |

Raw per-check logs stay in the runner's private temporary directory and are
deleted with it; only the allowlisted report is retained or uploaded.
