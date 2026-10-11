//! Conservative verification recognition and the identity inputs that make two
//! attempts interchangeable (#142).
//!
//! The scheduler only coordinates a command it can describe completely: a
//! single Cargo verification subcommand with no shell metacharacters, whose
//! artifact directory is resolvable to one canonical path. Anything else —
//! compound commands, pipelines, redirected output, shell expansion, unknown
//! target-directory strategy — keeps ordinary Bash execution.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::verification::VerificationWorkspaceIdentity;

/// Cargo verification subcommands the scheduler coordinates. `fmt` is accepted
/// only in its read-only `--check` form, matching `direct_verification_is_cacheable`.
const VERIFICATION_SUBCOMMANDS: &[&str] = &["bench", "build", "check", "clippy", "doc", "test"];

/// Shell metacharacters that make the executed program depend on the shell
/// rather than on the literal command text. Presence disables coordination.
const SHELL_METACHARACTERS: &[char] = &[
    '|', ';', '&', '\n', '\r', '<', '>', '$', '`', '*', '?', '[', ']', '{', '}', '~', '(', ')',
    '!', '#',
];

/// A command the scheduler is willing to coordinate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecognizedVerificationCommand {
    /// Command text every participant executes, with a redundant trailing
    /// `2>&1` merge removed so producers and subscribers agree byte for byte.
    pub(crate) command: String,
    /// Cargo subcommand, used for status text.
    pub(crate) subcommand: String,
    /// Canonical artifact directory. Two attempts sharing it must serialize.
    pub(crate) target_directory: PathBuf,
}

/// Effective execution policy of one attempt.
///
/// It participates in both keys, so a sandbox-escape run can never supply
/// evidence to a confined subscriber (and vice versa).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub(crate) struct VerificationExecutionPolicy {
    /// Whether the command actually ran under the live sandbox policy.
    pub(crate) confined: bool,
    /// Whether this single run was approved to step past the sandbox.
    pub(crate) escaped: bool,
    /// Whether the run was permitted network access.
    pub(crate) allow_network: bool,
    /// The shell that interprets the command.
    pub(crate) shell: String,
    /// The command's own execution timeout.
    pub(crate) timeout_secs: u64,
}

impl VerificationExecutionPolicy {
    pub(crate) fn fingerprint(&self) -> Result<String> {
        digest("bonsai:verification-execution-policy:v1", self)
    }
}

/// Inputs shared by every attempt that must serialize on one artifact
/// directory. This is the *compatibility* key: jobs whose result keys differ
/// still queue behind one another when this matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct VerificationLane {
    /// Git worktree (or project root when the workspace is unversioned).
    pub(crate) worktree_root: String,
    pub(crate) project_root: String,
    /// Canonical artifact directory.
    pub(crate) target_directory: String,
    /// Execution-policy fingerprint.
    pub(crate) policy_fingerprint: String,
    /// Keyed fingerprint of secret-bearing inputs. Never a plaintext value.
    pub(crate) secret_fingerprint: String,
}

/// The pair of keys one request is admitted under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerificationJobKeys {
    /// Exact identity: only an identical key may reuse a result.
    pub(crate) job_key: String,
    /// Compatibility: attempts sharing it serialize on one lane.
    pub(crate) lane_key: String,
}

/// Domain-separated, canonical digest of a serializable value.
pub(crate) fn digest<T: Serialize + ?Sized>(domain: &str, value: &T) -> Result<String> {
    let encoded = serde_json::to_vec(value).context("Failed to encode verification identity")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(domain.as_bytes());
    hasher.update(&[0]);
    hasher.update(&encoded);
    Ok(hasher.finalize().to_hex().to_string())
}

/// Recognized inputs stored with an attempt so a subscriber can prove its own
/// capture still matches before it reuses evidence.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub(crate) struct StoredVerificationIdentity {
    pub(crate) workspace: VerificationWorkspaceIdentity,
    pub(crate) command: String,
    pub(crate) target_directory: String,
    pub(crate) policy: VerificationExecutionPolicy,
}

/// Recognize a command the scheduler may coordinate.
///
/// Returns `None` — ordinary execution — for anything it cannot describe
/// completely, which is the conservative default.
pub(crate) fn recognize(
    command: &str,
    cwd: &Path,
    worktree_root: &Path,
) -> Option<RecognizedVerificationCommand> {
    let base = strip_trailing_stderr_merge(command.trim());
    if base.is_empty() || base.contains(SHELL_METACHARACTERS) {
        return None;
    }
    let mut tokens = base.split_whitespace();
    if tokens.next() != Some("cargo") {
        return None;
    }
    let subcommand = tokens.next()?;
    let mut remaining = tokens.peekable();

    let recognized = if subcommand == "fmt" {
        // Only `cargo fmt --check` is read-only; plain `cargo fmt` rewrites the
        // workspace and must never be shared.
        remaining.any(|token| token == "--check")
    } else {
        VERIFICATION_SUBCOMMANDS.contains(&subcommand)
    };
    if !recognized {
        return None;
    }

    // Cargo options that change where artifacts land or how the stream is
    // framed make the attempt incomparable or would be rewritten by the Bash
    // tool, so they keep ordinary execution.
    for token in base.split_whitespace() {
        if token == "--target-dir" || token.starts_with("--target-dir=") {
            return None;
        }
        if token == "--message-format" || token.starts_with("--message-format=") {
            return None;
        }
        if token.starts_with("-Z") {
            return None;
        }
    }

    let target_directory = cargo_target_directory(worktree_root, cwd)?;
    Some(RecognizedVerificationCommand {
        command: base.to_string(),
        subcommand: subcommand.to_string(),
        target_directory,
    })
}

fn strip_trailing_stderr_merge(command: &str) -> &str {
    command
        .strip_suffix("2>&1")
        .map(str::trim_end)
        .unwrap_or(command)
}

/// Resolve the directory Cargo writes this command's artifacts to.
///
/// `None` means the strategy is not fully known, which disables coordination
/// rather than risking two unrelated jobs sharing — or two related jobs not
/// sharing — one build lock.
pub(crate) fn cargo_target_directory(worktree_root: &Path, cwd: &Path) -> Option<PathBuf> {
    if let Some(value) = std::env::var_os("CARGO_TARGET_DIR") {
        let value = PathBuf::from(value);
        if value.as_os_str().is_empty() {
            return None;
        }
        let absolute = if value.is_absolute() {
            value
        } else {
            cwd.join(value)
        };
        return Some(canonicalize_allow_missing(&absolute));
    }

    match configured_target_directory(worktree_root, cwd) {
        // The override is ambiguous, so the artifact directory is not known.
        Some(None) => None,
        Some(Some(overridden)) => Some(canonicalize_allow_missing(&overridden)),
        None => Some(canonicalize_allow_missing(&worktree_root.join("target"))),
    }
}

/// Read `build.target-dir` from the nearest `.cargo/config.toml` at or above
/// `cwd`, stopping at the worktree root.
///
/// `None` means no override was found; `Some(None)` means an override exists
/// that the scheduler declines to resolve (a relative path, whose base Cargo
/// defines in terms of the config file's own directory), which disables
/// coordination rather than risking a mis-scoped build lock.
fn configured_target_directory(worktree_root: &Path, cwd: &Path) -> Option<Option<PathBuf>> {
    let mut directory = Some(cwd);
    while let Some(current) = directory {
        for name in [".cargo/config.toml", ".cargo/config"] {
            let candidate = current.join(name);
            let Ok(content) = std::fs::read_to_string(&candidate) else {
                continue;
            };
            let Ok(document) = toml::from_str::<toml::Value>(&content) else {
                continue;
            };
            let Some(value) = document
                .get("build")
                .and_then(|build| build.get("target-dir"))
                .and_then(|value| value.as_str())
            else {
                continue;
            };
            let path = PathBuf::from(value);
            if path.is_absolute() {
                return Some(Some(path));
            }
            return Some(None);
        }
        if current == worktree_root {
            break;
        }
        directory = current.parent();
    }
    None
}

/// Canonicalize a path that may not exist yet (Cargo creates `target/` lazily)
/// by canonicalizing its parent and re-attaching the final component.
pub(crate) fn canonicalize_allow_missing(path: &Path) -> PathBuf {
    if let Ok(canonical) = std::fs::canonicalize(path) {
        return canonical;
    }
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => {
            std::fs::canonicalize(parent)
                .map(|parent| parent.join(name))
                .unwrap_or_else(|_| path.to_path_buf())
        }
        _ => path.to_path_buf(),
    }
}

/// Keyed, domain-separated fingerprint of secret-bearing inputs.
///
/// Sessions share one key per database, and only the digest is ever persisted —
/// no secret value reaches the job row, a log line, or a diagnostic dump.
pub(crate) fn secret_environment_fingerprint(
    key: &[u8; 32],
    inputs: &BTreeMap<String, String>,
) -> String {
    let mut material: Vec<u8> = Vec::new();
    material.extend_from_slice(b"bonsai:verification-secret-environment:v1");
    material.push(0);
    for (name, value) in inputs {
        material.extend_from_slice(name.as_bytes());
        material.push(b'=');
        material.extend_from_slice(value.as_bytes());
        material.push(b'\n');
    }
    blake3::keyed_hash(key, &material).to_hex().to_string()
}

/// Secret-bearing inputs of this process, keyed by name so the digest is
/// stable regardless of environment ordering.
pub(crate) fn current_secret_environment() -> BTreeMap<String, String> {
    let mut inputs = BTreeMap::new();
    for (name, value) in std::env::vars_os() {
        let name = name.to_string_lossy().into_owned();
        if is_secret_environment_name(&name) {
            inputs.insert(name, value.to_string_lossy().into_owned());
        }
    }
    inputs
}

fn is_secret_environment_name(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    if upper.contains("TOKEN")
        || upper.contains("SECRET")
        || upper.contains("PASSWORD")
        || upper.ends_with("_API_KEY")
        || upper.contains("CREDENTIAL")
    {
        return true;
    }
    upper.ends_with("_KEY") && !upper.ends_with("_KEYRING")
}

/// Build the exact and compatible keys for one attempt.
pub(crate) fn job_keys(
    identity_digest: &str,
    lane: &VerificationLane,
    reuse_signature: &str,
) -> Result<VerificationJobKeys> {
    let lane_key = digest("bonsai:verification-lane:v1", lane)?;
    let job_key = digest(
        "bonsai:verification-job:v1",
        &(identity_digest, lane, reuse_signature, lane_key.as_str()),
    )?;
    Ok(VerificationJobKeys { job_key, lane_key })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> VerificationExecutionPolicy {
        VerificationExecutionPolicy {
            confined: true,
            escaped: false,
            allow_network: false,
            shell: "/bin/sh".to_string(),
            timeout_secs: 600,
        }
    }

    fn root() -> tempfile::TempDir {
        tempfile::TempDir::new().expect("temp dir")
    }

    #[test]
    fn recognizes_single_cargo_verification_commands_only() {
        let temp = root();
        std::fs::write(temp.path().join("Cargo.toml"), "[package]\nname='x'\n").unwrap();

        let recognized = recognize("cargo test --locked", temp.path(), temp.path())
            .expect("plain cargo test is coordinated");
        assert_eq!(recognized.command, "cargo test --locked");
        assert_eq!(recognized.subcommand, "test");

        // Compound commands, pipelines, redirects and expansion keep ordinary
        // execution: their executed program is not the literal command text.
        for command in [
            "cargo test --locked && cargo build --locked",
            "cargo test --locked | tail -40",
            "cargo test --locked > out.txt",
            "cargo test --locked 2>&1 | head",
            "cargo test --features \"$FEATURES\"",
            "cargo test --locked; echo done",
            "cargo run",
            "cargo fmt --all",
            "cargo test --target-dir /tmp/other",
            "cargo test --message-format=json",
        ] {
            assert!(
                recognize(command, temp.path(), temp.path()).is_none(),
                "{command} must not be coordinated"
            );
        }

        // A trailing stderr merge is removed rather than rejected, so producer
        // and subscriber agree on one command string.
        assert_eq!(
            recognize("cargo test --locked 2>&1", temp.path(), temp.path())
                .expect("stderr merge is normalized")
                .command,
            "cargo test --locked"
        );
        assert!(recognize("cargo fmt --check", temp.path(), temp.path()).is_some());
    }

    #[test]
    fn default_target_directory_is_one_per_worktree() {
        let temp = root();
        let nested = temp.path().join("crates/member");
        std::fs::create_dir_all(&nested).unwrap();

        let from_member = cargo_target_directory(temp.path(), &nested).expect("default target dir");
        let from_root =
            cargo_target_directory(temp.path(), temp.path()).expect("default target dir");

        assert_eq!(from_member, from_root);
        assert_eq!(
            from_member,
            canonicalize_allow_missing(&temp.path().join("target"))
        );
    }

    #[test]
    fn only_absolute_config_target_dir_is_honored() {
        let temp = root();
        let absolute = root();
        std::fs::create_dir_all(temp.path().join(".cargo")).unwrap();
        std::fs::write(
            temp.path().join(".cargo/config.toml"),
            format!("[build]\ntarget-dir = {:?}\n", absolute.path()),
        )
        .unwrap();
        assert!(matches!(
            configured_target_directory(temp.path(), temp.path()),
            Some(Some(_))
        ));

        std::fs::write(
            temp.path().join(".cargo/config.toml"),
            "[build]\ntarget-dir = \"build-artifacts\"\n",
        )
        .unwrap();
        assert!(
            cargo_target_directory(temp.path(), temp.path()).is_none(),
            "an ambiguous relative override must disable coordination"
        );
    }

    #[test]
    fn policy_participates_in_both_keys() {
        let lane = VerificationLane {
            worktree_root: "/w".into(),
            project_root: "/w".into(),
            target_directory: "/w/target".into(),
            policy_fingerprint: policy().fingerprint().unwrap(),
            secret_fingerprint: "secret".into(),
        };
        let keys = job_keys("identity", &lane, "600").unwrap();

        let mut escaped = lane.clone();
        escaped.policy_fingerprint = VerificationExecutionPolicy {
            escaped: true,
            confined: false,
            ..policy()
        }
        .fingerprint()
        .unwrap();
        let escaped_keys = job_keys("identity", &escaped, "600").unwrap();

        assert_ne!(keys.lane_key, escaped_keys.lane_key);
        assert_ne!(keys.job_key, escaped_keys.job_key);

        let mut other_secret = lane.clone();
        other_secret.secret_fingerprint = "other".into();
        assert_ne!(
            keys.job_key,
            job_keys("identity", &other_secret, "600").unwrap().job_key
        );
        assert_ne!(
            keys.lane_key,
            job_keys("identity", &other_secret, "600").unwrap().lane_key
        );
    }

    #[test]
    fn secret_fingerprint_is_keyed_and_never_the_value() {
        let inputs = BTreeMap::from([(
            "BONSAI_SCHEDULER_TEST_TOKEN".to_string(),
            "super-secret-value".to_string(),
        )]);

        let first = secret_environment_fingerprint(&[7u8; 32], &inputs);

        assert!(!first.contains("super-secret-value"));
        assert!(!first.contains("BONSAI_SCHEDULER_TEST_TOKEN"));
        assert_ne!(first, secret_environment_fingerprint(&[8u8; 32], &inputs));
        assert_ne!(
            first,
            secret_environment_fingerprint(&[7u8; 32], &BTreeMap::new())
        );
    }

    #[test]
    fn secret_names_cover_the_common_credential_shapes() {
        for name in [
            "CARGO_REGISTRY_TOKEN",
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "AWS_SECRET_ACCESS_KEY",
            "DB_PASSWORD",
            "SERVICE_CREDENTIALS",
        ] {
            assert!(is_secret_environment_name(name), "{name} must be keyed");
        }
        for name in ["PATH", "CARGO_HOME", "RUSTFLAGS", "HOME"] {
            assert!(!is_secret_environment_name(name), "{name} is not a secret");
        }
    }
}
