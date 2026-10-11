//! Cross-process verification scheduler (#142).
//!
//! Identical verification checks run once and share their result; compatible
//! Cargo jobs that share one canonical target directory queue behind a single
//! execution lease instead of racing Cargo's own build lock. Coordination lives
//! in the shared SQLite store, so it spans separate Bonsai processes rather than
//! one process's memory.
//!
//! The conservatism is deliberate: anything the scheduler cannot describe
//! completely (compound commands, pipelines, shell expansion, an unknown target
//! directory, an unreusable identity) falls back to ordinary Bash execution.

mod identity;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio_util::sync::CancellationToken;

pub(crate) use identity::{StoredVerificationIdentity, VerificationExecutionPolicy};
use identity::{VerificationLane, current_secret_environment};

use crate::output::SharedSink;
use crate::storage::{
    SessionId, Storage, VERIFICATION_JOB_LEASE_MS, VERIFICATION_JOB_RETENTION_MS,
    VerificationJobClaim, VerificationJobPayload, VerificationJobRequest, VerificationJobState,
};

/// Upper bound on a shared result body. Bash already bounds and spools its own
/// output; this is the second, scheduler-owned bound on what is persisted.
const SHARED_BODY_MAX_CHARS: usize = 200_000;

/// Marker line a shared result carries so the run loop can bind the session's
/// own verification evidence to the attempt that produced it.
pub(crate) const SHARED_VERIFICATION_MARKER: &str = "[scheduled verification]";

const LEASE_ACTIVE: u8 = 0;
const LEASE_LOST: u8 = 1;
const LEASE_UNNEEDED: u8 = 2;

/// Lease and polling discipline. Tests drive a faster window through
/// [`VerificationScheduler::with_timing`]; production uses [`Self::PRODUCTION`].
#[derive(Debug, Clone, Copy)]
pub(crate) struct SchedulerTiming {
    /// How long one confirmed producer lease lasts.
    pub(crate) lease: Duration,
    /// How often the producer renews it and re-checks its subscribers.
    pub(crate) refresh: Duration,
    /// Poll interval while waiting in a lane or following an attempt.
    pub(crate) poll: Duration,
    /// How long a subscriber waits for the queue before it gives up cleanly.
    pub(crate) wait_limit: Duration,
    /// How long a just-completed identical result stays reusable.
    pub(crate) reuse_window: Duration,
}

impl SchedulerTiming {
    pub(crate) const PRODUCTION: Self = Self {
        lease: Duration::from_millis(VERIFICATION_JOB_LEASE_MS as u64),
        refresh: Duration::from_secs(10),
        poll: Duration::from_millis(250),
        wait_limit: Duration::from_secs(30 * 60),
        reuse_window: Duration::from_secs(30),
    };

    fn lease_ms(self) -> i64 {
        i64::try_from(self.lease.as_millis()).unwrap_or(i64::MAX)
    }

    fn reuse_window_ms(self) -> i64 {
        i64::try_from(self.reuse_window.as_millis()).unwrap_or(i64::MAX)
    }
}

/// One session's request to run a possibly-shared verification check.
#[derive(Debug, Clone)]
pub(crate) struct VerificationRunRequest {
    pub(crate) command: String,
    pub(crate) cwd: PathBuf,
    pub(crate) timeout_secs: u64,
    pub(crate) tool_call_id: String,
    /// The session this attempt acts for, resolved per call so a runtime that
    /// switched sessions does not reuse another session's evidence.
    pub(crate) session_id: SessionId,
    pub(crate) policy: VerificationExecutionPolicy,
    /// The command runner's own cancellation handle. Bridging the producer lease
    /// to it lets the runner take the whole process group down and reap it,
    /// instead of the attempt being abandoned mid-flight by a dropped future.
    pub(crate) cancellation: Option<CancellationToken>,
}

/// The bounded result an attempt publishes for every subscriber.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SharedVerificationPayload {
    pub(crate) exit_code: Option<i32>,
    pub(crate) timed_out: bool,
    pub(crate) confined: bool,
    pub(crate) body: String,
    pub(crate) truncated: bool,
}

/// How this session's result was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SharedVerificationState {
    /// This session executed the attempt.
    Produced,
    /// This session waited for a lane, then executed the attempt.
    WaitedProduced,
    /// This session reused another attempt's exact result.
    Reused,
}

impl SharedVerificationState {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Produced => "produced",
            Self::WaitedProduced => "waited-producer",
            Self::Reused => "reused",
        }
    }
}

/// Provenance attached to every shared result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SharedVerificationProvenance {
    pub(crate) job_key: String,
    pub(crate) state: SharedVerificationState,
    /// Time spent waiting for the lane, kept apart from execution time.
    pub(crate) queue_ms: i64,
    /// Time the command itself ran.
    pub(crate) compute_ms: i64,
}

impl SharedVerificationProvenance {
    /// The single line appended to a shared result so the run loop can bind the
    /// session's own verification evidence to this attempt.
    pub(crate) fn marker(&self) -> String {
        format!(
            "{SHARED_VERIFICATION_MARKER} job={} state={} queue_ms={} compute_ms={}",
            self.job_key,
            self.state.label(),
            self.queue_ms,
            self.compute_ms,
        )
    }
}

/// Outcome of asking the scheduler to run a check.
#[derive(Debug)]
pub(crate) enum SharedVerification<T> {
    /// This session produced the result.
    Produced {
        value: T,
        provenance: SharedVerificationProvenance,
    },
    /// Another attempt produced an exact match that this session reused.
    Reused {
        payload: SharedVerificationPayload,
        provenance: SharedVerificationProvenance,
    },
    /// The attempt was abandoned without a result and must not be read as a
    /// pass; the caller surfaces the interruption instead of rerunning blindly.
    Interrupted { job_key: String, reason: String },
    /// The scheduler will not coordinate this check; run it ordinarily.
    Declined,
}

/// What a caller must expose for the scheduler to publish and reuse a result.
pub(crate) trait SharedVerificationOutcome {
    fn shared_payload(&self) -> SharedVerificationPayload;
}

/// One session's view of the shared verification store.
#[derive(Debug)]
pub(crate) struct VerificationScheduler {
    storage: Storage,
    project_root: PathBuf,
    timing: SchedulerTiming,
    swept: Arc<AtomicBool>,
}

impl VerificationScheduler {
    pub(crate) fn new(storage: Storage, project_root: PathBuf) -> Self {
        Self {
            storage,
            project_root,
            timing: SchedulerTiming::PRODUCTION,
            swept: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_timing(mut self, timing: SchedulerTiming) -> Self {
        self.timing = timing;
        self
    }

    /// Admit, execute, follow or decline one verification check.
    ///
    /// `producer` is only awaited when this session owns the attempt; on every
    /// other path it is dropped unstarted.
    pub(crate) async fn run<T, F>(
        &self,
        request: &VerificationRunRequest,
        sink: Option<&SharedSink>,
        producer: F,
    ) -> Result<SharedVerification<T>>
    where
        T: SharedVerificationOutcome,
        F: std::future::Future<Output = Result<T>>,
    {
        self.sweep_once().await;

        let Some((job_request, identity_digest)) = self.admit(request).await? else {
            return Ok(SharedVerification::Declined);
        };
        let claim = self.storage.claim_verification_job(&job_request).await?;
        let job_id = match claim {
            VerificationJobClaim::Started(id) => id,
            VerificationJobClaim::Following(id) => id,
            VerificationJobClaim::Queued { job_id, ahead } => {
                report_status(
                    sink,
                    &format!(
                        "verification queued behind {ahead} Cargo job(s) sharing {}",
                        job_request.command_cwd
                    ),
                );
                job_id
            }
        };
        let guard = SubscriptionGuard::new(
            self.storage.clone(),
            job_id,
            request.session_id,
            job_request.tool_call_id.clone(),
        );

        match claim {
            VerificationJobClaim::Started(id) => {
                self.produce(
                    id,
                    &job_request,
                    &identity_digest,
                    request,
                    sink,
                    guard,
                    producer,
                    false,
                )
                .await
            }
            VerificationJobClaim::Queued { job_id, .. } => {
                match self
                    .wait_for_lane(job_id, &identity_digest, request, sink, &guard)
                    .await?
                {
                    LaneOutcome::Promoted => {
                        self.produce(
                            job_id,
                            &job_request,
                            &identity_digest,
                            request,
                            sink,
                            guard,
                            producer,
                            true,
                        )
                        .await
                    }
                    LaneOutcome::Abandoned { reason } => Ok(SharedVerification::Interrupted {
                        job_key: job_request.job_key.clone(),
                        reason,
                    }),
                }
            }
            VerificationJobClaim::Following(id) => {
                self.follow(id, &job_request, &identity_digest, request, guard)
                    .await
            }
        }
    }

    /// Build the admission record for a request, or `None` when the check must
    /// keep ordinary execution.
    async fn admit(
        &self,
        request: &VerificationRunRequest,
    ) -> Result<Option<(VerificationJobRequest, String)>> {
        let Some(recognized) = identity::recognize(&request.command) else {
            return Ok(None);
        };
        // Recapture here, before queueing, so a request that is already stale
        // never occupies a lane.
        let Ok(workspace) = crate::agent::capture_verification_workspace_identity(
            &self.project_root,
            &request.cwd,
            &recognized.command,
        )
        .await
        else {
            return Ok(None);
        };
        // Resolve the artifact directory from the captured worktree rather than
        // from wherever this process happens to have been launched: a session
        // started inside a member crate of a Cargo workspace still writes to the
        // workspace root's `target/`, and two such sessions must derive one lane
        // key for it. A strategy that cannot be resolved keeps ordinary
        // execution instead of risking an unscoped lane.
        let Some(target_directory) = identity::cargo_target_directory(
            std::path::Path::new(&workspace.worktree_root),
            &request.cwd,
        ) else {
            return Ok(None);
        };
        let target_directory = target_directory.to_string_lossy().into_owned();
        let Ok(identity_digest) = workspace.digest() else {
            return Ok(None);
        };
        let policy_fingerprint = request.policy.fingerprint()?;
        let secret_key = self.storage.verification_scheduler_secret_key().await?;
        let lane = VerificationLane {
            worktree_root: workspace.worktree_root.clone(),
            project_root: workspace.project_root.clone(),
            target_directory: target_directory.clone(),
            policy_fingerprint,
            secret_fingerprint: identity::secret_environment_fingerprint(
                &secret_key,
                &current_secret_environment(),
            ),
        };
        let keys = identity::job_keys(&identity_digest, &lane, &request.timeout_secs.to_string())?;
        let stored = StoredVerificationIdentity {
            workspace,
            command: recognized.command.clone(),
            target_directory: lane.target_directory.clone(),
            policy: request.policy.clone(),
        };
        Ok(Some((
            VerificationJobRequest {
                project_root: self.project_root.clone(),
                job_key: keys.job_key,
                lane_key: keys.lane_key,
                session_id: request.session_id,
                tool_call_id: request.tool_call_id.clone(),
                command: recognized.command,
                command_cwd: request.cwd.to_string_lossy().into_owned(),
                timeout_secs: request.timeout_secs,
                identity_json: serde_json::to_string(&stored)
                    .context("Failed to encode the verification job identity")?,
                policy_json: serde_json::to_string(&request.policy)
                    .context("Failed to encode the verification execution policy")?,
                lease_ms: self.timing.lease_ms(),
                reuse_window_ms: self.timing.reuse_window_ms(),
            },
            identity_digest,
        )))
    }

    /// Wait for a lane to free up, then promote this attempt or follow the
    /// identical attempt that won the promotion race.
    async fn wait_for_lane(
        &self,
        job_id: i64,
        identity_digest: &str,
        request: &VerificationRunRequest,
        sink: Option<&SharedSink>,
        guard: &SubscriptionGuard,
    ) -> Result<LaneOutcome> {
        let deadline = Instant::now() + self.timing.wait_limit;
        loop {
            if guard.is_detached() {
                return Ok(LaneOutcome::Abandoned {
                    reason: "subscriber cancelled".to_string(),
                });
            }
            if Instant::now() >= deadline {
                self.storage
                    .cancel_verification_subscription(
                        job_id,
                        request.session_id,
                        &request.tool_call_id,
                    )
                    .await?;
                return Ok(LaneOutcome::Abandoned {
                    reason: format!(
                        "waited {}s for the Cargo lane without acquiring it; no evidence was produced",
                        self.timing.wait_limit.as_secs()
                    ),
                });
            }
            if self
                .storage
                .try_promote_verification_job(job_id, request.session_id, self.timing.lease_ms())
                .await?
            {
                // The workspace may have moved while we waited.
                if !self.identity_still_matches(request, identity_digest).await {
                    self.storage
                        .close_verification_job(
                            job_id,
                            VerificationJobState::EnvironmentBlocked,
                            "workspace changed while queued",
                        )
                        .await?;
                    return Ok(LaneOutcome::Abandoned {
                        reason: "workspace inputs changed while the check was queued".to_string(),
                    });
                }
                report_status(sink, "verification lane acquired; running the queued check");
                return Ok(LaneOutcome::Promoted);
            }
            tokio::time::sleep(self.timing.poll).await;
        }
    }

    /// Follow an attempt another session is executing (or an identical attempt
    /// that just completed inside the reuse window).
    async fn follow<T>(
        &self,
        job_id: i64,
        job_request: &VerificationJobRequest,
        identity_digest: &str,
        request: &VerificationRunRequest,
        guard: SubscriptionGuard,
    ) -> Result<SharedVerification<T>>
    where
        T: SharedVerificationOutcome,
    {
        let deadline = Instant::now() + self.timing.wait_limit;
        let mut detached = false;
        loop {
            if guard.is_detached() {
                detached = true;
                break;
            }
            if Instant::now() >= deadline {
                break;
            }
            let Some(snapshot) = self.storage.load_verification_job(job_id).await? else {
                break;
            };
            match snapshot.state {
                VerificationJobState::Queued | VerificationJobState::Running => {
                    // Reclaim a dead producer's attempt so followers are woken
                    // instead of waiting out the whole lease window.
                    self.storage.reap_verification_jobs().await?;
                    tokio::time::sleep(self.timing.poll).await;
                }
                VerificationJobState::Completed => {
                    let payload = SharedVerificationPayload {
                        exit_code: snapshot.exit_code,
                        timed_out: snapshot.timed_out,
                        confined: snapshot.confined,
                        body: snapshot.output_body.clone().unwrap_or_default(),
                        truncated: snapshot.output_truncated,
                    };
                    // Recapture before reuse: a moved workspace invalidates the
                    // shared evidence, so this session runs the check itself.
                    if !self.identity_still_matches(request, identity_digest).await {
                        self.storage
                            .cancel_verification_subscription(
                                job_id,
                                request.session_id,
                                &request.tool_call_id,
                            )
                            .await?;
                        return Ok(SharedVerification::Declined);
                    }
                    self.record_shared_evidence(
                        job_id,
                        job_request,
                        &request.tool_call_id,
                        identity_digest,
                    )
                    .await?;
                    self.storage
                        .complete_verification_subscription(
                            job_id,
                            request.session_id,
                            &request.tool_call_id,
                            snapshot.queue_ms(),
                            snapshot.compute_ms(),
                        )
                        .await?;
                    guard.disarm();
                    return Ok(SharedVerification::Reused {
                        payload,
                        provenance: SharedVerificationProvenance {
                            job_key: job_request.job_key.clone(),
                            state: SharedVerificationState::Reused,
                            queue_ms: snapshot.queue_ms(),
                            compute_ms: snapshot.compute_ms(),
                        },
                    });
                }
                VerificationJobState::Interrupted
                | VerificationJobState::Cancelled
                | VerificationJobState::EnvironmentBlocked => {
                    return Ok(SharedVerification::Interrupted {
                        job_key: job_request.job_key.clone(),
                        reason: snapshot
                            .terminal_reason
                            .unwrap_or_else(|| snapshot.state.label().to_string()),
                    });
                }
            }
        }
        let reason = if detached {
            "subscriber cancelled".to_string()
        } else {
            format!(
                "waited {}s for the shared check without a result",
                self.timing.wait_limit.as_secs()
            )
        };
        Ok(SharedVerification::Interrupted {
            job_key: job_request.job_key.clone(),
            reason,
        })
    }

    /// Execute the attempt as its producer, publish one terminal result, and
    /// hold the lease for as long as any subscriber still needs it.
    #[allow(clippy::too_many_arguments)]
    async fn produce<T, F>(
        &self,
        job_id: i64,
        job_request: &VerificationJobRequest,
        identity_digest: &str,
        request: &VerificationRunRequest,
        sink: Option<&SharedSink>,
        guard: SubscriptionGuard,
        producer: F,
        waited: bool,
    ) -> Result<SharedVerification<T>>
    where
        T: SharedVerificationOutcome,
        F: std::future::Future<Output = Result<T>>,
    {
        let lease = Arc::new(ProducerLease::new());
        let heartbeat = tokio::spawn(heartbeat_verification_lease(
            self.storage.clone(),
            job_id,
            request.session_id,
            lease.clone(),
            self.timing,
        ));
        let started = Instant::now();
        // Bridge the producer lease to the command runner's own cancellation
        // token, so losing the lease terminates the process group instead of
        // merely dropping a future that only kills the direct child.
        let bridge = request.cancellation.clone().map(|token| {
            let lease = lease.clone();
            tokio::spawn(async move {
                lease.cancellation.cancelled().await;
                token.cancel();
            })
        });
        let outcome = if bridge.is_some() {
            producer.await
        } else {
            tokio::select! {
                biased;
                _ = lease.cancellation.cancelled() => {
                    Err(anyhow::anyhow!(lease.cancellation_reason()))
                }
                result = producer => result,
            }
        };
        if let Some(bridge) = bridge {
            bridge.abort();
        }
        heartbeat.abort();

        match outcome {
            Ok(_value) if lease.status() != LEASE_ACTIVE => {
                // The command runner observed the cancellation and returned a
                // terminated result; it must never be published as evidence.
                let unneeded = lease.status() == LEASE_UNNEEDED;
                let reason = lease.cancellation_reason().to_string();
                self.storage
                    .close_verification_job(
                        job_id,
                        if unneeded {
                            VerificationJobState::Cancelled
                        } else {
                            VerificationJobState::Interrupted
                        },
                        &reason,
                    )
                    .await?;
                self.storage
                    .complete_verification_subscription(
                        job_id,
                        request.session_id,
                        &request.tool_call_id,
                        job_request_time_ms(job_id, &self.storage).await,
                        i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
                    )
                    .await?;
                guard.disarm();
                Ok(SharedVerification::Interrupted {
                    job_key: job_request.job_key.clone(),
                    reason,
                })
            }
            Ok(value) => {
                let payload = value.shared_payload();
                let published = self
                    .storage
                    .publish_verification_job_result(
                        job_id,
                        request.session_id,
                        &VerificationJobPayload {
                            exit_code: payload.exit_code,
                            timed_out: payload.timed_out,
                            confined: payload.confined,
                            body: bound_body(&payload.body),
                            truncated: payload.truncated
                                || payload.body.chars().count() > SHARED_BODY_MAX_CHARS,
                        },
                    )
                    .await?;
                if !published {
                    // The lease lapsed inside the safety margin; the attempt is
                    // not ours to complete any more.
                    return Ok(SharedVerification::Interrupted {
                        job_key: job_request.job_key.clone(),
                        reason: "producer lease was lost before the result could be committed"
                            .to_string(),
                    });
                }
                // Re-check the workspace after execution, before delivery: the
                // evidence is only authoritative for the identity it observed.
                if !self.identity_still_matches(request, identity_digest).await {
                    tracing::warn!(
                        job_key = %job_request.job_key,
                        "workspace inputs changed during the shared check"
                    );
                }
                let finished = match self.storage.load_verification_job(job_id).await {
                    Ok(Some(snapshot)) => snapshot,
                    _ => {
                        return Ok(SharedVerification::Interrupted {
                            job_key: job_request.job_key.clone(),
                            reason: "the shared attempt no longer exists".to_string(),
                        });
                    }
                };
                let queue_ms = finished.queue_ms();
                let compute_ms = finished.compute_ms();
                self.storage
                    .complete_verification_subscription(
                        job_id,
                        request.session_id,
                        &request.tool_call_id,
                        queue_ms,
                        compute_ms,
                    )
                    .await?;
                self.record_shared_evidence(
                    job_id,
                    job_request,
                    &request.tool_call_id,
                    identity_digest,
                )
                .await?;
                guard.disarm();
                Ok(SharedVerification::Produced {
                    value,
                    provenance: SharedVerificationProvenance {
                        job_key: job_request.job_key.clone(),
                        state: if waited {
                            SharedVerificationState::WaitedProduced
                        } else {
                            SharedVerificationState::Produced
                        },
                        queue_ms,
                        compute_ms,
                    },
                })
            }
            Err(error) => {
                let unneeded = lease.status() == LEASE_UNNEEDED;
                let state = if unneeded {
                    VerificationJobState::Cancelled
                } else {
                    VerificationJobState::Interrupted
                };
                let reason = if unneeded {
                    "every subscriber detached".to_string()
                } else {
                    format!("producer stopped: {error}")
                };
                report_status(
                    sink,
                    if unneeded {
                        "verification cancelled: no subscriber still needs the result"
                    } else {
                        "verification attempt was interrupted"
                    },
                );
                self.storage
                    .close_verification_job(job_id, state, &reason)
                    .await?;
                self.storage
                    .complete_verification_subscription(
                        job_id,
                        job_request.session_id,
                        &request.tool_call_id,
                        job_request_time_ms(job_id, &self.storage).await,
                        i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
                    )
                    .await?;
                guard.disarm();
                Err(error)
            }
        }
    }

    /// Whether this session's own capture still matches the workspace the
    /// attempt was admitted for.
    async fn identity_still_matches(
        &self,
        request: &VerificationRunRequest,
        expected_digest: &str,
    ) -> bool {
        let Some(recognized) = identity::recognize(&request.command) else {
            return false;
        };
        let Ok(workspace) = crate::agent::capture_verification_workspace_identity(
            &self.project_root,
            &request.cwd,
            &recognized.command,
        )
        .await
        else {
            return false;
        };
        matches!(workspace.digest(), Ok(digest) if digest == expected_digest)
    }

    /// Bind this session's evidence to the shared attempt that produced it.
    ///
    /// `evidence_digest` is this session's own capture digest — the workspace
    /// identity that authorized reusing the attempt — so the link records what
    /// the session actually bound, not a copy of the attempt's own key.
    async fn record_shared_evidence(
        &self,
        job_id: i64,
        job_request: &VerificationJobRequest,
        tool_call_id: &str,
        evidence_digest: &str,
    ) -> Result<()> {
        self.storage
            .link_verification_shared_job(
                job_id,
                job_request.session_id,
                tool_call_id,
                &job_request.job_key,
                evidence_digest,
            )
            .await
    }

    async fn sweep_once(&self) {
        if self.swept.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Err(error) = self
            .storage
            .expire_verification_jobs(VERIFICATION_JOB_RETENTION_MS)
            .await
        {
            tracing::debug!(%error, "verification scheduler retention sweep failed");
        }
    }
}

/// Outcome of waiting in a lane.
enum LaneOutcome {
    Promoted,
    Abandoned { reason: String },
}

/// Producer-side lease state. Cancellation covers both "lease lost" and "no
/// subscriber needs this any more", because both must stop the process group.
#[derive(Debug)]
struct ProducerLease {
    status: AtomicU8,
    cancellation: CancellationToken,
}

impl ProducerLease {
    fn new() -> Self {
        Self {
            status: AtomicU8::new(LEASE_ACTIVE),
            cancellation: CancellationToken::new(),
        }
    }

    fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    fn cancel_as(&self, status: u8) {
        if self
            .status
            .compare_exchange(LEASE_ACTIVE, status, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.cancellation.cancel();
        }
    }

    fn cancellation_reason(&self) -> &'static str {
        match self.status() {
            LEASE_UNNEEDED => {
                "every subscriber detached; the shared verification was cancelled and its process group reaped"
            }
            _ => "the verification producer lease was lost; the attempt was abandoned",
        }
    }
}

/// Renew the producer lease, and stop the producer as soon as no subscriber
/// still needs a result. A lapsed lease on a live process is never stolen by
/// the reclaim pass, so a transient database hiccup cannot cause a double run.
async fn heartbeat_verification_lease(
    storage: Storage,
    job_id: i64,
    session_id: SessionId,
    lease: Arc<ProducerLease>,
    timing: SchedulerTiming,
) {
    loop {
        tokio::time::sleep(timing.refresh).await;
        if lease.cancellation.is_cancelled() {
            return;
        }
        match storage.active_verification_subscribers(job_id).await {
            Ok(0) => {
                lease.cancel_as(LEASE_UNNEEDED);
                return;
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, job_id, "failed to count verification subscribers");
            }
        }
        match storage
            .refresh_verification_job_lease(job_id, session_id, timing.lease_ms())
            .await
        {
            Ok(Some(_)) => {}
            Ok(None) => {
                lease.cancel_as(LEASE_LOST);
                return;
            }
            Err(error) => {
                tracing::warn!(%error, job_id, "failed to renew the verification job lease");
            }
        }
    }
}

/// Detaches this session's subscription when the caller drops out — a cancelled
/// turn, an aborted request — without disturbing any other subscriber.
#[derive(Debug)]
struct SubscriptionGuard {
    storage: Storage,
    job_id: i64,
    session_id: SessionId,
    tool_call_id: String,
    detached: Arc<AtomicBool>,
}

impl SubscriptionGuard {
    fn new(storage: Storage, job_id: i64, session_id: SessionId, tool_call_id: String) -> Self {
        Self {
            storage,
            job_id,
            session_id,
            tool_call_id,
            detached: Arc::new(AtomicBool::new(false)),
        }
    }

    fn disarm(&self) {
        self.detached.store(true, Ordering::Release);
    }

    fn is_detached(&self) -> bool {
        self.detached.load(Ordering::Acquire)
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        if self.is_detached() {
            return;
        }
        let storage = self.storage.clone();
        let (job_id, session_id) = (self.job_id, self.session_id);
        let session_raw = session_id.as_i64();
        let tool_call_id = self.tool_call_id.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(error) = storage
                    .cancel_verification_subscription(job_id, session_id, &tool_call_id)
                    .await
                {
                    tracing::debug!(job_id, %error, "verification subscription detach failed");
                }
            });
        } else {
            // Dropping without a runtime cannot reach the store, so the
            // subscriber stays attached and keeps its attempt — and the lane
            // behind it — alive. That is worth saying out loud rather than
            // failing silently.
            tracing::warn!(
                job_id,
                session_id = session_raw,
                "verification subscription could not be detached: no Tokio runtime is available"
            );
        }
    }
}

async fn job_request_time_ms(job_id: i64, storage: &Storage) -> i64 {
    match storage.load_verification_job(job_id).await {
        Ok(Some(snapshot)) => snapshot.queue_ms(),
        _ => 0,
    }
}

fn bound_body(body: &str) -> String {
    if body.chars().count() <= SHARED_BODY_MAX_CHARS {
        return body.to_string();
    }
    body.chars().take(SHARED_BODY_MAX_CHARS).collect()
}

fn report_status(sink: Option<&SharedSink>, text: &str) {
    if let Some(sink) = sink {
        sink.status(text);
    }
}

/// Filesystem identity of one attempt's artifact directory, exposed for the
/// eval and regression tests that compare lanes across worktrees.
#[cfg(test)]
pub(crate) fn canonical_target_directory(
    worktree_root: &std::path::Path,
    cwd: &std::path::Path,
) -> Option<PathBuf> {
    identity::cargo_target_directory(worktree_root, cwd)
        .map(|path| identity::canonicalize_allow_missing(&path))
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::future::join_all;

    use super::*;
    use crate::provider::ReasoningSelection;
    use crate::storage::test_utils::TestStorage;
    use crate::util::time::now_ms;

    const COMMAND: &str = "cargo test --locked";
    const OTHER_COMMAND: &str = "cargo build --locked";

    #[derive(Debug)]
    struct FakeOutcome {
        exit_code: Option<i32>,
        body: String,
    }

    impl SharedVerificationOutcome for FakeOutcome {
        fn shared_payload(&self) -> SharedVerificationPayload {
            SharedVerificationPayload {
                exit_code: self.exit_code,
                timed_out: false,
                confined: false,
                body: self.body.clone(),
                truncated: false,
            }
        }
    }

    fn fast_timing() -> SchedulerTiming {
        SchedulerTiming {
            lease: Duration::from_millis(750),
            refresh: Duration::from_millis(100),
            poll: Duration::from_millis(20),
            wait_limit: Duration::from_secs(15),
            reuse_window: Duration::from_secs(5),
        }
    }

    fn confined_policy() -> VerificationExecutionPolicy {
        VerificationExecutionPolicy {
            confined: true,
            escaped: false,
            allow_network: false,
            shell: "/bin/sh".to_string(),
            timeout_secs: 600,
        }
    }

    fn escaped_policy() -> VerificationExecutionPolicy {
        VerificationExecutionPolicy {
            confined: false,
            escaped: true,
            ..confined_policy()
        }
    }

    fn scheduler_for(storage: &Storage, root: &Path) -> VerificationScheduler {
        VerificationScheduler::new(storage.clone(), root.to_path_buf()).with_timing(fast_timing())
    }

    fn request(
        root: &Path,
        session_id: SessionId,
        command: &str,
        tool_call_id: &str,
        policy: VerificationExecutionPolicy,
        cancellation: Option<CancellationToken>,
    ) -> VerificationRunRequest {
        VerificationRunRequest {
            command: command.to_string(),
            cwd: root.to_path_buf(),
            timeout_secs: 600,
            tool_call_id: tool_call_id.to_string(),
            session_id,
            policy,
            cancellation,
        }
    }

    async fn fake_execution(executions: Arc<AtomicUsize>, delay: Duration) -> Result<FakeOutcome> {
        executions.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(delay).await;
        Ok(FakeOutcome {
            exit_code: Some(0),
            body: "test result: ok".to_string(),
        })
    }

    async fn admitted(
        fixture: &TestStorage,
        root: &Path,
        session: SessionId,
        command: &str,
        tool_call_id: &str,
    ) -> VerificationJobRequest {
        scheduler_for(&fixture.storage, root)
            .admit(&request(
                root,
                session,
                command,
                tool_call_id,
                confined_policy(),
                None,
            ))
            .await
            .expect("admission ran")
            .expect("a single Cargo verification command is coordinated")
            .0
    }

    /// Wait until at least `expected` attempt rows exist. A session's first
    /// identity capture shells out and hashes a toolchain, so a fixed sleep
    /// would race it.
    async fn wait_for_jobs(storage: &Storage, expected: usize) {
        for _ in 0..400 {
            if storage.verification_job_count_for_tests().await.unwrap() as usize >= expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("expected at least {expected} verification job rows");
    }

    /// Wait until `job_id` has `expected` sessions attached to it.
    async fn wait_for_subscriptions(storage: &Storage, job_id: i64, expected: u32) {
        for _ in 0..400 {
            if storage
                .active_verification_subscribers(job_id)
                .await
                .unwrap()
                >= expected
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("expected {expected} subscribers on verification job {job_id}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn identical_requests_run_one_process_and_share_the_result() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let sessions = [
            fixture.start_session().await,
            fixture.start_session().await,
            fixture.start_session().await,
        ];
        let executions = Arc::new(AtomicUsize::new(0));

        let first = {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(
                &root,
                sessions[0],
                COMMAND,
                "call-a",
                confined_policy(),
                None,
            );
            let executions = executions.clone();
            tokio::spawn(async move {
                scheduler
                    .run(
                        &request,
                        None,
                        fake_execution(executions, Duration::from_millis(250)),
                    )
                    .await
            })
        };
        // Wait for the first session to claim before the others arrive, so the
        // test exercises joining rather than a slow first identity capture.
        wait_for_jobs(&fixture.storage, 1).await;

        let followers = join_all(sessions[1..].iter().enumerate().map(|(index, session)| {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(
                &root,
                *session,
                COMMAND,
                &format!("call-{index}"),
                confined_policy(),
                None,
            );
            let executions = executions.clone();
            async move {
                scheduler
                    .run(&request, None, fake_execution(executions, Duration::ZERO))
                    .await
            }
        }))
        .await;

        let produced = first.await.expect("producer task").expect("scheduler ran");
        assert!(
            matches!(produced, SharedVerification::Produced { .. }),
            "{produced:?}"
        );
        for follower in followers {
            let follower = follower.expect("follower ran");
            assert!(
                matches!(follower, SharedVerification::Reused { .. }),
                "an identical check must reuse the live attempt: {follower:?}"
            );
        }
        assert_eq!(
            executions.load(Ordering::SeqCst),
            1,
            "identical checks must execute once"
        );
        assert_eq!(
            fixture
                .storage
                .verification_job_count_for_tests()
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_privileged_producer_never_supplies_evidence_to_a_confined_subscriber() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let owner = fixture.start_session().await;
        let other = fixture.start_session().await;
        let executions = Arc::new(AtomicUsize::new(0));

        let first = {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(&root, owner, COMMAND, "call-a", escaped_policy(), None);
            let executions = executions.clone();
            tokio::spawn(async move {
                scheduler
                    .run(
                        &request,
                        None,
                        fake_execution(executions, Duration::from_millis(250)),
                    )
                    .await
            })
        };
        wait_for_jobs(&fixture.storage, 1).await;

        let second = scheduler_for(&fixture.storage, &root)
            .run(
                &request(&root, other, COMMAND, "call-b", confined_policy(), None),
                None,
                fake_execution(executions.clone(), Duration::ZERO),
            )
            .await
            .expect("scheduler ran");

        let first = first.await.expect("producer task").expect("scheduler ran");
        assert!(matches!(first, SharedVerification::Produced { .. }));
        assert!(
            matches!(second, SharedVerification::Produced { .. }),
            "a confined subscriber must produce its own evidence: {second:?}"
        );
        assert_eq!(executions.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn incompatible_cargo_jobs_sharing_a_target_directory_serialize() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        // A genuinely separate connection: coordination must not depend on one
        // process's memory.
        let second_storage = Storage::open_at(fixture.storage.db_path().to_path_buf())
            .await
            .expect("second connection");
        let first_session = fixture.start_session().await;
        let second_session = second_storage
            .start_session(
                &root,
                "anthropic",
                "claude-sonnet-4-5",
                ReasoningSelection::default(),
            )
            .await
            .expect("second session");
        assert_eq!(
            canonical_target_directory(&root, &root),
            canonical_target_directory(&root, &root),
            "both jobs must resolve one artifact directory"
        );

        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let executions = Arc::new(AtomicUsize::new(0));
        let run = |scheduler: VerificationScheduler,
                   request: VerificationRunRequest,
                   delay: Duration,
                   in_flight: Arc<AtomicUsize>,
                   peak: Arc<AtomicUsize>,
                   executions: Arc<AtomicUsize>| async move {
            scheduler
                .run(&request, None, async move {
                    executions.fetch_add(1, Ordering::SeqCst);
                    let live = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(live, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(FakeOutcome {
                        exit_code: Some(0),
                        body: "ok".to_string(),
                    })
                })
                .await
        };

        let first = tokio::spawn(run(
            scheduler_for(&fixture.storage, &root),
            request(
                &root,
                first_session,
                COMMAND,
                "call-a",
                confined_policy(),
                None,
            ),
            Duration::from_millis(250),
            in_flight.clone(),
            peak.clone(),
            executions.clone(),
        ));
        wait_for_jobs(&fixture.storage, 1).await;
        let second = tokio::spawn(run(
            scheduler_for(&second_storage, &root),
            request(
                &root,
                second_session,
                OTHER_COMMAND,
                "call-b",
                confined_policy(),
                None,
            ),
            Duration::from_millis(10),
            in_flight.clone(),
            peak.clone(),
            executions.clone(),
        ));

        let first = first.await.expect("first task").expect("scheduler ran");
        let second = second.await.expect("second task").expect("scheduler ran");
        assert!(matches!(first, SharedVerification::Produced { .. }));
        match second {
            SharedVerification::Produced { provenance, .. } => assert_eq!(
                provenance.state,
                SharedVerificationState::WaitedProduced,
                "the incompatible job must wait for the lane rather than race it"
            ),
            other => panic!("expected a queued producer, got {other:?}"),
        }
        assert_eq!(executions.load(Ordering::SeqCst), 2);
        assert_eq!(
            peak.load(Ordering::SeqCst),
            1,
            "two Cargo jobs must never share one target directory concurrently"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_workspace_that_moved_while_queued_declines_shared_evidence() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        std::fs::create_dir_all(root.join("src")).expect("source tree");
        std::fs::write(root.join("src/lib.rs"), "pub fn one() {}\n").expect("seed source");
        let owner = fixture.start_session().await;
        let follower = fixture.start_session().await;
        let executions = Arc::new(AtomicUsize::new(0));

        let first = {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(&root, owner, COMMAND, "call-a", confined_policy(), None);
            let executions = executions.clone();
            tokio::spawn(async move {
                scheduler
                    .run(
                        &request,
                        None,
                        fake_execution(executions, Duration::from_millis(400)),
                    )
                    .await
            })
        };
        wait_for_jobs(&fixture.storage, 1).await;

        let follower_run = {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(&root, follower, COMMAND, "call-b", confined_policy(), None);
            let executions = executions.clone();
            tokio::spawn(async move {
                scheduler
                    .run(&request, None, fake_execution(executions, Duration::ZERO))
                    .await
            })
        };
        let job_id = fixture
            .storage
            .verification_job_ids_for_tests()
            .await
            .unwrap()
            .first()
            .copied()
            .expect("the attempt was admitted");
        wait_for_subscriptions(&fixture.storage, job_id, 2).await;
        std::fs::write(root.join("src/changed.rs"), "pub fn two() {}\n")
            .expect("mid-flight workspace change");

        let first = first.await.expect("producer task").expect("scheduler ran");
        let follower = follower_run
            .await
            .expect("follower task")
            .expect("scheduler ran");
        assert!(matches!(first, SharedVerification::Produced { .. }));
        assert!(
            matches!(follower, SharedVerification::Declined),
            "a moved workspace must not reuse the earlier attempt: {follower:?}"
        );
        assert_eq!(
            executions.load(Ordering::SeqCst),
            1,
            "only the producer ran; the follower declined instead of reusing"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_subscriber_detaching_cancels_the_producer() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let session = fixture.start_session().await;
        let token = CancellationToken::new();
        let scheduler = scheduler_for(&fixture.storage, &root);
        let request = request(
            &root,
            session,
            COMMAND,
            "call-a",
            confined_policy(),
            Some(token.clone()),
        );

        let handle = tokio::spawn(async move {
            scheduler
                .run(&request, None, async move {
                    token.cancelled().await;
                    Ok(FakeOutcome {
                        exit_code: None,
                        body: String::new(),
                    })
                })
                .await
        });
        wait_for_jobs(&fixture.storage, 1).await;
        let job_id = fixture
            .storage
            .verification_job_ids_for_tests()
            .await
            .unwrap()
            .first()
            .copied()
            .expect("the check was admitted");

        let remaining = fixture
            .storage
            .cancel_verification_subscription(job_id, session, "call-a")
            .await
            .unwrap();
        assert_eq!(remaining, 0);

        let outcome = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the producer must stop once no subscriber needs it")
            .expect("producer task")
            .expect("scheduler ran");
        assert!(
            matches!(outcome, SharedVerification::Interrupted { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            fixture
                .storage
                .verification_job_state_for_tests(job_id)
                .await
                .unwrap(),
            Some(VerificationJobState::Cancelled)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_producer_interrupts_its_followers_instead_of_racing_them() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let owner = fixture.start_session().await;
        let follower = fixture.start_session().await;
        let executions = Arc::new(AtomicUsize::new(0));

        // The owner claims the attempt, then dies: its lease lapses and the pid
        // it recorded belongs to a process that is gone.
        let job_request = admitted(&fixture, &root, owner, COMMAND, "call-a").await;
        let job_id = match fixture
            .storage
            .claim_verification_job(&job_request)
            .await
            .unwrap()
        {
            VerificationJobClaim::Started(id) => id,
            other => panic!("expected a fresh attempt, got {other:?}"),
        };
        // A second session attaches and waits *before* the producer dies, so the
        // follower observes the wake-up path rather than starting a new attempt.
        let follower_task = {
            let scheduler = scheduler_for(&fixture.storage, &root);
            let request = request(&root, follower, COMMAND, "call-b", confined_policy(), None);
            let executions = executions.clone();
            tokio::spawn(async move {
                scheduler
                    .run(&request, None, fake_execution(executions, Duration::ZERO))
                    .await
            })
        };
        wait_for_subscriptions(&fixture.storage, job_id, 2).await;
        fixture
            .storage
            .expire_verification_job_for_tests(job_id, i64::from(i32::MAX))
            .await
            .unwrap();

        let outcome = tokio::time::timeout(Duration::from_secs(10), follower_task)
            .await
            .expect("a dead producer must wake its followers")
            .expect("follower task")
            .expect("scheduler ran");

        assert!(
            matches!(outcome, SharedVerification::Interrupted { .. }),
            "{outcome:?}"
        );
        assert_eq!(
            executions.load(Ordering::SeqCst),
            0,
            "an interrupted attempt must not be read as a pass or silently rerun"
        );
        assert_eq!(
            fixture
                .storage
                .verification_job_state_for_tests(job_id)
                .await
                .unwrap(),
            Some(VerificationJobState::Interrupted)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retention_expires_unreferenced_attempts_and_keeps_referenced_ones() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let session = fixture.start_session().await;

        let kept_request = admitted(&fixture, &root, session, COMMAND, "call-a").await;
        let kept = match fixture
            .storage
            .claim_verification_job(&kept_request)
            .await
            .unwrap()
        {
            VerificationJobClaim::Started(id) => id,
            other => panic!("expected a fresh attempt, got {other:?}"),
        };
        assert!(
            fixture
                .storage
                .publish_verification_job_result(
                    kept,
                    session,
                    &VerificationJobPayload {
                        exit_code: Some(0),
                        timed_out: false,
                        confined: false,
                        body: "ok".to_string(),
                        truncated: false,
                    },
                )
                .await
                .unwrap()
        );

        let dropped_request = admitted(&fixture, &root, session, OTHER_COMMAND, "call-b").await;
        let dropped = match fixture
            .storage
            .claim_verification_job(&dropped_request)
            .await
            .unwrap()
        {
            VerificationJobClaim::Started(id) => id,
            other => panic!("expected a fresh attempt, got {other:?}"),
        };
        assert!(
            fixture
                .storage
                .publish_verification_job_result(
                    dropped,
                    session,
                    &VerificationJobPayload {
                        exit_code: Some(0),
                        timed_out: false,
                        confined: false,
                        body: "ok".to_string(),
                        truncated: false,
                    },
                )
                .await
                .unwrap()
        );

        fixture
            .storage
            .link_verification_shared_job(kept, session, "call-a", &kept_request.job_key, "digest")
            .await
            .unwrap();
        let stale = now_ms() - VERIFICATION_JOB_RETENTION_MS - 60_000;
        fixture
            .storage
            .age_verification_job_for_tests(kept, stale)
            .await
            .unwrap();
        fixture
            .storage
            .age_verification_job_for_tests(dropped, stale)
            .await
            .unwrap();

        let expired = fixture
            .storage
            .expire_verification_jobs(VERIFICATION_JOB_RETENTION_MS)
            .await
            .unwrap();
        assert_eq!(expired, 1, "only the unreferenced attempt may expire");
        assert_eq!(
            fixture
                .storage
                .verification_job_state_for_tests(kept)
                .await
                .unwrap(),
            Some(VerificationJobState::Completed),
            "an attempt a session's evidence references is retained"
        );
        assert_eq!(
            fixture
                .storage
                .verification_job_state_for_tests(dropped)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            fixture
                .storage
                .linked_shared_job_count_for_tests(session)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn secret_environment_values_never_reach_the_job_row() {
        let fixture = TestStorage::new().await;
        let root = fixture.project_path().to_path_buf();
        let session = fixture.start_session().await;
        let executions = Arc::new(AtomicUsize::new(0));

        scheduler_for(&fixture.storage, &root)
            .run(
                &request(&root, session, COMMAND, "call-a", confined_policy(), None),
                None,
                fake_execution(executions, Duration::ZERO),
            )
            .await
            .expect("scheduler ran");

        let stored = fixture
            .storage
            .verification_job_documents_for_tests()
            .await
            .unwrap();
        let mut checked = 0;
        for value in identity::current_secret_environment().values() {
            if value.len() < 8 {
                continue;
            }
            checked += 1;
            assert!(
                !stored.contains(value.as_str()),
                "a secret environment value reached the persisted job row"
            );
        }
        // The digest is always present, so the row is keyed even without secrets.
        assert!(!stored.is_empty());
        let _ = checked;
    }
}
