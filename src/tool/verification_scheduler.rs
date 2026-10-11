//! Tool-facing handle for the cross-process verification scheduler.
//!
//! Mirrors [`crate::tool::WorkspaceLockContext`]: a Bash tool either carries a
//! live scheduler bound to the shared store and the session the runtime is
//! currently serving, or it is `disabled` and runs every check ordinarily. The
//! session is resolved per call because a runtime can switch sessions without
//! rebuilding its tool registry.

use std::path::PathBuf;
use std::sync::Arc;

use crate::storage::{SessionId, Storage};
use crate::tool::SharedActiveSessionId;
use crate::verification_scheduler::VerificationScheduler;

#[derive(Debug, Clone)]
pub(crate) struct VerificationSchedulerContext {
    inner: Option<VerificationSchedulerContextInner>,
}

#[derive(Debug, Clone)]
struct VerificationSchedulerContextInner {
    scheduler: Arc<VerificationScheduler>,
    active_session_id: SharedActiveSessionId,
}

impl VerificationSchedulerContext {
    /// Keep every Bash tool on the plain execution path.
    pub(crate) fn disabled() -> Self {
        Self { inner: None }
    }

    pub(crate) fn new(
        project_root: PathBuf,
        storage: Storage,
        active_session_id: SharedActiveSessionId,
    ) -> Self {
        Self {
            inner: Some(VerificationSchedulerContextInner {
                scheduler: Arc::new(VerificationScheduler::new(storage, project_root)),
                active_session_id,
            }),
        }
    }

    /// A live scheduler and the session it must act for, or `None` when the
    /// scheduler is disabled or no session is active yet.
    pub(crate) async fn scheduler(&self) -> Option<(Arc<VerificationScheduler>, SessionId)> {
        let inner = self.inner.as_ref()?;
        let session_id = (*inner.active_session_id.lock().await)?;
        Some((inner.scheduler.clone(), session_id))
    }
}
