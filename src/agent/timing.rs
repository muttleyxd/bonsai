//! The agent's handle on the run timing ledger (issue #146).
//!
//! [`crate::timing`] owns the interval arithmetic and
//! [`crate::storage::timing`](crate::storage) owns its durability; this module is
//! the small agent-side surface between them: re-anchor the run clock at the
//! start of a task run, and hand the increments to storage when the run reaches
//! a checkpoint.

use super::Agent;
use crate::timing::{SharedTimingLedger, TimingCheckpoint};

impl Agent {
    /// Install the runtime's shared ledger.
    ///
    /// The runtime owns ledger creation because two producers need the same one:
    /// the agent records model, tool, and child intervals, while the interaction
    /// service records the permission and user waits that happen inside tool
    /// execution.
    ///
    /// An agent built outside the runtime (a test, an eval harness, a delegated
    /// child) keeps the private ledger its builder created. That fallback is
    /// deliberate — such a run must still be able to record intervals — but its
    /// checkpoints are not the authoritative ones: only the runtime-built agent's
    /// ledger is the one the interaction service also writes to.
    pub(crate) fn set_timing_ledger(&mut self, timing: SharedTimingLedger) {
        self.timing = timing;
    }

    /// Re-anchor the run clock at the start of a task run.
    pub(crate) fn begin_timing_run(&self) {
        self.timing.begin_run();
    }

    /// Close anything still open and return this run's increments since the
    /// previous checkpoint.
    ///
    /// Cancellation and failure paths reach a checkpoint with intervals still
    /// open. Closing them here is what makes "every terminal task accounts for
    /// its elapsed interval" true instead of nearly true, and closing is
    /// idempotent, so a second checkpoint cannot count the same interval twice.
    pub(crate) fn take_timing_checkpoint(&self) -> TimingCheckpoint {
        self.timing.finish();
        self.timing.take_checkpoint()
    }
}
