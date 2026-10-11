//! Aggregate inspection-batching telemetry (issue #143).
//!
//! Per-turn diagnostics already live in `usage_turns.inspection_batch_json`: one
//! record per parent turn that emitted an inspection, carrying how the turn's
//! calls were planned and executed, how the turn was classified, and the
//! run-local singleton streak the batching guard was tracking. That is enough to
//! compute the issue's acceptance metrics — singleton share, streak lengths, and
//! the excess turns a long serial streak cost — without a separate study
//! harness, and to say *why* each remaining streak happened.
//!
//! Streaks are reconstructed from `singleton_streak` (the guard's own counter,
//! which already means "consecutive turns that each carried exactly one cheap
//! inspection") rather than from planned widths, because the counter also
//! survives turns that emitted nothing. A value that is not exactly one more
//! than its predecessor starts a new run, so a gap cannot merge two streaks.

use super::*;
use crate::agent::{InspectionBatchClass, InspectionBatchDiagnostics};

/// Cohort window for the `/usage` report: the batching metrics describe recent
/// emission behaviour, and a bounded window also keeps the unindexed predicate
/// off the whole table.
pub(crate) const INSPECTION_COHORT_WINDOW_MS: i64 = 30 * 24 * 60 * 60 * 1_000;

/// Start of the `/usage` inspection cohort, relative to `now_ms`.
pub(crate) fn inspection_cohort_since_ms(now_ms: i64) -> i64 {
    now_ms.saturating_sub(INSPECTION_COHORT_WINDOW_MS)
}

/// Aggregated inspection-batching telemetry over one cohort of turns.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InspectionBatchingStats {
    /// Parent turns carrying inspection diagnostics.
    pub(crate) turns: usize,
    /// Diagnostics that could not be decoded. Excluded from every other number
    /// here rather than silently counted as batched work.
    pub(crate) unreadable_turns: usize,
    /// Execution groups: one per batch a turn planned or ran.
    pub(crate) groups: usize,
    /// Groups that ran (or were planned) as a single call.
    pub(crate) singleton_groups: usize,
    /// Longest run of consecutive single-cheap-inspection turns.
    pub(crate) longest_singleton_streak: usize,
    /// Runs of at least three such turns.
    pub(crate) streaks_of_three_or_more: usize,
    /// Turns in those runs beyond the first two: what a batchable plan saves.
    pub(crate) excess_serial_turns: usize,
    /// Serials the model explained as depending on earlier output.
    pub(crate) declared_dependent: usize,
    /// Serials the model declared independent but still emitted alone.
    pub(crate) declared_independent: usize,
    /// Turns where the batching advisory fired.
    pub(crate) advisories: usize,
    /// Turn counts per classification, in enum order.
    pub(crate) classification: Vec<(InspectionBatchClass, usize)>,
}

impl InspectionBatchingStats {
    /// Share of execution groups that ran as a single call.
    pub(crate) fn singleton_share_percent(&self) -> Option<u8> {
        if self.groups == 0 {
            return None;
        }
        let percent = self.singleton_groups.saturating_mul(100) / self.groups;
        // `singleton_groups` never exceeds `groups` in a real fold, so this is a
        // genuine percentage; the conversion is the single guard against a
        // hand-built aggregate overflowing it.
        Some(u8::try_from(percent).unwrap_or(100))
    }

    /// Serial turns with no explanation at all: the avoidable, unlabelled
    /// remainder that declarations and the advisory exist to shrink.
    pub(crate) fn unexplained_serials(&self) -> usize {
        self.count(InspectionBatchClass::Unexplained)
    }

    /// Turn count for one classification.
    pub(crate) fn count(&self, classification: InspectionBatchClass) -> usize {
        self.classification
            .iter()
            .find(|(class, _)| *class == classification)
            .map_or(0, |(_, count)| *count)
    }

    /// Whether the cohort recorded anything worth rendering.
    pub(crate) fn has_data(&self) -> bool {
        self.turns > 0
    }
}

/// Fold one cohort's per-turn diagnostics into the acceptance metrics.
///
/// `rows` must be ordered by (session, turn) so runs reconstruct correctly, and
/// carries the session id because a streak never spans two sessions.
pub(crate) fn summarize(rows: &[(i64, InspectionBatchDiagnostics)]) -> InspectionBatchingStats {
    let mut stats = InspectionBatchingStats::default();
    let mut previous: Option<(i64, usize)> = None;
    for (session_id, row) in rows {
        stats.turns = stats.turns.saturating_add(1);
        let widths = if row.actual_widths.is_empty() {
            &row.planned_widths
        } else {
            &row.actual_widths
        };
        stats.groups = stats.groups.saturating_add(widths.len());
        stats.singleton_groups = stats
            .singleton_groups
            .saturating_add(widths.iter().filter(|width| **width == 1).count());
        match stats
            .classification
            .iter_mut()
            .find(|(class, _)| *class == row.classification)
        {
            Some((_, count)) => *count = count.saturating_add(1),
            None => stats.classification.push((row.classification, 1)),
        }
        if row.dependency_reason.is_some() {
            match row.classification {
                InspectionBatchClass::Dependent => {
                    stats.declared_dependent = stats.declared_dependent.saturating_add(1);
                }
                InspectionBatchClass::IndependentSerial => {
                    stats.declared_independent = stats.declared_independent.saturating_add(1);
                }
                _ => {}
            }
        }
        if row.advisory_emitted {
            stats.advisories = stats.advisories.saturating_add(1);
        }

        // Streak reconstruction. `streak` is the guard's counter for this turn,
        // so it is also the run's length as of this turn.
        let streak = row.singleton_streak;
        stats.longest_singleton_streak = stats.longest_singleton_streak.max(streak);
        let continued = previous.is_some_and(|(session, previous_streak)| {
            session == *session_id && streak == previous_streak.saturating_add(1)
        });
        if streak >= 3 {
            // A run reaches its third turn exactly once, whether we watched it
            // grow one turn at a time or joined it already in progress.
            if !continued || streak == 3 {
                stats.streaks_of_three_or_more = stats.streaks_of_three_or_more.saturating_add(1);
            }
            // Every turn past the second is an excess turn: a run we join in
            // progress owes the whole `streak - 2` so far, and after that one
            // per turn.
            if continued {
                stats.excess_serial_turns = stats.excess_serial_turns.saturating_add(1);
            } else {
                stats.excess_serial_turns = stats.excess_serial_turns.saturating_add(streak - 2);
            }
        }
        previous = Some((*session_id, streak));
    }
    stats
        .classification
        .sort_by_key(|(class, _)| class_rank(*class));
    stats
}

/// Presentation order for the classification breakdown.
fn class_rank(classification: InspectionBatchClass) -> u8 {
    match classification {
        InspectionBatchClass::Batched => 0,
        InspectionBatchClass::Dependent => 1,
        InspectionBatchClass::IndependentSerial => 2,
        InspectionBatchClass::ConflictForced => 3,
        InspectionBatchClass::Unexplained => 4,
    }
}

impl Storage {
    /// Aggregate parent-lane inspection batching for the cohort of task-session
    /// turns at or after `since_ms` (all of them when `None`).
    ///
    /// Subagent and self-review lanes are excluded: the issue measures the
    /// parent's emission behaviour, and a child's own guard would otherwise mix
    /// its streaks into the parent's.
    ///
    /// Callers should pass a window. The predicate has no index of its own and
    /// every matched row is JSON-decoded, so "all of history" is a deliberate
    /// audit query, not a dashboard default.
    pub(crate) async fn inspection_batching_stats(
        &self,
        since_ms: Option<i64>,
    ) -> Result<InspectionBatchingStats> {
        // Two literal queries rather than one built string: sqlx requires the
        // statement to be static, and an audited dynamic string here would buy
        // nothing but an injection-shaped hole.
        let rows = if let Some(since_ms) = since_ms {
            sqlx::query(
                "SELECT ut.session_id AS session_id, \
                 ut.inspection_batch_json AS diagnostics \
                 FROM usage_turns ut JOIN sessions s ON s.id = ut.session_id \
                 WHERE ut.inspection_batch_json IS NOT NULL \
                   AND ut.lane_kind = 'parent' AND s.kind = 'task' \
                   AND ut.created_at_ms >= ? \
                 ORDER BY ut.session_id, ut.seq",
            )
            .bind(since_ms)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                "SELECT ut.session_id AS session_id, \
                 ut.inspection_batch_json AS diagnostics \
                 FROM usage_turns ut JOIN sessions s ON s.id = ut.session_id \
                 WHERE ut.inspection_batch_json IS NOT NULL \
                   AND ut.lane_kind = 'parent' AND s.kind = 'task' \
                 ORDER BY ut.session_id, ut.seq",
            )
            .fetch_all(&self.pool)
            .await
        }
        .context("Failed to load inspection batching telemetry")?;

        let mut decoded = Vec::with_capacity(rows.len());
        let mut unreadable = 0_usize;
        for row in rows {
            let session_id: i64 = row.try_get("session_id")?;
            let raw: String = row.try_get("diagnostics")?;
            match serde_json::from_str::<InspectionBatchDiagnostics>(&raw) {
                Ok(diagnostics) => decoded.push((session_id, diagnostics)),
                Err(error) => {
                    unreadable = unreadable.saturating_add(1);
                    tracing::debug!(
                        session_id,
                        error = %error,
                        "skipping undecodable inspection batching diagnostics"
                    );
                }
            }
        }
        let mut stats = summarize(&decoded);
        stats.unreadable_turns = unreadable;
        Ok(stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_utils::TestStorage;

    /// One diagnostics record with the given plan and guard counter.
    fn row(
        classification: InspectionBatchClass,
        actual_widths: &[usize],
        singleton_streak: usize,
    ) -> InspectionBatchDiagnostics {
        InspectionBatchDiagnostics {
            emitted_calls: actual_widths.iter().sum(),
            planned_widths: actual_widths.to_vec(),
            actual_widths: actual_widths.to_vec(),
            classification,
            dependency_reason: None,
            singleton_streak,
            advisory_emitted: false,
        }
    }

    fn singles(session: i64, streaks: &[usize]) -> Vec<(i64, InspectionBatchDiagnostics)> {
        streaks
            .iter()
            .map(|streak| {
                (
                    session,
                    row(InspectionBatchClass::Unexplained, &[1], *streak),
                )
            })
            .collect()
    }

    #[test]
    fn an_empty_cohort_reports_nothing_rather_than_zero_percent() {
        let stats = summarize(&[]);
        assert!(!stats.has_data());
        assert_eq!(stats.singleton_share_percent(), None);
        assert_eq!(stats.excess_serial_turns, 0);
    }

    #[test]
    fn batched_only_cohort_has_no_serial_turns() {
        let stats = summarize(&[(1, row(InspectionBatchClass::Batched, &[3], 0))]);
        assert_eq!(stats.groups, 1);
        assert_eq!(stats.singleton_groups, 0);
        assert_eq!(stats.singleton_share_percent(), Some(0));
        assert_eq!(stats.longest_singleton_streak, 0);
        assert_eq!(stats.streaks_of_three_or_more, 0);
    }

    #[test]
    fn a_five_turn_streak_costs_three_excess_turns_and_counts_once() {
        let stats = summarize(&singles(1, &[1, 2, 3, 4, 5]));
        assert_eq!(stats.turns, 5);
        assert_eq!(stats.groups, 5);
        assert_eq!(stats.singleton_share_percent(), Some(100));
        assert_eq!(stats.longest_singleton_streak, 5);
        assert_eq!(stats.streaks_of_three_or_more, 1);
        assert_eq!(stats.excess_serial_turns, 3);
        assert_eq!(stats.unexplained_serials(), 5);
    }

    #[test]
    fn a_multi_call_turn_splits_two_streaks() {
        let mut rows = singles(1, &[1, 2]);
        rows.push((1, row(InspectionBatchClass::Batched, &[2], 0)));
        rows.extend(singles(1, &[1, 2, 3]));
        let stats = summarize(&rows);
        assert_eq!(stats.longest_singleton_streak, 3);
        assert_eq!(stats.streaks_of_three_or_more, 1);
        assert_eq!(stats.excess_serial_turns, 1);
        assert_eq!(stats.singleton_groups, 5);
        assert_eq!(stats.groups, 6);
    }

    #[test]
    fn a_streak_never_spans_two_sessions() {
        let mut rows = singles(1, &[1, 2]);
        rows.extend(singles(2, &[3]));
        let stats = summarize(&rows);
        // Session 2 starts by joining a run already three long, which still
        // counts as one streak of at least three rather than merging with the
        // two-turn run that ended the previous session.
        assert_eq!(stats.streaks_of_three_or_more, 1);
        assert_eq!(stats.excess_serial_turns, 1);
        assert_eq!(stats.longest_singleton_streak, 3);
    }

    #[test]
    fn declarations_and_advisories_are_attributed_by_class() {
        let mut dependent = row(InspectionBatchClass::Dependent, &[1], 1);
        dependent.dependency_reason = Some("the path came from the previous read".to_string());
        let mut independent = row(InspectionBatchClass::IndependentSerial, &[1], 2);
        independent.dependency_reason = Some("known sibling crates".to_string());
        let mut advertised = row(InspectionBatchClass::Unexplained, &[1], 3);
        advertised.advisory_emitted = true;
        let stats = summarize(&[
            (1, dependent),
            (1, independent),
            (1, advertised),
            (1, row(InspectionBatchClass::ConflictForced, &[1, 1], 0)),
        ]);
        assert_eq!(stats.declared_dependent, 1);
        assert_eq!(stats.declared_independent, 1);
        assert_eq!(stats.advisories, 1);
        assert_eq!(stats.count(InspectionBatchClass::ConflictForced), 1);
        assert_eq!(stats.unexplained_serials(), 1);
        // Enum order, so a report renders the same sequence every time.
        assert_eq!(
            stats.classification.first().map(|(class, _)| *class),
            Some(InspectionBatchClass::Dependent)
        );
    }

    #[test]
    fn planned_widths_stand_in_before_a_turn_executes() {
        let mut pending = row(InspectionBatchClass::Batched, &[2], 0);
        pending.planned_widths = vec![1, 1];
        pending.actual_widths.clear();
        let stats = summarize(&[(1, pending)]);
        assert_eq!(stats.groups, 2);
        assert_eq!(stats.singleton_groups, 2);
    }

    #[tokio::test]
    async fn the_cohort_query_scopes_to_parent_task_turns() {
        let fixture = TestStorage::new().await;
        let session = fixture.start_session().await;
        fixture
            .storage
            .start_task_run(session, None, "Measure inspection batching")
            .await
            .unwrap();
        for (seq, streak) in [(1_i64, 1_usize), (2, 2), (3, 3)] {
            insert_diagnostics(
                &fixture,
                session,
                seq,
                "parent",
                &row(InspectionBatchClass::Unexplained, &[1], streak),
            )
            .await;
        }
        // A child lane must not enter the cohort: its own guard tracks its own
        // streaks, which would otherwise inflate the parent's longest run.
        insert_diagnostics(
            &fixture,
            session,
            4,
            "subagent",
            &row(InspectionBatchClass::Unexplained, &[1], 9),
        )
        .await;

        let stats = fixture
            .storage
            .inspection_batching_stats(None)
            .await
            .unwrap();
        assert_eq!(stats.turns, 3);
        assert_eq!(stats.longest_singleton_streak, 3);
        assert_eq!(stats.excess_serial_turns, 1);
        assert_eq!(stats.streaks_of_three_or_more, 1);
        assert_eq!(stats.unreadable_turns, 0);

        // A cohort window after every row means no cohort at all.
        let empty = fixture
            .storage
            .inspection_batching_stats(Some(now_ms() + 1_000))
            .await
            .unwrap();
        assert!(!empty.has_data());
    }

    async fn insert_diagnostics(
        fixture: &TestStorage,
        session: SessionId,
        seq: i64,
        lane_kind: &str,
        diagnostics: &InspectionBatchDiagnostics,
    ) {
        let json = serde_json::to_string(diagnostics).unwrap();
        let created_at_ms = now_ms() + seq;
        sqlx::query(
            "INSERT INTO usage_turns (session_id, seq, lane_kind, lane_id, lane_seq, status, \
             reasoning_chars, provider_attempts_json, tool_schema_names_json, rewrite_kind, \
             created_at_ms, inspection_executed, inspection_reused, inspection_rejected, \
             inspection_returned_chars, inspection_avoided_chars, delegated_parent_overlap, \
             inspection_batch_json) \
             VALUES (?, ?, ?, 'local', ?, 'reported', 0, '[]', '[]', 'none', ?, 0, 0, 0, 0, 0, 0, ?)",
        )
        .bind(session.as_i64())
        .bind(seq)
        .bind(lane_kind)
        .bind(seq)
        .bind(created_at_ms)
        .bind(json)
        .execute(&fixture.storage.pool)
        .await
        .unwrap();
    }

    #[test]
    fn a_row_predating_a_field_still_decodes() {
        // Migration safety: adding a field to the diagnostics must not turn every
        // historical row into "no data" on the dashboard. This row is missing
        // `classification` entirely, and decodes as the honest default.
        let historical = r#"{"emitted_calls":1,"planned_widths":[1],"actual_widths":[1],
            "dependency_reason":null,"singleton_streak":3,"advisory_emitted":false}"#;
        let decoded: InspectionBatchDiagnostics = serde_json::from_str(historical).unwrap();
        assert_eq!(decoded.classification, InspectionBatchClass::Unexplained);
        assert_eq!(decoded.singleton_streak, 3);
        let stats = summarize(&[(1, decoded)]);
        assert_eq!(stats.turns, 1);
        assert_eq!(stats.longest_singleton_streak, 3);
    }

    #[test]
    fn the_cohort_window_is_trailing() {
        let now = 1_700_000_000_000;
        let since = inspection_cohort_since_ms(now);
        assert_eq!(now - since, INSPECTION_COHORT_WINDOW_MS);
        assert!(since < now);
    }
}
