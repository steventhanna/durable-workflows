//! When the supervisor restarts a failed task and when it stops the runtime
//! instead (INVARIANTS §2.1). Pure state, no I/O: `supervise` feeds each
//! failure to the task's [`TaskRestarts`] and acts on the
//! [`RestartDecision`].

use std::{fmt, time::Duration};

use crate::error::Transience;

use super::RuntimeConfig;

/// Why a runtime task stopped. Decided once, where the failure is built
/// (`RuntimeTaskFailure::from(DurableError)` classifies through
/// `DurableError::transience`); the restart policy matches on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TaskFailureKind {
    /// The database rolled back or could not be reached. Restarted with
    /// backoff without spending the restart budget, up to
    /// `RuntimeConfig::max_transient_outage`.
    Transient(Transience),
    /// Any other error, or a task that returned on its own. Spends the
    /// restart budget.
    Fault,
    /// The task panicked. Spends the restart budget.
    Panic,
}

impl TaskFailureKind {
    pub(super) fn panicked(self) -> bool {
        match self {
            Self::Panic => true,
            Self::Transient(_) | Self::Fault => false,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Transient(Transience::RolledBack) => "transient_rolled_back",
            Self::Transient(Transience::Unavailable) => "transient_unavailable",
            Self::Fault => "fault",
            Self::Panic => "panic",
        }
    }
}

/// The restart bounds of [`RuntimeConfig`], read once per runtime.
#[derive(Debug, Clone, Copy)]
pub(super) struct RestartPolicy {
    initial_backoff: Duration,
    /// Never below `initial_backoff`.
    max_backoff: Duration,
    healthy_after: Duration,
    max_restarts: u32,
    window: Duration,
    max_transient_outage: Duration,
}

impl RestartPolicy {
    pub(super) fn new(config: &RuntimeConfig) -> Self {
        Self {
            initial_backoff: config.restart_backoff,
            // A cap below the initial delay keeps the delay fixed at
            // `restart_backoff`, the behavior before the cap existed.
            max_backoff: config.max_restart_backoff.max(config.restart_backoff),
            healthy_after: config.task_healthy_after,
            max_restarts: config.max_task_restarts,
            window: config.restart_window,
            max_transient_outage: config.max_transient_outage,
        }
    }
}

/// What the supervisor does with a failed task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RestartDecision {
    Restart {
        delay: Duration,
    },
    /// Cancel the whole runtime.
    Stop(StopReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StopReason {
    /// More than `max_task_restarts` faults or panics within `restart_window`.
    BudgetExhausted { max_restarts: u32, window: Duration },
    /// Transient failures with no healthy run between them for longer than
    /// `max_transient_outage`.
    TransientOutage { lasted: Duration, limit: Duration },
    /// The activity-execution manager failed; it owns the running
    /// activities and is never restarted.
    Unrestartable,
}

impl StopReason {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::BudgetExhausted { .. } => "restart_budget_exhausted",
            Self::TransientOutage { .. } => "transient_outage_exceeded",
            Self::Unrestartable => "unrestartable_task_failed",
        }
    }
}

impl fmt::Display for StopReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExhausted {
                max_restarts,
                window,
            } => write!(
                f,
                "more than {max_restarts} restarts after errors or panics within {window:?}"
            ),
            Self::TransientOutage { lasted, limit } => write!(
                f,
                "transient database errors for {lasted:?}, longer than the {limit:?} limit"
            ),
            Self::Unrestartable => write!(f, "a task that cannot be restarted failed"),
        }
    }
}

/// The restart state of one task.
#[derive(Debug)]
pub(super) struct TaskRestarts<I = tokio::time::Instant> {
    budget: RestartBudget<I>,
    backoff: RestartBackoff,
    /// The first transient failure since the task last ran healthy.
    outage_started: Option<I>,
}

impl<I: BudgetInstant> TaskRestarts<I> {
    pub(super) fn new(now: I, policy: &RestartPolicy) -> Self {
        Self {
            budget: RestartBudget::new(now),
            backoff: RestartBackoff::new(policy.initial_backoff),
            outage_started: None,
        }
    }

    /// Decides what follows a failure at `now` of a run that lasted
    /// `ran_for` (from the end of its restart delay). A run of at least
    /// `task_healthy_after` first resets the backoff and ends a transient
    /// outage. Then a transient failure checks the outage limit and a fault
    /// or panic spends the budget; only these two stop the runtime. Every
    /// restart waits the current backoff, which doubles up to the cap.
    pub(super) fn on_failure(
        &mut self,
        now: I,
        ran_for: Duration,
        kind: TaskFailureKind,
        policy: &RestartPolicy,
    ) -> RestartDecision {
        if ran_for >= policy.healthy_after {
            self.backoff = RestartBackoff::new(policy.initial_backoff);
            self.outage_started = None;
        }
        match kind {
            TaskFailureKind::Transient(_) => {
                let started = *self.outage_started.get_or_insert(now);
                let lasted = now.saturating_duration_since(started);
                if lasted > policy.max_transient_outage {
                    return RestartDecision::Stop(StopReason::TransientOutage {
                        lasted,
                        limit: policy.max_transient_outage,
                    });
                }
            }
            TaskFailureKind::Fault | TaskFailureKind::Panic => {
                if !self.budget.record(now, policy.window, policy.max_restarts) {
                    return RestartDecision::Stop(StopReason::BudgetExhausted {
                        max_restarts: policy.max_restarts,
                        window: policy.window,
                    });
                }
            }
        }
        RestartDecision::Restart {
            delay: self.backoff.advance(policy.max_backoff),
        }
    }
}

/// The delay before a task's next restart: starts at `restart_backoff` and
/// doubles after each restart, capped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RestartBackoff {
    next: Duration,
}

impl RestartBackoff {
    fn new(initial: Duration) -> Self {
        Self { next: initial }
    }

    /// Returns the delay for this restart and doubles the next one,
    /// saturating, up to `max`.
    fn advance(&mut self, max: Duration) -> Duration {
        let delay = self.next;
        self.next = self.next.saturating_add(self.next).min(max);
        delay
    }
}

/// Restarts of one task counted within a window that starts at the first
/// restart after the previous window ended.
#[derive(Debug)]
struct RestartBudget<I = tokio::time::Instant> {
    count: u32,
    window_started: I,
}

/// The clock a [`RestartBudget`] reads: `tokio::time::Instant` in the
/// runtime. Generic only so the Kani proof can use a model clock (Kani
/// cannot call `clock_gettime`, so it cannot build a real `Instant`).
pub(super) trait BudgetInstant: Copy {
    fn saturating_duration_since(self, earlier: Self) -> Duration;
}

impl BudgetInstant for tokio::time::Instant {
    fn saturating_duration_since(self, earlier: Self) -> Duration {
        tokio::time::Instant::saturating_duration_since(&self, earlier)
    }
}

impl<I: BudgetInstant> RestartBudget<I> {
    fn new(now: I) -> Self {
        Self {
            count: 0,
            window_started: now,
        }
    }

    /// Counts one restart at `now`; false once the window holds more than
    /// `max_restarts`.
    fn record(&mut self, now: I, window: Duration, max_restarts: u32) -> bool {
        if now.saturating_duration_since(self.window_started) > window {
            self.count = 0;
            self.window_started = now;
        }
        self.count = self.count.saturating_add(1);
        self.count <= max_restarts
    }
}

/// Kani proofs (`cargo kani`; CLAUDE.md, "Bounded model checking").
#[cfg(kani)]
mod restart_budget_verification {
    use super::*;

    /// A model clock: the time since an arbitrary origin.
    #[derive(Debug, Clone, Copy, PartialEq)]
    struct ModelInstant(Duration);

    impl BudgetInstant for ModelInstant {
        fn saturating_duration_since(self, earlier: Self) -> Duration {
            self.0.saturating_sub(earlier.0)
        }
    }

    /// Any `Duration`, built without the division `Duration::from_nanos`
    /// needs (a 64-bit division makes the proof take minutes).
    fn any_duration() -> Duration {
        let nanos: u32 = kani::any();
        kani::assume(nanos < 1_000_000_000);
        Duration::new(kani::any(), nanos)
    }

    /// `later > earlier + span`, computed on (seconds, nanoseconds) pairs
    /// with an explicit carry; no multiplication, which SAT solvers handle
    /// badly when they must prove it equal to `Duration` arithmetic.
    fn exceeds(later: Duration, earlier: Duration, span: Duration) -> bool {
        let nanos = earlier.subsec_nanos() + span.subsec_nanos();
        let carry = nanos >= 1_000_000_000;
        let end_nanos = if carry { nanos - 1_000_000_000 } else { nanos };
        let end_secs =
            u128::from(earlier.as_secs()) + u128::from(span.as_secs()) + u128::from(carry);
        let later_secs = u128::from(later.as_secs());
        later_secs > end_secs || (later_secs == end_secs && later.subsec_nanos() > end_nanos)
    }

    /// For any budget state, restart time, window and limit: a restart
    /// more than `window` after the window start opens a new window with a
    /// count of 1; otherwise the count grows by one, saturating, so it
    /// never wraps to 0; the result is `count <= max_restarts`. A restart
    /// time before the window start (the clock is monotonic, but the model
    /// allows it) counts in the current window.
    #[kani::proof]
    fn restart_budget_resets_after_the_window_and_never_wraps() {
        let started = any_duration();
        let now = any_duration();
        let window = any_duration();
        let count: u32 = kani::any();
        let max_restarts: u32 = kani::any();
        let mut budget = RestartBudget {
            count,
            window_started: ModelInstant(started),
        };

        let allowed = budget.record(ModelInstant(now), window, max_restarts);

        if exceeds(now, started, window) {
            assert!(budget.count == 1);
            assert!(budget.window_started == ModelInstant(now));
        } else {
            assert!(budget.count == count.saturating_add(1));
            assert!(budget.window_started == ModelInstant(started));
        }
        assert!(budget.count >= 1);
        assert!(allowed == (budget.count <= max_restarts));
    }

    /// For any delay at or below the cap: a restart waits the current delay;
    /// the next one never shrinks, never exceeds the cap, and is either the
    /// cap or exactly double. No panic on overflow (saturating).
    #[kani::proof]
    fn restart_backoff_doubles_up_to_the_cap() {
        let next = any_duration();
        let max = any_duration();
        kani::assume(next <= max);
        let mut backoff = RestartBackoff { next };

        let delay = backoff.advance(max);

        assert!(delay == next);
        assert!(backoff.next >= next);
        assert!(backoff.next <= max);
        assert!(backoff.next == max || next.checked_add(next) == Some(backoff.next));
    }

    /// For any restart state, policy, time and run length: a transient
    /// failure never touches the restart budget, and stops the runtime only
    /// when its outage has lasted longer than `max_transient_outage`; a
    /// fault or panic stops it only when the budget refuses the restart. A
    /// restart's delay never exceeds the backoff cap.
    #[kani::proof]
    fn transient_failures_never_spend_the_restart_budget() {
        let max_backoff = any_duration();
        let initial_backoff = any_duration();
        kani::assume(initial_backoff <= max_backoff);
        let policy = RestartPolicy {
            initial_backoff,
            max_backoff,
            healthy_after: any_duration(),
            max_restarts: kani::any(),
            window: any_duration(),
            max_transient_outage: any_duration(),
        };
        let next = any_duration();
        kani::assume(next <= max_backoff);
        let count: u32 = kani::any();
        let window_started = ModelInstant(any_duration());
        let outage_started = if kani::any() {
            Some(ModelInstant(any_duration()))
        } else {
            None
        };
        let mut restarts = TaskRestarts {
            budget: RestartBudget {
                count,
                window_started,
            },
            backoff: RestartBackoff { next },
            outage_started,
        };
        let now = ModelInstant(any_duration());
        let ran_for = any_duration();
        let transient: bool = kani::any();
        let kind = if transient {
            TaskFailureKind::Transient(Transience::Unavailable)
        } else if kani::any() {
            TaskFailureKind::Fault
        } else {
            TaskFailureKind::Panic
        };

        let decision = restarts.on_failure(now, ran_for, kind, &policy);

        if transient {
            assert!(restarts.budget.count == count);
            assert!(restarts.budget.window_started == window_started);
            let started = restarts.outage_started.expect("an outage is open");
            let stopped = matches!(decision, RestartDecision::Stop(_));
            assert!(stopped == exceeds(now.0, started.0, policy.max_transient_outage));
        } else {
            assert!(restarts.outage_started == outage_started || ran_for >= policy.healthy_after);
            assert!(
                matches!(decision, RestartDecision::Stop(_))
                    == (restarts.budget.count > policy.max_restarts)
            );
        }
        if let RestartDecision::Restart { delay } = decision {
            assert!(delay <= max_backoff);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UNAVAILABLE: TaskFailureKind = TaskFailureKind::Transient(Transience::Unavailable);

    fn policy() -> RestartPolicy {
        RestartPolicy::new(
            &RuntimeConfig::default()
                .with_restart_backoff(Duration::from_secs(1))
                .with_max_restart_backoff(Duration::from_secs(60))
                .with_task_healthy_after(Duration::from_secs(120))
                .with_max_task_restarts(2)
                .with_restart_window(Duration::from_secs(600))
                .with_max_transient_outage(Duration::from_secs(900)),
        )
    }

    fn delay(decision: RestartDecision) -> Duration {
        match decision {
            RestartDecision::Restart { delay } => delay,
            RestartDecision::Stop(reason) => panic!("expected a restart, got a stop: {reason}"),
        }
    }

    #[test]
    fn restart_budget_counts_within_the_window_and_resets_after_it() {
        let window = Duration::from_secs(600);
        let start = tokio::time::Instant::now();
        let mut budget = RestartBudget::new(start);
        assert!(budget.record(start, window, 2));
        assert!(budget.record(start + Duration::from_secs(300), window, 2));
        assert!(
            !budget.record(start + window, window, 2),
            "a third restart inside the window exceeds a budget of 2"
        );

        let later = start + window + Duration::from_secs(1);
        assert!(
            budget.record(later, window, 2),
            "a restart after the window starts a new count"
        );
        assert!(budget.record(later + Duration::from_secs(1), window, 2));
        assert!(!budget.record(later + Duration::from_secs(2), window, 2));
    }

    #[test]
    fn restart_backoff_doubles_from_the_initial_delay_and_stops_at_the_cap() {
        let policy = policy();
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        let delays: Vec<u64> = (0..9)
            .map(|_| delay(restarts.on_failure(start, Duration::ZERO, UNAVAILABLE, &policy)))
            .map(|delay| delay.as_secs())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 32, 60, 60, 60]);
    }

    #[test]
    fn restart_backoff_starts_over_after_a_healthy_run() {
        let policy = policy();
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        for _ in 0..4 {
            restarts.on_failure(start, Duration::ZERO, UNAVAILABLE, &policy);
        }
        assert_eq!(
            delay(restarts.on_failure(start, Duration::from_secs(119), UNAVAILABLE, &policy)),
            Duration::from_secs(16),
            "a run shorter than task_healthy_after keeps doubling"
        );
        assert_eq!(
            delay(restarts.on_failure(start, Duration::from_secs(120), UNAVAILABLE, &policy)),
            Duration::from_secs(1),
            "a healthy run starts the backoff over"
        );
    }

    #[test]
    fn a_cap_below_the_initial_backoff_keeps_the_delay_fixed() {
        let policy = RestartPolicy::new(
            &RuntimeConfig::default()
                .with_restart_backoff(Duration::from_secs(5))
                .with_max_restart_backoff(Duration::from_secs(1)),
        );
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        for _ in 0..3 {
            assert_eq!(
                delay(restarts.on_failure(start, Duration::ZERO, UNAVAILABLE, &policy)),
                Duration::from_secs(5)
            );
        }
    }

    #[test]
    fn transient_failures_do_not_spend_the_restart_budget() {
        let policy = policy();
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        // The incident: a database outage fails the task many more times than
        // `max_task_restarts` (2 here) within one window.
        for second in 0..100 {
            let kind = if second % 2 == 0 {
                UNAVAILABLE
            } else {
                TaskFailureKind::Transient(Transience::RolledBack)
            };
            let now = start + Duration::from_secs(second);
            assert!(matches!(
                restarts.on_failure(now, Duration::ZERO, kind, &policy),
                RestartDecision::Restart { .. }
            ));
        }
        assert_eq!(restarts.budget.count, 0);
        let now = start + Duration::from_secs(100);
        assert!(matches!(
            restarts.on_failure(now, Duration::ZERO, TaskFailureKind::Fault, &policy),
            RestartDecision::Restart { .. }
        ));
        assert!(matches!(
            restarts.on_failure(now, Duration::ZERO, TaskFailureKind::Panic, &policy),
            RestartDecision::Restart { .. }
        ));
        assert_eq!(
            restarts.on_failure(now, Duration::ZERO, TaskFailureKind::Fault, &policy),
            RestartDecision::Stop(StopReason::BudgetExhausted {
                max_restarts: 2,
                window: Duration::from_secs(600),
            }),
            "a third fault or panic in the window exceeds a budget of 2"
        );
    }

    #[test]
    fn a_transient_outage_longer_than_the_limit_stops_the_runtime() {
        let policy = policy();
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        let limit = Duration::from_secs(900);
        assert!(matches!(
            restarts.on_failure(start, Duration::ZERO, UNAVAILABLE, &policy),
            RestartDecision::Restart { .. }
        ));
        assert!(
            matches!(
                restarts.on_failure(start + limit, Duration::from_secs(30), UNAVAILABLE, &policy),
                RestartDecision::Restart { .. }
            ),
            "an outage of exactly the limit is still restarted"
        );
        let past = limit + Duration::from_secs(1);
        assert_eq!(
            restarts.on_failure(start + past, Duration::from_secs(30), UNAVAILABLE, &policy),
            RestartDecision::Stop(StopReason::TransientOutage {
                lasted: past,
                limit
            })
        );
    }

    #[test]
    fn a_healthy_run_ends_the_transient_outage() {
        let policy = policy();
        let start = tokio::time::Instant::now();
        let mut restarts = TaskRestarts::new(start, &policy);
        restarts.on_failure(start, Duration::ZERO, UNAVAILABLE, &policy);
        let later = start + Duration::from_secs(1_000);
        assert!(
            matches!(
                restarts.on_failure(later, Duration::from_secs(120), UNAVAILABLE, &policy),
                RestartDecision::Restart { .. }
            ),
            "the task ran healthy in between, so a new outage starts here"
        );
        assert_eq!(restarts.outage_started, Some(later));
    }
}
