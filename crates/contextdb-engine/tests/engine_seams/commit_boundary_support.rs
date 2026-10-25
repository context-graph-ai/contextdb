//! Test support for reads that wait at the commit boundary: a bounded read
//! that declares the routing allowance bounding that wait, the typed refusal
//! for a wait that outlasts it, and mechanism-neutral events from the
//! boundary itself. Each function is bound to the engine once the engine
//! offers the surface it names; until then it reports what is missing.

use contextdb_core::read_contract::ReadFailure;
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::sync::Arc;

/// Run a bounded read whose wait for the commit boundary is bounded by
/// `routing_retry_ms`, the routing allowance the read declares, and never by
/// its active time.
pub fn execute_declaring_routing_allowance(
    database: &Database,
    request: &bounded::BoundedReadRequest,
    routing_retry_ms: u64,
) -> Result<bounded::TestResult, bounded::TestError> {
    let _ = (database, request, routing_retry_ms);
    unimplemented!(
        "a bounded read cannot yet declare the routing allowance that bounds its commit-boundary wait"
    )
}

/// Whether `failure` is the typed refusal for a commit-boundary wait that
/// outlasted the read's declared routing allowance.
pub fn is_commit_boundary_wait_exceeded(failure: &ReadFailure) -> bool {
    let _ = failure;
    unimplemented!("reads have no typed refusal for an exceeded commit-boundary wait yet")
}

/// Who the commit boundary was given to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryParty {
    Reader,
    /// The committer numbered by [`as_committer`].
    Committer(usize),
}

/// What the commit boundary reports, whatever mechanism it uses to share
/// itself between readers and committers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryEvent {
    /// A reader could not take the boundary and is waiting for it.
    ReaderWaiting,
    /// The numbered committer could not take the boundary and is waiting.
    CommitterWaiting(usize),
    /// The boundary was given to this party.
    Admitted(BoundaryParty),
}

/// Report every commit-boundary event of `database`, from any thread, to
/// `observer` while `operation` runs.
pub fn observe_commit_boundary<T>(
    database: &Database,
    observer: Arc<dyn Fn(BoundaryEvent) + Send + Sync>,
    operation: impl FnOnce() -> T,
) -> T {
    let _ = (database, observer, operation);
    unimplemented!("the commit boundary reports no reader or committer events yet")
}

/// Run `operation` on this thread as committer `number`, so the boundary's
/// events name the commits it makes.
pub fn as_committer<T>(number: usize, operation: impl FnOnce() -> T) -> T {
    let _ = (number, operation);
    unimplemented!("committers cannot yet be named in commit-boundary events")
}
