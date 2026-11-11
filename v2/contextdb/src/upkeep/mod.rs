//! upkeep: background work in small steps, and the report of one cycle of it. A cycle seals,
//! which is moving the vectors that wait on a column's pending lists into blocks, and then
//! advances by one step every grouping the vector part has due or is building. The sealing is
//! staged here; `database` drives the steps, each through its one commit function.
//!
//! It never blocks a write or a query: every commit of a cycle holds a bounded amount of work,
//! and what a cycle has not done waits for the next. Nothing here starts a thread or reads a
//! clock; a cycle runs when it is called.

use crate::memory::Budget;
use crate::schema::Catalog;
use crate::store::ChangeSet;
use crate::types::Result;
use crate::vector::{self, Sealed};

/// The most vectors one cycle seals.
pub(crate) const SEAL_ROWS: usize = 4096;

/// What one cycle of upkeep did, as [`crate::Database::run_maintenance_cycle`] returns it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct MaintenanceReport {
    /// What the cycle did for vector search.
    pub vector: VectorMaintenanceReport,
}

/// The part of a [`MaintenanceReport`] about vector search.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct VectorMaintenanceReport {
    /// How many vectors this cycle moved from pending lists into blocks, added to how many
    /// it packed into the blocks of a grouping being built.
    pub maintenance_vectors_done: u64,
    /// How many vectors still wait after this cycle, over every column of the store: those on
    /// a pending list, those a build under way has not packed, and those of partitions whose
    /// grouping is due and not begun. Those of a stalled build are not counted: no further
    /// cycle packs them until it has the memory.
    pub maintenance_vectors_remaining: u64,
    /// How many partitions have a grouping due or being built after this cycle.
    pub remaining_partitions: u64,
    /// How many of those had their last step refused for want of memory. A further cycle does
    /// nothing for them until the memory limit is raised or memory is let go, and with no
    /// limit declared there are none. A host stops calling when `remaining_partitions` is no
    /// more than this and `maintenance_vectors_remaining` is zero.
    pub stalled_partitions: u64,
}

/// Stages the sealing of one cycle into `changes`, for the caller to commit, and says what it
/// staged: the vectors sealed and those still pending. The caller holds the commit lock from before `changes` was begun.
pub(crate) fn cycle(
    changes: &mut ChangeSet,
    catalog: &Catalog,
    budget: &Budget,
) -> Result<MaintenanceReport> {
    let Sealed { done, remaining } = vector::seal(changes, catalog, budget, SEAL_ROWS)?;
    Ok(MaintenanceReport {
        vector: VectorMaintenanceReport {
            maintenance_vectors_done: done,
            maintenance_vectors_remaining: remaining,
            ..VectorMaintenanceReport::default()
        },
    })
}
