//! upkeep: background work in small steps. This version has one kind of work, sealing: moving
//! the vectors that wait on a column's pending lists into blocks.
//!
//! It never blocks a write or a query: a cycle does a bounded amount of work in one commit, and
//! what it has not done waits for the next cycle. Nothing here starts a thread or reads a
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
    /// How many vectors this cycle moved from pending lists into blocks.
    pub maintenance_vectors_done: u64,
    /// How many vectors are still on a pending list after this cycle, over every column of the
    /// store. Zero means a further cycle has nothing to seal.
    pub maintenance_vectors_remaining: u64,
}

/// Stages one cycle of upkeep into `changes`, for the caller to commit, and says what it
/// staged. The caller holds the commit lock from before `changes` was begun.
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
        },
    })
}
