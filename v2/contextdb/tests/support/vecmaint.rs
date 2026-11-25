//! What the vector maintenance, memory and partition tests read off a cycle's report, one copy
//! for the three. Each says `#[path = "support/vecmaint.rs"] mod vecmaint;`.

use contextdb::VectorMaintenanceReport;

use crate::support::count;

/// What one cycle reports: the vectors it sealed or packed and the vectors still unindexed.
pub fn done_left(report: &VectorMaintenanceReport) -> (u64, u64) {
    (
        count(report.maintenance_vectors_done),
        count(report.maintenance_vectors_remaining),
    )
}
