//! A maintenance loop that never settles fails with its cycle count and the
//! last report instead of hanging the test binary.
use contextdb_engine::Database;

/// Runs maintenance cycles until no vector partition has build or replacement
/// work left. The bound is the cycle budget one caller-driven build of `rows`
/// single-row batches needs: ingestion, ten training passes, assignment and
/// seeding each take at most one cycle per row, three more cover the probe,
/// ingestion completion and publication, and one per row drains the sealed runs.
pub(crate) fn settle_vector_maintenance(db: &Database, rows: usize) {
    let bound = rows * (10 + 3) + rows + 3;
    for cycle in 1..=bound {
        let report = db
            .run_maintenance_cycle()
            .expect("one caller-driven maintenance cycle returns")
            .vector;
        if report.remaining_partitions == 0 {
            return;
        }
        assert!(
            cycle < bound,
            "maintenance did not complete within {bound} cycles: {report:?}"
        );
    }
}
