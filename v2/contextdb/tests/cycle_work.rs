//! What one maintenance cycle reads. A camera box keeps up with 500 rows a second for a day only
//! when a cycle reads each row it seals or packs once and no other row: the difference across
//! the cycle in `Database::work_done()`, the store's running totals since it opened.

mod support;
use support::count;

use contextdb::Database;

/// Runs one cycle and gives the rows it read, by the store's totals, and its report.
fn one_cycle(db: &Database) -> (u64, contextdb::VectorMaintenanceReport) {
    let read = || db.work_done().expect("a writer is answered").rows_read;
    let before = read();
    let report = db.run_maintenance_cycle().expect("a maintenance cycle");
    (read() - before, report.vector)
}

#[test]
fn a_cycle_reads_each_row_it_seals_once() {
    let folder = support::folder("cycle-reads");
    let db = support::open(&folder);
    support::declare_docs(&db, 768, "INDEXING_THRESHOLD 300");
    // Fewer vectors than a block holds: their commit seals none, and one cycle seals them all.
    support::load_docs(&db, 1..=100, "a", |n| support::gathered(n, 768));
    let (read, report) = one_cycle(&db);
    assert_eq!(
        (read, count(report.maintenance_vectors_done)),
        (100, 100),
        "the rows read by the cycle that seals the 100 vectors left, and the vectors it sealed"
    );
    // An IVF made due: each cycle of its build reads the rows it seals or packs, once each.
    support::load_docs(&db, 101..=400, "a", |n| support::gathered(n, 768));
    let mut done = 0;
    for ran in 1..=1000 {
        let (read, report) = one_cycle(&db);
        assert_eq!(
            read,
            count(report.maintenance_vectors_done),
            "the rows cycle {ran} of the build read, against the vectors it sealed or packed"
        );
        done += count(report.maintenance_vectors_done);
        if count(report.maintenance_vectors_remaining) == 0
            && count(report.remaining_partitions) == 0
        {
            assert!(done >= 400, "the build packed {done} of the 400 vectors");
            return support::close(db);
        }
    }
    panic!("1,000 cycles and the build had work left");
}
