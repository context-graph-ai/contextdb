//! What one maintenance cycle reads. Sealing and packing copy slots and read no row (design
//! 1078, 1793): the difference across the cycle in `Database::work_done()`, the store's totals.

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
fn a_cycle_reads_no_row_to_seal_or_pack() {
    let folder = support::folder("cycle-reads");
    let db = support::open(&folder);
    support::declare_docs(&db, 768, "INDEXING_THRESHOLD 300");
    // Fewer vectors than a block holds: their commit seals none, and one cycle seals them all.
    support::load_docs(&db, 1..=100, "a", |n| support::gathered(n, 768));
    let (read, report) = one_cycle(&db);
    assert_eq!(
        (read, count(report.maintenance_vectors_done)),
        (0, 100),
        "sealing copies the 100 vectors' slots and reads no row: rows read, vectors sealed"
    );
    // An IVF made due: a cycle of its build copies the slots it seals or packs, reading no row.
    support::load_docs(&db, 101..=400, "a", |n| support::gathered(n, 768));
    let mut done = 0;
    for ran in 1..=1000 {
        let (read, report) = one_cycle(&db);
        assert_eq!(
            read, 0,
            "cycle {ran} sealed or packed by copying slots, yet read rows"
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
