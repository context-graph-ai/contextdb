//! A commit's durable write must cost work proportional to its rows.
//!
//! One transaction of N rows (an import file, a bulk insert) is committed at
//! N = 1,024, 2,048, 4,096 and 8,192. Two exact work counters are read around
//! each commit, on the committing thread:
//!
//! * pending manifest deltas examined while each row's version membership is
//!   overlaid on the unpublished deltas of the rows before it;
//! * opens, closes, deletes and relocations of historical-root-protected
//!   tables, each of which makes redb finalize the checksums of every page the
//!   transaction has dirtied in the tables closed since the previous one.
//!
//! Doubling the rows may at most double each counter (plus a fixed allowance
//! for per-table work). Wall time is printed as a fact and never asserted.

use crate::persistence::content_identity::version_manifest::commit_row_work;
use crate::{Database, MaintenancePolicy};
use contextdb_core::Value;
use std::collections::HashMap;

const ROW_COUNTS: [usize; 4] = [1_024, 2_048, 4_096, 8_192];
const FIXED_ALLOWANCE: u64 = 1_024;

#[derive(Debug, Clone, Copy)]
struct CommitWork {
    rows: usize,
    overlay_entries: u64,
    protected_root_events: u64,
}

fn commit_one_transaction(rows: usize) -> CommitWork {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = Database::open(root.path().join("rows.db")).expect("open the database");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE imported (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
         payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .expect("declare the imported table");
    db.execute(
        "CREATE INDEX imported_context ON imported (context_id, id)",
        &HashMap::new(),
    )
    .expect("declare the context index");
    let tx = db.begin().expect("one transaction for the whole import");
    for row in 0..rows {
        db.execute_in_tx(
            tx,
            "INSERT INTO imported VALUES ($id, $context, $payload)",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(0x1aa0 << 112 | row as u128))),
                (
                    "context".into(),
                    Value::Uuid(uuid::Uuid::from_u128(0xc0de << 112 | (row % 2) as u128)),
                ),
                ("payload".into(), Value::Text(format!("{row:08x}").repeat(64))),
            ]),
        )
        .expect("stage one imported row");
    }
    commit_row_work::take_overlay_entries();
    commit_row_work::take_protected_root_events();
    let started = std::time::Instant::now();
    db.commit(tx).expect("commit the import");
    let wall = started.elapsed();
    let work = CommitWork {
        rows,
        overlay_entries: commit_row_work::take_overlay_entries(),
        protected_root_events: commit_row_work::take_protected_root_events(),
    };
    eprintln!("commit_row_scaling {work:?} wall={wall:?}");
    let committed = db
        .execute("SELECT id FROM imported", &HashMap::new())
        .expect("read the committed rows");
    assert_eq!(committed.rows.len(), rows, "every staged row is committed");
    db.close().expect("close the database");
    work
}

#[test]
fn a_one_transaction_import_commits_with_work_linear_in_its_rows() {
    let mut previous: Option<CommitWork> = None;
    for rows in ROW_COUNTS {
        let work = commit_one_transaction(rows);
        assert!(
            work.overlay_entries >= rows as u64,
            "every committed row consults the pending manifest overlay: {work:?}"
        );
        if let Some(previous) = previous {
            for (name, before, after) in [
                ("overlay entries", previous.overlay_entries, work.overlay_entries),
                (
                    "protected-table root events",
                    previous.protected_root_events,
                    work.protected_root_events,
                ),
            ] {
                assert!(
                    after <= before.saturating_mul(2).saturating_add(FIXED_ALLOWANCE),
                    "{name} grew faster than the rows: {previous:?} -> {work:?}"
                );
            }
        }
        previous = Some(work);
    }
}
