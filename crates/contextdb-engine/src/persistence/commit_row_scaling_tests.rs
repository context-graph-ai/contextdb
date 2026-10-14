//! A commit's durable write must cost work proportional to its rows.
//!
//! One transaction of N rows (an import file, a bulk insert) is committed at
//! N = 1,024, 2,048, 4,096 and 8,192; one transaction deleting all of them is
//! committed at N = 512, 1,024, 2,048 and 4,096. (One 8,192-row delete of
//! these rows is refused by the writer's fixed 16 MiB source budget, on the
//! base revision as well; that limit is not this test's subject.) The import
//! is repeated with its keys staged in shuffled order, and with its rows
//! alternating between two tables (A1, B1, A2, B2, ...), both as an insert
//! and as one transaction updating every row in that alternating order
//! (N = 512 to 4,096 for the update). Two exact work counters are read around
//! each commit, on the committing thread:
//!
//! * pending manifest overlay comparisons: every comparison of a pending
//!   delta's coordinate made while each row's version membership is indexed
//!   and overlaid on the unpublished deltas of the rows before it, plus every
//!   rebalancing step of that index (the unindexed overlay of the base
//!   revision counts one per list element it examines, in the same unit);
//! * opens, closes, deletes and relocations of historical-root-protected
//!   tables, each of which makes redb finalize the checksums of every page the
//!   transaction has dirtied in the tables closed since the previous one.
//!
//! Doubling the rows from N to 2N may at most grow the overlay comparisons by
//! the N log N factor, 2N log2(2N) / (N log2 N), plus a fixed allowance: a
//! seek is one root-to-leaf descent of a balanced index, never a rescan of
//! the deltas already pending (which grows them 4x or 8x per doubling). Protected-table root events must not grow with the rows at
//! all: each one costs work proportional to the pages already dirtied, so one
//! per row (or one per change of table between consecutive rows) is
//! quadratic. Wall time is printed as a fact and never asserted.

use crate::persistence::content_identity::version_manifest::commit_row_work;
use crate::{Database, MaintenancePolicy};
use contextdb_core::Value;
use std::collections::HashMap;

const INSERT_ROW_COUNTS: [usize; 4] = [1_024, 2_048, 4_096, 8_192];
const DELETE_ROW_COUNTS: [usize; 4] = [512, 1_024, 2_048, 4_096];
const TABLES: [&str; 2] = ["imported", "mirrored"];
const FIXED_ALLOWANCE: u64 = 1_024;
const PROTECTED_ROOT_EVENT_ALLOWANCE: u64 = 64;

#[derive(Debug, Clone, Copy)]
struct CommitWork {
    rows: usize,
    overlay_comparisons: u64,
    protected_root_events: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Measured {
    Insert,
    Delete,
    ShuffledInsert,
    AlternatingInsert,
    AlternatingUpdate,
}

impl Measured {
    fn row_counts(self) -> [usize; 4] {
        match self {
            Measured::Insert | Measured::ShuffledInsert | Measured::AlternatingInsert => {
                INSERT_ROW_COUNTS
            }
            Measured::Delete | Measured::AlternatingUpdate => DELETE_ROW_COUNTS,
        }
    }

    /// The table row `row` is staged in.
    fn table(self, row: usize) -> &'static str {
        match self {
            Measured::AlternatingInsert | Measured::AlternatingUpdate => TABLES[row % 2],
            _ => TABLES[0],
        }
    }

    /// The primary key of row `row`; shuffled imports stage keys out of order.
    fn key(self, row: usize, rows: usize) -> Value {
        let ordinal = match self {
            // An odd multiplier permutes the power-of-two row range.
            Measured::ShuffledInsert => row.wrapping_mul(0x9e37_79b1) % rows,
            _ => row,
        };
        Value::Uuid(uuid::Uuid::from_u128(0x1aa0 << 112 | ordinal as u128))
    }
}

fn take_work(rows: usize) -> CommitWork {
    CommitWork {
        rows,
        overlay_comparisons: commit_row_work::take_overlay_comparisons(),
        protected_root_events: commit_row_work::take_protected_root_events(),
    }
}

fn commit_one_transaction(rows: usize, measured: Measured) -> CommitWork {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = Database::open(root.path().join("rows.db")).expect("open the database");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for table in TABLES {
        db.execute(
            &format!(
                "CREATE TABLE {table} (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
                 payload TEXT NOT NULL)"
            ),
            &HashMap::new(),
        )
        .expect("declare the imported table");
        db.execute(
            &format!("CREATE INDEX {table}_context ON {table} (context_id, id)"),
            &HashMap::new(),
        )
        .expect("declare the context index");
    }
    let tx = db.begin().expect("one transaction for the whole import");
    for row in 0..rows {
        db.execute_in_tx(
            tx,
            &format!(
                "INSERT INTO {} VALUES ($id, $context, $payload)",
                measured.table(row)
            ),
            &HashMap::from([
                ("id".into(), measured.key(row, rows)),
                (
                    "context".into(),
                    Value::Uuid(uuid::Uuid::from_u128(0xc0de << 112 | (row % 2) as u128)),
                ),
                (
                    "payload".into(),
                    Value::Text(format!("{row:08x}").repeat(64)),
                ),
            ]),
        )
        .expect("stage one imported row");
    }
    take_work(rows);
    let started = std::time::Instant::now();
    db.commit(tx).expect("commit the import");
    let mut wall = started.elapsed();
    let mut work = take_work(rows);
    let committed: usize = TABLES
        .iter()
        .map(|table| {
            db.execute(&format!("SELECT id FROM {table}"), &HashMap::new())
                .expect("read the committed rows")
                .rows
                .len()
        })
        .sum();
    assert_eq!(committed, rows, "every staged row is committed");
    if measured == Measured::Delete {
        let tx = db.begin().expect("one transaction for the whole delete");
        db.execute_in_tx(tx, "DELETE FROM imported", &HashMap::new())
            .expect("stage the bulk delete");
        take_work(rows);
        let started = std::time::Instant::now();
        db.commit(tx).expect("commit the bulk delete");
        wall = started.elapsed();
        work = take_work(rows);
        let remaining = db
            .execute("SELECT id FROM imported", &HashMap::new())
            .expect("read the remaining rows");
        assert!(remaining.rows.is_empty(), "every deleted row is gone");
    }
    if measured == Measured::AlternatingUpdate {
        let tx = db.begin().expect("one transaction for the whole update");
        for row in 0..rows {
            db.execute_in_tx(
                tx,
                &format!(
                    "UPDATE {} SET payload = $payload WHERE id = $id",
                    measured.table(row)
                ),
                &HashMap::from([
                    ("id".into(), measured.key(row, rows)),
                    (
                        "payload".into(),
                        Value::Text(format!("{:08x}", !row).repeat(64)),
                    ),
                ]),
            )
            .expect("stage one row update");
        }
        take_work(rows);
        let started = std::time::Instant::now();
        db.commit(tx).expect("commit the update");
        wall = started.elapsed();
        work = take_work(rows);
        let updated: usize = TABLES
            .iter()
            .map(|table| {
                db.execute(
                    &format!("SELECT id FROM {table} WHERE payload LIKE 'f%'"),
                    &HashMap::new(),
                )
                .expect("read the updated rows")
                .rows
                .len()
            })
            .sum();
        assert_eq!(updated, rows, "every staged update is committed");
    }
    eprintln!("commit_row_scaling {measured:?} {work:?} wall={wall:?}");
    db.close().expect("close the database");
    work
}

fn assert_linear(measured: Measured) {
    let mut previous: Option<CommitWork> = None;
    for rows in measured.row_counts() {
        let work = commit_one_transaction(rows, measured);
        assert!(
            work.overlay_comparisons >= rows as u64,
            "every committed row consults the pending manifest overlay: {work:?}"
        );
        if let Some(previous) = previous {
            let n_log_n = |rows: usize| rows as u128 * u128::from(rows.ilog2());
            let allowed = u128::from(previous.overlay_comparisons) * n_log_n(rows)
                / n_log_n(previous.rows)
                + u128::from(FIXED_ALLOWANCE);
            assert!(
                u128::from(work.overlay_comparisons) <= allowed,
                "overlay comparisons grew faster than N log N: {previous:?} -> {work:?}, \
                 allowed {allowed}"
            );
            assert!(
                work.protected_root_events
                    <= previous
                        .protected_root_events
                        .saturating_add(PROTECTED_ROOT_EVENT_ALLOWANCE),
                "protected-table root events grew with the rows: {previous:?} -> {work:?}"
            );
        }
        previous = Some(work);
    }
}

#[test]
fn a_one_transaction_import_commits_with_work_linear_in_its_rows() {
    assert_linear(Measured::Insert);
}

#[test]
fn a_one_transaction_bulk_delete_commits_with_work_linear_in_its_rows() {
    assert_linear(Measured::Delete);
}

#[test]
fn a_shuffled_one_transaction_import_commits_with_work_linear_in_its_rows() {
    assert_linear(Measured::ShuffledInsert);
}

#[test]
fn an_import_alternating_between_two_tables_opens_each_relation_once_per_commit() {
    assert_linear(Measured::AlternatingInsert);
}

#[test]
fn an_update_alternating_between_two_tables_opens_each_relation_once_per_commit() {
    assert_linear(Measured::AlternatingUpdate);
}

/// The indexed overlay answers every seek exactly as a rescan of the
/// publication-ordered list does: across duplicate coordinates, removals and
/// re-additions, coordinates that prefix one another, and inclusive and
/// exclusive bounds, after every delta of a deterministic sequence.
#[test]
fn the_indexed_pending_overlay_answers_every_seek_as_the_list_rescan_does() {
    use super::{NO_PENDING_NODE, PendingManifestMutations};
    use crate::persistence::content_identity::version_manifest::ManifestOverlay;
    use crate::persistence::content_identity::{ContentIdentityMutation, LogicalFamily};

    // A manifest key is the ordered-history magic, one kind byte, then the
    // coordinate; the overlay reads only the coordinate.
    const KEY_PREFIX: &[u8] = b"\xffcontextdb-history\0\x01";
    const ALPHABET: [u8; 4] = [0x00, 0x01, 0x7f, 0xff];
    let coordinates: Vec<Vec<u8>> = (1..=3)
        .flat_map(|length| {
            (0..ALPHABET.len().pow(length)).map(move |mut ordinal| {
                (0..length)
                    .map(|_| {
                        let byte = ALPHABET[ordinal % ALPHABET.len()];
                        ordinal /= ALPHABET.len();
                        byte
                    })
                    .collect()
            })
        })
        .collect();
    let mut probes = coordinates.clone();
    probes.push(Vec::new());
    let deltas = 1_500;
    let mut index = PendingManifestMutations {
        nodes: Vec::with_capacity(deltas),
        root: NO_PENDING_NODE,
        list: Vec::with_capacity(deltas),
    };
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for delta in 0..deltas {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let coordinate = &coordinates[(state % coordinates.len() as u64) as usize];
        let mut key = KEY_PREFIX.to_vec();
        key.extend_from_slice(coordinate);
        index.list.push(if (state >> 40) & 3 == 0 {
            ContentIdentityMutation::remove(LogicalFamily::ChangeAndDdl, key)
        } else {
            ContentIdentityMutation::put(LogicalFamily::ChangeAndDdl, key, [1])
        });
        index.index_last(delta as u32);
        // A balanced tree of n nodes is under 1.45 log2(n + 2) high.
        let n = (delta + 2) as f64;
        assert!(
            f64::from(index.height(index.root)) <= 1.45 * n.log2(),
            "the index stays balanced at {} deltas",
            delta + 1
        );
        if delta % 37 != 0 && delta + 1 != deltas {
            continue;
        }
        let list: &[ContentIdentityMutation] = &index.list;
        for probe in &probes {
            assert_eq!(
                index.latest_present(probe),
                list.latest_present(probe),
                "latest delta at {probe:?} after {} deltas",
                delta + 1
            );
            for exclusive in [false, true] {
                assert_eq!(
                    index.first_present_from(probe, exclusive),
                    list.first_present_from(probe, exclusive),
                    "first present from {probe:?} (exclusive {exclusive}) after {} deltas",
                    delta + 1
                );
            }
        }
    }
}
