//! A graph edge derived from a SQL row belongs to exactly that row version
//! while it is staged, stamped, reassigned, superseded and committed.
//!
//! Promise (Policy 12): an authoritative erasure of one row removes the graph
//! artifacts that row owns and nothing a sibling row owns. Two rows between the
//! same nodes with the same edge type are two owners, so the transaction's
//! write set must keep them as two entries, move each entry and its owner to
//! the commit's transaction and LSN together, leave the before-image owner of a
//! deletion exactly as it was, and drop the edge a superseded staging of the
//! same row version created.
//!
//! Reason (2026-10-16): the write set kept one edge per source, target and type
//! with no owner, so sibling rows could not be told apart once staged. No test
//! stages a row-owned edge, so a stamp or reassignment that updated only the
//! entry or only the owner would pass every existing check.

#[path = "support/read_memory.rs"]
mod test_read_memory;

use contextdb_core::NodeId as Uuid;
use contextdb_core::{AdjEntry, Lsn, RowId, TxId, Value, VersionedRow};
use contextdb_tx::{OwnedAdjEntry, RowEdgeOwner, TransactionManager, WriteSet, WriteSetApplicator};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct MockStore {
    applied: Mutex<Vec<WriteSet>>,
    next_row_id: AtomicU64,
}

impl WriteSetApplicator for MockStore {
    fn apply(
        &self,
        ws: &WriteSet,
        _read_memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> contextdb_core::Result<()> {
        self.applied.lock().push(ws.clone());
        Ok(())
    }

    fn new_row_id(&self) -> RowId {
        RowId(self.next_row_id.fetch_add(1, Ordering::SeqCst))
    }
}

struct Triad {
    source: Uuid,
    target: Uuid,
}

fn triad() -> Triad {
    Triad {
        source: Uuid::from_u128(1),
        target: Uuid::from_u128(2),
    }
}

fn owner(row_id: u64, created_tx: u64, lsn: u64) -> RowEdgeOwner {
    RowEdgeOwner {
        table: "edges".to_string(),
        table_generation: 1,
        row_id: RowId(row_id),
        created_tx: TxId(created_tx),
        lsn: Lsn(lsn),
    }
}

fn owned(triad: &Triad, owner: RowEdgeOwner, tag: &str) -> OwnedAdjEntry {
    OwnedAdjEntry {
        entry: AdjEntry {
            source: triad.source,
            target: triad.target,
            edge_type: "CITES".to_string(),
            properties: HashMap::from([("tag".to_string(), Value::Text(tag.to_string()))]),
            created_tx: owner.created_tx,
            deleted_tx: None,
            lsn: owner.lsn,
        },
        owner,
    }
}

fn tag(entry: &OwnedAdjEntry) -> &Value {
    &entry.entry.properties["tag"]
}

fn versioned_row(row_id: u64, tx: u64) -> VersionedRow {
    VersionedRow {
        row_id: RowId(row_id),
        values: HashMap::new(),
        created_tx: TxId(tx),
        deleted_tx: None,
        lsn: Lsn(0),
        created_at: None,
    }
}

fn memory() -> Arc<test_read_memory::TestReadMemory> {
    Arc::new(test_read_memory::TestReadMemory::default())
}

#[test]
fn a_write_set_with_only_a_row_owned_edge_is_not_empty() {
    let triad = triad();
    let mut ws = WriteSet::new();
    assert!(ws.is_empty());
    ws.row_adj_inserts.push(owned(&triad, owner(1, 5, 0), "a"));
    assert!(!ws.is_empty());
    let mut deletes = WriteSet::new();
    deletes
        .row_adj_deletes
        .push((owned(&triad, owner(1, 3, 3), "a"), TxId(5)));
    assert!(!deletes.is_empty());
}

#[test]
fn stamping_a_commit_lsn_moves_each_owner_and_its_entry_together() {
    let triad = triad();
    let mut ws = WriteSet::new();
    ws.row_adj_inserts
        .push(owned(&triad, owner(1, 5, 0), "first"));
    ws.row_adj_inserts
        .push(owned(&triad, owner(2, 5, 0), "second"));
    ws.stamp_lsn(Lsn(77));
    assert_eq!(ws.row_adj_inserts.len(), 2, "siblings stay two entries");
    for entry in &ws.row_adj_inserts {
        assert_eq!(entry.owner.lsn, Lsn(77), "owner lsn");
        assert_eq!(entry.entry.lsn, Lsn(77), "entry lsn");
    }
    assert_ne!(ws.row_adj_inserts[0].owner, ws.row_adj_inserts[1].owner);
}

#[test]
fn reassigning_the_transaction_moves_each_sibling_owner_and_entry_together() {
    let triad = triad();
    let mut ws = WriteSet::new();
    ws.row_adj_inserts
        .push(owned(&triad, owner(1, 5, 0), "first"));
    ws.row_adj_inserts
        .push(owned(&triad, owner(2, 5, 0), "second"));
    ws.row_adj_inserts
        .push(owned(&triad, owner(3, 4, 0), "earlier"));
    ws.reassign_tx(TxId(5), TxId(9));
    let moved: Vec<_> = ws
        .row_adj_inserts
        .iter()
        .map(|entry| {
            (
                entry.owner.row_id,
                entry.owner.created_tx,
                entry.entry.created_tx,
            )
        })
        .collect();
    assert_eq!(
        moved,
        vec![
            (RowId(1), TxId(9), TxId(9)),
            (RowId(2), TxId(9), TxId(9)),
            (RowId(3), TxId(4), TxId(4)),
        ],
        "only the reassigned transaction's siblings move, each owner with its entry"
    );
}

#[test]
fn reassigning_a_deleting_transaction_leaves_the_before_image_owner_alone() {
    let triad = triad();
    let before = owned(&triad, owner(1, 3, 3), "before");
    let mut ws = WriteSet::new();
    ws.row_adj_deletes.push((before.clone(), TxId(5)));
    ws.reassign_tx(TxId(5), TxId(9));
    ws.stamp_lsn(Lsn(77));
    let (kept, deleting) = &ws.row_adj_deletes[0];
    assert_eq!(*deleting, TxId(9), "the deleting transaction is reassigned");
    assert_eq!(
        *kept, before,
        "the deleted version's owner and entry are exactly what the store holds"
    );
}

#[test]
fn a_deletion_whose_before_image_was_created_by_the_reassigned_transaction_keeps_its_owner() {
    // The row version being deleted was created in an earlier transaction; a
    // reassignment of the deleting transaction must not match it by number.
    let triad = triad();
    let before = owned(&triad, owner(1, 5, 4), "before");
    let mut ws = WriteSet::new();
    ws.row_adj_deletes.push((before.clone(), TxId(5)));
    ws.reassign_tx(TxId(5), TxId(9));
    let (kept, deleting) = &ws.row_adj_deletes[0];
    assert_eq!(*deleting, TxId(9));
    assert_eq!(
        kept.owner, before.owner,
        "the stored owner is the selection key and never rewritten by a reassignment"
    );
    assert_eq!(kept.entry.created_tx, before.entry.created_tx);
}

#[test]
fn canonicalizing_drops_the_edge_a_superseded_staging_of_the_same_row_created() {
    let triad = triad();
    let mut ws = WriteSet::new();
    ws.relational_inserts
        .push(("edges".to_string(), versioned_row(1, 5)));
    ws.relational_inserts
        .push(("edges".to_string(), versioned_row(1, 5)));
    ws.relational_inserts
        .push(("edges".to_string(), versioned_row(2, 5)));
    ws.row_adj_inserts
        .push(owned(&triad, owner(1, 5, 0), "superseded"));
    ws.row_adj_inserts
        .push(owned(&triad, owner(2, 5, 0), "sibling"));
    ws.row_adj_inserts
        .push(owned(&triad, owner(1, 5, 0), "final"));
    ws.canonicalize_final_state();
    let kept: Vec<_> = ws
        .row_adj_inserts
        .iter()
        .map(|entry| (entry.owner.row_id, tag(entry).clone()))
        .collect();
    assert_eq!(
        kept,
        vec![
            (RowId(2), Value::Text("sibling".into())),
            (RowId(1), Value::Text("final".into())),
        ],
        "one edge per row version: the superseded staging is gone, the sibling stays"
    );
}

#[test]
fn canonicalizing_keeps_every_edge_of_rows_that_were_staged_once() {
    let triad = triad();
    let mut ws = WriteSet::new();
    for row in 1..=3_u64 {
        ws.relational_inserts
            .push(("edges".to_string(), versioned_row(row, 5)));
        ws.row_adj_inserts
            .push(owned(&triad, owner(row, 5, 0), "kept"));
    }
    ws.canonicalize_final_state();
    assert_eq!(ws.row_adj_inserts.len(), 3);
}

#[test]
fn a_commit_hands_the_applicator_each_sibling_stamped_with_the_commit() {
    let triad = triad();
    let txm = TransactionManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.row_adj_inserts
            .push(owned(&triad, owner(1, tx.0, 0), "first"));
        ws.row_adj_inserts
            .push(owned(&triad, owner(2, tx.0, 0), "second"));
    })
    .unwrap();
    txm.commit(tx, memory())
        .expect("a write set with only owned edges commits");
    let applied = txm.store().applied.lock();
    assert_eq!(
        applied.len(),
        1,
        "the owned-edge-only commit reached the applicator"
    );
    let entries = &applied[0].row_adj_inserts;
    assert_eq!(entries.len(), 2);
    let lsn = applied[0].commit_lsn.expect("a commit lsn");
    for entry in entries {
        assert_eq!(entry.owner.lsn, lsn);
        assert_eq!(entry.entry.lsn, lsn);
        assert_eq!(entry.owner.created_tx, entry.entry.created_tx);
        assert!(entry.owner.created_tx >= tx);
    }
    assert_ne!(entries[0].owner, entries[1].owner);
}

#[test]
fn a_rolled_back_transaction_hands_the_applicator_no_owned_edge() {
    let triad = triad();
    let txm = TransactionManager::new(MockStore {
        next_row_id: AtomicU64::new(1),
        ..Default::default()
    });
    let tx = txm.begin();
    txm.with_write_set(tx, |ws| {
        ws.row_adj_inserts
            .push(owned(&triad, owner(1, tx.0, 0), "first"));
        ws.row_adj_deletes
            .push((owned(&triad, owner(2, 1, 1), "before"), tx));
    })
    .unwrap();
    let returned = txm.rollback_write_set(tx).expect("roll back");
    assert_eq!(
        returned.row_adj_inserts.len(),
        1,
        "the staged edge is handed back, not applied"
    );
    assert!(txm.store().applied.lock().is_empty());
    assert!(txm.commit(tx, memory()).is_err(), "the transaction is gone");
}
