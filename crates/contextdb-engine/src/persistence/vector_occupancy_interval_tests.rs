//! CDB-120: the durable per-route occupancy-interval table answers every hold
//! exactly as a scan of the route's membership directory would, and its rows
//! stay maximal, non-adjacent and non-empty after every kind of membership
//! write (append, receive, load, replay, retire, shrink).
use super::*;
use crate::persistence::vector_membership_pages as pages;
use crate::persistence::{VectorPartitionMembershipRecord, VectorPartitionRecordKind};
use contextdb_core::{SnapshotId, VectorPartitionKey};
use std::collections::BTreeMap;

const OPEN: u64 = u64::MAX;

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}

fn route_key() -> VectorPartitionKey {
    VectorPartitionKey::from_components(vec![contextdb_core::VectorPartitionComponent::Text(
        "route".to_owned(),
    )])
}

fn record(row: u64, from: u64, to: Option<u64>) -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index: index(),
        partition_key: route_key(),
        row_id: RowId(row),
        row_created_tx: TxId(1),
        row_lsn: Lsn(row),
        vector_created_tx: TxId(1),
        vector_lsn: Lsn(row),
        visible_from: TxId(from),
        deleted_tx: to.map(TxId),
    }
}

fn route() -> Vec<u8> {
    RedbPersistence::vector_partition_prefix(&index(), &route_key())
}

/// What the test believes the directory holds: row -> (from, to).
type Model = BTreeMap<u64, (u64, Option<u64>)>;

struct Harness {
    dir: tempfile::TempDir,
    db: Option<ProofFixtureDatabase>,
}

impl Harness {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = ProofFixtureDatabase::create(dir.path().join("occupancy.db")).unwrap();
        let write = db.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        crate::persistence::content_size::initialize(&write).unwrap();
        crate::persistence::content_size::ensure_table(&write, "items").unwrap();
        write.commit().unwrap();
        Self { dir, db: Some(db) }
    }

    fn db(&self) -> &ProofFixtureDatabase {
        self.db.as_ref().unwrap()
    }

    /// Close and reopen the file, as a restart would.
    fn reopen(&mut self) {
        self.db = None;
        let path = self.dir.path().join("occupancy.db");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let owner = super::super::writer_file::LockedWriterFile::lock(file, path.parent()).unwrap();
        let database = redb::Database::builder()
            .create_with_backend(owner.view())
            .unwrap();
        self.db = Some(ProofFixtureDatabase { database, owner });
    }

    /// Stage a set of membership writes in one transaction. `Some` puts the
    /// record, `None` removes the row's membership. Returns the open write.
    fn stage(
        &self,
        ops: &[(u64, Option<VectorPartitionMembershipRecord>)],
    ) -> redb::WriteTransaction {
        let write = self.db().begin_write().unwrap();
        for (row, next) in ops {
            let identity = record(*row, 1, None);
            let key = RedbPersistence::vector_partition_membership_key(&identity);
            let encoded = next.as_ref().map(|record| {
                RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::Membership,
                    record,
                )
                .unwrap()
            });
            pages::publish_membership(&write, &key, encoded.as_deref(), None, true).unwrap();
        }
        write
    }

    fn commit(&self, ops: &[(u64, Option<VectorPartitionMembershipRecord>)], settle: bool) {
        let write = self.stage(ops);
        if settle {
            pages::settle_dirty_occupancy(&write, None).unwrap();
        }
        write.commit().unwrap();
    }

    /// The route's occupancy rows as committed.
    fn rows(&self) -> Vec<(u64, u64)> {
        let read = self.db().begin_read().unwrap();
        let Ok(table) = read.open_table(pages::OCCUPANCY) else {
            return Vec::new();
        };
        let route = route();
        let mut out = Vec::new();
        for item in table.prefix_bounded(route.as_slice()).unwrap() {
            let (key, value) = item.unwrap();
            let start = u64::from_be_bytes(key.value()[route.len()..].try_into().unwrap());
            let end = u64::from_be_bytes(value.value()[..8].try_into().unwrap());
            out.push((start, end));
        }
        out
    }

    fn dirty(&self) -> bool {
        let read = self.db().begin_read().unwrap();
        read.open_table(pages::OCCUPANCY_DIRTY)
            .map(|table| table.get(route().as_slice()).unwrap().is_some())
            .unwrap_or(false)
    }

    /// The directory scan: every committed source record of the route.
    fn directory(&self) -> Vec<(u64, u64)> {
        let read = self.db().begin_read().unwrap();
        let Ok(table) = read.open_table(pages::SOURCE) else {
            return Vec::new();
        };
        let route = route();
        let mut out = Vec::new();
        for item in table.prefix_bounded(route.as_slice()).unwrap() {
            let (key, value) = item.unwrap();
            let record = pages::decode_source_member(key.value(), value.value()).unwrap();
            out.push((
                record.visible_from.0,
                record.deleted_tx.map_or(OPEN, |tx| tx.0),
            ));
        }
        out
    }

    /// The interval table's answer for one hold.
    fn answer(&self, hold: u64) -> Option<bool> {
        let read = self.db().begin_read().unwrap();
        pages::occupancy_visible_at_any_in_read(&read, &route(), &[SnapshotId(hold)]).unwrap()
    }
}

fn scan_answer(directory: &[(u64, u64)], hold: u64) -> bool {
    directory
        .iter()
        .any(|(from, to)| *from <= hold && hold < *to)
}

fn reference_union(directory: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut intervals: Vec<(u64, u64)> = directory.iter().copied().filter(|(s, e)| e > s).collect();
    intervals.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (start, end) in intervals {
        match out.last_mut() {
            Some((_, last_end)) if start <= *last_end => *last_end = (*last_end).max(end),
            _ => out.push((start, end)),
        }
    }
    out
}

/// Every invariant the table owes, checked against the directory scan.
fn check(h: &Harness, holds: &[u64], context: &str) {
    let directory = h.directory();
    let rows = h.rows();
    assert_eq!(
        rows,
        reference_union(&directory),
        "{context}: rows equal the union of the directory"
    );
    for pair in rows.windows(2) {
        assert!(
            pair[0].1 < pair[1].0,
            "{context}: rows {pair:?} must be non-adjacent"
        );
    }
    for (start, end) in &rows {
        assert!(
            end > start,
            "{context}: row ({start},{end}) must be non-empty"
        );
    }
    assert!(
        !h.dirty(),
        "{context}: the route is not left on the dirty list"
    );
    for hold in holds {
        assert_eq!(
            h.answer(*hold),
            Some(scan_answer(&directory, *hold)),
            "{context}: hold {hold} agrees with the directory scan"
        );
    }
}

/// Holds around every boundary of the model plus a few far points.
fn holds_for(directory: &[(u64, u64)]) -> Vec<u64> {
    let mut holds: Vec<u64> = vec![0, 1, 1_000_000];
    for (from, to) in directory {
        for base in [*from, *to] {
            if base != OPEN {
                holds.extend([base.saturating_sub(1), base, base + 1]);
            }
        }
    }
    holds.sort_unstable();
    holds.dedup();
    holds
}

fn all_holds(h: &Harness) -> Vec<u64> {
    holds_for(&h.directory())
}

// ---------------------------------------------------------------------------
// Interval R2 extension (v4): each case is checked before publication, after
// publication, after an abort and after a reopen.
// ---------------------------------------------------------------------------

/// Publish `ops` on top of `setup`, checking all four moments.
fn case(
    setup: &[(u64, Option<VectorPartitionMembershipRecord>)],
    ops: &[(u64, Option<VectorPartitionMembershipRecord>)],
    expect_rows: &[(u64, u64)],
    holds: &[u64],
) {
    let mut h = Harness::new();
    h.commit(setup, true);
    let before = h.rows();
    check(&h, holds, "before publication");

    // An aborted publication leaves the committed answers untouched.
    drop(h.stage(ops));
    assert_eq!(h.rows(), before, "after an abort the rows are unchanged");
    check(&h, holds, "after abort");

    h.commit(ops, true);
    assert_eq!(h.rows(), expect_rows, "after publication");
    let mut with_new = holds.to_vec();
    with_new.extend(all_holds(&h));
    check(&h, &with_new, "after publication");

    h.reopen();
    assert_eq!(h.rows(), expect_rows, "after reopen");
    check(&h, &with_new, "after reopen");
}

fn put(row: u64, from: u64, to: Option<u64>) -> (u64, Option<VectorPartitionMembershipRecord>) {
    (row, Some(record(row, from, to)))
}

#[test]
fn a_received_closed_membership_above_an_existing_one_is_its_own_row() {
    // [10,20) then a received closed [30,40): a hold at 25 must NOT see the route.
    case(
        &[put(1, 10, Some(20))],
        &[put(2, 30, Some(40))],
        &[(10, 20), (30, 40)],
        &[15, 25, 35, 45],
    );
}

#[test]
fn the_first_loaded_closed_membership_on_an_empty_table_gets_a_row() {
    case(&[], &[put(1, 30, Some(40))], &[(30, 40)], &[29, 30, 39, 40]);
}

#[test]
fn a_received_open_membership_below_and_above_the_newest_start() {
    case(
        &[put(1, 20, Some(30))],
        &[put(2, 5, None)],
        &[(5, OPEN)],
        &[4, 5, 25, 99],
    );
    case(
        &[put(1, 20, Some(30))],
        &[put(2, 50, None)],
        &[(20, 30), (50, OPEN)],
        &[25, 35, 50, 99],
    );
}

#[test]
fn a_same_transaction_delete_plus_insert_merges_into_one_maximal_row() {
    // Row 1 closes at 20 and row 2 opens at 20 in the same publication.
    case(
        &[put(1, 10, None)],
        &[put(1, 10, Some(20)), put(2, 20, None)],
        &[(10, OPEN)],
        &[9, 10, 19, 20, 21],
    );
}

#[test]
#[serial_test::serial]
fn a_replacement_swaps_one_lifetime_for_another() {
    case(
        &[put(1, 10, Some(20))],
        &[put(1, 30, Some(40))],
        &[(30, 40)],
        &[15, 35],
    );
    // The same native decoder and row owner used by occupancy rebuilding:
    // inspect actual allocations, not a guessed struct layout or RSS.
    use super::proof_allocation_oracle_tests::{
        MaintenanceReaderSpy, native_free_observation_for_test, native_requests_for_test,
        watch_native_free_for_test,
    };
    use crate::read_image_memory::ImageMemory;
    use std::sync::{Arc, atomic::Ordering};
    let writer = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        1_610_612_736,
    ));
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_073_741_824);
    let child = parent.child().unwrap();
    let child_control = parent.retained_bytes_for_test();
    let spy = MaintenanceReaderSpy::new(child.clone(), writer.clone().into());
    let owner = ImageMemory::operation_reader(writer.into(), spy.clone()).unwrap();
    let row_owner = owner.row_owner().unwrap();
    let baseline = owner.held();
    let input = record(1, 10, Some(20));
    let payload = bincode::serde::encode_to_vec(&input, bincode::config::standard()).unwrap();
    let (decoded, requests, count) = row_owner.scope(|| {
        native_requests_for_test(|| {
            crate::read_image_memory::decode::<VectorPartitionMembershipRecord>(&payload)
                .unwrap()
                .0
        })
    });
    assert!(count >= 3);
    assert_eq!(requests[0].0, decoded.index.table.as_ptr() as usize);
    assert_eq!(requests[1].0, decoded.index.column.as_ptr() as usize);
    let contextdb_core::VectorPartitionComponent::Text(route_text) =
        &decoded.partition_key.components()[0]
    else {
        unreachable!()
    };
    assert_eq!(requests[2].0, route_text.as_ptr() as usize);
    let invoiced = spy.used.load(Ordering::SeqCst);
    assert!(row_owner.retained_payload_bytes() > 0);
    watch_native_free_for_test(requests, &spy.used as *const _ as usize);
    drop(decoded);
    let frees = native_free_observation_for_test();
    assert_eq!(
        frees[1],
        (invoiced, invoiced, 1),
        "column allocation physically freed before its row invoice"
    );
    assert_eq!(
        frees[2],
        (invoiced, invoiced, 1),
        "partition allocation physically freed before its row invoice"
    );
    assert_eq!(
        spy.used.load(Ordering::SeqCst),
        invoiced,
        "payload drop alone cannot refund another live row owner"
    );
    drop(row_owner);
    assert_eq!(spy.used.load(Ordering::SeqCst), baseline);
    let h = Harness::new();
    let write =
        owner.scope(|| h.stage(&[put(1, 10, Some(20)), put(1, 30, Some(40)), put(2, 15, None)]));
    assert_eq!(
        owner.held(),
        baseline,
        "actual membership publication returns previous/next decode invoices within this still-live operation"
    );
    assert_eq!(spy.used.load(Ordering::SeqCst), baseline);
    write.commit().unwrap();
    check(
        &h,
        &[9, 10, 14, 15, 20, 30, 40],
        "provided-child publication",
    );
    drop(owner);
    assert_eq!(spy.used.load(Ordering::SeqCst), 0);
    spy.assert_payload_lifetime();
    assert_eq!(parent.retained_bytes_for_test(), child_control);
    drop(spy);
    drop(child);
    assert_eq!(parent.retained_bytes_for_test(), 0);
}

#[test]
fn a_received_historical_close_splits_a_union() {
    // Row 1 covers [10,OPEN); row 2 covers [30,40). A historical close of
    // row 1 at 25 leaves [10,25) and [30,40): the gap holds must say no.
    case(
        &[put(1, 10, None), put(2, 30, Some(40))],
        &[put(1, 10, Some(25))],
        &[(10, 25), (30, 40)],
        &[24, 25, 29, 30, 39, 40],
    );
}

#[test]
fn a_route_end_equal_to_the_publishing_tx_then_a_new_open_membership_leaves_one_open_row() {
    case(
        &[put(1, 10, Some(50))],
        &[put(2, 50, None)],
        &[(10, OPEN)],
        &[10, 49, 50, 51],
    );
}

#[test]
fn removing_the_only_membership_of_a_route_clears_its_rows() {
    let h = Harness::new();
    h.commit(&[put(1, 10, Some(20))], true);
    assert_eq!(h.rows(), vec![(10, 20)]);
    h.commit(&[(1, None)], true);
    assert_eq!(h.rows(), Vec::<(u64, u64)>::new());
    check(&h, &[0, 15, 25], "after removing the last membership");
    // Selected deletion records must retain their native row owners across
    // later fallible decoding; an error destroys payloads and refunds once.
    use super::proof_allocation_oracle_tests::MaintenanceReaderSpy;
    use crate::persistence::{
        VectorPartitionJournalChange, VectorPartitionJournalRecord, VectorPartitionTombstoneCause,
        VectorPartitionTombstoneRecord,
    };
    use crate::read_image_memory::ImageMemory;
    use std::sync::{Arc, atomic::Ordering};
    let directory = tempfile::tempdir().unwrap();
    let writer = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        1_610_612_736,
    ));
    let root = crate::memory_accounting::WriterBudget::from_accountant(writer.clone()).unwrap();
    let handle: crate::memory_accounting::AccountantHandle = writer.clone().into();
    let persistence = RedbPersistence::create_with_accountant(
        &directory.path().join("selected-row.db"),
        Some(handle.clone()),
        crate::persistence::REDB_CACHE_BYTES,
    )
    .unwrap();
    let records: Vec<_> = (1..=4).map(|row| record(row, 10, Some(20))).collect();
    persistence
        .with_db(|db| {
            let write = db.begin_write().unwrap();
            crate::persistence::content_size::ensure_table(&write, "items").unwrap();
            for record in &records {
                let key = RedbPersistence::vector_partition_membership_key(record);
                let encoded = RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::Membership,
                    record,
                )
                .unwrap();
                write
                    .open_table(crate::persistence::VECTOR_PARTITION_MEMBERSHIP_TABLE)
                    .unwrap()
                    .insert(key.as_slice(), encoded.as_slice())
                    .unwrap();
                persistence
                    .write_open_directory_entry(&write, &key, Some(&encoded))
                    .unwrap();
                let tombstone = VectorPartitionTombstoneRecord {
                    membership: record.clone(),
                    deleted_tx: TxId(20),
                    lsn: record.row_lsn,
                    cause: VectorPartitionTombstoneCause::Delete,
                };
                let mut tombstone_key = key;
                tombstone_key.extend_from_slice(&20_u64.to_be_bytes());
                let bytes = RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::Tombstone,
                    &tombstone,
                )
                .unwrap();
                write
                    .open_table(crate::persistence::VECTOR_PARTITION_TOMBSTONE_TABLE)
                    .unwrap()
                    .insert(tombstone_key.as_slice(), bytes.as_slice())
                    .unwrap();
                let journal = VectorPartitionJournalRecord {
                    index: record.index.clone(),
                    partition_key: record.partition_key.clone(),
                    lsn: record.row_lsn,
                    ordinal: 0,
                    change: VectorPartitionJournalChange::Tombstone(tombstone),
                };
                let key = RedbPersistence::vector_partition_journal_key(&journal);
                let bytes = RedbPersistence::encode_vector_partition_record(
                    VectorPartitionRecordKind::Journal,
                    &journal,
                )
                .unwrap();
                write
                    .open_table(crate::persistence::VECTOR_PARTITION_JOURNAL_TABLE)
                    .unwrap()
                    .insert(key.as_slice(), bytes.as_slice())
                    .unwrap();
            }
            write.commit().unwrap();
            Ok(())
        })
        .unwrap();
    let snapshot = || {
        persistence
            .with_db(|db| {
                let read = db.begin_read().unwrap();
                let mut out = Vec::new();
                for definition in [
                    crate::persistence::VECTOR_PARTITION_MEMBERSHIP_TABLE,
                    crate::persistence::VECTOR_PARTITION_TOMBSTONE_TABLE,
                    crate::persistence::VECTOR_PARTITION_JOURNAL_TABLE,
                    pages::SOURCE,
                    pages::OCCUPANCY,
                    pages::OCCUPANCY_DIRTY,
                ] {
                    let rows = match read.open_table(definition) {
                        Ok(table) => table
                            .iter()
                            .unwrap()
                            .map(|entry| {
                                let (key, value) = entry.unwrap();
                                (key.value().to_vec(), value.value().to_vec())
                            })
                            .collect::<Vec<_>>(),
                        Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                        Err(error) => panic!("{error}"),
                    };
                    out.push(rows);
                }
                Ok(out)
            })
            .unwrap()
    };
    let before = snapshot();
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_073_741_824);
    let child = parent.child().unwrap();
    let child_control = parent.retained_bytes_for_test();
    let spy = MaintenanceReaderSpy::new(child.clone(), handle.clone());
    spy.fail_at.store(usize::MAX, Ordering::SeqCst);
    let owner = ImageMemory::operation_reader(handle.clone(), spy.clone()).unwrap();
    let mut workspace = crate::memory_accounting::OwnedMemoryReservation::new(handle.clone());
    let result = owner.scope(|| {
        persistence.with_db(|db| {
            let write = db.begin_write().unwrap();
            persistence.prune_vector_memberships_in_write(
                &write,
                records[..2].iter().map(|record| {
                    (
                        &record.index,
                        record.row_id,
                        record.vector_created_tx,
                        record.vector_lsn,
                    )
                }),
                Some(&mut workspace),
            )
        })
    });
    spy.assert_payload_lifetime();
    let error = result.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("injected reader invoice failure"),
        "{error:?}"
    );
    assert_eq!(
        spy.live_payload_owners_at_failure.load(Ordering::SeqCst),
        2,
        "both selected records retain their own payload invoices through the next fallible operation"
    );
    assert!(spy.retained_at_failure.load(Ordering::SeqCst) > owner.held());
    drop(owner);
    assert!(
        workspace.bytes() > 0,
        "failed preparation retains its admitted writer scratch until its outer owner drops"
    );
    drop(workspace);
    assert_eq!(spy.used.load(Ordering::SeqCst), 0);
    spy.assert_payload_lifetime();
    assert_eq!(parent.retained_bytes_for_test(), child_control);
    assert_eq!(
        snapshot(),
        before,
        "failure publishes no membership, tombstone, journal, source, interval, or dirty-marker change"
    );
    spy.fail_at.store(0, Ordering::SeqCst);
    let owner = ImageMemory::operation_reader(handle.clone(), spy.clone()).unwrap();
    let owner_control = owner.held();
    let mut workspace = crate::memory_accounting::OwnedMemoryReservation::new(handle);
    owner
        .scope(|| {
            persistence.with_db(|db| {
                let write = db.begin_write().unwrap();
                persistence.prune_vector_memberships_in_write(
                    &write,
                    records[..2].iter().map(|record| {
                        (
                            &record.index,
                            record.row_id,
                            record.vector_created_tx,
                            record.vector_lsn,
                        )
                    }),
                    Some(&mut workspace),
                )?;
                write.commit().unwrap();
                Ok(())
            })
        })
        .unwrap();
    assert_eq!(
        owner.held(),
        owner_control,
        "membership scans, selected deletion records and temporary journals all return their native payload invoices"
    );
    assert_eq!(
        workspace.bytes(),
        0,
        "the actual selected-record and journal writer scratch is empty after successful deletion"
    );
    drop(workspace);
    drop(owner);
    assert_eq!(spy.used.load(Ordering::SeqCst), 0);
    assert_eq!(spy.wrong_funder.load(Ordering::SeqCst), 0);
    spy.assert_payload_lifetime();
    assert_eq!(parent.retained_bytes_for_test(), child_control);
    let after = snapshot();
    for (old, new) in before[..4].iter().zip(&after[..4]) {
        assert_eq!(new.len(), 2);
        assert!(
            new.iter().all(|row| old.contains(row)),
            "only selected identities are deleted"
        );
    }
    assert_eq!(
        after[4], before[4],
        "surviving equal lifetimes preserve their canonical interval"
    );
    assert!(after[5].is_empty());
    drop(spy);
    drop(child);
    assert_eq!(parent.retained_bytes_for_test(), 0);
    drop(persistence);
    drop(root);
}

// ---------------------------------------------------------------------------
// Property row: random sequences of append, receive, load, replay, retire and
// shrink. After every step the interval set equals the directory scan for
// every hold, rows are maximal / non-adjacent / non-empty, and a removing pass
// leaves nothing dirty.
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn run_sequence(seed: u64, steps: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let h = Harness::new();
    let mut model: Model = BTreeMap::new();
    let mut next_row = 1_u64;
    let mut clock = 10_u64;
    for step in 0..steps {
        let kind = rng.below(8);
        let context = format!("seed {seed} step {step} kind {kind}");
        let mut settle = false;
        let ops: Vec<(u64, Option<VectorPartitionMembershipRecord>)> = match kind {
            // append: a new open membership at the publishing tx
            0 => {
                clock += 1 + rng.below(3);
                let row = next_row;
                next_row += 1;
                model.insert(row, (clock, None));
                vec![put(row, clock, None)]
            }
            // append-close: close an open membership at the publishing tx
            1 => {
                let open: Vec<u64> = model
                    .iter()
                    .filter(|(_, (_, to))| to.is_none())
                    .map(|(r, _)| *r)
                    .collect();
                if open.is_empty() {
                    continue;
                }
                let row = open[rng.below(open.len() as u64) as usize];
                clock += 1 + rng.below(3);
                let from = model[&row].0;
                if clock <= from {
                    continue;
                }
                model.insert(row, (from, Some(clock)));
                vec![put(row, from, Some(clock))]
            }
            // receive / load: an arbitrary closed or open membership
            2 | 3 => {
                let from = 1 + rng.below(clock + 10);
                let to = if rng.below(3) == 0 {
                    None
                } else {
                    Some(from + 1 + rng.below(12))
                };
                let row = next_row;
                next_row += 1;
                model.insert(row, (from, to));
                vec![put(row, from, to)]
            }
            // replay: the same record again
            4 => {
                let rows: Vec<u64> = model.keys().copied().collect();
                if rows.is_empty() {
                    continue;
                }
                let row = rows[rng.below(rows.len() as u64) as usize];
                let (from, to) = model[&row];
                vec![put(row, from, to)]
            }
            // retire: a retention removal
            5 => {
                let rows: Vec<u64> = model.keys().copied().collect();
                if rows.is_empty() {
                    continue;
                }
                let row = rows[rng.below(rows.len() as u64) as usize];
                model.remove(&row);
                settle = true;
                vec![(row, None)]
            }
            // shrink: a historical close or a lowered endpoint
            6 => {
                let rows: Vec<u64> = model.keys().copied().collect();
                if rows.is_empty() {
                    continue;
                }
                let row = rows[rng.below(rows.len() as u64) as usize];
                let (from, to) = model[&row];
                let ceiling = to.unwrap_or(from + 12);
                if ceiling <= from + 1 {
                    continue;
                }
                let lowered = from + 1 + rng.below(ceiling - from - 1);
                model.insert(row, (from, Some(lowered)));
                settle = true;
                vec![put(row, from, Some(lowered))]
            }
            // same publication: close one membership and open another at the same tx
            _ => {
                let open: Vec<u64> = model
                    .iter()
                    .filter(|(_, (_, to))| to.is_none())
                    .map(|(r, _)| *r)
                    .collect();
                if open.is_empty() {
                    continue;
                }
                let row = open[rng.below(open.len() as u64) as usize];
                clock += 1 + rng.below(3);
                let from = model[&row].0;
                if clock <= from {
                    continue;
                }
                let fresh = next_row;
                next_row += 1;
                model.insert(row, (from, Some(clock)));
                model.insert(fresh, (clock, None));
                vec![put(row, from, Some(clock)), put(fresh, clock, None)]
            }
        };
        h.commit(&ops, settle);
        let directory = h.directory();
        let expected: Vec<(u64, u64)> = model
            .values()
            .map(|(f, t)| (*f, t.unwrap_or(OPEN)))
            .collect();
        let (mut a, mut b) = (directory.clone(), expected);
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "{context}: the directory holds what the model says");
        check(&h, &holds_for(&directory), &context);
    }
}

#[test]
fn random_membership_writes_keep_the_interval_set_equal_to_the_directory_scan() {
    for seed in 1..=60 {
        run_sequence(seed, 80);
    }
    use super::proof_allocation_oracle_tests::MaintenanceReaderSpy;
    use crate::read_image_memory::ImageMemory;
    use std::sync::{Arc, atomic::Ordering};
    for disjoint in [false, true] {
        let mut receipts = Vec::new();
        for history in [32_u64, 64, 128] {
            let h = Harness::new();
            let ops: Vec<_> = (1..=history)
                .map(|row| {
                    let start = if disjoint {
                        10 + 2 * ((row * 17) % history)
                    } else {
                        10
                    };
                    put(row, start, Some(start + if disjoint { 1 } else { 10 }))
                })
                .chain(std::iter::once(put(0, 1, Some(2))))
                .collect();
            h.commit(&ops, true);
            h.commit(&[(0, None)], false);
            assert!(h.dirty());
            let before = h.rows();
            let writer = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
                1_610_612_736,
            ));
            let root =
                crate::memory_accounting::WriterBudget::from_accountant(writer.clone()).unwrap();
            let handle: crate::memory_accounting::AccountantHandle = writer.clone().into();
            let writer_baseline = writer.usage().used;
            let parent =
                crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_073_741_824);
            let child = parent.child().unwrap();
            let child_control = parent.retained_bytes_for_test();
            let child_observer =
                crate::vector_read_memory::declared_child_observer(&child).unwrap();
            let spy = MaintenanceReaderSpy::new(child.clone(), handle.clone());
            let owner = ImageMemory::operation_reader(handle.clone(), spy.clone()).unwrap();
            let owner_control = owner.held();
            // Abort an actual completed rebuild: neither old rows nor its
            // committed dirty marker may be partially published.
            let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let counted_writes = writes.clone();
            let write_observer = h.db().observe_table_writes_for_test(Arc::new(move |event| {
                if event.table == "vector_membership_occupancy_pages"
                    && matches!(
                        event.operation,
                        redb::TableWriteOperationForTest::Insert
                            | redb::TableWriteOperationForTest::Remove
                    )
                {
                    counted_writes.fetch_add(1, Ordering::SeqCst);
                }
            }));
            let write = h.db().begin_write().unwrap();
            owner
                .scope(|| pages::settle_dirty_occupancy(&write, Some(handle.clone())))
                .unwrap();
            drop(write_observer);
            spy.assert_payload_lifetime();
            assert_eq!(
                owner.held(),
                owner_control,
                "temporary decoded memberships refund before the next operation"
            );
            assert_eq!(spy.used.load(Ordering::SeqCst), owner_control);
            assert_eq!(
                child_observer.sample().charged_bytes as usize,
                owner_control
            );
            assert_eq!(
                parent.retained_bytes_for_test(),
                child_control + owner_control as u64
            );
            let first_requests = spy.decode_requests.load(Ordering::SeqCst);
            let reader_peak = spy.peak.load(Ordering::SeqCst);
            let writer_peak = spy.writer_peak.load(Ordering::SeqCst) - writer_baseline;
            drop(write);
            assert_eq!(h.rows(), before);
            assert!(h.dirty());
            assert_eq!(writer.usage().used, writer_baseline);
            drop(owner);
            assert_eq!(spy.used.load(Ordering::SeqCst), 0);

            // One byte below the measured successful native reader envelope
            // refuses as reader memory with this SAME writer. Abort retains
            // the old durable rows and dirty marker for the correct child.
            let small_parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                child_control + reader_peak as u64 - 1,
            );
            let small_child = small_parent.child().unwrap();
            let small_spy = MaintenanceReaderSpy::new(small_child.clone(), handle.clone());
            let small_owner =
                ImageMemory::operation_reader(handle.clone(), small_spy.clone()).unwrap();
            let write = h.db().begin_write().unwrap();
            let error = small_owner
                .scope(|| pages::settle_dirty_occupancy(&write, Some(handle.clone())))
                .unwrap_err();
            assert!(
                matches!(error, contextdb_core::Error::MemoryBudgetExceeded { ref subsystem, .. } if subsystem == "reader"),
                "{error:?}"
            );
            drop(write);
            drop(small_owner);
            assert_eq!(small_spy.used.load(Ordering::SeqCst), 0);
            drop(small_spy);
            drop(small_child);
            assert_eq!(small_parent.retained_bytes_for_test(), 0);
            assert_eq!(h.rows(), before);
            assert!(h.dirty());
            assert_eq!(writer.usage().used, writer_baseline);

            let owner = ImageMemory::operation_reader(handle.clone(), spy.clone()).unwrap();
            let write = h.db().begin_write().unwrap();
            owner
                .scope(|| pages::settle_dirty_occupancy(&write, Some(handle.clone())))
                .unwrap();
            write.commit().unwrap();
            drop(owner);
            assert_eq!(spy.used.load(Ordering::SeqCst), 0);
            assert_eq!(spy.wrong_funder.load(Ordering::SeqCst), 0);
            assert_eq!(writer.usage().used, writer_baseline);
            check(&h, &all_holds(&h), "native fixed-budget rebuild");
            assert_eq!(h.rows().len(), if disjoint { history as usize } else { 1 });
            receipts.push((
                reader_peak,
                writer_peak,
                first_requests,
                writes.load(Ordering::SeqCst),
            ));
            drop(spy);
            drop(child);
            assert_eq!(parent.retained_bytes_for_test(), 0);
            drop(root);
        }
        for receipt in &receipts[1..] {
            assert_eq!(
                receipts[0].0, receipt.0,
                "N, 2N and 4N keep identical native live reader invoices"
            );
            assert_eq!(
                receipts[0].1, receipt.1,
                "N, 2N and 4N keep identical writer scratch"
            );
        }
        assert_eq!(
            receipts[2].2 - receipts[1].2,
            2 * (receipts[1].2 - receipts[0].2),
            "doubling source records doubles added native decode work, preserving fixed route work"
        );
        assert!(receipts[0].3 > 0);
        assert_eq!(
            receipts[2].3 - receipts[1].3,
            2 * (receipts[1].3 - receipts[0].3),
            "actual durable occupancy mutations have linear growth for overlapping and arbitrary-order disjoint inputs"
        );
    }
}

// ---------------------------------------------------------------------------
// The hold lookup reads no membership record: with the route's entire
// membership directory removed from underneath it, every hold is still
// answered, and answered as the directory would have answered it.
// ---------------------------------------------------------------------------

#[test]
fn a_hold_is_answered_from_the_interval_rows_without_reading_any_membership() {
    for history in [16_u64, 1024] {
        let h = Harness::new();
        // Two bursts of closed memberships with a gap, then one open membership.
        let mut ops = Vec::new();
        for n in 0..history {
            ops.push(put(n + 1, 100, Some(200)));
            ops.push(put(history + n + 1, 300, Some(400)));
        }
        ops.push(put(2 * history + 1, 500, None));
        for chunk in ops.chunks(256) {
            h.commit(chunk, true);
        }
        let directory = h.directory();
        let holds = [
            0_u64, 99, 100, 199, 200, 250, 299, 300, 399, 400, 450, 499, 500, 10_000,
        ];
        let expected: Vec<bool> = holds
            .iter()
            .map(|hold| scan_answer(&directory, *hold))
            .collect();
        assert_eq!(h.rows(), vec![(100, 200), (300, 400), (500, OPEN)]);

        // Remove every membership record; keep only the interval rows.
        let write = h.db().begin_write().unwrap();
        {
            let mut source = write.open_table(pages::SOURCE).unwrap();
            let keys: Vec<Vec<u8>> = source
                .prefix_bounded(route().as_slice())
                .unwrap()
                .map(|item| item.unwrap().0.value().to_vec())
                .collect();
            for key in keys {
                source.remove(key.as_slice()).unwrap();
            }
        }
        write.commit().unwrap();
        assert!(h.directory().is_empty(), "the directory is gone");
        let answers: Vec<Option<bool>> = holds.iter().map(|hold| h.answer(*hold)).collect();
        assert_eq!(
            answers,
            expected
                .iter()
                .map(|answer| Some(*answer))
                .collect::<Vec<_>>(),
            "history {history}: every hold is answered from the interval rows alone"
        );
    }
}

// ---------------------------------------------------------------------------
// F3: a real process death in the window after the durable commit and before
// vector in-memory publication, on commits that change a route's occupancy.
// The child names the scenario through the environment, arms the seam for the
// one commit under test, and dies inside it; the parent proves the seam was
// reached, reopens, and compares the durable interval rows with the durable
// memberships, the cap figures with independent recounts, and the exact cap
// boundary with real admissions.
// ---------------------------------------------------------------------------

const SCENARIO_ENV: &str = "CDB120_CRASH_WINDOW_SCENARIO";
const STORE_ENV: &str = "CDB120_CRASH_WINDOW_STORE";
const MARKER_ENV: &str = "CDB120_CRASH_WINDOW_MARKER";

fn sql(
    db: &crate::Database,
    text: &str,
    pairs: Vec<(&str, contextdb_core::Value)>,
) -> contextdb_core::Result<()> {
    let params = pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect();
    db.execute(text, &params).map(|_| ())
}

fn put_row(db: &crate::Database, row: u128, scope: &str) -> contextdb_core::Result<()> {
    sql(
        db,
        "INSERT INTO items (id, scope, embedding) VALUES ($id, $scope, $v)",
        vec![
            (
                "id",
                contextdb_core::Value::Uuid(uuid::Uuid::from_u128(row)),
            ),
            ("scope", contextdb_core::Value::Text(scope.to_owned())),
            ("v", contextdb_core::Value::Vector(vec![1.0, 0.5])),
        ],
    )
}

fn drop_row(db: &crate::Database, row: u128) {
    sql(
        db,
        "DELETE FROM items WHERE id = $id",
        vec![(
            "id",
            contextdb_core::Value::Uuid(uuid::Uuid::from_u128(row)),
        )],
    )
    .unwrap();
}

#[test]
fn cdb120_crash_window_child() {
    let (Ok(scenario), Ok(store), Ok(marker)) = (
        std::env::var(SCENARIO_ENV),
        std::env::var(STORE_ENV),
        std::env::var(MARKER_ENV),
    ) else {
        return;
    };
    let db = crate::Database::open(&store).expect("child opens its store");
    db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
    sql(
        &db,
        "CREATE TABLE items (id UUID PRIMARY KEY, scope TEXT NOT NULL, \
         embedding VECTOR(2) PARTITION_KEY (scope) MAX_PARTITIONS 4) HISTORY CURRENT ONLY SYNC OFF",
        vec![],
    )
    .unwrap();
    for n in 1..=4_u128 {
        put_row(&db, n, &format!("k{n}")).unwrap();
    }
    if scenario == "new-key-at-the-cap" {
        // k1 is counted-empty (no pin, no maintenance yet): the next new key
        // reclaims it at the cap edge.
        drop_row(&db, 1);
    }
    // Arm the seam for exactly the commit under test.
    unsafe {
        std::env::set_var(
            crate::composite_store::ABORT_BEFORE_VECTOR_PUBLICATION_ENV,
            &marker,
        )
    };
    match scenario.as_str() {
        "last-vector-leaves" => drop_row(&db, 1),
        "new-key-at-the-cap" => put_row(&db, 9, "k9").unwrap(),
        other => panic!("unknown scenario {other}"),
    }
    unreachable!("the seam aborts the process inside the commit above");
}

/// Every route's interval rows against the union of its durable memberships.
fn assert_durable_intervals_match_memberships(store: &std::path::Path) {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(store)
        .unwrap();
    let owner = super::super::writer_file::LockedWriterFile::lock(file, store.parent()).unwrap();
    let database = redb::Database::builder()
        .create_with_backend(owner.view())
        .unwrap();
    let read = database.begin_read().unwrap();
    let mut directory: BTreeMap<Vec<u8>, Vec<(u64, u64)>> = BTreeMap::new();
    if let Ok(source) = read.open_table(pages::SOURCE) {
        for item in source.iter().unwrap() {
            let (key, value) = item.unwrap();
            let record = pages::decode_source_member(key.value(), value.value()).unwrap();
            let route =
                RedbPersistence::vector_partition_prefix(&record.index, &record.partition_key);
            directory.entry(route).or_default().push((
                record.visible_from.0,
                record.deleted_tx.map_or(OPEN, |tx| tx.0),
            ));
        }
    }
    let mut rows: BTreeMap<Vec<u8>, Vec<(u64, u64)>> = BTreeMap::new();
    if let Ok(table) = read.open_table(pages::OCCUPANCY) {
        for item in table.iter().unwrap() {
            let (key, value) = item.unwrap();
            let split = key.value().len() - 8;
            let start = u64::from_be_bytes(key.value()[split..].try_into().unwrap());
            let end = u64::from_be_bytes(value.value()[..8].try_into().unwrap());
            rows.entry(key.value()[..split].to_vec())
                .or_default()
                .push((start, end));
        }
    }
    let dirty = read
        .open_table(pages::OCCUPANCY_DIRTY)
        .map(|table| table.iter().unwrap().count() as u64)
        .unwrap_or(0);
    assert_eq!(dirty, 0, "no route is left awaiting an interval rebuild");
    let expected: BTreeMap<Vec<u8>, Vec<(u64, u64)>> = directory
        .iter()
        .map(|(route, intervals)| (route.clone(), reference_union(intervals)))
        .filter(|(_, union)| !union.is_empty())
        .collect();
    assert_eq!(
        rows, expected,
        "durable interval rows equal the union of the durable memberships, route by route"
    );
}

fn crash_window(scenario: &str, expect_rows: usize, expect_occupied: usize) {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("window.redb");
    let marker = dir.path().join("reached.marker");
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "persistence::content_identity::vector_occupancy_interval_tests::cdb120_crash_window_child", "--test-threads=1", "--nocapture"])
        .env(SCENARIO_ENV, scenario)
        .env(STORE_ENV, &store)
        .env(MARKER_ENV, &marker)
        .output()
        .expect("spawn the crash child");
    assert!(
        !output.status.success(),
        "{scenario}: the child must die, got {:?}",
        output.status
    );
    assert_eq!(
        std::fs::read(&marker).unwrap_or_default(),
        b"reached",
        "{scenario}: the child died at the durable-commit-before-publication seam, not elsewhere: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    {
        let db = crate::Database::open(&store).expect("reopen after the process death");
        db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
        let rows = db
            .execute("SELECT id FROM items", &std::collections::HashMap::new())
            .unwrap()
            .rows
            .len();
        assert_eq!(
            rows, expect_rows,
            "{scenario}: the durable commit survived the death"
        );
        let audit = db
            .vector_store_for_test()
            .partition_limit_audit_for_test(&index())
            .unwrap();
        assert_eq!(
            audit.occupied, audit.recounted_occupied,
            "{scenario}: occupied"
        );
        assert_eq!(
            audit.directory_bytes, audit.recounted_directory_bytes,
            "{scenario}: directory bytes"
        );
        assert_eq!(
            audit.occupied, expect_occupied,
            "{scenario}: counted routes after reopen"
        );
        // The exact boundary: the free slots admit, the next key refuses.
        for n in 0..(4 - expect_occupied) {
            put_row(&db, 100 + n as u128, &format!("fresh{n}"))
                .unwrap_or_else(|e| panic!("{scenario}: free slot {n}: {e}"));
        }
        let refusal = put_row(&db, 200, "one-too-many").unwrap_err();
        assert!(
            matches!(
                refusal,
                contextdb_core::Error::VectorPartitionLimitExceeded {
                    max_partitions: 4,
                    ..
                }
            ),
            "{scenario}: {refusal:?}"
        );
        let audit = db
            .vector_store_for_test()
            .partition_limit_audit_for_test(&index())
            .unwrap();
        assert_eq!(
            (audit.occupied, audit.recounted_occupied),
            (4, 4),
            "{scenario}: full and exact"
        );
    }
    assert_durable_intervals_match_memberships(&store);
}

#[test]
fn cdb120_death_before_publication_of_a_last_vector_leaving_a_route_reopens_exact() {
    // k1's only vector is deleted durably; the route no longer counts.
    crash_window("last-vector-leaves", 3, 3);
}

#[test]
fn cdb120_death_before_publication_of_a_new_key_at_the_cap_reopens_exact() {
    // k1 was already emptied; the new key reclaims its slot durably, so four
    // live routes remain and the cap refuses the next key.
    crash_window("new-key-at-the-cap", 4, 4);
}
