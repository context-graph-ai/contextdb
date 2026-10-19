//! The maintained cap ledger equals a full recount
//! after every kind of publication, and the exact cap edge visits only
//! counted-empty routes with indexed hold lookups, never route history.
use super::*;
use contextdb_core::VectorPartitionComponent;

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
    refuse: AtomicBool,
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_owned().into(),
                operation: operation.to_owned().into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: hint.to_owned().into(),
            });
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::SeqCst);
    }
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}

fn key(n: u64) -> VectorPartitionKey {
    VectorPartitionKey::from_components(vec![VectorPartitionComponent::Integer(n as i64)])
}

fn entry(row: u64, created: u64) -> VectorEntry {
    VectorEntry {
        index: index(),
        row_id: RowId(row),
        vector: vec![1.0, (row % 89) as f32 / 89.0],
        created_tx: TxId(created),
        deleted_tx: None,
        lsn: Lsn(created),
    }
}

struct Fixture {
    store: VectorStore,
    budget: Arc<Budget>,
    executing: Arc<super::tests::CountingMemoryBudget>,
    next_lsn: u64,
}

impl Fixture {
    fn new(max_partitions: u32) -> Self {
        let budget = Arc::new(Budget::default());
        let store = VectorStore::default();
        let layout = VectorIndexLayout::new(
            2,
            VectorQuantization::F32,
            vec!["scope".to_owned()],
            Some(max_partitions),
            VectorSearchMode::Auto,
        );
        let mut workspace = VectorCopyAdmission::new(budget.clone());
        store
            .register_index_with_layout(index(), layout, budget.clone(), &mut workspace)
            .unwrap();
        drop(workspace);
        Self {
            store,
            budget,
            executing: Arc::new(super::tests::CountingMemoryBudget::default()),
            next_lsn: 1,
        }
    }

    fn admission(&self) -> VectorCopyAdmission {
        VectorCopyAdmission::new(self.executing.clone())
    }

    fn prepare(
        &self,
        deletes: Vec<PartitionedVectorDelete>,
        inserts: Vec<PartitionedVectorEntry>,
        moves: Vec<PartitionedVectorMove>,
        holds: &[SnapshotId],
    ) -> Result<PreparedPartitionedVectorBatch> {
        self.store
            .prepare_partitioned_batch_with_registered_snapshots(
                deletes,
                inserts,
                moves,
                holds,
                self.admission(),
            )
    }

    fn publish(&mut self, prepared: PreparedPartitionedVectorBatch) {
        let lsn = self.next_lsn;
        self.next_lsn += 1;
        self.store
            .publish_prepared_partitioned_batch(prepared, Lsn(lsn), None);
    }

    fn insert(&mut self, k: u64, row: u64, tx: TxId, holds: &[SnapshotId]) -> Result<()> {
        let prepared = self.prepare(
            vec![],
            vec![PartitionedVectorEntry::new(key(k), entry(row, tx.0))],
            vec![],
            holds,
        )?;
        self.publish(prepared);
        Ok(())
    }

    fn delete(&mut self, k: u64, row: u64, tx: TxId, created: u64, holds: &[SnapshotId]) {
        let prepared = self
            .prepare(
                vec![PartitionedVectorDelete::new(
                    index(),
                    key(k),
                    RowId(row),
                    tx,
                    TxId(created),
                    Lsn(created),
                )],
                vec![],
                vec![],
                holds,
            )
            .unwrap();
        self.publish(prepared);
    }

    fn audit(&self, step: &str) -> VectorPartitionLimitAuditForTest {
        let audit = self.store.partition_limit_audit_for_test(&index()).unwrap();
        assert_eq!(audit.occupied, audit.recounted_occupied, "{step}: occupied");
        let (mut a, mut b) = (
            audit.empty_counted.clone(),
            audit.recounted_empty_counted.clone(),
        );
        a.sort();
        b.sort();
        assert_eq!(a, b, "{step}: counted-empty routes");
        assert_eq!(
            audit.directory_bytes, audit.recounted_directory_bytes,
            "{step}: directory bytes"
        );
        audit
    }

    fn state(&self, k: u64) -> Arc<IndexState> {
        self.store
            .try_column(&index())
            .unwrap()
            .partition_state(&key(k))
            .unwrap()
    }

    /// Pretend a saved graph covers everything through `covered`, so no
    /// pending tombstone keeps the cap-time sweep away from the route.
    fn cover(&self, k: u64, covered: u64) {
        let state = self.state(k);
        state.inspection_generation_id.store(1, Ordering::SeqCst);
        state.inspection_base_tx.store(covered, Ordering::SeqCst);
    }
}

fn cap_refusal(error: &Error, cap: u32) -> bool {
    matches!(error, Error::VectorPartitionLimitExceeded { index: got, max_partitions } if *got == index() && *max_partitions == cap)
}

// ---------------------------------------------------------------------------
// R2 (store door): maintained == recounted after every publication kind.
// ---------------------------------------------------------------------------

#[test]
fn cdb120_ledger_equals_a_recount_after_every_kind_of_publication() {
    let mut f = Fixture::new(4);
    for k in 0..4 {
        f.insert(k, 10 + k, TxId(1 + k), &[]).unwrap();
        f.audit("insert through the cap");
    }
    let refusal = f.insert(4, 20, TxId(5), &[]).unwrap_err();
    assert!(cap_refusal(&refusal, 4), "{refusal:?}");
    f.audit("failed preparation: the cap refusal");

    f.budget.refuse.store(true, Ordering::SeqCst);
    let _ = f.insert(0, 21, TxId(6), &[]);
    f.budget.refuse.store(false, Ordering::SeqCst);
    f.audit("failed preparation: the budget refusal");

    let prepared = f
        .prepare(
            vec![],
            vec![PartitionedVectorEntry::new(key(0), entry(22, 7))],
            vec![],
            &[],
        )
        .unwrap();
    drop(prepared);
    f.audit("an aborted prepared batch");

    let loaded = |f: &mut Fixture, k: u64, row: u64| {
        f.store.insert_loaded_partitioned_vector(
            PartitionedVectorEntry::new(key(k), entry(row, 8)),
            &mut f.admission(),
        )
    };
    let refusal = loaded(&mut f, 4, 30).unwrap_err();
    assert!(
        cap_refusal(&refusal, 4),
        "a loaded insert into a fifth key is refused too: {refusal:?}"
    );
    f.audit("loaded insert at cap + 1 is refused");
    loaded(&mut f, 0, 31).unwrap();
    f.store.account_loaded_state(f.budget.clone()).unwrap();
    assert_eq!(
        f.audit("loaded").occupied,
        4,
        "a loaded insert into an existing route adds no route"
    );

    f.delete(0, 10, TxId(9), 1, &[SnapshotId(5)]);
    f.delete(0, 31, TxId(9), 8, &[SnapshotId(5)]);
    let audit = f.audit("delete of a last vector under a hold");
    assert!(
        audit.empty_counted.contains(&key(0)),
        "the emptied route is counted-empty"
    );

    let mv = PartitionedVectorMove::new(
        index(),
        key(1),
        key(1),
        RowId(11),
        RowId(40),
        TxId(10),
        TxId(2),
        Lsn(2),
    );
    let prepared = f.prepare(vec![], vec![], vec![mv], &[]).unwrap();
    f.publish(prepared);
    f.audit("move within a route");

    let retired = f
        .store
        .retire_snapshot_free_partition_states(&[], f.budget.as_ref(), &mut f.admission())
        .unwrap();
    assert!(retired >= 1, "the unheld emptied route retires");
    let audit = f.audit("retirement to historical-only");
    assert!(!audit.empty_counted.contains(&key(0)));

    f.insert(0, 50, TxId(13), &[])
        .unwrap_or_else(|e| panic!("reactivating a historical route: {e}"));
    f.audit("historical route reactivated");
}

// ---------------------------------------------------------------------------
// R3 (store door): the exact edge with every kind of empty route.
// ---------------------------------------------------------------------------

const CAP: u32 = 8;
/// Registered after the reclaimable route's last close and before the held
/// routes lose their vector.
const HOLD: SnapshotId = SnapshotId(12);

fn row_of(k: u64) -> u64 {
    1_000 + k
}

/// Historical-only routes: more of them than the cap, all uncounted.
fn make_historical(f: &mut Fixture) {
    for wave in [100_u64, 110] {
        for k in wave..wave + u64::from(CAP) {
            f.insert(k, row_of(k), TxId(1), &[]).unwrap();
        }
        for k in wave..wave + u64::from(CAP) {
            f.delete(k, row_of(k), TxId(2), 1, &[]);
        }
        f.store
            .retire_snapshot_free_partition_states(&[], f.budget.as_ref(), &mut f.admission())
            .unwrap();
        assert!(
            f.state(wave).is_historical_only(),
            "an unheld emptied route is historical-only"
        );
        assert_eq!(f.audit("historical wave").occupied, 0);
    }
}

/// Counted routes, all created at tx 10: live 0..2; held 10..13 and
/// ineligible 20..22 lose their vector at tx 15 (HOLD sees them, and only the
/// held ones are covered by a saved graph); reclaimable 30 (when asked) holds
/// `1 + extra_history` memberships closed at tx 11 (HOLD does not see them).
fn edge_fixture(with_reclaimable: bool, extra_history: u64) -> Fixture {
    let mut f = Fixture::new(CAP);
    make_historical(&mut f);
    for k in (0..2).chain(10..13).chain(20..22) {
        f.insert(k, row_of(k), TxId(10), &[]).unwrap();
    }
    if with_reclaimable {
        for n in 0..=extra_history {
            f.insert(30, 500_000 + n, TxId(10), &[])
                .unwrap_or_else(|e| panic!("route 30 row {n}: {e}"));
        }
        for n in 0..=extra_history {
            f.delete(30, 500_000 + n, TxId(11), 10, &[SnapshotId(10)]);
        }
        f.cover(30, 15);
    }
    for k in (10..13).chain(20..22) {
        f.delete(k, row_of(k), TxId(15), 10, &[HOLD]);
    }
    for k in 10..13 {
        f.cover(k, 15);
    }
    f
}

fn work(f: &Fixture) -> (VectorCapWorkCountersForTest, u64) {
    let inspected = f
        .store
        .try_column(&index())
        .unwrap()
        .partition_snapshots()
        .iter()
        .map(|(_, state)| state.raw_directory_entries_inspected.load(Ordering::SeqCst))
        .sum();
    (f.store.cap_work_counters_for_test(), inspected)
}

#[test]
fn cdb120_edge_admits_through_the_one_reclaimable_route_and_visits_only_counted_empties() {
    let mut f = edge_fixture(true, 0);
    let audit = f.audit("edge fixture");
    assert_eq!(
        audit.occupied, CAP as usize,
        "the cap is full of counted routes"
    );
    assert_eq!(
        audit.empty_counted.len(),
        6,
        "three held, two ineligible, one reclaimable"
    );
    let (before, _) = work(&f);
    f.insert(40, row_of(40), TxId(16), &[HOLD])
        .unwrap_or_else(|e| panic!("the reclaimable route must admit the new key: {e}"));
    let (after, _) = work(&f);
    let visits = after.cap_edge_route_visits - before.cap_edge_route_visits;
    let lookups = after.hold_lookups - before.hold_lookups;
    assert!(
        (1..=6).contains(&visits),
        "visits {visits}: at most the six counted-empty routes, none of the 16 historical ones"
    );
    assert!(
        lookups <= 4,
        "hold lookups {lookups}: only the four eligible routes are looked up, once per hold"
    );
    let audit = f.audit("after the edge admission");
    assert!(
        !audit.empty_counted.contains(&key(30)),
        "the reclaimed route left the counted-empty set"
    );
    assert!(
        audit.empty_counted.contains(&key(10)),
        "a held route stays counted"
    );
}

#[test]
fn cdb120_edge_with_no_reclaimable_route_refuses_exactly_as_the_base_does() {
    let mut f = edge_fixture(false, 0);
    let audit = f.audit("edge fixture");
    assert_eq!(audit.occupied, 7, "seven counted routes");
    f.insert(41, row_of(41), TxId(16), &[HOLD]).unwrap(); // the eighth: the cap itself
    let (before, _) = work(&f);
    let refusal = f.insert(40, row_of(40), TxId(17), &[HOLD]).unwrap_err();
    assert!(cap_refusal(&refusal, CAP), "{refusal:?}");
    let (after, _) = work(&f);
    let visits = after.cap_edge_route_visits - before.cap_edge_route_visits;
    assert!(
        visits <= 5,
        "visits {visits}: at most the five counted-empty routes"
    );
    assert!(
        after.hold_lookups - before.hold_lookups <= 3,
        "only the three held routes are eligible"
    );
    f.audit("after the refusal");
}

#[test]
fn cdb120_edge_cost_does_not_grow_with_a_candidate_routes_retained_history() {
    let mut measured = Vec::new();
    for extra in [15_u64, 4095] {
        let mut f = edge_fixture(true, extra);
        let (before, inspected_before) = work(&f);
        f.insert(40, row_of(40), TxId(16), &[HOLD]).unwrap();
        let (after, inspected_after) = work(&f);
        measured.push((
            after.cap_edge_route_visits - before.cap_edge_route_visits,
            after.hold_lookups - before.hold_lookups,
            inspected_after - inspected_before,
        ));
        f.audit("after the edge admission");
    }
    for (n, (visits, lookups, inspected)) in [15_u64, 4095].into_iter().zip(&measured) {
        assert!((1..=6).contains(visits), "extra={n}: visits {visits}");
        assert!(*lookups <= 4, "extra={n}: hold lookups {lookups}");
        assert!(
            *inspected <= 64,
            "extra={n}: the edge walked {inspected} raw directory entries; a candidate's retained history must not be walked"
        );
    }
}

// ---------------------------------------------------------------------------
// F1: the cap edge confirms a candidate's identity with one map probe, never
// by searching the placement history of the candidate's first retained row.
// One row R is placed in `history` routes that all became historical-only,
// then lands in the counted-empty candidate last: a history search would
// probe once per historical key.
// ---------------------------------------------------------------------------

#[test]
fn cdb120_edge_confirms_a_candidate_with_one_probe_however_long_its_first_rows_placement_history() {
    const SHARED_ROW: u64 = 777_000;
    let mut measured = Vec::new();
    for history in [16_u64, 4096] {
        let mut f = Fixture::new(CAP);
        let mut tx = 100;
        for h in 0..history {
            f.insert(1_000 + h, SHARED_ROW, TxId(tx), &[]).unwrap();
            f.delete(1_000 + h, SHARED_ROW, TxId(tx + 1), tx, &[]);
            tx += 2;
            f.store
                .retire_snapshot_free_partition_states(&[], f.budget.as_ref(), &mut f.admission())
                .unwrap();
        }
        assert_eq!(f.audit("history routes retired").occupied, 0);
        // Seven live routes, then the candidate: R's last placement, closed.
        for k in 0..7 {
            f.insert(k, row_of(k), TxId(tx), &[]).unwrap();
        }
        let candidate_key = 9_000_000;
        f.insert(candidate_key, SHARED_ROW, TxId(tx), &[]).unwrap();
        f.delete(
            candidate_key,
            SHARED_ROW,
            TxId(tx + 1),
            tx,
            &[SnapshotId(tx)],
        );
        f.cover(candidate_key, tx + 5);
        let audit = f.audit("candidate is the only counted-empty route");
        assert_eq!(audit.occupied, CAP as usize);
        assert_eq!(audit.empty_counted, vec![key(candidate_key)]);

        let (before, _) = work(&f);
        f.insert(40, row_of(40), TxId(tx + 10), &[SnapshotId(tx + 5)])
            .unwrap_or_else(|e| {
                panic!("history {history}: the reclaimable candidate admits the new key: {e}")
            });
        let (after, _) = work(&f);
        let visits = after.cap_edge_route_visits - before.cap_edge_route_visits;
        let probes = after.cap_edge_identity_probes - before.cap_edge_identity_probes;
        assert_eq!(visits, 1, "history {history}: one candidate examined");
        assert_eq!(
            probes, visits,
            "history {history}: exactly one identity probe per examined candidate"
        );
        measured.push((visits, probes));
        f.audit("after the edge admission");
    }
    assert_eq!(
        measured[0], measured[1],
        "edge work is identical at 16 and 4096 historical placements"
    );
}
