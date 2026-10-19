//! Deterministic funder proofs, including the native graph-scratch ownership rows.
use super::*;
use contextdb_vector::store::{
    DurableRawColumnFactory, DurableRawPartitionFactory, RawVectorReverseMembershipSource,
};
use contextdb_vector::{MemoryBudget, PrepaidVectorCopyCharge, VectorCopyAdmission};
use serial_test::serial;

type CreatedSources = (
    Arc<dyn RawVectorReverseMembershipSource>,
    Arc<dyn DurableRawPartitionFactory>,
);
struct Credits(Vec<Arc<VectorCopyAdmission>>);
impl DurableRawColumnFactory for Credits {
    fn visit_retained_copy_credits(
        &self,
        caller: &dyn MemoryBudget,
        visit: &mut dyn FnMut(usize, usize, bool),
    ) {
        for credit in &self.0 {
            VectorCopyAdmission::visit_retained_copy_credit(credit, caller, visit);
        }
    }
    fn create(
        &self,
        _: &VectorIndexRef,
        _: &VectorIndexLayout,
        _: VectorCopyAdmission,
        _: VectorCopyAdmission,
    ) -> Result<CreatedSources> {
        Err(Error::Other(
            "proof visits existing credits without loading".into(),
        ))
    }
}
struct Prepaid(VectorCopyAdmission);
impl PrepaidVectorCopyCharge for Prepaid {
    fn accountant(&self) -> Arc<dyn MemoryBudget> {
        self.0.funder()
    }
    fn retained_bytes(&self) -> usize {
        self.0.retained_bytes()
    }
    fn transfer(self: Box<Self>) {
        self.0.transfer();
    }
}
fn credit(budget: Arc<dyn MemoryBudget>, bytes: usize) -> Arc<VectorCopyAdmission> {
    let mut credit = VectorCopyAdmission::new(budget);
    credit.reserve(bytes).unwrap();
    Arc::new(credit)
}

// Real received-body installation followed by loaded-state header promotion
// drives quantized funder matching and directory settlement without test seams.
fn mixed_payload_owners(body: Arc<dyn MemoryBudget>, header: Arc<dyn MemoryBudget>) -> VectorStore {
    let store = VectorStore::default();
    let index = VectorIndexRef::new("funder_payload", "v");
    let publication = store
        .prepare_partitioned_received_schema_publication(
            vec![(
                index.clone(),
                VectorIndexLayout::unpartitioned(3, VectorQuantization::F32),
            )],
            vec![contextdb_vector::PartitionedVectorEntry::new(
                VectorPartitionKey::unpartitioned(),
                VectorEntry {
                    index,
                    row_id: RowId(1),
                    vector: vec![1.0, 0.0, 0.0],
                    created_tx: TxId(1),
                    deleted_tx: None,
                    lsn: Lsn(1),
                },
            )],
            body.clone(),
            VectorCopyAdmission::new(body.clone()),
        )
        .unwrap();
    store.publish_prepared_received_schema(publication, body);
    let before = store.memory_ownership_snapshot_for_test();
    assert_eq!(before.raw_vector_bodies, 12);
    store.account_loaded_state(header).unwrap();
    let after = store.memory_ownership_snapshot_for_test();
    assert_eq!(
        after.raw_vector_bodies, before.raw_vector_bodies,
        "body stays charged to its original owner"
    );
    assert!(
        after.raw_reverse_directory > before.raw_reverse_directory,
        "actual shared header is retained and counted"
    );
    store
}

#[test]
#[serial]
fn default_funder_identity_preserves_trait_data_address() {
    let first = Arc::new(MemoryAccountant::no_limit());
    let second = Arc::new(MemoryAccountant::no_limit());
    let erased: Arc<dyn MemoryBudget> = first.clone();
    let clone = erased.clone();
    let other: Arc<dyn MemoryBudget> = second.clone();
    for budget in [&erased, &clone, &other] {
        assert_eq!(
            budget.stable_funder_identity(),
            Arc::as_ptr(budget) as *const () as usize
        );
    }
    assert_eq!(
        erased.stable_funder_identity(),
        clone.stable_funder_identity()
    );
    assert_ne!(
        erased.stable_funder_identity(),
        other.stable_funder_identity()
    );
    let root = crate::memory_accounting::WriterBudget::from_accountant(first.clone()).unwrap();
    let adapter_a = crate::memory_accounting::AccountantHandle::from_host(first.clone())
        .vector_budget()
        .unwrap();
    let adapter_b = crate::memory_accounting::AccountantHandle::from_host(first.clone())
        .vector_budget()
        .unwrap();
    assert_eq!(
        adapter_a.stable_funder_identity(),
        erased.stable_funder_identity()
    );
    assert_eq!(
        adapter_b.stable_funder_identity(),
        erased.stable_funder_identity()
    );
    assert_ne!(
        Arc::as_ptr(&adapter_a) as *const () as usize,
        Arc::as_ptr(&adapter_b) as *const () as usize
    );
    drop(adapter_a);
    drop(adapter_b);
    drop(root);
    assert_eq!(first.usage().used, 0);
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_000_000);
    let reader = crate::vector_read_memory::budget(parent.child().unwrap());
    assert_eq!(
        reader.stable_funder_identity(),
        Arc::as_ptr(&reader) as *const () as usize
    );
    println!(
        "CDB127_DEFAULT_COMPAT raw_accountant=unchanged reader_impl=unchanged clones=equal distinct_objects=distinct"
    );
}

#[test]
#[serial]
fn independent_writer_invocations_keep_caller_credit_and_allocation_dedup() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(accountant.clone());
    let baseline = accountant.usage().used;
    let first = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
        .vector_budget()
        .unwrap();
    let first_credit = credit(first.clone(), 37);
    let second = crate::memory_accounting::AccountantHandle::from_host(accountant.clone())
        .vector_budget()
        .unwrap();
    assert!(
        !Arc::ptr_eq(&first, &second),
        "independent factory calls allocate separate adapters"
    );
    let second_credit = credit(second.clone(), 19);
    let mut visited = Vec::new();
    VectorCopyAdmission::visit_retained_copy_credit(
        &first_credit,
        second.as_ref(),
        &mut |id, bytes, caller| visited.push((id, bytes, caller)),
    );
    assert_eq!(
        visited,
        vec![(Arc::as_ptr(&first_credit) as usize, 37, true)],
        "same accountant must be caller funded through independent adapters"
    );
    let before_prepaid = accountant.usage().used;
    let mut prepaid = VectorCopyAdmission::new(first.clone());
    prepaid.reserve(41).unwrap();
    let (mut adopted, actual_requests, request_count) =
        crate::persistence::content_identity::proof_allocation_oracle_tests::native_requests_for_test(||
            VectorCopyAdmission::from_prepaid(second.clone(), Prepaid(prepaid)).unwrap());
    assert_eq!(request_count, 1, "the prepaid control is one actual Box allocation");
    assert_eq!(actual_requests[0].1, std::mem::size_of::<Prepaid>());
    adopted.reserve(41).unwrap();
    assert_eq!(
        accountant.usage().used,
        before_prepaid + 41 + actual_requests[0].1,
        "prepaid payload is charged once and its actual Box control stays funded"
    );
    drop(adopted);
    assert_eq!(accountant.usage().used, before_prepaid);
    let scores =
        contextdb_vector::mem::SearchScores::new(&VectorIndexRef::new("proof", "v"), first.clone())
            .unwrap();
    let (rows, charge) = scores.into_prepaid(&second).unwrap();
    let prepaid_scores = VectorCopyAdmission::from_prepaid(second.clone(), charge).unwrap();
    drop(rows);
    drop(prepaid_scores);
    assert_eq!(accountant.usage().used, before_prepaid);
    let (scores, trace) = db
        .vector
        .search_with_mode_owned(
            VectorIndexRef::new("trace", "v"),
            &[],
            0,
            None,
            SnapshotId(0),
            contextdb_core::VectorSearchMode::Exact,
            first.clone(),
        )
        .unwrap();
    drop(scores);
    let (trace, charge) = trace.into_prepaid(&second).unwrap();
    let trace_prepaid = VectorCopyAdmission::from_prepaid(second.clone(), charge).unwrap();
    drop(trace);
    drop(trace_prepaid);
    assert_eq!(
        accountant.usage().used,
        before_prepaid,
        "trace prepaid retains and releases its original owner once"
    );
    let payload = mixed_payload_owners(first.clone(), second.clone());
    assert!(accountant.usage().used > before_prepaid);
    drop(payload);
    assert_eq!(
        accountant.usage().used,
        before_prepaid,
        "separate invocation body/header/directory owners reconcile on final release"
    );
    db.vector_store
        .register_durable_raw_column_factory(Arc::new(Credits(vec![
            first_credit.clone(),
            first_credit.clone(),
            second_credit.clone(),
        ])));
    let mut complete = Vec::new();
    db.visit_source_copy_credits(&mut |id, bytes, caller| complete.push((id, bytes, caller)));
    assert_eq!(complete.len(), 3);
    assert!(
        complete.iter().all(|x| x.2),
        "Database forwards its stored vector funder"
    );
    assert_eq!(
        complete[0].0, complete[1].0,
        "shared credit clones retain one allocation identity"
    );
    assert_ne!(
        complete[0].0, complete[2].0,
        "different credits retain distinct allocation identities for one funder"
    );
    let receipt = db.vector_memory_ownership_receipt().unwrap();
    assert_eq!(
        receipt.source_metadata, 56,
        "aggregate deduplicates allocation, not funder"
    );
    assert_eq!(receipt.reader_source_metadata, 0);
    db.vector_store
        .register_durable_raw_column_factory(Arc::new(Credits(Vec::new())));
    drop(first_credit);
    drop(second_credit);
    drop(first);
    drop(second);
    assert_eq!(
        accountant.usage().used,
        baseline,
        "every original owner releases exactly once"
    );
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    println!(
        "CDB127_SAME_FUNDER independent_invocations=2 distinct_adapter_allocations=2 caller_funded=true prepaid_no_second_charge=true score_prepaid=true trace_prepaid=true body_header_and_directory=true visit_rows=3 unique_credit_allocations=2 aggregate_writer=56 aggregate_reader=0 final_used=0"
    );
}

#[test]
#[serial]
fn distinct_writer_and_reader_funders_refuse_foreign_prepaid_and_refund_original() {
    embedded_graph_reader_scratch_rows();
    let first = Arc::new(MemoryAccountant::no_limit());
    let second = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(first.clone());
    let other_root =
        crate::memory_accounting::WriterBudget::from_accountant(second.clone()).unwrap();
    let a = crate::memory_accounting::AccountantHandle::from_host(first.clone())
        .vector_budget()
        .unwrap();
    let b = crate::memory_accounting::AccountantHandle::from_host(second.clone())
        .vector_budget()
        .unwrap();
    let first_baseline = first.usage().used;
    let second_baseline = second.usage().used;
    let a_credit = credit(a.clone(), 13);
    let b_credit = credit(b.clone(), 29);
    let mut foreign = None;
    VectorCopyAdmission::visit_retained_copy_credit(&a_credit, b.as_ref(), &mut |_, _, caller| {
        foreign = Some(caller)
    });
    assert_eq!(foreign, Some(false));
    let mut prepaid = VectorCopyAdmission::new(a.clone());
    prepaid.reserve(31).unwrap();
    assert!(VectorCopyAdmission::from_prepaid(b.clone(), Prepaid(prepaid)).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    assert_eq!(second.usage().used, second_baseline + 29);
    let scores =
        contextdb_vector::mem::SearchScores::new(&VectorIndexRef::new("proof", "v"), a.clone())
            .unwrap();
    assert!(scores.into_prepaid(&b).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    let (scores, trace) = db
        .vector
        .search_with_mode_owned(
            VectorIndexRef::new("trace", "v"),
            &[],
            0,
            None,
            SnapshotId(0),
            contextdb_core::VectorSearchMode::Exact,
            a.clone(),
        )
        .unwrap();
    drop(scores);
    assert!(trace.into_prepaid(&b).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    let payload = mixed_payload_owners(a.clone(), b.clone());
    assert!(
        first.usage().used > first_baseline + 13,
        "original funder retains body and directory charge"
    );
    assert!(
        second.usage().used > second_baseline + 29,
        "different funder retains only its new header charge"
    );
    drop(payload);
    assert_eq!(first.usage().used, first_baseline + 13);
    assert_eq!(second.usage().used, second_baseline + 29);
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_000_000);
    let reader_a = crate::vector_read_memory::budget(parent.child().unwrap());
    let reader_b = crate::vector_read_memory::budget(parent.child().unwrap());
    let controls = parent.retained_bytes_for_test();
    let r_credit = credit(reader_a.clone(), 23);
    let mut reader_foreign = None;
    VectorCopyAdmission::visit_retained_copy_credit(
        &r_credit,
        reader_b.as_ref(),
        &mut |_, _, caller| reader_foreign = Some(caller),
    );
    assert_eq!(
        reader_foreign,
        Some(false),
        "reader children sharing a parent remain distinct funders"
    );
    let mut prepaid = VectorCopyAdmission::new(reader_a.clone());
    prepaid.reserve(17).unwrap();
    assert!(VectorCopyAdmission::from_prepaid(reader_b.clone(), Prepaid(prepaid)).is_err());
    assert_eq!(parent.retained_bytes_for_test(), controls + 23);
    db.vector_store
        .register_durable_raw_column_factory(Arc::new(Credits(vec![
            a_credit.clone(),
            b_credit.clone(),
            r_credit.clone(),
        ])));
    let receipt = db.vector_memory_ownership_receipt().unwrap();
    assert_eq!(receipt.source_metadata, 13);
    assert_eq!(receipt.reader_source_metadata, 52);
    db.vector_store
        .register_durable_raw_column_factory(Arc::new(Credits(Vec::new())));
    drop(a_credit);
    assert_eq!(first.usage().used, first_baseline);
    assert_eq!(second.usage().used, second_baseline + 29);
    drop(b_credit);
    assert_eq!(second.usage().used, second_baseline);
    drop(r_credit);
    assert_eq!(parent.retained_bytes_for_test(), controls);
    drop(reader_a);
    drop(reader_b);
    assert_eq!(parent.retained_bytes_for_test(), 0);
    drop(a);
    drop(b);
    drop(db);
    drop(other_root);
    assert_eq!(first.usage().used, 0);
    assert_eq!(second.usage().used, 0);
    println!(
        "CDB127_DISTINCT_FUNDER writers=distinct reader_children=distinct foreign_prepaid=refused foreign_score=refused foreign_trace=refused mixed_body_header_refunds=true aggregate_writer=13 aggregate_other=52 original_owner_refunds=true final_writers=0 final_readers=0"
    );
}


// Public embedding ruling: a search uses its operation child, even with no
// writer headroom. This row belongs to the existing distinct-funder proof;
// integration tests cannot call the private explicit-owner entry point.
fn embedded_graph_reader_scratch_rows() {
    use contextdb_core::read_memory::ReadMemoryBudget;
    use crate::vector_read_memory::{DeclaredReadMemory, DeclaredReadChildObserver, declared_child_observer};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use contextdb_vector::hnsw::{HnswCandidateObserver, HnswCandidateDistanceEvent, with_hnsw_candidate_observer};
    struct Distances(AtomicUsize);
    impl HnswCandidateObserver for Distances {
        fn before_candidate_distance(&self, _: HnswCandidateDistanceEvent) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct ReaderSpy {
        inner: Arc<dyn MemoryBudget>,
        child: DeclaredReadChildObserver,
        sibling: DeclaredReadChildObserver,
        parent: Arc<DeclaredReadMemory>,
        graph_calls: AtomicUsize,
        debit: AtomicUsize,
        refund: AtomicUsize,
        refuse_at: usize,
        pressure: AtomicUsize,
        distances: Arc<Distances>,
        refused_distances: AtomicUsize,
        body_invoice: AtomicUsize,
        body_seen: AtomicUsize,
        body_frees: AtomicUsize,
    }
    impl std::fmt::Debug for ReaderSpy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("ReaderSpy") }
    }
    impl MemoryBudget for ReaderSpy {
        fn control_invoice(&self) -> Option<contextdb_core::read_memory::ReadShared<contextdb_core::read_memory::ReadCredit>> { self.inner.control_invoice() }
        fn stable_funder_identity(&self) -> usize { self.inner.stable_funder_identity() }
        fn try_allocate_for(&self, bytes: usize, subsystem: &str, operation: &str, hint: &str) -> Result<()> {
            if bytes != 0 && operation == "graph_scratch" {
                let ordinal = self.graph_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if ordinal == self.refuse_at {
                    let available = 8 * 1024 * 1024 - self.parent.retained_bytes_for_test() as usize;
                    let pressure = available.checked_sub(bytes - 1).expect("clean reader has room for graph scratch");
                    self.parent.try_reserve(pressure, "proof", "competing_reader", "test pressure")?;
                    self.pressure.store(pressure, Ordering::SeqCst);
                    self.refused_distances.store(self.distances.0.load(Ordering::SeqCst), Ordering::SeqCst);
                }
            }
            let before = self.child.sample();
            let result = self.inner.try_allocate_for(bytes, subsystem, operation, hint);
            let after = self.child.sample();
            assert_ne!(after.owner_identity, after.parent_identity, "operation is a child, not its parent");
            assert_ne!(after.owner_identity, self.sibling.sample().owner_identity);
            assert_eq!(self.sibling.sample().charged_bytes, 0, "sibling is never the executing owner");
            assert_eq!(after.charged_bytes - before.charged_bytes,
                if result.is_ok() { bytes as u64 } else { 0 }, "the actual child receives every vector debit");
            if result.is_ok() { self.debit.fetch_add(bytes, Ordering::SeqCst); }
            result
        }
        fn release(&self, bytes: usize) {
            let body = self.body_invoice.load(Ordering::SeqCst);
            if body != 0 && body == bytes {
                let freed = crate::persistence::content_identity::proof_allocation_oracle_tests::source_free_for_test(1);
                assert_eq!(freed[0].2, 1, "the actual body invoice refunds only after its F32 allocation physically frees");
                self.body_invoice.store(0, Ordering::SeqCst);
                self.body_frees.fetch_add(1, Ordering::SeqCst);
            }
            let before = self.child.sample().charged_bytes;
            self.inner.release(bytes);
            assert_eq!(before - self.child.sample().charged_bytes, bytes as u64,
                "refund returns to the actual child");
            self.refund.fetch_add(bytes, Ordering::SeqCst);
        }
    }
    struct PhysicalBodies(Arc<ReaderSpy>);
    impl contextdb_vector::mem::NativeVectorScoreObserver for PhysicalBodies {
        fn before_native_vector_score(&self) {}
        fn after_native_vector_score(&self, event: contextdb_vector::mem::NativeVectorScoreEvent) {
            if let Some((pointer, bytes, invoice)) = event.body_allocation() {
                assert_eq!(self.0.body_invoice.swap(invoice, Ordering::SeqCst), 0,
                    "a previous physical body must refund before another body is scored");
                crate::persistence::content_identity::proof_allocation_oracle_tests::watch_source_free_for_test(
                    &[(pointer, bytes, std::mem::align_of::<f32>())], 0);
                self.0.body_seen.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    let vector = |seed: u64| {
        let mut state = seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(17);
        (0..32).map(|_| {
            state ^= state << 13; state ^= state >> 7; state ^= state << 17;
            ((state >> 40) as i32 - (1 << 23)) as f32 / (1 << 23) as f32
        }).collect::<Vec<_>>()
    };
    for file_backed in [false, true] {
    let directory = tempfile::tempdir().unwrap();
    let db = if file_backed {
        Database::open(directory.path().join("reader-physical.db")).unwrap()
    } else { Database::open_memory() };
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, embedding VECTOR(32))", &HashMap::new()).unwrap();
    for id in 1..=256 {
        db.execute("INSERT INTO items (id, embedding) VALUES ($id, $v)", &HashMap::from([
            ("id".into(), Value::Int64(id)), ("v".into(), Value::Vector(vector(id as u64))),
        ])).unwrap();
    }
    for _ in 0..32 { db.run_maintenance_cycle().unwrap(); }
    let mut query = SemanticQuery::new("items", "embedding", vector(10_000), 10);
    query.search_mode = Some(contextdb_core::VectorSearchMode::Indexed);
    let control = db.semantic_search(query.clone()).unwrap();
    let answer = |rows: &[SearchResult]| rows.iter().map(|row| (row.row_id, row.vector_score.to_bits())).collect::<Vec<_>>();
    assert_eq!(control.len(), 10);
    let mut exact_query = query.clone();
    exact_query.search_mode = Some(contextdb_core::VectorSearchMode::Exact);
    let exact_control = db.semantic_search(exact_query).unwrap();
    let writer = db.accountant.usage().used;
    db.set_memory_limit(Some(writer)).unwrap();
    assert_eq!(answer(&db.semantic_search(query.clone()).unwrap()), answer(&control));
    let mut successful_graph_calls = 0;
    for (mode, refuse_at) in [
        (contextdb_core::VectorSearchMode::Indexed, 0),
        (contextdb_core::VectorSearchMode::Indexed, 1),
        (contextdb_core::VectorSearchMode::Indexed, 3),
        (contextdb_core::VectorSearchMode::Indexed, 0),
        (contextdb_core::VectorSearchMode::Exact, 0),
    ] {
        let expected = if mode == contextdb_core::VectorSearchMode::Exact { &exact_control } else { &control };
        let parent = DeclaredReadMemory::from_reader_limit(8 * 1024 * 1024);
        let child = parent.child().unwrap();
        let sibling = parent.child().unwrap();
        let observer = declared_child_observer(&child).unwrap();
        let standing = parent.retained_bytes_for_test();
        let distances = Arc::new(Distances(AtomicUsize::new(0)));
        let spy = Arc::new(ReaderSpy {
            inner: crate::vector_read_memory::budget(child.clone()),
            child: observer.clone(), sibling: declared_child_observer(&sibling).unwrap(),
            parent: parent.clone(), graph_calls: AtomicUsize::new(0),
            debit: AtomicUsize::new(0), refund: AtomicUsize::new(0), refuse_at,
            pressure: AtomicUsize::new(0), distances: distances.clone(),
            refused_distances: AtomicUsize::new(usize::MAX),
            body_invoice: AtomicUsize::new(0), body_seen: AtomicUsize::new(0), body_frees: AtomicUsize::new(0),
        });
        let mut owner = VectorCopyAdmission::new(spy.clone());
        // Public answer equivalence was checked above. Measure the same native
        // graph-search operation, including its owned scores and trace through
        // physical destruction; legacy SearchResult value copying is outside
        // the graph-scratch contract and retains its separate failed receipt.
        let operation_index = VectorIndexRef::new("items", "embedding");
        let source_scope = db.writer_read_scope(child.clone()).unwrap();
        let vector_scope = db.vector_read_scope(db.snapshot(), child.clone(), &mut owner).unwrap();
        let physical_bodies = Arc::new(PhysicalBodies(spy.clone()));
        let operation = || contextdb_vector::mem::with_native_vector_score_observer_for_test(
            physical_bodies.clone(), || with_hnsw_candidate_observer(distances.clone(), || {
            let (rows, trace) = db.owner_vector_search(
                operation_index, &query.query, 10, None, db.snapshot(),
                mode, spy.clone(), child.clone(), &mut owner)?;
            assert_eq!(rows.len(), expected.len());
            assert!(rows.iter().zip(expected).all(|((id, score), expected)|
                *id == expected.row_id && score.to_bits() == expected.vector_score.to_bits()));
            drop(rows);
            drop(trace);
            Ok::<(), Error>(())
        }));
        let result = if refuse_at == 0 {
            let result = crate::persistence::content_identity::proof_allocation_oracle_tests::reader_requests_for_test(
                observer.physical_funding_counter_for_test(), operation);
            let physical = crate::persistence::content_identity::proof_allocation_oracle_tests::source_unfunded_peak_for_test();
            assert_eq!(physical.0, 0, "actual operation allocations stay paid through physical free (file={file_backed}, mode={mode:?}): {physical:?}");
            result
        } else { operation() };
        if refuse_at == 0 {
            result.unwrap();
            if mode == contextdb_core::VectorSearchMode::Indexed {
                successful_graph_calls = spy.graph_calls.load(Ordering::SeqCst);
                assert!(successful_graph_calls >= 3, "real graph reservations executed");
                assert!(distances.0.load(Ordering::SeqCst) > 0, "real graph distances executed");
            } else {
                assert_eq!(spy.graph_calls.load(Ordering::SeqCst), 0, "exhaustive bodies do not allocate graph scratch");
            }
        } else {
            assert!(matches!(result, Err(Error::MemoryBudgetExceeded { ref operation, .. }) if operation == "graph_scratch"), "real reader pressure must refuse graph scratch: {result:?}");
            assert_eq!(spy.graph_calls.load(Ordering::SeqCst), refuse_at,
                "no later graph reservation after refusal");
            assert_eq!(distances.0.load(Ordering::SeqCst), spy.refused_distances.load(Ordering::SeqCst),
                "no later distance after refused reservation");
        }
        assert_eq!(spy.body_invoice.load(Ordering::SeqCst), 0);
        assert_eq!(spy.body_seen.load(Ordering::SeqCst), spy.body_frees.load(Ordering::SeqCst),
            "every real copied body physically frees before its matching invoice refunds");
        if file_backed && mode == contextdb_core::VectorSearchMode::Exact {
            assert_eq!(spy.body_frees.load(Ordering::SeqCst), 256, "all real file-backed exhaustive bodies are physically witnessed");
        }
        db.clear_last_vector_search_trace();
        drop(vector_scope);
        drop(source_scope);
        drop(owner);
        let pressure = spy.pressure.swap(0, Ordering::SeqCst);
        parent.release(pressure);
        assert_eq!(spy.debit.load(Ordering::SeqCst), spy.refund.load(Ordering::SeqCst));
        assert_eq!(observer.sample().charged_bytes, 0);
        assert_eq!(parent.retained_bytes_for_test(), standing);
        assert_eq!(db.accountant.usage().used, writer, "reader neither debits nor refunds writer");
        drop(physical_bodies);
        drop(spy); drop(child); drop(sibling);
        assert_eq!(parent.retained_bytes_for_test(), 0, "operation controls also retire");
    }
    assert!(successful_graph_calls >= 3);
    }
}
