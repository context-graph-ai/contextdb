//! Owner-authorized deterministic addendum4 rows; no existing test is edited.
use super::*;
use contextdb_vector::{MemoryBudget, PrepaidVectorCopyCharge, VectorCopyAdmission};
use contextdb_vector::store::{DurableRawColumnFactory, DurableRawPartitionFactory, RawVectorReverseMembershipSource};
use serial_test::serial;

type CreatedSources = (Arc<dyn RawVectorReverseMembershipSource>, Arc<dyn DurableRawPartitionFactory>);
struct Credits(Vec<Arc<VectorCopyAdmission>>);
impl DurableRawColumnFactory for Credits {
    fn visit_retained_copy_credits(&self, caller: &dyn MemoryBudget, visit: &mut dyn FnMut(usize, usize, bool)) {
        for credit in &self.0 {
            VectorCopyAdmission::visit_retained_copy_credit(credit, caller, visit);
        }
    }
    fn create(&self, _: &VectorIndexRef, _: &VectorIndexLayout, _: VectorCopyAdmission, _: VectorCopyAdmission) -> Result<CreatedSources> {
        Err(Error::Other("proof visits existing credits without loading".into()))
    }
}
struct Prepaid(VectorCopyAdmission);
impl PrepaidVectorCopyCharge for Prepaid {
    fn accountant(&self) -> Arc<dyn MemoryBudget> { self.0.funder() }
    fn retained_bytes(&self) -> usize { self.0.retained_bytes() }
    fn transfer(self: Box<Self>) { self.0.transfer(); }
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
    let publication = store.prepare_partitioned_received_schema_publication(
        vec![(index.clone(), VectorIndexLayout::unpartitioned(3, VectorQuantization::F32))],
        vec![contextdb_vector::PartitionedVectorEntry::new(VectorPartitionKey::unpartitioned(), VectorEntry {
            index, row_id: RowId(1), vector: vec![1.0, 0.0, 0.0], created_tx: TxId(1), deleted_tx: None, lsn: Lsn(1),
        })], body.clone(), VectorCopyAdmission::new(body.clone()),
    ).unwrap();
    store.publish_prepared_received_schema(publication, body);
    let before = store.memory_ownership_snapshot_for_test();
    assert_eq!(before.raw_vector_bodies, 12);
    store.account_loaded_state(header).unwrap();
    let after = store.memory_ownership_snapshot_for_test();
    assert_eq!(after.raw_vector_bodies, before.raw_vector_bodies, "body stays charged to its original owner");
    assert!(after.raw_reverse_directory > before.raw_reverse_directory, "actual shared header is retained and counted");
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
        assert_eq!(budget.stable_funder_identity(), Arc::as_ptr(budget) as *const () as usize);
    }
    assert_eq!(erased.stable_funder_identity(), clone.stable_funder_identity());
    assert_ne!(erased.stable_funder_identity(), other.stable_funder_identity());
    let root = crate::memory_accounting::WriterBudget::from_accountant(first.clone()).unwrap();
    let adapter_a = crate::memory_accounting::AccountantHandle::from_host(first.clone()).vector_budget().unwrap();
    let adapter_b = crate::memory_accounting::AccountantHandle::from_host(first.clone()).vector_budget().unwrap();
    assert_eq!(adapter_a.stable_funder_identity(), erased.stable_funder_identity());
    assert_eq!(adapter_b.stable_funder_identity(), erased.stable_funder_identity());
    assert_ne!(Arc::as_ptr(&adapter_a) as *const () as usize, Arc::as_ptr(&adapter_b) as *const () as usize);
    drop(adapter_a);drop(adapter_b);drop(root);
    assert_eq!(first.usage().used, 0);
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_000_000);
    let reader = crate::vector_read_memory::budget(parent.child().unwrap());
    assert_eq!(reader.stable_funder_identity(), Arc::as_ptr(&reader) as *const () as usize);
    println!("CDB127_DEFAULT_COMPAT raw_accountant=unchanged reader_impl=unchanged clones=equal distinct_objects=distinct");
}

#[test]
#[serial]
fn independent_writer_invocations_keep_caller_credit_and_allocation_dedup() {
    let accountant = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(accountant.clone());
    let baseline = accountant.usage().used;
    let first = crate::memory_accounting::AccountantHandle::from_host(accountant.clone()).vector_budget().unwrap();
    let first_credit = credit(first.clone(), 37);
    let second = crate::memory_accounting::AccountantHandle::from_host(accountant.clone()).vector_budget().unwrap();
    assert!(!Arc::ptr_eq(&first, &second), "independent factory calls allocate separate adapters");
    let second_credit = credit(second.clone(), 19);
    let mut visited = Vec::new();
    VectorCopyAdmission::visit_retained_copy_credit(&first_credit, second.as_ref(), &mut |id, bytes, caller| visited.push((id, bytes, caller)));
    assert_eq!(visited, vec![(Arc::as_ptr(&first_credit) as usize, 37, true)], "same accountant must be caller funded through independent adapters");
    let before_prepaid = accountant.usage().used;
    let mut prepaid = VectorCopyAdmission::new(first.clone());
    prepaid.reserve(41).unwrap();
    let mut adopted = VectorCopyAdmission::from_prepaid(second.clone(), Prepaid(prepaid)).unwrap();
    adopted.reserve(41).unwrap();
    assert_eq!(accountant.usage().used, before_prepaid + 41, "prepaid capacity is not charged twice");
    drop(adopted);
    assert_eq!(accountant.usage().used, before_prepaid);
    let scores = contextdb_vector::mem::SearchScores::new(&VectorIndexRef::new("proof", "v"), first.clone()).unwrap();
    let (rows, charge) = scores.into_prepaid(&second).unwrap();
    let prepaid_scores = VectorCopyAdmission::from_prepaid(second.clone(), charge).unwrap();
    drop(rows);
    drop(prepaid_scores);
    assert_eq!(accountant.usage().used, before_prepaid);
    let (scores, trace) = db.vector.search_with_mode_owned(VectorIndexRef::new("trace", "v"), &[], 0, None, SnapshotId(0), contextdb_core::VectorSearchMode::Exact, first.clone()).unwrap();
    drop(scores);
    let (trace, charge) = trace.into_prepaid(&second).unwrap();
    let trace_prepaid = VectorCopyAdmission::from_prepaid(second.clone(), charge).unwrap();
    drop(trace);drop(trace_prepaid);
    assert_eq!(accountant.usage().used, before_prepaid, "trace prepaid retains and releases its original owner once");
    let payload = mixed_payload_owners(first.clone(), second.clone());
    assert!(accountant.usage().used > before_prepaid);
    drop(payload);
    assert_eq!(accountant.usage().used, before_prepaid, "separate invocation body/header/directory owners reconcile on final release");
    db.vector_store.register_durable_raw_column_factory(Arc::new(Credits(vec![first_credit.clone(), first_credit.clone(), second_credit.clone()])));
    let mut complete = Vec::new();
    db.visit_source_copy_credits(&mut |id, bytes, caller| complete.push((id, bytes, caller)));
    assert_eq!(complete.len(), 3);
    assert!(complete.iter().all(|x| x.2), "Database forwards its stored vector funder");
    assert_eq!(complete[0].0, complete[1].0, "shared credit clones retain one allocation identity");
    assert_ne!(complete[0].0, complete[2].0, "different credits retain distinct allocation identities for one funder");
    let receipt = db.vector_memory_ownership_receipt().unwrap();
    assert_eq!(receipt.source_metadata, 56, "aggregate deduplicates allocation, not funder");
    assert_eq!(receipt.reader_source_metadata, 0);
    db.vector_store.register_durable_raw_column_factory(Arc::new(Credits(Vec::new())));
    drop(first_credit);drop(second_credit);drop(first);drop(second);
    assert_eq!(accountant.usage().used, baseline, "every original owner releases exactly once");
    drop(db);
    assert_eq!(accountant.usage().used, 0);
    println!("CDB127_SAME_FUNDER independent_invocations=2 distinct_adapter_allocations=2 caller_funded=true prepaid_no_second_charge=true score_prepaid=true trace_prepaid=true body_header_and_directory=true visit_rows=3 unique_credit_allocations=2 aggregate_writer=56 aggregate_reader=0 final_used=0");
}

#[test]
#[serial]
fn distinct_writer_and_reader_funders_refuse_foreign_prepaid_and_refund_original() {
    let first = Arc::new(MemoryAccountant::no_limit());
    let second = Arc::new(MemoryAccountant::no_limit());
    let db = Database::open_memory_with_accountant(first.clone());
    let other_root = crate::memory_accounting::WriterBudget::from_accountant(second.clone()).unwrap();
    let a = crate::memory_accounting::AccountantHandle::from_host(first.clone()).vector_budget().unwrap();
    let b = crate::memory_accounting::AccountantHandle::from_host(second.clone()).vector_budget().unwrap();
    let first_baseline = first.usage().used;
    let second_baseline = second.usage().used;
    let a_credit = credit(a.clone(), 13);
    let b_credit = credit(b.clone(), 29);
    let mut foreign = None;
    VectorCopyAdmission::visit_retained_copy_credit(&a_credit, b.as_ref(), &mut |_, _, caller| foreign = Some(caller));
    assert_eq!(foreign, Some(false));
    let mut prepaid = VectorCopyAdmission::new(a.clone());prepaid.reserve(31).unwrap();
    assert!(VectorCopyAdmission::from_prepaid(b.clone(), Prepaid(prepaid)).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    assert_eq!(second.usage().used, second_baseline + 29);
    let scores = contextdb_vector::mem::SearchScores::new(&VectorIndexRef::new("proof", "v"), a.clone()).unwrap();
    assert!(scores.into_prepaid(&b).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    let (scores, trace) = db.vector.search_with_mode_owned(VectorIndexRef::new("trace", "v"), &[], 0, None, SnapshotId(0), contextdb_core::VectorSearchMode::Exact, a.clone()).unwrap();
    drop(scores);
    assert!(trace.into_prepaid(&b).is_err());
    assert_eq!(first.usage().used, first_baseline + 13);
    let payload = mixed_payload_owners(a.clone(), b.clone());
    assert!(first.usage().used > first_baseline + 13, "original funder retains body and directory charge");
    assert!(second.usage().used > second_baseline + 29, "different funder retains only its new header charge");
    drop(payload);
    assert_eq!(first.usage().used, first_baseline + 13);
    assert_eq!(second.usage().used, second_baseline + 29);
    let parent = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(1_000_000);
    let reader_a = crate::vector_read_memory::budget(parent.child().unwrap());
    let reader_b = crate::vector_read_memory::budget(parent.child().unwrap());
    let controls = parent.retained_bytes_for_test();
    let r_credit = credit(reader_a.clone(), 23);
    let mut reader_foreign = None;
    VectorCopyAdmission::visit_retained_copy_credit(&r_credit, reader_b.as_ref(), &mut |_, _, caller| reader_foreign = Some(caller));
    assert_eq!(reader_foreign, Some(false), "reader children sharing a parent remain distinct funders");
    let mut prepaid = VectorCopyAdmission::new(reader_a.clone());prepaid.reserve(17).unwrap();
    assert!(VectorCopyAdmission::from_prepaid(reader_b.clone(), Prepaid(prepaid)).is_err());
    assert_eq!(parent.retained_bytes_for_test(), controls + 23);
    db.vector_store.register_durable_raw_column_factory(Arc::new(Credits(vec![a_credit.clone(), b_credit.clone(), r_credit.clone()])));
    let receipt = db.vector_memory_ownership_receipt().unwrap();
    assert_eq!(receipt.source_metadata, 13);
    assert_eq!(receipt.reader_source_metadata, 52);
    db.vector_store.register_durable_raw_column_factory(Arc::new(Credits(Vec::new())));
    drop(a_credit);assert_eq!(first.usage().used, first_baseline);assert_eq!(second.usage().used, second_baseline + 29);
    drop(b_credit);assert_eq!(second.usage().used, second_baseline);
    drop(r_credit);assert_eq!(parent.retained_bytes_for_test(), controls);
    drop(reader_a);drop(reader_b);assert_eq!(parent.retained_bytes_for_test(), 0);
    drop(a);drop(b);drop(db);drop(other_root);
    assert_eq!(first.usage().used, 0);assert_eq!(second.usage().used, 0);
    println!("CDB127_DISTINCT_FUNDER writers=distinct reader_children=distinct foreign_prepaid=refused foreign_score=refused foreign_trace=refused mixed_body_header_refunds=true aggregate_writer=13 aggregate_other=52 original_owner_refunds=true final_writers=0 final_readers=0");
}
