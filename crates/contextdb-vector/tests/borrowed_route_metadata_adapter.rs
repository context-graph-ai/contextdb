//! The source adapter validates current identities and retains only scalar
//! marks. This separate test binary measures large allocation requests on the
//! calling thread, including temporary clones that are freed before return.

use contextdb_core::{
    Error, Lsn, RowId, TxId, VectorEntry, VectorIndexRef, VectorPartitionComponent,
    VectorPartitionKey, VectorQuantization, VectorSearchMode,
};
use contextdb_vector::{
    MemoryBudget, PartitionedVectorDelete, PartitionedVectorEntry, VectorCopyAdmission,
    VectorIndexLayout, VectorMaintenanceFailure, VectorMaintenanceFailureDetails,
    VectorPartitionRef, VectorPartitionSourceMaintenanceMetadata, VectorRouteQuarantineReason,
    VectorStore,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingAllocator;

thread_local! {
    static LARGE_ALLOCATIONS: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

fn note_allocation(bytes: usize) {
    let _ = LARGE_ALLOCATIONS.try_with(|counter| {
        if let Some((minimum, count)) = counter.get()
            && bytes >= minimum
        {
            counter.set(Some((minimum, count + 1)));
        }
    });
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            note_allocation(layout.size());
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            note_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !pointer.is_null() {
            note_allocation(new_size);
        }
        pointer
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

struct AllocationWatch;

impl Drop for AllocationWatch {
    fn drop(&mut self) {
        LARGE_ALLOCATIONS.with(|counter| counter.set(None));
    }
}

fn count_large_allocations<T>(minimum: usize, run: impl FnOnce() -> T) -> (T, usize) {
    LARGE_ALLOCATIONS.with(|counter| {
        assert!(counter.replace(Some((minimum, 0))).is_none());
    });
    let watch = AllocationWatch;
    let result = run();
    let count = LARGE_ALLOCATIONS.with(|counter| counter.get().unwrap().1);
    drop(watch);
    (result, count)
}

#[derive(Debug, Default)]
struct Budget {
    used: AtomicUsize,
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        _subsystem: &str,
        _operation: &str,
        _hint: &str,
    ) -> contextdb_core::Result<()> {
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.used.fetch_sub(bytes, Ordering::SeqCst);
        assert!(previous >= bytes, "each graph charge is released once");
    }
}

fn insert_partition_fixture(
    store: &VectorStore,
    body_owner: &Arc<Budget>,
    entries: Vec<PartitionedVectorEntry>,
    executing_owner: VectorCopyAdmission,
) -> contextdb_core::Result<()> {
    let mut bodies = VectorCopyAdmission::new(body_owner.clone());
    bodies.reserve(
        entries
            .iter()
            .map(|entry| entry.entry.vector.len() * std::mem::size_of::<f32>())
            .sum(),
    )?;
    let result = store.apply_partitioned_inserts(entries, executing_owner);
    if result.is_ok() {
        bodies.transfer();
    }
    result
}
fn insert_inline_fixture(
    store: &VectorStore,
    body_owner: &Arc<Budget>,
    entries: Vec<VectorEntry>,
    executing_owner: VectorCopyAdmission,
) -> contextdb_core::Result<()> {
    let mut bodies = VectorCopyAdmission::new(body_owner.clone());
    bodies.reserve(
        entries
            .iter()
            .map(|entry| entry.vector.len() * std::mem::size_of::<f32>())
            .sum(),
    )?;
    let result = store.apply_inserts(entries, executing_owner);
    if result.is_ok() {
        bodies.transfer();
    }
    result
}

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "embedding")
}

fn failure() -> VectorMaintenanceFailureDetails {
    VectorMaintenanceFailureDetails::from_error(&Error::MemoryBudgetExceeded {
        subsystem: "vector_index".into(),
        operation: "prepare_vector_generation".into(),
        requested_bytes: 128,
        available_bytes: 64,
        budget_limit_bytes: 1024,
        hint: "raise memory".into(),
    })
}

fn empty_metadata() -> VectorPartitionSourceMaintenanceMetadata {
    VectorPartitionSourceMaintenanceMetadata {
        historical_only: false,
        quarantine: None,
        failure: None,
    }
}

fn text_key(value: impl Into<String>) -> VectorPartitionKey {
    VectorPartitionKey::from_components(vec![VectorPartitionComponent::Text(value.into())])
}

fn partitioned_layout() -> VectorIndexLayout {
    VectorIndexLayout::new(
        2,
        VectorQuantization::F32,
        vec!["scope".into()],
        Some(4),
        VectorSearchMode::Auto,
    )
}

fn entry(index: &VectorIndexRef, row: u64) -> VectorEntry {
    VectorEntry {
        index: index.clone(),
        row_id: RowId(row),
        vector: vec![1.0, 0.0],
        created_tx: TxId(row),
        deleted_tx: None,
        lsn: Lsn(row),
    }
}

#[test]
fn fixed_metadata_can_be_sampled_from_source_and_publication_views() {
    fn require_copy<T: Copy>(_: T) {}

    let declared_column_owner = Arc::new(Budget::default());
    let _executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    store
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let state = store.state(&index).unwrap();
    let partition = VectorPartitionRef::new(index.clone(), VectorPartitionKey::unpartitioned());
    let metadata = store
        .with_partition_generation_publication(&partition, || state.source_maintenance_metadata());
    assert_eq!(metadata, empty_metadata());
    require_copy(metadata);
    let (sampled, allocations) = count_large_allocations(1, || state.source_maintenance_metadata());
    assert_eq!(sampled, metadata);
    assert_eq!(allocations, 0, "fixed scalar sampling allocates nothing");
    store
        .with_partition_sources::<Error, _>(&index, |sources| {
            assert_eq!(sources.len(), 1);
            sources.visit(|_, source| {
                assert_eq!(source.source_maintenance_metadata(), metadata);
                Ok::<(), Error>(())
            })
        })
        .unwrap();
    // The source view has ended before the adapter takes maintenance.
    assert!(store.apply_partition_source_maintenance_metadata(&index, &state, None, None));
}

#[test]
fn scalar_marks_and_none_leave_existing_fields_and_serving_state_intact() {
    let declared_column_owner = Arc::new(Budget::default());
    let _executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    store
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let state = store.state(&index).unwrap();
    let partition = VectorPartitionRef::new(index.clone(), VectorPartitionKey::unpartitioned());
    let before = store.partition_graph_generation_status(&partition);
    let failure = failure();
    assert!(store.apply_partition_source_maintenance_metadata(&index, &state, Some(failure), None));
    assert_eq!(state.source_maintenance_metadata().quarantine, None);
    assert!(store.apply_partition_source_maintenance_metadata(
        &index,
        &state,
        None,
        Some(VectorRouteQuarantineReason::CorruptChanges),
    ));
    let marked = VectorPartitionSourceMaintenanceMetadata {
        historical_only: false,
        quarantine: Some(VectorRouteQuarantineReason::CorruptChanges),
        failure: Some(failure),
    };
    assert_eq!(state.source_maintenance_metadata(), marked);
    assert!(store.apply_partition_source_maintenance_metadata(&index, &state, None, None));
    store
        .record_partition_maintenance_error(&partition, &Error::ReadCancelled)
        .unwrap();
    assert_eq!(state.source_maintenance_metadata(), marked);
    assert_eq!(store.partition_graph_generation_status(&partition), before);
}

#[test]
fn removed_index_rejects_its_sampled_source() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    store
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let sampled = store.state(&index).unwrap();
    assert!(
        store
            .deregister_index(
                &index,
                declared_column_owner.as_ref(),
                &mut VectorCopyAdmission::new(executing_owner.clone())
            )
            .unwrap()
            .is_empty()
    );
    assert!(!store.apply_partition_source_maintenance_metadata(
        &index,
        &sampled,
        Some(failure()),
        Some(VectorRouteQuarantineReason::CorruptBase),
    ));
    assert_eq!(sampled.source_maintenance_metadata(), empty_metadata());
    assert!(!store.contains_index(&index));
}

#[test]
fn replacement_column_rejects_the_old_state_without_marking_the_new_source() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    store
        .register_index(
            index.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let sampled = store.state(&index).unwrap();
    store
        .register_index_with_layout(
            index.clone(),
            VectorIndexLayout::unpartitioned(3, VectorQuantization::F32),
            declared_column_owner.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    let replacement = store.state(&index).unwrap();
    assert!(!Arc::ptr_eq(&sampled, &replacement));
    assert!(!store.apply_partition_source_maintenance_metadata(
        &index,
        &sampled,
        Some(failure()),
        Some(VectorRouteQuarantineReason::CorruptBase),
    ));
    assert_eq!(sampled.source_maintenance_metadata(), empty_metadata());
    assert_eq!(replacement.source_maintenance_metadata(), empty_metadata());
    assert!(store.apply_partition_source_maintenance_metadata(
        &index,
        &replacement,
        Some(failure()),
        None,
    ));
}

#[test]
fn removed_and_recreated_partition_rejects_the_old_state_in_the_same_column() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    let key = text_key("same-route");
    store
        .register_index_with_layout(
            index.clone(),
            partitioned_layout(),
            declared_column_owner.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    insert_partition_fixture(
        &store,
        &declared_column_owner,
        vec![PartitionedVectorEntry::new(key.clone(), entry(&index, 1))],
        VectorCopyAdmission::new(executing_owner.clone()),
    )
    .unwrap();
    let sampled = store.partition_state(&index, &key).unwrap();
    store
        .prune_row_ids_in_indexes(
            &HashSet::from([RowId(1)]),
            std::slice::from_ref(&index),
            declared_column_owner.as_ref(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    assert!(store.try_partition_state(&index, &key).is_none());
    assert!(!store.apply_partition_source_maintenance_metadata(
        &index,
        &sampled,
        Some(failure()),
        None
    ));
    insert_partition_fixture(
        &store,
        &declared_column_owner,
        vec![PartitionedVectorEntry::new(key.clone(), entry(&index, 2))],
        VectorCopyAdmission::new(executing_owner.clone()),
    )
    .unwrap();
    let replacement = store.partition_state(&index, &key).unwrap();
    assert!(!Arc::ptr_eq(&sampled, &replacement));
    assert!(!store.apply_partition_source_maintenance_metadata(
        &index,
        &sampled,
        Some(failure()),
        Some(VectorRouteQuarantineReason::CorruptBase),
    ));
    assert_eq!(sampled.source_maintenance_metadata(), empty_metadata());
    assert_eq!(replacement.source_maintenance_metadata(), empty_metadata());
}

#[test]
fn renamed_index_rejects_the_old_name_even_when_that_name_is_reused() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let old = index();
    let renamed = VectorIndexRef::new("items", "renamed_embedding");
    store
        .register_index(
            old.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let sampled = store.state(&old).unwrap();
    store
        .prepare_index_rename(
            &old,
            &renamed,
            VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap()
        .publish();
    assert!(!store.apply_partition_source_maintenance_metadata(
        &old,
        &sampled,
        Some(failure()),
        None
    ));
    store
        .register_index(
            old.clone(),
            2,
            VectorQuantization::F32,
            declared_column_owner.clone(),
        )
        .unwrap();
    let replacement = store.state(&old).unwrap();
    assert!(!store.apply_partition_source_maintenance_metadata(
        &old,
        &sampled,
        Some(failure()),
        Some(VectorRouteQuarantineReason::IncompatibleFormat),
    ));
    assert_eq!(replacement.source_maintenance_metadata(), empty_metadata());
    assert_eq!(sampled.source_maintenance_metadata(), empty_metadata());
    assert!(Arc::ptr_eq(&store.state(&renamed).unwrap(), &sampled));
    assert!(store.apply_partition_source_maintenance_metadata(
        &renamed,
        &sampled,
        Some(failure()),
        None
    ));
}

#[test]
fn scalar_marking_preserves_historical_only_sources() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    let key = text_key("historical");
    store
        .register_index_with_layout(
            index.clone(),
            partitioned_layout(),
            declared_column_owner.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    insert_partition_fixture(
        &store,
        &declared_column_owner,
        vec![PartitionedVectorEntry::new(key.clone(), entry(&index, 1))],
        VectorCopyAdmission::new(executing_owner.clone()),
    )
    .unwrap();
    store
        .apply_partitioned_deletes(
            vec![PartitionedVectorDelete::new(
                index.clone(),
                key.clone(),
                RowId(1),
                TxId(2),
                TxId(1),
                Lsn(1),
            )],
            VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    // This isolated store has no concurrent commits or registered snapshots.
    assert_eq!(
        store
            .retire_snapshot_free_partition_states(
                &[],
                declared_column_owner.as_ref(),
                &mut VectorCopyAdmission::new(executing_owner.clone())
            )
            .unwrap(),
        1
    );
    let source = store.partition_state(&index, &key).unwrap();
    assert!(source.source_maintenance_metadata().historical_only);
    assert!(store.apply_partition_source_maintenance_metadata(
        &index,
        &source,
        Some(failure()),
        None
    ));
    let metadata = source.source_maintenance_metadata();
    assert!(metadata.historical_only);
    assert_eq!(metadata.failure, Some(failure()));
    assert_eq!(metadata.quarantine, None);
    assert!(
        store
            .partition_keys(
                &index,
                &mut VectorCopyAdmission::new(executing_owner.clone())
            )
            .unwrap()
            .is_empty()
    );
    store
        .with_partition_sources::<Error, _>(&index, |sources| {
            assert_eq!(sources.len(), 1);
            sources.visit(|_, state| {
                assert_eq!(state.source_maintenance_metadata(), metadata);
                Ok::<(), Error>(())
            })
        })
        .unwrap();
}

#[test]
fn successful_ordinary_publication_clears_an_adapter_failure() {
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    store
        .register_index_with_layout(
            index.clone(),
            VectorIndexLayout::unpartitioned(2, VectorQuantization::F32).with_policy(
                Some(1),
                Some(4),
                Some(64),
                Some(16),
                1,
            ),
            declared_column_owner.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    insert_inline_fixture(
        &store,
        &declared_column_owner,
        vec![entry(&index, 1), entry(&index, 2)],
        VectorCopyAdmission::new(executing_owner.clone()),
    )
    .unwrap();
    let sampled = store.state(&index).unwrap();
    assert!(store.apply_partition_source_maintenance_metadata(
        &index,
        &sampled,
        Some(failure()),
        None
    ));
    let budget = Arc::new(Budget::default());
    let report = store
        .run_hnsw_maintenance_cycle(
            budget.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    assert_eq!(report.built_partitions, 1);
    assert_eq!(sampled.source_maintenance_metadata().failure, None);
    assert!(store.has_hnsw_index_for(&index));
    assert!(store.apply_partition_source_maintenance_metadata(&index, &sampled, None, None));
    assert_eq!(sampled.source_maintenance_metadata().failure, None);
    store.clear_hnsw(budget.as_ref());
    assert_eq!(budget.used.load(Ordering::SeqCst), 0);
}

#[test]
fn giant_source_metadata_and_marking_allocate_no_giant_key_copy_or_encoding() {
    const GIANT: usize = 1024 * 1024;
    let declared_column_owner = Arc::new(Budget::default());
    let executing_owner = Arc::new(Budget::default());
    let store = VectorStore::default();
    let index = index();
    let key = text_key("k".repeat(GIANT));
    store
        .register_index_with_layout(
            index.clone(),
            partitioned_layout(),
            declared_column_owner.clone(),
            &mut VectorCopyAdmission::new(executing_owner.clone()),
        )
        .unwrap();
    insert_partition_fixture(
        &store,
        &declared_column_owner,
        vec![PartitionedVectorEntry::new(key.clone(), entry(&index, 1))],
        VectorCopyAdmission::new(executing_owner.clone()),
    )
    .unwrap();
    let source = store.partition_state(&index, &key).unwrap();
    let details = failure();
    assert_eq!(details.failure, VectorMaintenanceFailure::MemoryLimit);
    let (_, clone_allocations) = count_large_allocations(GIANT, || {
        std::hint::black_box(key.clone());
    });
    let (_, encoding_allocations) = count_large_allocations(GIANT, || {
        std::hint::black_box(key.canonical_bytes());
    });
    assert!(
        clone_allocations > 0,
        "the counter observes an actual giant typed-key clone"
    );
    assert!(
        encoding_allocations > 0,
        "the counter observes an actual canonical-key allocation"
    );
    // Enter the bulk boundary before measuring: the existing test-seams
    // passive observer copies registry keys when entering a bulk read. This
    // callback holds neither a source view nor the maintenance mutex, so the
    // adapter still takes and validates its own maintenance/registry guards.
    let ((applied, metadata), allocations) = store
        .with_registered_partition_state(&index, &key, |current| {
            assert!(Arc::ptr_eq(&source, &current));
            count_large_allocations(GIANT, || {
                let applied = store.apply_partition_source_maintenance_metadata(
                    &index,
                    &source,
                    Some(details),
                    None,
                );
                (applied, source.source_maintenance_metadata())
            })
        })
        .unwrap();
    assert!(applied);
    assert_eq!(metadata.failure, Some(details));
    assert_eq!(metadata.quarantine, None);
    assert_eq!(
        allocations, 0,
        "retaining a scalar failure cannot clone or encode the giant key"
    );
}
