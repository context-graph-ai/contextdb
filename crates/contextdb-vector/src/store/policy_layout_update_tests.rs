//! A policy-only layout change is admitted before it allocates, refused
//! without leaving anything charged or changed, returns every charge when it
//! is dropped unpublished, and when published keeps the column's partitions.

use super::*;

/// A budget that refuses anything past `limit` bytes in use.
#[derive(Debug)]
struct Budget {
    used: AtomicUsize,
    limit: usize,
}

impl Budget {
    fn with_limit(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
}

impl MemoryBudget for Budget {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        let used = self.used.load(Ordering::SeqCst);
        if used.saturating_add(bytes) > self.limit {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: self.limit.saturating_sub(used),
                budget_limit_bytes: self.limit,
                hint: hint.into(),
            });
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        let previous = self.used.fetch_sub(bytes, Ordering::SeqCst);
        assert!(previous >= bytes, "every charge is released once");
    }

    fn available_bytes(&self) -> Option<usize> {
        Some(self.limit.saturating_sub(self.used()))
    }
}

/// A partitioned index whose names and partition key are `width` bytes wide.
fn wide_index(n: usize, width: usize) -> (VectorIndexRef, VectorIndexLayout) {
    let index = VectorIndexRef::new(format!("t{n}_{}", "t".repeat(width)), "e".repeat(width));
    let mut layout = VectorIndexLayout::unpartitioned(3, VectorQuantization::F32);
    layout.partition_key_columns = vec!["k".repeat(width)];
    layout.max_partitions = Some(4);
    layout.search_mode = VectorSearchMode::Indexed;
    (index, layout)
}

fn policy_only(layout: &VectorIndexLayout) -> VectorIndexLayout {
    let mut changed = layout.clone();
    changed.search_mode = VectorSearchMode::Auto;
    changed.auto_index_at = Some(7);
    changed
}

fn store_with(indexes: &[(VectorIndexRef, VectorIndexLayout)]) -> VectorStore {
    let store = VectorStore::default();
    let declared: Arc<dyn MemoryBudget> = Budget::with_limit(usize::MAX);
    let mut owner = VectorCopyAdmission::new(declared.clone());
    for (index, layout) in indexes {
        store
            .register_index_with_layout(index.clone(), layout.clone(), declared.clone(), &mut owner)
            .expect("register one wide index");
    }
    store
}

fn layout_of(store: &VectorStore, index: &VectorIndexRef) -> VectorIndexLayout {
    store
        .with_index_layout(index, Clone::clone)
        .expect("the index is registered")
}

#[test]
fn a_policy_update_that_does_not_fit_is_refused_before_it_copies_anything() {
    let (index, layout) = wide_index(0, 4096);
    let store = store_with(&[(index.clone(), layout.clone())]);
    let budget = Budget::with_limit(index.table.len());
    let mut owner = VectorCopyAdmission::new(budget.clone());
    let refusal = store
        .prepare_policy_layout_update(&index, policy_only(&layout), &mut owner)
        .err()
        .expect("the copy of a wide index name does not fit");
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "a typed budget refusal: {refusal:?}"
    );
    drop(owner);
    assert_eq!(
        budget.used(),
        0,
        "a refused preparation leaves nothing charged"
    );
    assert_eq!(
        layout_of(&store, &index),
        layout,
        "the declaration is unchanged"
    );
}

#[test]
fn several_policy_updates_return_every_charge_when_dropped_unpublished() {
    let indexes = (0..6).map(|n| wide_index(n, 2048)).collect::<Vec<_>>();
    let store = store_with(&indexes);
    let budget = Budget::with_limit(usize::MAX);
    let mut owner = VectorCopyAdmission::new(budget.clone());
    let prepared = indexes
        .iter()
        .map(|(index, layout)| {
            store
                .prepare_policy_layout_update(index, policy_only(layout), &mut owner)
                .expect("an admitted policy update")
                .expect("a changed policy")
        })
        .collect::<Vec<_>>();
    let names: usize = indexes
        .iter()
        .map(|(index, _)| index.table.len() + index.column.len())
        .sum();
    assert!(
        budget.used() >= names,
        "every prepared copy of an index name is admitted: {} < {names}",
        budget.used()
    );
    drop(prepared);
    drop(owner);
    assert_eq!(budget.used(), 0, "unpublished updates return every charge");
    for (index, layout) in &indexes {
        assert_eq!(&layout_of(&store, index), layout, "nothing was published");
    }
}

#[test]
fn a_published_policy_update_changes_only_the_policy() {
    let (index, layout) = wide_index(0, 512);
    let store = store_with(&[(index.clone(), layout.clone())]);
    let budget = Budget::with_limit(usize::MAX);
    let mut owner = VectorCopyAdmission::new(budget.clone());
    let changed = policy_only(&layout);
    assert!(
        store
            .prepare_policy_layout_update(&index, layout.clone(), &mut owner)
            .expect("an unchanged declaration is admitted")
            .is_none(),
        "an unchanged declaration needs no update"
    );
    let prepared = store
        .prepare_policy_layout_update(&index, changed.clone(), &mut owner)
        .expect("an admitted policy update")
        .expect("a changed policy");
    store.publish_policy_layout_update(prepared);
    assert_eq!(layout_of(&store, &index), changed);
    drop(owner);
    assert_eq!(
        budget.used(),
        0,
        "the published update's copy owner is returned"
    );
    let mut reshaped = layout.clone();
    reshaped.partition_key_columns = vec!["other".into()];
    let mut owner = VectorCopyAdmission::new(budget.clone());
    assert!(
        store
            .prepare_policy_layout_update(&index, reshaped, &mut owner)
            .is_err(),
        "a storage-shape change is not a policy update"
    );
}

/// A refusable owner for a column's own directory charges.
#[derive(Debug, Default)]
struct Refusable {
    used: AtomicUsize,
    refuse: AtomicBool,
}

impl MemoryBudget for Refusable {
    fn try_allocate_for(
        &self,
        bytes: usize,
        subsystem: &str,
        operation: &str,
        hint: &str,
    ) -> Result<()> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.into(),
                operation: operation.into(),
                requested_bytes: bytes,
                available_bytes: 0,
                budget_limit_bytes: self.used.load(Ordering::SeqCst),
                hint: hint.into(),
            });
        }
        self.used.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::SeqCst);
    }
}

fn scoped_index() -> VectorIndexRef {
    VectorIndexRef::new("scoped", "embedding")
}

fn scope(n: usize) -> VectorPartitionKey {
    VectorPartitionKey::from_components(vec![contextdb_core::VectorPartitionComponent::Integer(
        n as i64,
    )])
}

/// A store with `partitions` healthy partitions of one vector each.
fn partitioned_store(partitions: usize, owner: Arc<Refusable>) -> (VectorStore, VectorIndexLayout) {
    let store = VectorStore::default();
    let layout = VectorIndexLayout::new(
        2,
        VectorQuantization::F32,
        vec!["scope".to_owned()],
        Some(partitions as u32),
        VectorSearchMode::Indexed,
    );
    let mut admission = VectorCopyAdmission::new(owner.clone());
    store
        .register_index_with_layout(
            scoped_index(),
            layout.clone(),
            owner.clone(),
            &mut admission,
        )
        .expect("register the scoped index");
    for n in 0..partitions {
        store
            .insert_loaded_partitioned_vector(
                PartitionedVectorEntry::new(
                    scope(n),
                    VectorEntry {
                        index: scoped_index(),
                        row_id: RowId(n as u64 + 1),
                        vector: vec![1.0, n as f32],
                        created_tx: TxId(1),
                        deleted_tx: None,
                        lsn: Lsn(1),
                    },
                ),
                &mut admission,
            )
            .expect("load one vector into its own partition");
    }
    (store, layout)
}

/// Publish `target` as a policy-only change and run the follow-up recheck
/// under `recheck_owner`; returns the partitions the recheck looked at.
fn publish_and_recheck(
    store: &VectorStore,
    target: VectorIndexLayout,
    recheck_owner: &mut VectorCopyAdmission,
) -> usize {
    let mut prepare_owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
    let prepared = store
        .prepare_policy_layout_update(&scoped_index(), target, &mut prepare_owner)
        .expect("admit the policy change")
        .expect("the policy changed");
    let published = store
        .publish_policy_layout_update(prepared)
        .expect("the column is still current");
    FAILURE_RECHECK_VISITS.with(|visits| visits.set(0));
    store
        .recheck_policy_maintenance_failures(&published, recheck_owner)
        .expect("the recheck is admitted");
    FAILURE_RECHECK_VISITS.with(|visits| visits.get())
}

/// A second policy change after `policy_only`.
fn policy_again(layout: &VectorIndexLayout) -> VectorIndexLayout {
    let mut changed = policy_only(layout);
    changed.auto_index_at = Some(9);
    changed
}

fn failure_of(store: &VectorStore, n: usize) -> Option<VectorMaintenanceFailure> {
    let mut owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
    store
        .partition_info(&scoped_index(), &scope(n), &mut owner)
        .expect("inspect one partition")
        .and_then(|info| info.maintenance_failure)
}

#[test]
fn a_policy_recheck_with_no_failures_looks_at_no_partition() {
    for partitions in [4, 16, 64] {
        let (store, layout) = partitioned_store(partitions, Arc::new(Refusable::default()));
        // A recheck budget too small to copy any partition is enough.
        let mut owner = VectorCopyAdmission::new(Budget::with_limit(0));
        assert_eq!(
            publish_and_recheck(&store, policy_only(&layout), &mut owner),
            0,
            "{partitions} healthy partitions are neither copied nor locked"
        );
    }
}

#[test]
fn a_policy_recheck_looks_only_at_the_failed_partition() {
    for partitions in [4, 16, 64] {
        let (store, layout) = partitioned_store(partitions, Arc::new(Refusable::default()));
        store
            .record_partition_maintenance_failure(
                &VectorPartitionRef::new(scoped_index(), scope(1)),
                VectorMaintenanceFailure::MemoryLimit,
            )
            .expect("record one partition's failure");
        let mut owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
        assert_eq!(
            publish_and_recheck(&store, policy_only(&layout), &mut owner),
            1,
            "only the failed partition of {partitions} is looked at"
        );
        assert_eq!(
            failure_of(&store, 1),
            None,
            "the new policy needs no work there, so its failure is cleared"
        );
        assert_eq!(
            publish_and_recheck(&store, policy_again(&layout), &mut owner),
            0,
            "a cleared failure is not looked at again"
        );
    }
}

#[test]
fn a_failure_the_column_cannot_index_is_still_rechecked() {
    let column_owner = Arc::new(Refusable::default());
    let (store, layout) = partitioned_store(16, column_owner.clone());
    column_owner.refuse.store(true, Ordering::SeqCst);
    store
        .record_partition_maintenance_failure(
            &VectorPartitionRef::new(scoped_index(), scope(3)),
            VectorMaintenanceFailure::MemoryLimit,
        )
        .expect("record a failure whose index entry is refused");
    column_owner.refuse.store(false, Ordering::SeqCst);
    let mut owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
    assert_eq!(
        publish_and_recheck(&store, policy_only(&layout), &mut owner),
        16,
        "an incomplete index makes the recheck look at every partition once"
    );
    assert_eq!(
        failure_of(&store, 3),
        None,
        "the unindexed failure is cleared"
    );
    assert_eq!(
        publish_and_recheck(&store, policy_again(&layout), &mut owner),
        0,
        "the rebuilt index is trusted again"
    );
}
