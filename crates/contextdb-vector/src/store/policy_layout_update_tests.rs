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
                subsystem: subsystem.to_owned().into(),
                operation: operation.to_owned().into(),
                requested_bytes: bytes,
                available_bytes: self.limit.saturating_sub(used),
                budget_limit_bytes: self.limit,
                hint: hint.to_owned().into(),
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

/// As [`publish_and_recheck`], returning the recheck's refusal instead of
/// panicking on it.
fn publish_and_try_recheck(
    store: &VectorStore,
    target: VectorIndexLayout,
    recheck_owner: &mut VectorCopyAdmission,
) -> Result<usize> {
    let mut prepare_owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
    let prepared = store
        .prepare_policy_layout_update(&scoped_index(), target, &mut prepare_owner)
        .expect("admit the policy change")
        .expect("the policy changed");
    let published = store
        .publish_policy_layout_update(prepared)
        .expect("the column is still current");
    FAILURE_RECHECK_VISITS.with(|visits| visits.set(0));
    store.recheck_policy_maintenance_failures(&published, recheck_owner)?;
    Ok(FAILURE_RECHECK_VISITS.with(|visits| visits.get()))
}

/// A distinct policy-only change for each `n`.
fn policy_number(layout: &VectorIndexLayout, n: u32) -> VectorIndexLayout {
    let mut changed = exact_policy(layout);
    changed.auto_index_at = Some(100 + n);
    changed
}

/// A policy under which a partition needs no maintenance at all: exact search
/// builds no graph, so a recheck can clear any recorded failure.
fn exact_policy(layout: &VectorIndexLayout) -> VectorIndexLayout {
    let mut changed = layout.clone();
    changed.search_mode = VectorSearchMode::Exact;
    changed
}

/// A second policy change after `exact_policy`.
fn policy_again(layout: &VectorIndexLayout) -> VectorIndexLayout {
    let mut changed = exact_policy(layout);
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
            publish_and_recheck(&store, exact_policy(&layout), &mut owner),
            1,
            "only the failed partition of {partitions} is looked at"
        );
        // A loaded partition with no graph still needs its initial build under
        // any policy, so its failure stands and stays noted; the engine test
        // a_synced_policy_change_clears_a_failure_the_new_policy_no_longer_needs
        // proves clearing where no work remains.
        assert_eq!(
            failure_of(&store, 1),
            Some(VectorMaintenanceFailure::MemoryLimit),
            "a failure the new policy still needs is kept"
        );
        assert_eq!(
            publish_and_recheck(&store, policy_again(&layout), &mut owner),
            1,
            "a kept failure is looked at again, and nothing else of {partitions} partitions"
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
        publish_and_recheck(&store, exact_policy(&layout), &mut owner),
        16,
        "an incomplete index makes the recheck look at every partition once"
    );
    assert_eq!(
        failure_of(&store, 3),
        Some(VectorMaintenanceFailure::MemoryLimit),
        "the unindexed failure is still needed, so it stands"
    );
    assert_eq!(
        publish_and_recheck(&store, policy_again(&layout), &mut owner),
        1,
        "the rebuilt index is trusted again: only the still-failing partition is looked at"
    );
}

/// A recheck refused while it rebuilds an incomplete failure index keeps the
/// index incomplete: whether the full partition snapshot or a later
/// partition inspection is refused, the next funded recheck looks at every
/// partition again and the outstanding failure is still found.
#[test]
fn a_refused_full_recheck_keeps_the_failure_index_incomplete() {
    let partitions = 16;
    let column_owner = Arc::new(Refusable::default());
    let (store, layout) = partitioned_store(partitions, column_owner.clone());
    column_owner.refuse.store(true, Ordering::SeqCst);
    store
        .record_partition_maintenance_failure(
            &VectorPartitionRef::new(scoped_index(), scope(3)),
            VectorMaintenanceFailure::MemoryLimit,
        )
        .expect("record a failure whose index entry is refused");
    column_owner.refuse.store(false, Ordering::SeqCst);
    let snapshot_bytes = (0..partitions).fold(0usize, |bytes, n| {
        bytes
            .saturating_add(std::mem::size_of::<(VectorPartitionKey, Arc<IndexState>)>())
            .saturating_add(scope(n).estimated_bytes())
    });

    let mut refused_snapshot = VectorCopyAdmission::new(Budget::with_limit(snapshot_bytes - 1));
    assert!(
        matches!(
            publish_and_try_recheck(&store, policy_number(&layout, 1), &mut refused_snapshot),
            Err(Error::MemoryBudgetExceeded { .. })
        ),
        "a snapshot the recheck cannot fund is refused typed"
    );
    let mut owner = VectorCopyAdmission::new(Budget::with_limit(usize::MAX));
    assert_eq!(
        publish_and_try_recheck(&store, policy_number(&layout, 2), &mut owner)
            .expect("a funded recheck"),
        partitions,
        "after a refused snapshot the index is still incomplete: every partition is looked at"
    );

    // The rebuilt index is complete again; make it incomplete once more and
    // refuse the inspection of the failed partition after the snapshot.
    column_owner.refuse.store(true, Ordering::SeqCst);
    store
        .record_partition_maintenance_failure(
            &VectorPartitionRef::new(scoped_index(), scope(9)),
            VectorMaintenanceFailure::MemoryLimit,
        )
        .expect("record a second failure whose index entry is refused");
    column_owner.refuse.store(false, Ordering::SeqCst);
    let mut refused_inspection = VectorCopyAdmission::new(Budget::with_limit(snapshot_bytes));
    assert!(
        matches!(
            publish_and_try_recheck(&store, policy_number(&layout, 3), &mut refused_inspection),
            Err(Error::MemoryBudgetExceeded { .. })
        ),
        "an inspection the recheck cannot fund is refused typed"
    );
    assert_eq!(
        publish_and_try_recheck(&store, policy_number(&layout, 4), &mut owner)
            .expect("a funded recheck"),
        partitions,
        "after a refused inspection the index is still incomplete: every partition is looked at"
    );
    for n in [3, 9] {
        assert_eq!(
            failure_of(&store, n),
            Some(VectorMaintenanceFailure::MemoryLimit),
            "the outstanding failure on partition {n} is still found"
        );
    }
    assert_eq!(
        publish_and_try_recheck(&store, policy_number(&layout, 5), &mut owner)
            .expect("a funded recheck"),
        2,
        "the completed index names exactly the two outstanding failures"
    );
}
