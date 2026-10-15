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
