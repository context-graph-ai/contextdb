#![cfg(feature = "test-seams")]
//! Read-only catalog inspection tells the truth about derived damage without
//! re-hashing on every call. On a verdict-cache miss it verifies the
//! codebook and directories once and reports the live verdict, labelled
//! unrecorded until a maintenance call persists it; a second inspection
//! within the same admitted manifest window reads no codebook or directory
//! page; after that window is evicted one re-verification is allowed and no
//! more; once maintenance has recorded the verdict, inspection reports it as
//! recorded.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, PageStore, ReadPages, WritePages};
use contextdb_vector::ivf::{
    CatalogState, ClusteredIndex, Policy, TrainingPolicy, inspect_catalog,
};
use std::sync::Arc;

const UNBOUND: &str = "the fixture does not expose this inspection-verdict observation";
const CAP: u64 = 2;

/// Evicts the fixture's admitted manifest/control window, and the
/// inspection verdicts it holds with it, as the native holder's bounded
/// window would.
fn evict_manifest_window(_store: &ivf_pages::Pages) {
    unimplemented!("{UNBOUND}: evicting the admitted manifest window")
}

fn training() -> TrainingPolicy {
    TrainingPolicy {
        iterations: 2,
        reservoir_samples: 128,
        seed: 0,
        ingest_batch_keys: 8,
        phase_batch_entries: 16,
    }
}

fn policy() -> Policy {
    Policy::checked(2, 4, 4, 256, 192, true)
        .unwrap()
        .with_max_delta_runs(CAP)
        .unwrap()
        .with_coalesce_pages_per_step(32)
        .unwrap()
}

fn built() -> ivf_pages::Pages {
    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy(),
        training(),
        budget.into(),
    )
    .unwrap();
    index
        .build_partition(
            0,
            (0..16u64).map(|row| {
                let x = row as f32 * 0.031;
                (row, [x.cos(), x.sin()])
            }),
        )
        .unwrap();
    index.into_storage()
}

/// Inspects read-only and reports whether any codebook or directory page
/// was read doing it.
fn inspect(store: &ivf_pages::Pages) -> (CatalogState, bool) {
    store.touched.borrow_mut().clear();
    let state = inspect_catalog(store, 0, CAP).unwrap().unwrap();
    let verified = store.touched.borrow().iter().any(|key| {
        matches!(
            key.kind,
            PageKind::Codebook | PageKind::Directory | PageKind::DirectoryPresence
        )
    });
    (state, verified)
}

#[test]
fn a_healthy_partition_is_verified_at_most_once_per_admitted_window() {
    let store = built();
    let (first, _) = inspect(&store);
    assert_eq!(
        first.recovery_action, None,
        "a healthy partition shows no recovery action"
    );
    let (second, verified) = inspect(&store);
    assert_eq!(second, first);
    assert!(
        !verified,
        "a second inspection in the same window reads no codebook or directory page"
    );
    evict_manifest_window(&store);
    let (after_eviction, _) = inspect(&store);
    assert_eq!(
        after_eviction, first,
        "re-verification after eviction agrees"
    );
    let (again, verified) = inspect(&store);
    assert_eq!(again, first);
    assert!(!verified, "one re-verification per window, no more");
}

#[test]
fn unrecorded_damage_is_reported_live_and_once_recorded_is_reported_recorded() {
    let mut store = built();
    let manifest = store.manifest(0).unwrap().unwrap();
    // The first codebook page after the identity header page holds centroid
    // values: one changed byte no longer matches the digest the manifest's
    // codebook identity binds.
    let key = manifest.key(PageKind::Codebook, 0, 1);
    let mut page = [0u8; PAGE_BYTES];
    assert!(store.read_admitted_page(key, &mut page).unwrap());
    page[0] ^= 0x40;
    let mut tx = store.begin().unwrap();
    tx.put_page(key, &page).unwrap();
    tx.commit().unwrap();

    let (live, verified) = inspect(&store);
    assert!(verified, "a cache miss verifies the derived pages");
    assert_eq!(
        (live.recovery_action, live.verdict_recorded),
        (Some("rebuild"), false),
        "inspection reports the live damage, labelled unrecorded, before maintenance records it: {live:?}"
    );
    let (second, verified) = inspect(&store);
    assert_eq!(second, live);
    assert!(
        !verified,
        "a second inspection in the same window reads no codebook or directory page"
    );
    assert!(
        !second.verdict_recorded,
        "read-only inspection never persists the verdict"
    );

    evict_manifest_window(&store);
    let (after_eviction, _) = inspect(&store);
    assert_eq!(
        (
            after_eviction.recovery_action,
            after_eviction.verdict_recorded
        ),
        (Some("rebuild"), false),
        "after eviction the one re-verification still reports the live damage"
    );
    let (_, verified) = inspect(&store);
    assert!(!verified, "one re-verification per window, no more");

    let budget = Arc::new(ivf_pages::Budget::default());
    let mut index = ClusteredIndex::new(store, policy(), training(), budget.into()).unwrap();
    let persisted = index.inspect_catalog(0).unwrap().unwrap();
    assert_eq!(persisted.recovery_action, Some("rebuild"));
    let (recorded, verified) = inspect(index.storage());
    assert_eq!(
        (recorded.recovery_action, recorded.verdict_recorded),
        (Some("rebuild"), true),
        "once a maintenance call persists it, inspection reports the verdict as recorded"
    );
    assert!(!verified, "a recorded verdict is read, not re-verified");
}
