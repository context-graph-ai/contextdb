#![cfg(feature = "test-seams")]
//! Purge overwrites both physical representations in the serving generation.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;

use contextdb_vector::ivf::storage::{PAGE_BYTES, PageKind, ReadPages};
use contextdb_vector::ivf::{ClusteredIndex, Hit, Mode, Policy, TrainingPolicy, search};
use std::sync::Arc;

#[test]
fn purge_overwrites_canonical_and_quantized_slot_without_rebuilding() {
    let budget = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 1, 1, 16, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 1,
        reservoir_samples: 2,
        seed: 0,
        ingest_batch_keys: 2,
        phase_batch_entries: 2,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        budget.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, [(11, [1.0_f32, 0.0]), (37, [0.0, 1.0])].into_iter())
        .unwrap();
    let manifest = index.storage().manifest(0).unwrap().unwrap();
    let canonical_key = manifest.key(PageKind::Canonical, 0, 0);
    let list_key = manifest.key(PageKind::Lists, 0, 0);
    let mut canonical = [0; PAGE_BYTES];
    let mut list = [0; PAGE_BYTES];
    assert!(
        index
            .storage()
            .read_admitted_page(canonical_key, &mut canonical)
            .unwrap()
    );
    assert!(
        index
            .storage()
            .read_admitted_page(list_key, &mut list)
            .unwrap()
    );
    // Canonical stride = row id + two f32 values; list stride = slot + row + two int8 values.
    let canonical_at = 16;
    let list_at = 18;
    let mut expected_canonical = Vec::new();
    expected_canonical.extend_from_slice(&37_u64.to_le_bytes());
    expected_canonical.extend_from_slice(&0.0_f32.to_le_bytes());
    expected_canonical.extend_from_slice(&1.0_f32.to_le_bytes());
    assert_eq!(
        &canonical[canonical_at..canonical_at + 16],
        expected_canonical
    );
    assert_eq!(&list[list_at..list_at + 8], &1_u64.to_le_bytes());
    assert_eq!(&list[list_at + 8..list_at + 16], &37_u64.to_le_bytes());
    assert_eq!(&list[list_at + 16..list_at + 18], &[0, 127]);
    let query = [0.0, 1.0];
    let mut hits = [Hit::default(); 2];
    let (count, _) = search(
        index.storage(),
        policy,
        &budget.clone().into(),
        &query,
        None,
        Mode::Indexed,
        &mut hits,
    )
    .unwrap();
    assert_eq!(count, 2);
    assert!(hits[..count].iter().any(|hit| hit.row == 37));
    assert!(hits[..count].iter().any(|hit| hit.row == 11));

    index.erase_row(0, 1).unwrap();

    assert_eq!(
        index.storage().manifest(0).unwrap().unwrap().generation,
        manifest.generation
    );
    assert!(
        index
            .storage()
            .read_admitted_page(canonical_key, &mut canonical)
            .unwrap()
    );
    assert!(
        index
            .storage()
            .read_admitted_page(list_key, &mut list)
            .unwrap()
    );
    assert_ne!(
        &canonical[canonical_at..canonical_at + 8],
        &37_u64.to_le_bytes()
    );
    assert_ne!(
        &canonical[canonical_at + 12..canonical_at + 16],
        &1.0_f32.to_le_bytes()
    );
    assert_ne!(&list[list_at + 8..list_at + 16], &37_u64.to_le_bytes());
    assert_ne!(&list[list_at + 16..list_at + 18], &[0, 127]);
    for mode in [Mode::Exact, Mode::Indexed] {
        let (count, _) = search(
            index.storage(),
            policy,
            &budget.clone().into(),
            &query,
            None,
            mode,
            &mut hits,
        )
        .unwrap();
        let rows: Vec<_> = hits[..count].iter().map(|hit| hit.row).collect();
        assert_eq!(
            rows,
            vec![11],
            "{mode:?} retains the survivor and excludes the erased row"
        );
    }
}
