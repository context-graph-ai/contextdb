#![cfg(feature = "test-seams")]
//! Purge overwrites both physical representations in the serving generation.
#[path = "support/hiding_pages.rs"]
mod hiding_pages;
#[path = "support/ivf_pages.rs"]
mod ivf_pages;

use contextdb_core::Error;
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
    // Rows: a list record that fills one page, then records spanning two.
    {
        // Each dimension with the fixed pages one of its list records spans.
        for (dimensions, record_pages) in
            [(PAGE_BYTES - 16, 1_u64), (PAGE_BYTES - 15, 2), (4096, 2)]
        {
            let budget = Arc::new(ivf_pages::Budget::default());
            let policy = Policy::checked(dimensions, 1, 1, 16, 1 << 20, true).unwrap();
            let training = TrainingPolicy {
                iterations: 1,
                reservoir_samples: 2,
                seed: 0,
                ingest_batch_keys: 2,
                phase_batch_entries: 2,
            };
            let mut index = ClusteredIndex::new(
                hiding_pages::Hiding::default(),
                policy,
                training,
                budget.clone().into(),
            )
            .unwrap();
            // Every coordinate is nonzero, so every record byte past its identities is too.
            let vector = |odd: usize| -> Vec<f32> {
                (0..dimensions)
                    .map(|at| if at % 2 == odd { 1.0 } else { 0.25 })
                    .collect()
            };
            let (survivor, victim) = (vector(0), vector(1));
            index
                .build_partition(
                    0,
                    [(11, survivor.clone()), (37, victim.clone())].into_iter(),
                )
                .unwrap();
            let manifest = index.storage().manifest(0).unwrap().unwrap();
            let read = |index: &ClusteredIndex<hiding_pages::Hiding>, ordinal: u64| {
                let mut page = [0; PAGE_BYTES];
                assert!(
                    index
                        .storage()
                        .read_admitted_page(manifest.key(PageKind::Lists, 0, ordinal), &mut page)
                        .unwrap()
                );
                page
            };
            let answers = |index: &ClusteredIndex<hiding_pages::Hiding>, mode: Mode| -> Vec<u64> {
                let mut hits = [Hit::default(); 2];
                let (count, _) = search(
                    index.storage(),
                    policy,
                    &budget.clone().into(),
                    &victim,
                    None,
                    mode,
                    &mut hits,
                )
                .unwrap();
                hits[..count].iter().map(|hit| hit.row).collect()
            };
            // Slots follow row order, each record alone in its block: survivor then victim.
            let survivor_pages: Vec<_> = (0..record_pages).map(|at| read(&index, at)).collect();
            let victim_pages: Vec<_> = (record_pages..2 * record_pages)
                .map(|at| read(&index, at))
                .collect();
            let tail = dimensions.min(PAGE_BYTES - 16);
            assert_eq!(&victim_pages[0][..8], &1_u64.to_le_bytes());
            assert_eq!(&victim_pages[0][8..16], &37_u64.to_le_bytes());
            assert!(victim_pages[0][16..16 + tail].iter().all(|byte| *byte != 0));
            let last = *victim_pages.last().unwrap();
            if record_pages > 1 {
                let spilled = dimensions + 16 - PAGE_BYTES;
                assert!(
                    last[..spilled].iter().all(|byte| *byte != 0),
                    "the continuation holds this vector's bytes before the erasure"
                );
            }

            // A missing last page of the record refuses and commits no part of the erasure.
            let last_key = manifest.key(PageKind::Lists, 0, 2 * record_pages - 1);
            index.storage().hide_key.set(Some(last_key));
            assert!(
                matches!(index.erase_row(0, 1), Err(Error::StoreCorrupted { .. })),
                "a missing record page refuses the erasure at {dimensions} dimensions"
            );
            index.storage().hide_key.set(None);
            for (offset, before) in victim_pages.iter().enumerate() {
                assert_eq!(
                    &read(&index, record_pages + offset as u64),
                    before,
                    "the refused erasure rolled back at {dimensions} dimensions"
                );
            }
            for mode in [Mode::Exact, Mode::Indexed] {
                assert_eq!(
                    answers(&index, mode),
                    vec![37, 11],
                    "the refused erasure left the row answering, {mode:?}"
                );
            }

            index.erase_row(0, 1).unwrap();
            let erased: Vec<_> = (record_pages..2 * record_pages)
                .map(|at| read(&index, at))
                .collect();
            assert_eq!(
                &erased[0][..8],
                &1_u64.to_le_bytes(),
                "the slot address stays"
            );
            assert!(erased[0][8..].iter().all(|byte| *byte == 0));
            for continuation in &erased[1..] {
                assert!(
                    continuation.iter().all(|byte| *byte == 0),
                    "no vector byte survives in a continuation at {dimensions} dimensions"
                );
            }
            for (at, before) in survivor_pages.iter().enumerate() {
                assert_eq!(
                    &read(&index, at as u64),
                    before,
                    "the neighbouring record is untouched"
                );
            }
            for mode in [Mode::Exact, Mode::Indexed] {
                assert_eq!(answers(&index, mode), vec![11], "{mode:?}");
            }
        }
    }
}
