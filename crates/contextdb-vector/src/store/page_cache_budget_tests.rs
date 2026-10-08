//! Private companion rows exercise the actual sealed generation charge and
//! bounded search callbacks. Numerical decode bounds alone are not the oracle.
use super::*;
use crate::hnsw::page_ownership_tests::{capture_with_funder, installed_controls, retained_directory, is_freed, observe_payload_retirement, Allocation};
use crate::hnsw::{ReadOnlyVectorGraphPageSource, ReadOnlyVectorGraphPageReader, HnswAllowedSearchLimits};
use hnsw_rs::hnsw::{HnswSearchScratchContainer, HnswSearchScratchEvent};

#[derive(Debug)]
struct Budget {
    held: AtomicUsize,
    limit: usize,
    release_boundary: std::sync::Mutex<Option<ReleaseBoundary>>,
    boundary_refunds: AtomicUsize,
    boundary_failures: AtomicUsize,
}
#[derive(Debug)]
struct ReleaseBoundary {
    controls: Vec<Allocation>,
    freed_payloads: usize,
    directories: [Arc<Budget>; 2],
}
impl MemoryBudget for Budget {
    fn try_allocate_for(&self, bytes: usize, _: &str, _: &str, _: &str) -> Result<()> {
        self.held.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |held| held.checked_add(bytes).filter(|next| *next <= self.limit))
            .map_err(|_| Error::Other("declared allocation refused".into()))?;
        Ok(())
    }
    fn release(&self, bytes: usize) {
        if let Some(boundary) = self.release_boundary.lock().unwrap().as_ref() {
            // Sample before the actual balance decrement. Store failures rather
            // than panic inside Drop, so an ordering mutant has one clean RED.
            let wrong_payloads = boundary.controls.iter().filter(|control| is_freed(control.payload_pointer)).count() != boundary.freed_payloads;
            let freed_control = boundary.controls.iter().any(|control| is_freed(control.pointer));
            let directory_held = boundary.directories.iter().map(|directory| directory.held.load(Ordering::SeqCst)).sum::<usize>();
            let physical_controls = boundary.controls.iter().map(|control| control.size).sum::<usize>();
            let unfunded_controls = directory_held < physical_controls;
            let failures = usize::from(wrong_payloads) | (usize::from(freed_control) << 1) | (usize::from(unfunded_controls) << 2);
            self.boundary_failures.fetch_or(failures, Ordering::SeqCst);
            self.boundary_refunds.fetch_add(1, Ordering::SeqCst);
        }
        assert!(self.held.fetch_sub(bytes, Ordering::SeqCst) >= bytes);
    }
}
impl Budget {
    fn new(limit: usize) -> Self {
        Self { held: AtomicUsize::new(0), limit, release_boundary: std::sync::Mutex::new(None), boundary_refunds: AtomicUsize::new(0), boundary_failures: AtomicUsize::new(0) }
    }
    fn watch_release(&self, controls: &[Allocation], freed_payloads: usize, directories: [Arc<Budget>; 2]) {
        *self.release_boundary.lock().unwrap() = Some(ReleaseBoundary { controls: controls.to_vec(), freed_payloads, directories });
    }
    fn acquire(&self, event: HnswSearchScratchEvent) -> Result<()> {
        let bytes = match event {
            HnswSearchScratchEvent::Reserve { previous_capacity, requested_capacity, element_bytes, .. } => (requested_capacity - previous_capacity) * element_bytes,
            HnswSearchScratchEvent::Reconcile { requested_capacity, actual_capacity, element_bytes, .. } => actual_capacity.saturating_sub(requested_capacity) * element_bytes,
            _ => unreachable!(),
        };
        self.try_allocate_for(bytes, "search", "scratch", "")
    }
    fn refund(&self, event: HnswSearchScratchEvent) {
        let HnswSearchScratchEvent::Release { capacity, element_bytes, .. } = event else { unreachable!() };
        self.release(capacity * element_bytes);
    }
}
struct Source(Arc<Vec<u8>>);
impl ReadOnlyVectorGraphPageSource for Source {
    fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>> { Ok(Box::new(Reader(self.0.clone()))) }
}
struct Reader(Arc<Vec<u8>>);
impl ReadOnlyVectorGraphPageReader for Reader {
    fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> { Ok(self.0[offset..offset + len].to_vec()) }
}

#[test]
fn prepaid_generation_and_search_directory_funders_cover_shared_controls_until_physical_retirement() {
    for reverse_drop_order in [false, true] {
    let index = VectorIndexRef::new("items", "embedding");
    let entries = (0..48).map(|ordinal| VectorEntry {
        index: index.clone(), row_id: RowId(ordinal + 1), created_tx: TxId(1), deleted_tx: None, lsn: Lsn(1),
        vector: (0..768).map(|coordinate| ((ordinal * 41 + coordinate * 17 + 3) % 1009) as f32 / 1009.0).collect(),
    }).collect::<Vec<_>>();
    let graph = HnswIndex::from_vector_entries_with_policy(&entries, 768, VectorQuantization::F32, VectorIndexLayout::unpartitioned(768, VectorQuantization::F32).resolve_policy(entries.len(), 3));
    let (m, ef_construction, _) = graph.policy_values();
    let bytes = Arc::new(graph.encode_durable_generation().unwrap());
    drop(graph);
    let cache_bytes = 2 * 64 * 1024;
    let resident = HnswIndex::durable_reader_resident_bound_for_cache(&bytes, 768, VectorQuantization::F32, m, cache_bytes).unwrap();
    let owner = Arc::new(Budget::new(resident));
    // This is the real generation admission and custody pair, with the same
    // original MemoryBudget retained by SealedHnswGenerationContents.
    owner.try_allocate_for(resident, "vector_index", "load_retained_vector_generation", "").unwrap();
    let graph = HnswIndex::decode_paged_durable_generation(&bytes, Arc::new(Source(bytes.clone())), 768, VectorQuantization::F32, resident, cache_bytes).unwrap();
    let actual = graph.estimated_resident_bytes();
    // The public legacy decoder must admit the same measured resident bytes
    // before constructing its cache; one byte less is a typed budget refusal.
    #[cfg(feature = "test-seams")]
    if !reverse_drop_order {
        use crate::hnsw::{PagedVectorObserver, with_paged_vector_observer};
        #[derive(Default)]
        struct LegacyPages {
            constructed: std::sync::Mutex<Vec<usize>>,
            controls: AtomicUsize,
            fills: AtomicUsize,
            locks: AtomicUsize,
            scores: AtomicUsize,
        }
        impl PagedVectorObserver for LegacyPages {
            fn page_cache_constructed(&self, slots: usize) {
                self.constructed.lock().unwrap().push(slots);
            }
            fn page_control_allocated(&self, _: usize) {
                self.controls.fetch_add(1, Ordering::SeqCst);
            }
            fn page_filled(&self) { self.fills.fetch_add(1, Ordering::SeqCst); }
            fn page_cache_locked(&self) { self.locks.fetch_add(1, Ordering::SeqCst); }
            fn vector_decoded(&self) { self.scores.fetch_add(1, Ordering::SeqCst); }
        }
        let observation = Arc::new(LegacyPages::default());
        let control = with_paged_vector_observer(observation.clone(), || {
            HnswIndex::decode_paged_durable_generation(
                &bytes, Arc::new(Source(bytes.clone())), 768,
                VectorQuantization::F32, actual, cache_bytes,
            )
        }).expect("a fresh public legacy decode fits its measured resident requirement");
        assert_eq!(control.estimated_resident_bytes(), actual, "the fresh control retains exactly the measured resident requirement");
        let constructed = observation.constructed.lock().unwrap().clone();
        assert_eq!(constructed.len(), 1, "the public control reaches actual legacy cache construction");
        assert!(constructed[0] > 0, "the control constructs a cache with serving slots");
        assert_eq!(observation.controls.load(Ordering::SeqCst), 0, "decode constructs the directory before search installs any page Arc");
        let query = vec![0.12345; 768];
        let scratch = Budget::new(usize::MAX);
        let scratch_before = scratch.held.load(Ordering::SeqCst);
        let scratch_events = std::cell::Cell::new(0usize);
        let answer = with_paged_vector_observer(observation.clone(), || {
            control.search_with_bounded_memory::<Error, _, _, _, _>(
                &index, &query, 3, || Ok(()), || Ok(()),
                |event| { scratch_events.set(scratch_events.get() + 1); scratch.acquire(event) },
                |event| { scratch_events.set(scratch_events.get() + 1); scratch.refund(event) },
            )
        }).expect("the freshly decoded public graph completes a bounded search");
        assert!(!answer.rows.is_empty(), "the control returns a real search answer");
        let retained = answer.retained_bytes;
        drop(answer);
        scratch.release(retained);
        assert!(scratch_events.get() > 0, "the control uses the real bounded-search callbacks");
        assert!(observation.controls.load(Ordering::SeqCst) > 0, "the control search installs actual page Arc controls");
        assert!(observation.fills.load(Ordering::SeqCst) > 0 && observation.locks.load(Ordering::SeqCst) > 0 && observation.scores.load(Ordering::SeqCst) > 0, "the control reads and searches real cached pages");
        assert_eq!(scratch.held.load(Ordering::SeqCst), scratch_before, "the completed control releases its search scratch");
        let requested = control.estimated_resident_bytes();
        drop(control);

        let limit = requested.checked_sub(1).expect("the measured resident requirement is positive");
        let observation = Arc::new(LegacyPages::default());
        let scratch_before = scratch.held.load(Ordering::SeqCst);
        let events_before = scratch_events.get();
        let refused = with_paged_vector_observer(observation.clone(), || {
            HnswIndex::decode_paged_durable_generation(
                &bytes, Arc::new(Source(bytes.clone())), 768,
                VectorQuantization::F32, limit, cache_bytes,
            )
        });
        // No search is attempted on a refused decode. The existing scratch
        // owner is observed, without inventing a decoder funding ledger.
        assert!(observation.constructed.lock().unwrap().is_empty(), "a one-byte-short decode constructs no legacy cache");
        assert_eq!((observation.controls.load(Ordering::SeqCst), observation.fills.load(Ordering::SeqCst), observation.locks.load(Ordering::SeqCst), observation.scores.load(Ordering::SeqCst)), (0, 0, 0, 0), "a refused decode allocates no page controls and starts no page search");
        assert_eq!(scratch_events.get(), events_before, "the refused decode reaches no bounded-search callback");
        assert_eq!(scratch.held.load(Ordering::SeqCst), scratch_before, "the refused decode preserves the actual pre-call search-scratch balance");
        match refused {
            Err(Error::MemoryBudgetExceeded { requested_bytes, available_bytes, budget_limit_bytes, .. }) => {
                assert_eq!(requested_bytes, requested, "the refusal requests the successful control's actual resident bytes");
                assert_eq!(available_bytes, limit, "the refusal reports the supplied one-byte-short allowance");
                assert_eq!(budget_limit_bytes, limit, "the refusal preserves the supplied limit");
            }
            Err(other) => panic!("a one-byte-short public legacy decode must return the typed memory budget error: {other:?}"),
            Ok(_) => panic!("a one-byte-short public legacy decode must refuse before cache construction"),
        }
    }
    owner.release(resident - actual);
    let generation = SealedHnswGeneration::new(VectorGraphGeneration {
        generation_id: 1, covered_tx: TxId(1), covered_lsn: Lsn(1), durable_bytes: bytes.len() as u64,
        policy_revision: 1, hnsw_m: m as u32, hnsw_ef_construction: ef_construction as u32,
    }, graph, actual, Some(owner.clone()));
    let owner_identity = Arc::as_ptr(&owner) as usize;
    let query = vec![0.12345; 768];
    // Unfiltered, exact verification, complete filtered seeds and graph
    // filtered admissions all bind the same fold to the actual cache owner.
    let mut last_census = None;
    for route in 0..5 {
        let scratch = Budget::new(usize::MAX);
        let mut acquire = |event| scratch.acquire(event);
        let mut release = |event| scratch.refund(event);
        let (retained, census) = capture_with_funder(&owner.held, owner_identity, || {
            let graph = generation.graph();
            if route < 2 {
                let q = if route == 1 { entries[0].vector.as_slice() } else { query.as_slice() };
                let answer = graph.search_with_bounded_memory::<Error, _, _, _, _>(&index, q, 3, || Ok(()), || Ok(()), &mut acquire, &mut release).unwrap();
                let retained = answer.retained_bytes;
                assert!(!answer.rows.is_empty());
                drop(answer);
                retained
            } else if route == 4 {
                let mut pages = graph.serving_page_reader().unwrap().unwrap();
                for row in 1..=48 {
                    let similarity = graph.serving_page_similarity::<Error, _, _, _, _>(RowId(row), &query, HnswIndex::stored_query_norm(&query), &mut || Ok(()), &mut || Ok(()), &mut acquire, &mut release, &mut pages).unwrap().unwrap();
                    assert!(similarity.is_finite());
                }
                0
            } else {
                let count = if route == 2 { 3 } else { 48 };
                let answer = graph.search_allowed_with_bounded_memory_at_ef::<Error, _, _, _, _, _, _>(
                    &index, &query, 3, count, HnswAllowedSearchLimits { max_visited_nodes: usize::MAX, max_vector_evaluations: usize::MAX },
                    8, || 1..=count as u64, true, |row| row.0 <= count as u64,
                    || Ok(()), || Ok(()), &mut acquire, &mut release,
                ).unwrap();
                let retained = answer.retained_bytes;
                assert!(!answer.rows.is_empty());
                drop(answer);
                retained
            }
        });
        last_census = Some(census);
        scratch.release(retained);
        assert_eq!(scratch.held.load(Ordering::SeqCst), 0);
        assert_eq!(owner.held.load(Ordering::SeqCst), generation.bytes());
        for allocation in census.iter().filter(|allocation| allocation.pointer != 0 && allocation.is_control) {
            assert_eq!(allocation.owner_identity, owner_identity);
            assert!(allocation.owner_held >= generation.bytes(), "prepaid original cache owner before every accepted Arc allocation");
        }

    }
    // A real directory admission refuses before its construction, including
    // the control allowance. It neither reads a page nor leaks earlier work.
    let denied = Budget::new(usize::MAX);
    let answer = generation.graph().search_with_bounded_memory::<Error, _, _, _, _>(&index, &query, 3, || Ok(()), || Ok(()), |event| {
        if matches!(event, HnswSearchScratchEvent::Reserve { container: HnswSearchScratchContainer::ServingPageDirectory, .. }) { return Err(Error::Other("directory refusal".into())); }
        denied.acquire(event)
    }, |event| denied.refund(event));
    assert!(answer.is_err());
    assert_eq!(denied.held.load(Ordering::SeqCst), 0);

    // Two admitted directories retain dead control identities after cache
    // close. Their independent allowances cover the final Weak in either order.
    let directory_bytes = 2 * crate::hnsw::SERVING_PAGE_DIRECTORY_ENTRY_BYTES;
    let a_owner = Arc::new(Budget::new(directory_bytes));
    let b_owner = Arc::new(Budget::new(directory_bytes));
    a_owner.try_allocate_for(directory_bytes, "search", "directory", "").unwrap();
    b_owner.try_allocate_for(directory_bytes, "search", "directory", "").unwrap();
    let pointers = installed_controls(generation.graph());
    let census = last_census.unwrap();
    let controls = pointers.into_iter().filter(|pointer| *pointer != 0).map(|pointer| {
        // Address reuse is a new allocation identity: only the latest record
        // containing each currently installed Arc belongs to this lifetime.
        *census.iter().rev().find(|allocation| allocation.is_control && allocation.pointer <= pointer && pointer < allocation.pointer + allocation.size).unwrap()
    }).collect::<Vec<_>>();
    assert!(!controls.is_empty(), "retirement proof binds actual installed Arc identities");
    let physical_headers = controls.iter().map(|control| control.size).sum::<usize>();
    let a = retained_directory(generation.graph());
    let b = retained_directory(generation.graph());
    let payloads = controls.iter().map(|control| control.payload_pointer).collect::<Vec<_>>();
    assert!(controls.len() > 1, "the mapped shrink row has occupied slots on both sides of the kept boundary");
    let shrink_payloads = controls.len() - 1;
    owner.watch_release(&controls, shrink_payloads, [a_owner.clone(), b_owner.clone()]);
    let before_shrink = owner.held.load(Ordering::SeqCst);
    let (freed, (before_free, after_free, held_at_free)) = observe_payload_retirement(&owner.held, &payloads, || generation.give_back_serving_slots(1));
    assert!(freed > 0);
    assert_eq!((before_free, after_free), (shrink_payloads, shrink_payloads), "shrink physically retires exactly the removed payload identities");
    assert_eq!(held_at_free, before_shrink, "original cache funding remains held across physical shrink frees");
    assert_eq!(owner.boundary_refunds.load(Ordering::SeqCst), 1, "the shrink release callback was observed");
    assert_eq!(owner.boundary_failures.load(Ordering::SeqCst), 0, "at shrink refund: removed payloads are physically freed and surviving Weak controls are still directory-funded");
    assert_eq!(owner.held.load(Ordering::SeqCst), generation.bytes());
    owner.watch_release(&controls, controls.len(), [a_owner.clone(), b_owner.clone()]);
    let before_teardown = owner.held.load(Ordering::SeqCst);
    let (_, (before_free, after_free, held_at_free)) = observe_payload_retirement(&owner.held, &payloads, || drop(generation));
    let kept_payloads = controls.len() - shrink_payloads;
    assert_eq!((before_free, after_free), (kept_payloads, kept_payloads), "teardown physically retires the remaining payload identities");
    assert_eq!(held_at_free, before_teardown, "original cache funding remains held across physical teardown free");
    assert_eq!(owner.boundary_refunds.load(Ordering::SeqCst), 2, "the teardown release callback was observed");
    assert_eq!(owner.boundary_failures.load(Ordering::SeqCst), 0, "at teardown refund: payloads are physically freed and surviving Weak controls are still directory-funded");
    *owner.release_boundary.lock().unwrap() = None;
    assert_eq!(owner.held.load(Ordering::SeqCst), 0);
    for control in &controls {
        assert!(is_freed(control.payload_pointer), "generation teardown physically frees payloads");
        assert!(!is_freed(control.pointer), "prepaid directories retain each exact dead control");
    }
    assert!(a_owner.held.load(Ordering::SeqCst) >= directory_bytes);
    assert!(b_owner.held.load(Ordering::SeqCst) >= directory_bytes);
    assert!(a_owner.held.load(Ordering::SeqCst) + b_owner.held.load(Ordering::SeqCst) >= physical_headers);
    if reverse_drop_order {
        drop(b); b_owner.release(directory_bytes);
        for control in &controls { assert!(!is_freed(control.pointer)); }
        assert!(a_owner.held.load(Ordering::SeqCst) + b_owner.held.load(Ordering::SeqCst) >= physical_headers);
        drop(a);
        for control in &controls { assert!(is_freed(control.pointer)); }
        a_owner.release(directory_bytes);
    } else {
        drop(a); a_owner.release(directory_bytes);
        for control in &controls { assert!(!is_freed(control.pointer)); }
        assert!(a_owner.held.load(Ordering::SeqCst) + b_owner.held.load(Ordering::SeqCst) >= physical_headers);
        drop(b);
        for control in &controls { assert!(is_freed(control.pointer)); }
        b_owner.release(directory_bytes);
    }
    for control in &controls { assert!(is_freed(control.pointer), "last Weak physically frees the control before directory refund"); }
    assert_eq!(a_owner.held.load(Ordering::SeqCst) + b_owner.held.load(Ordering::SeqCst), 0);
    }
}
