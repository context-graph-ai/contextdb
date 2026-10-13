//! Private proof overlay. The allocator records allocation identities and only
//! marks a backing freed after System.dealloc has returned.
use super::*;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Allocation {
    pub(crate) pointer: usize,
    pub(crate) size: usize,
    pub(crate) align: usize,
    pub(crate) freed: bool,
    pub(crate) owner_identity: usize,
    pub(crate) owner_held: usize,
    pub(crate) is_control: bool,
    pub(crate) payload_pointer: usize,
}
const EMPTY: Allocation = Allocation {
    pointer: 0,
    size: 0,
    align: 0,
    freed: false,
    owner_identity: 0,
    owner_held: 0,
    is_control: false,
    payload_pointer: 0,
};
static ALLOCATIONS: std::sync::Mutex<[Allocation; 512]> = std::sync::Mutex::new([EMPTY; 512]);
thread_local! {
    static RECORD: Cell<bool> = const { Cell::new(false) };
    static CONTROL: Cell<usize> = const { Cell::new(0) };
    static FUNDER: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    // Borrowed only for observe_payload_retirement's synchronous action.
    static RETIRING: Cell<(usize, usize, usize)> = const { Cell::new((0, 0, 0)) };
    static FREE_BEFORE: Cell<usize> = const { Cell::new(0) };
    static FREE_AFTER: Cell<usize> = const { Cell::new(0) };
    static FREE_HELD_FLOOR: Cell<usize> = const { Cell::new(usize::MAX) };
}
struct ObservedAllocator;
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;

fn recording() -> bool {
    RECORD.try_with(Cell::get).unwrap_or(false)
}
fn allocated(pointer: *mut u8, layout: Layout) {
    if pointer.is_null() || !recording() {
        return;
    }
    let mut entries = ALLOCATIONS.lock().unwrap();
    let slot = entries
        .iter_mut()
        .find(|entry| entry.pointer == 0)
        .expect("fixed proof census capacity");
    let (counter, owner_identity) = FUNDER.with(Cell::get);
    let owner_held = if counter == 0 {
        0
    } else {
        unsafe { (&*(counter as *const AtomicUsize)).load(Ordering::SeqCst) }
    };
    *slot = Allocation {
        pointer: pointer as usize,
        size: layout.size(),
        align: layout.align(),
        freed: false,
        owner_identity,
        owner_held,
        is_control: CONTROL.with(Cell::get) != 0,
        payload_pointer: CONTROL.with(Cell::get),
    };
}
fn freed(pointer: *mut u8) {
    let mut entries = ALLOCATIONS.lock().unwrap();
    for entry in entries
        .iter_mut()
        .filter(|entry| entry.pointer == pointer as usize && !entry.freed)
    {
        entry.freed = true;
    }
}
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        allocated(pointer, layout);
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        payload_free_boundary(pointer, false);
        unsafe { System.dealloc(pointer, layout) };
        payload_free_boundary(pointer, true);
        freed(pointer);
    }
    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let result = unsafe { System.realloc(pointer, layout, size) };
        if !result.is_null() {
            freed(pointer);
            allocated(
                result,
                Layout::from_size_align(size, layout.align()).unwrap(),
            );
        }
        result
    }
}
fn capture<T>(action: impl FnOnce() -> T) -> (T, [Allocation; 512]) {
    *ALLOCATIONS.lock().unwrap() = [EMPTY; 512];
    RECORD.with(|record| record.set(true));
    let answer = action();
    RECORD.with(|record| record.set(false));
    (answer, *ALLOCATIONS.lock().unwrap())
}
// The existing allocator samples the actual original owner on both sides
// of System.dealloc; it never infers physical destruction from a budget total.
fn payload_free_boundary(pointer: *mut u8, after: bool) {
    let _ = RETIRING.try_with(|watch| {
        let (counter, payloads, len) = watch.get();
        if counter == 0 {
            return;
        }
        // The guard below keeps both references alive until its action returns.
        let payloads = unsafe { std::slice::from_raw_parts(payloads as *const usize, len) };
        if !payloads.contains(&(pointer as usize)) {
            return;
        }
        let held = unsafe { (&*(counter as *const AtomicUsize)).load(Ordering::SeqCst) };
        FREE_HELD_FLOOR.with(|floor| floor.set(floor.get().min(held)));
        let count = if after { &FREE_AFTER } else { &FREE_BEFORE };
        count.with(|count| count.set(count.get() + 1));
    });
}

pub(crate) fn observe_payload_retirement<T>(
    counter: &AtomicUsize,
    payloads: &[usize],
    action: impl FnOnce() -> T,
) -> (T, (usize, usize, usize)) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            RETIRING.with(|watch| watch.set((0, 0, 0)));
        }
    }
    RETIRING.with(|watch| {
        assert_eq!(watch.get().0, 0, "retirement observation is not nested");
        watch.set((
            counter as *const AtomicUsize as usize,
            payloads.as_ptr() as usize,
            payloads.len(),
        ));
    });
    FREE_BEFORE.with(|count| count.set(0));
    FREE_AFTER.with(|count| count.set(0));
    FREE_HELD_FLOOR.with(|floor| floor.set(usize::MAX));
    let _reset = Reset;
    let answer = action();
    (
        answer,
        (
            FREE_BEFORE.with(Cell::get),
            FREE_AFTER.with(Cell::get),
            FREE_HELD_FLOOR.with(Cell::get),
        ),
    )
}

pub(crate) fn is_freed(pointer: usize) -> bool {
    ALLOCATIONS
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|entry| entry.pointer == pointer)
        .is_some_and(|entry| entry.freed)
}

/// Route2's shared install invariant is asserted once beside the real install:
/// duplicate/held rejection allocates nothing, hits/upgrades allocate nothing,
/// and actual header identity lasts through the final Weak after payload free.
#[test]
fn owned_page_rejection_and_shared_page_retirement_preserve_physical_custody() {
    let mut cache = PagedVectorPageCache::new(1);
    let body = vec![7; PAGED_VECTOR_SOURCE_PAGE_BYTES];
    let payload = body.as_ptr() as usize;
    let (accepted, census) = capture(|| cache.install(0, body));
    let ReadPageBacking::Shared(shared) = accepted else {
        panic!("empty prepaid slot accepts");
    };
    let shared_pointer = Arc::as_ptr(&shared) as usize;
    let control = census
        .iter()
        .find(|entry| {
            entry.pointer != 0
                && entry.pointer <= shared_pointer
                && shared_pointer < entry.pointer + entry.size
        })
        .unwrap();
    let padded = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<PageBody>())
        .unwrap()
        .0
        .pad_to_align();
    assert_eq!(
        (control.size, control.align),
        (padded.size(), padded.align())
    );
    assert_eq!(
        control.size, SERVING_PAGE_HANDLE_BYTES,
        "pinned padded Arc request witness"
    );
    assert_eq!(shared.0.as_ptr() as usize, payload);
    let directory_a = Arc::downgrade(&shared);
    let directory_b = Arc::downgrade(&shared);
    let (hit, hit_census) = capture(|| cache.get(0).unwrap());
    assert!(hit_census.iter().all(|entry| entry.pointer == 0));
    let (upgraded, upgrade_census) = capture(|| directory_a.upgrade().unwrap());
    assert!(upgrade_census.iter().all(|entry| entry.pointer == 0));
    drop(hit);
    drop(upgraded);
    for offset in [0, PAGED_VECTOR_SOURCE_PAGE_BYTES] {
        let body = vec![9; PAGED_VECTOR_SOURCE_PAGE_BYTES];
        let (rejected, census) = capture(|| cache.install(offset, body));
        assert!(matches!(rejected, ReadPageBacking::Owned(_)));
        assert!(
            census.iter().all(|entry| entry.pointer == 0),
            "duplicate and all-held rejection have no control or conversion allocation"
        );
        drop(rejected);
    }
    // Record exact physical identities again: capture intentionally scopes
    // one operation, so keep this lifecycle's census through both drops.
    drop(directory_a);
    drop(directory_b);
    drop(shared);
    drop(cache);
    let mut cache = PagedVectorPageCache::new(1);
    let (accepted, census) = capture(|| cache.install(0, vec![7; PAGED_VECTOR_SOURCE_PAGE_BYTES]));
    let ReadPageBacking::Shared(shared) = accepted else {
        panic!("empty slot accepts");
    };
    let payload = shared.0.as_ptr() as usize;
    let shared_pointer = Arc::as_ptr(&shared) as usize;
    let control = census
        .iter()
        .find(|entry| {
            entry.pointer != 0
                && entry.pointer <= shared_pointer
                && shared_pointer < entry.pointer + entry.size
        })
        .unwrap()
        .pointer;
    let a = Arc::downgrade(&shared);
    let b = Arc::downgrade(&shared);
    drop(shared);
    drop(cache);
    assert!(is_freed(payload));
    assert!(!is_freed(control));
    assert!(a.upgrade().is_none());
    drop(b);
    assert!(!is_freed(control));
    drop(a);
    assert!(is_freed(control));
    // A rejected page has no Weak or header; physical free precedes refund.
    let (body, _) = capture(|| vec![3; PAGED_VECTOR_SOURCE_PAGE_BYTES]);
    let payload = body.as_ptr() as usize;
    let mut released = false;
    let mut release = |event| {
        assert!(is_freed(payload));
        assert!(matches!(
            event,
            HnswSearchScratchEvent::Release {
                capacity: PAGED_VECTOR_SOURCE_PAGE_BYTES,
                ..
            }
        ));
        released = true;
    };
    drop(ReadPage::new(
        ReadPageBacking::Owned(body),
        PAGED_VECTOR_SOURCE_PAGE_BYTES,
        &mut release,
    ));
    assert!(released);
    exercise_directory_collision_row();
    exercise_overflow_rows();
    exercise_upgrade_victim_row();
    exercise_racing_fill_row();
    exercise_page_fold_rows();
}

fn exercise_racing_fill_row() {
    let cache = Arc::new(Mutex::new(PagedVectorPageCache::new(1)));
    let read_complete = Arc::new(std::sync::Barrier::new(2));
    let winner_published = Arc::new(std::sync::Barrier::new(2));
    let loser = std::thread::spawn({
        let cache = cache.clone();
        let read_complete = read_complete.clone();
        let winner_published = winner_published.clone();
        move || {
            // Both copies have completed their source read before publication.
            let copy = vec![1; PAGED_VECTOR_SOURCE_PAGE_BYTES];
            read_complete.wait();
            winner_published.wait();
            let (answer, census) = capture(|| cache.lock().install(0, copy));
            assert!(matches!(answer, ReadPageBacking::Owned(_)));
            assert!(census.iter().all(|entry| entry.pointer == 0));
            drop(answer);
        }
    });
    let copy = vec![1; PAGED_VECTOR_SOURCE_PAGE_BYTES];
    read_complete.wait();
    assert!(matches!(
        cache.lock().install(0, copy),
        ReadPageBacking::Shared(_)
    ));
    winner_published.wait();
    loser.join().unwrap();
}

#[derive(Clone, Copy)]
enum ReadMode {
    Exact,
    ExcessCapacity,
    Short,
    Error,
}
struct BytesSource;
impl ReadOnlyVectorGraphPageSource for BytesSource {
    fn reader(&self) -> Result<Box<dyn ReadOnlyVectorGraphPageReader>> {
        unreachable!()
    }
}
struct BytesReader {
    bytes: Vec<u8>,
    mode: ReadMode,
    reads: Arc<AtomicUsize>,
    last_pointer: Arc<AtomicUsize>,
}
impl ReadOnlyVectorGraphPageReader for BytesReader {
    fn read_range(&mut self, offset: usize, len: usize) -> Result<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if matches!(self.mode, ReadMode::Error) {
            return Err(Error::Other("reader refusal".into()));
        }
        let mut page = Vec::with_capacity(
            len + usize::from(matches!(self.mode, ReadMode::ExcessCapacity)) * 8,
        );
        page.extend_from_slice(&self.bytes[offset..offset + len]);
        if matches!(self.mode, ReadMode::Short) {
            page.pop();
        }
        self.last_pointer
            .store(page.as_ptr() as usize, Ordering::SeqCst);
        Ok(page)
    }
}

// One mapped row helper uses the real coordinate fold and distance arithmetic.
// Seven-byte source pages force a float carry and an actual partial final page.
fn exercise_page_fold_rows() {
    for (mode, held_full, directory_entries, deny_reserve, deny_reconcile) in [
        (ReadMode::Exact, false, 1, false, false),
        (ReadMode::ExcessCapacity, false, 1, false, false),
        (ReadMode::Exact, true, 1, false, false),
        (ReadMode::ExcessCapacity, true, 0, false, false),
        (ReadMode::Short, false, 1, false, false),
        (ReadMode::Error, false, 1, false, false),
        (ReadMode::Exact, false, 1, true, false),
        (ReadMode::ExcessCapacity, false, 1, false, true),
    ] {
        let mut bytes = vec![0; 5];
        for coordinate in [0.25_f32, 0.5] {
            bytes.extend_from_slice(&coordinate.to_le_bytes());
        }
        let paged = PagedHnsw {
            points: Vec::new(),
            entry_point: None,
            source: Arc::new(BytesSource),
            dimension: 2,
            max_nb_connection: 1,
            layer0_max_degree: 2,
            resident_bytes: AtomicUsize::new(0),
            graph_len: bytes.len(),
            source_page_bytes: 7,
            page_slots: 1,
            page_cache: Mutex::new(PagedVectorPageCache::new(1)),
        };
        let held = if held_full {
            let ReadPageBacking::Shared(page) = paged.page_cache.lock().install(100, vec![0; 7])
            else {
                unreachable!()
            };
            Some(page)
        } else {
            None
        };
        let point = PagedPoint {
            origin_id: 0,
            vector_offset: 5,
            neighbours: Vec::new(),
        };
        let reads = Arc::new(AtomicUsize::new(0));
        let pointer = Arc::new(AtomicUsize::new(0));
        let mut pages = ServingPages::new(
            Box::new(BytesReader {
                bytes,
                mode,
                reads: reads.clone(),
                last_pointer: pointer.clone(),
            }),
            directory_entries,
        );
        let charged = Cell::new(0);
        let query = [0.25_f32, 0.5];
        let (answer, _) = capture(|| {
            paged.distance(
                &point,
                &query,
                PagedHnsw::query_norm(&query),
                &mut || Ok::<_, Error>(()),
                &mut || Ok::<_, Error>(()),
                &mut |event| {
                    match event {
                        HnswSearchScratchEvent::Reserve {
                            requested_capacity, ..
                        } => {
                            if deny_reserve {
                                return Err(Error::Other("scratch refusal".into()));
                            }
                            charged.set(charged.get() + requested_capacity);
                        }
                        HnswSearchScratchEvent::Reconcile {
                            requested_capacity,
                            actual_capacity,
                            ..
                        } => {
                            if deny_reconcile {
                                return Err(Error::Other("reconcile refusal".into()));
                            }
                            charged.set(charged.get() + actual_capacity - requested_capacity);
                        }
                        _ => unreachable!(),
                    }
                    Ok(())
                },
                &mut |event| {
                    let HnswSearchScratchEvent::Release { capacity, .. } = event else {
                        unreachable!()
                    };
                    let pointer = pointer.load(Ordering::SeqCst);
                    if pointer != 0 {
                        let physically_freed = is_freed(pointer);
                        let cache = paged.page_cache.lock();
                        let transferred = cache
                            .slots
                            .iter()
                            .flatten()
                            .any(|slot| slot.bytes.0.as_ptr() as usize == pointer);
                        assert!(
                            physically_freed || transferred,
                            "a payload refund needs physical free or completed cache transfer"
                        );
                    }
                    charged.set(charged.get() - capacity);
                },
                &mut pages,
            )
        });
        assert_eq!(charged.get(), 0);
        if matches!(mode, ReadMode::Short | ReadMode::Error) || deny_reserve || deny_reconcile {
            assert!(answer.is_err());
            assert!(paged.page_cache.lock().slot_by_offset.is_empty());
        } else {
            assert!(answer.unwrap().abs() <= 1.0e-6);
            assert_eq!(reads.load(Ordering::SeqCst), 2);
        }
        if deny_reserve {
            assert_eq!(reads.load(Ordering::SeqCst), 0);
        }
        if answer_is_retryable(mode, deny_reserve, deny_reconcile) {
            let mut bytes = vec![0; 5];
            for coordinate in query {
                bytes.extend_from_slice(&coordinate.to_le_bytes());
            }
            pointer.store(0, Ordering::SeqCst);
            pages.reader = Box::new(BytesReader {
                bytes,
                mode: ReadMode::Exact,
                reads: reads.clone(),
                last_pointer: pointer.clone(),
            });
            let retry = paged
                .distance(
                    &point,
                    &query,
                    PagedHnsw::query_norm(&query),
                    &mut || Ok::<_, Error>(()),
                    &mut || Ok::<_, Error>(()),
                    &mut |event| {
                        let additional = match event {
                            HnswSearchScratchEvent::Reserve {
                                previous_capacity,
                                requested_capacity,
                                ..
                            } => requested_capacity - previous_capacity,
                            HnswSearchScratchEvent::Reconcile {
                                requested_capacity,
                                actual_capacity,
                                ..
                            } => actual_capacity - requested_capacity,
                            _ => unreachable!(),
                        };
                        charged.set(charged.get() + additional);
                        Ok::<_, Error>(())
                    },
                    &mut |event| {
                        let HnswSearchScratchEvent::Release { capacity, .. } = event else {
                            unreachable!()
                        };
                        charged.set(charged.get() - capacity);
                    },
                    &mut pages,
                )
                .unwrap();
            assert!(retry.abs() <= 1.0e-6);
            assert_eq!(
                charged.get(),
                0,
                "retry on the same reader/cache leaves no charge from the refusal"
            );
        }
        if held_full {
            assert!(pages.resolved.iter().all(Option::is_none));
        }
        drop(held);
        drop(pages);
        drop(paged);
    }
}

// Test-only collector uses the supplied actual MemoryBudget counter. It does
// not manufacture a charge or infer funding from the numerical decode bound.
pub(crate) fn capture_with_funder<T>(
    counter: &AtomicUsize,
    identity: usize,
    action: impl FnOnce() -> T,
) -> (T, [Allocation; 512]) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            FUNDER.with(|slot| slot.set((0, 0)));
            RECORD.with(|slot| slot.set(false));
        }
    }
    FUNDER.with(|slot| slot.set((counter as *const AtomicUsize as usize, identity)));
    let _reset = Reset;
    capture(action)
}
pub(crate) fn installed_controls(index: &HnswIndex) -> [usize; 4] {
    let HnswInner::F32Paged(graph) = &index.hnsw else {
        unreachable!()
    };
    let cache = graph.page_cache.lock();
    let mut answer = [0; 4];
    for (destination, page) in answer.iter_mut().zip(cache.slots.iter().flatten()) {
        *destination = Arc::as_ptr(&page.bytes) as usize;
    }
    answer
}
pub(crate) fn retained_directory(index: &HnswIndex) -> ServingPages {
    let HnswInner::F32Paged(graph) = &index.hnsw else {
        unreachable!()
    };
    let mut directory = ServingPages::new(graph.source.reader().unwrap(), graph.page_slots);
    for slot in graph.page_cache.lock().slots.iter().flatten() {
        directory.hold(slot.offset, &slot.bytes, graph.source_page_bytes);
    }
    directory
}

pub(super) fn allocating_control(body: PageBody) -> Arc<PageBody> {
    CONTROL.with(|slot| slot.set(body.0.as_ptr() as usize));
    let answer = Arc::new(body);
    CONTROL.with(|slot| slot.set(0));
    answer
}

fn exercise_overflow_rows() {
    let paged = PagedHnsw {
        points: Vec::new(),
        entry_point: None,
        source: Arc::new(BytesSource),
        dimension: usize::MAX,
        max_nb_connection: 1,
        layer0_max_degree: 2,
        resident_bytes: AtomicUsize::new(0),
        graph_len: 0,
        source_page_bytes: 7,
        page_slots: 1,
        page_cache: Mutex::new(PagedVectorPageCache::new(1)),
    };
    let point = PagedPoint {
        origin_id: 0,
        vector_offset: 0,
        neighbours: Vec::new(),
    };
    let reads = Arc::new(AtomicUsize::new(0));
    let mut pages = ServingPages::new(
        Box::new(BytesReader {
            bytes: Vec::new(),
            mode: ReadMode::Exact,
            reads: reads.clone(),
            last_pointer: Arc::new(AtomicUsize::new(0)),
        }),
        0,
    );
    let reservations = Cell::new(0);
    let error = paged.distance(
        &point,
        &[],
        0.0,
        &mut || Ok::<_, Error>(()),
        &mut || Ok::<_, Error>(()),
        &mut |_| {
            reservations.set(reservations.get() + 1);
            Ok::<_, Error>(())
        },
        &mut |_| panic!("overflow admitted no scratch"),
        &mut pages,
    );
    assert!(matches!(
        error,
        Err(HnswBoundedSearchError::CapacityOverflow(
            "paged vector byte length"
        ))
    ));
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    assert_eq!(reservations.get(), 0);
    let error = paged.distance(
        &point,
        &[],
        0.0,
        &mut || Err::<(), _>(Error::Other("first refusal".into())),
        &mut || Ok::<_, Error>(()),
        &mut |_| panic!("first refusal precedes admission"),
        &mut |_| panic!("first refusal admitted nothing"),
        &mut pages,
    );
    assert!(
        matches!(error, Err(HnswBoundedSearchError::Callback(Error::Other(message))) if message == "first refusal")
    );
}
fn exercise_upgrade_victim_row() {
    let mut cache = PagedVectorPageCache::new(1);
    let ReadPageBacking::Shared(page) = cache.install(0, vec![1; 7]) else {
        unreachable!()
    };
    let weak = Arc::downgrade(&page);
    drop(page);
    let upgraded = weak.upgrade().unwrap();
    let body = vec![2; 7];
    let (answer, census) = capture(|| cache.install(7, body));
    assert!(matches!(answer, ReadPageBacking::Owned(_)));
    assert!(census.iter().all(|allocation| allocation.pointer == 0));
    assert!(
        cache.get(0).is_some(),
        "a Weak upgrade protects the victim and restoration preserves its lookup"
    );
    drop(answer);
    drop(upgraded);
    assert!(matches!(
        cache.install(7, vec![2; 7]),
        ReadPageBacking::Shared(_)
    ));
    assert!(
        weak.upgrade().is_none(),
        "victim unwrap atomically disassociates the directory"
    );
}

fn exercise_directory_collision_row() {
    capture(|| {
        let mut cache = PagedVectorPageCache::new(1);
        let ReadPageBacking::Shared(first) = cache.install(0, vec![1; 7]) else {
            unreachable!()
        };
        let body = Arc::as_ptr(&first) as usize;
        let control = ALLOCATIONS
            .lock()
            .unwrap()
            .iter()
            .find(|entry| {
                entry.pointer != 0 && entry.pointer <= body && body < entry.pointer + entry.size
            })
            .unwrap()
            .pointer;
        let payload = first.0.as_ptr() as usize;
        let mut pages = ServingPages::new(
            Box::new(BytesReader {
                bytes: Vec::new(),
                mode: ReadMode::Exact,
                reads: Arc::new(AtomicUsize::new(0)),
                last_pointer: Arc::new(AtomicUsize::new(0)),
            }),
            1,
        );
        pages.hold(0, &first, 7);
        drop(first);
        let ReadPageBacking::Shared(next) = cache.install(7, vec![2; 7]) else {
            unreachable!()
        };
        assert!(is_freed(payload));
        assert!(
            !is_freed(control),
            "old entry still covers the exact Weak-retained header after eviction"
        );
        assert!(pages.held(0, 7).is_none());
        pages.hold(7, &next, 7);
        assert!(
            is_freed(control),
            "colliding directory replacement drops the last Weak physically"
        );
        assert!(pages.held(7, 7).is_some());
    });
}

fn answer_is_retryable(mode: ReadMode, deny_reserve: bool, deny_reconcile: bool) -> bool {
    matches!(mode, ReadMode::Short | ReadMode::Error) || deny_reserve || deny_reconcile
}
