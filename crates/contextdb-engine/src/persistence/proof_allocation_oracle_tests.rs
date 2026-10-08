//! An allocator oracle beside the accountant: what the proof paths really
//! allocate, measured independently of the counters they keep about
//! themselves. The accountant can only prove what it was told; this measures
//! what the process actually held.

use super::*;
use crate::memory_accounting::MemoryAccountant;
use serial_test::serial;
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

struct MeasuredAllocator;
// Measurement is scoped to the THREAD under test, not to the process. A
// process-global switch measures every other test thread running beside this
// one and reports their allocations as this subsystem's, which is exactly the
// contamination that makes an allocator oracle meaningless. The flag is a
// const-initialised thread-local, so reading it inside the allocator neither
// allocates nor runs a destructor; deallocation uses the flag recorded in the
// allocation's own header, so a block freed on another thread still leaves the
// measured total correct.
thread_local! {
    static MEASURING_HERE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
static MEASURING: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn measuring_on_this_thread() -> bool {
    MEASURING.load(AtomicOrdering::Relaxed)
        && MEASURING_HERE
            .try_with(std::cell::Cell::get)
            .unwrap_or(false)
}

#[repr(C)]
struct Header {
    measured: bool,
    bytes: usize,
}

fn allocation_layout(layout: Layout) -> (Layout, usize) {
    let alignment = layout.align().max(std::mem::align_of::<Header>());
    let offset = std::mem::size_of::<Header>().next_multiple_of(alignment);
    (
        Layout::from_size_align(layout.size() + offset, alignment).expect("measured layout"),
        offset,
    )
}

unsafe impl GlobalAlloc for MeasuredAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let (allocation, offset) = allocation_layout(layout);
        let base = unsafe { System.alloc(allocation) };
        if base.is_null() {
            return base;
        }
        let pointer = unsafe { base.add(offset) };
        let measured = measuring_on_this_thread();
        unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .write(Header {
                    measured,
                    bytes: allocation.size(),
                });
        }
        if measured {
            let live =
                LIVE.fetch_add(allocation.size(), AtomicOrdering::SeqCst) + allocation.size();
            PEAK.fetch_max(live, AtomicOrdering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        let (allocation, offset) = allocation_layout(layout);
        let header = unsafe {
            pointer
                .sub(std::mem::size_of::<Header>())
                .cast::<Header>()
                .read()
        };
        if header.measured {
            LIVE.fetch_sub(header.bytes, AtomicOrdering::SeqCst);
        }
        unsafe {
            System.dealloc(pointer.sub(offset), allocation);
        }
    }
}

#[global_allocator]
static ALLOCATOR: MeasuredAllocator = MeasuredAllocator;

/// Run `operation` with every allocation on this thread measured, returning
/// its peak live bytes and what it still held when it ended.
pub(crate) fn measured<T>(operation: impl FnOnce() -> T) -> (T, usize, usize) {
    LIVE.store(0, AtomicOrdering::SeqCst);
    PEAK.store(0, AtomicOrdering::SeqCst);
    MEASURING_HERE.with(|measuring| measuring.set(true));
    MEASURING.store(true, AtomicOrdering::SeqCst);
    let value = operation();
    MEASURING.store(false, AtomicOrdering::SeqCst);
    MEASURING_HERE.with(|measuring| measuring.set(false));
    let peak = PEAK.load(AtomicOrdering::SeqCst);
    let live = LIVE.load(AtomicOrdering::SeqCst);
    (value, peak, live)
}

/// The oracle measures this thread and no other. A neighbour allocating
/// megabytes throughout the measurement must not appear in it - that is the
/// difference between an attributed measurement and a process census.
#[test]
#[serial]
fn the_allocation_oracle_measures_only_the_thread_under_test() {
    let running = Arc::new(AtomicBool::new(true));
    let neighbour = {
        let running = Arc::clone(&running);
        std::thread::spawn(move || {
            let mut held: Vec<Vec<u8>> = Vec::new();
            while running.load(AtomicOrdering::SeqCst) {
                held.push(vec![0u8; 1024 * 1024]);
                if held.len() > 8 {
                    held.clear();
                }
                std::thread::yield_now();
            }
        })
    };
    let ((), peak, _) = measured(|| {
        let small = vec![0u8; 4096];
        std::hint::black_box(&small);
        std::thread::sleep(std::time::Duration::from_millis(50));
    });
    running.store(false, AtomicOrdering::SeqCst);
    neighbour.join().expect("the neighbour thread finishes");
    assert!(
        peak < 1024 * 1024,
        "a neighbouring thread's allocations are not attributed to the measured work: peak={peak}"
    );
}

fn wide_posting(row: u64, key_bytes: usize) -> ContentIdentityMutation {
    let key = crate::persistence::DurableRelationalIndexKey::new(
        vec![contextdb_core::DirectedValue::Asc(
            contextdb_core::TotalOrdAsc(Value::Text(format!("{row:012}{}", "w".repeat(key_bytes)))),
        )],
        RowId(row),
        TxId(0),
    )
    .expect("encode ordered posting key");
    relational_index_posting_mutation("wide", "by_key", &key.0)
}

fn accountant(limit: usize) -> Arc<MemoryAccountant> {
    Arc::new(MemoryAccountant::with_budget(limit))
}

/// The writer path, with keys above one chunk: what the mutation really
/// allocates has to stay inside what it admitted, and a refusal has to give
/// back both.
#[test]
#[serial]
fn a_wide_key_writer_allocates_no_more_than_it_admitted() {
    let root = tempfile::tempdir().unwrap();
    let database = ProofFixtureDatabase::create(root.path().join("writer.db")).unwrap();
    let records = (0..24_u64)
        .map(|row| wide_posting(row, 70 * 1024))
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();

    let replacements = records
        .iter()
        .map(|record| {
            ContentIdentityMutation::put(
                record.family,
                record.logical_key.clone(),
                b"replaced".to_vec(),
            )
        })
        .collect::<Vec<_>>();

    // Control: the same key bytes handed to the same storage engine with no
    // proof maintenance at all. Redb's staging of the values it is given is
    // not this code's allocation to admit, so the control is what separates
    // the storage cost from the proof cost.
    let control_store = redb::Database::create(root.path().join("control.db")).unwrap();
    let control_write = control_store.begin_write().unwrap();
    let (_, control_peak, _) = measured(|| {
        let mut table = control_write
            .open_table(CONTENT_IDENTITY_NODES_TABLE)
            .unwrap();
        for record in &records {
            let blob = std::iter::once(0)
                .chain(record.logical_key.iter().copied())
                .collect::<Vec<_>>();
            let key = node_storage_key(record.family, &key_hash(&record.logical_key));
            table.insert(key.as_slice(), blob.as_slice()).unwrap();
        }
    });
    drop(control_write);

    let budget = accountant(64 * 1024 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&budget)));
    reset_work_for_test();
    let (result, peak, retained) = measured(|| {
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &replacements,
            &mut admission,
        )
    });
    result.expect("an admitted wide-key transaction commits");
    let admitted = admission.peak();
    let handed_to_storage = take_work_bytes_for_test().1 as usize;
    eprintln!(
        "writer allocation oracle: measured_peak={peak} retained_at_end={retained} \
         admitted_peak={admitted} storage_control_peak={control_peak} \
         handed_to_storage={handed_to_storage}"
    );
    // What the process really held decomposes into three things this test can
    // name: the proof memory this transaction admitted, what the storage engine
    // costs for the same key bytes (the control), and the bytes the transaction
    // handed the storage engine to stage. A global allocator cannot attribute
    // an allocation to a subsystem, so this is a bound, not an equality - the
    // discriminating parts of this oracle are the refusal unwind below and the
    // sufficiency of the admission itself.
    assert!(
        peak <= admitted
            .saturating_add(control_peak)
            .saturating_add(handed_to_storage),
        "the mutation allocated beyond its admission and the storage it drove: \
         measured={peak} admitted={admitted} control={control_peak} \
         storage={handed_to_storage}"
    );
    // The other direction, and the one that stops over-admission from making
    // the bound above easy to pass: the admission must never charge for bytes
    // the process did not actually hold. Storage is attributed once - the
    // control measures the engine's cost for the same key blobs, and
    // `handed_to_storage` the distinct chunk bytes this path stages - and
    // neither is allowed to pad the admission.
    assert!(
        admitted <= peak,
        "the admission charges for memory that was never allocated: \
         admitted={admitted} measured={peak}"
    );
    drop(admission);
    drop(write);
    assert_eq!(budget.usage().used, 0, "the admission is returned");

    // The admission is a faithful bound on what the work needs: a budget the
    // size of the admitted peak is enough to run it, and half of it is not.
    let exact = accountant(admitted + 64 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&exact)));
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &replacements,
        &mut admission,
    )
    .expect("a budget the size of the admission runs the transaction");
    drop(admission);
    drop(write);
    let halved = accountant(admitted / 2);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&halved)));
    assert!(
        matches!(
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &replacements,
                &mut admission,
            ),
            Err(Error::MemoryBudgetExceeded { .. })
        ),
        "half the admitted peak is not enough, so the admission is not a phantom"
    );
    drop(admission);
    drop(write);

    // A refusal unwinds: nothing admitted and nothing still allocated.
    let tight = accountant(192 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&tight)));
    let (refused, refusal_peak, refusal_retained) = measured(|| {
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &replacements,
            &mut admission,
        )
    });
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "{refused:?}"
    );
    drop(admission);
    drop(write);
    eprintln!(
        "writer refusal oracle: measured_peak={refusal_peak} retained_at_end={refusal_retained} accountant_after={}",
        tight.usage().used
    );
    assert_eq!(
        tight.usage().used,
        0,
        "a refused transaction admits nothing"
    );
    assert!(
        refusal_retained <= refusal_peak / 2,
        "a refused transaction does not keep what it allocated: retained={refusal_retained} peak={refusal_peak}"
    );
}

/// The reader path over the same keys: the authenticated walk's real
/// allocation stays inside the reservation its caller admitted, and ends
/// holding nothing.
#[test]
#[serial]
fn a_wide_key_reader_allocates_no_more_than_its_admitted_pool() {
    let root = tempfile::tempdir().unwrap();
    let database = ProofFixtureDatabase::create(root.path().join("reader.db")).unwrap();
    let records = (0..24_u64)
        .map(|row| wide_posting(row, 70 * 1024))
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();

    let read = database.begin_read().unwrap();
    let budget = accountant(64 * 1024 * 1024);
    let verifier = ContentIdentityReadVerifier::open(
        &read,
        std::path::Path::new("reader.db"),
        Some(Arc::clone(&budget)),
    )
    .unwrap();
    let baseline = budget.usage().used;

    let proof = ProofMemory::admitted(Some(Arc::clone(&budget)), 8 * 1024 * 1024);
    let mut delivered = 0;
    let (visited, peak, retained) = measured(|| {
        verifier.visit_relational_index_postings::<Error>(
            "wide",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &proof,
            &mut || Ok(()),
            &mut |_| {
                delivered += 1;
                Ok(true)
            },
        )
    });
    visited.unwrap();
    assert_eq!(delivered, records.len());
    let admitted = proof.peak();
    eprintln!(
        "reader allocation oracle: measured_peak={peak} retained_at_end={retained} admitted_peak={admitted} accountant_peak={}",
        budget.high_water_for_test()
    );
    assert!(
        peak <= admitted,
        "the walk allocated more than its pool admitted: measured={peak} admitted={admitted}"
    );
    drop(proof);

    // The other direction: the admission is not a phantom. Half the pool it
    // admitted cannot carry the same walk to the end, so those bytes stood for
    // memory the walk genuinely needed.
    let halved = ProofMemory::admitted(Some(Arc::clone(&budget)), admitted / 2);
    let mut partial = 0;
    verifier
        .visit_relational_index_postings::<Error>(
            "wide",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &halved,
            &mut || Ok(()),
            &mut |_| {
                partial += 1;
                Ok(true)
            },
        )
        .expect("a walk that runs out of pool stops, it does not fail");
    assert!(
        partial < delivered,
        "half the admitted pool cannot carry the whole walk: partial={partial} whole={delivered}"
    );
    drop(halved);

    drop(verifier);
    assert_eq!(
        budget.usage().used,
        baseline,
        "the reader returns every admitted byte"
    );
}

// ---------------------------------------------------------------------------
// Family reset: bounded, admitted, inside the same write transaction.
//
// A reset must delete the selected families' proof nodes in bounded batches it
// has admitted, never by materialising every selected key at once. The oracle
// measures what the reset really holds; the accountant says what it admitted.
// Two scales of committed proof nodes separate a bounded peak from one that
// grows with the store.
// ---------------------------------------------------------------------------

fn reset_family_fixture(path: &std::path::Path, nodes: u64) -> ProofFixtureDatabase {
    let database = ProofFixtureDatabase::create(path).unwrap();
    // The family this test resets, plus a second family that must survive it
    // untouched.
    let mut records = (0..nodes)
        .map(|row| wide_posting(row, 512))
        .collect::<Vec<_>>();
    records.extend((0..16_u64).map(|row| {
        ContentIdentityMutation::put(
            LogicalFamily::Graph,
            format!("edge-{row:08}").into_bytes(),
            b"kept".to_vec(),
        )
    }));
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();
    database
}

fn family_root(database: &redb::Database, family: LogicalFamily) -> Vec<u8> {
    let read = database.begin_read().unwrap();
    let roots = read.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
    roots
        .get(family as u8)
        .unwrap()
        .expect("a stored family root")
        .value()
        .to_vec()
}

fn family_node_count(database: &redb::Database, family: LogicalFamily) -> usize {
    let read = database.begin_read().unwrap();
    let nodes = read.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    nodes
        .iter()
        .unwrap()
        .filter(|entry| {
            entry
                .as_ref()
                .unwrap()
                .0
                .value()
                .first()
                .is_some_and(|tag| *tag == family as u8)
        })
        .count()
}

#[test]
#[serial]
fn a_family_reset_admits_its_batches_and_its_peak_does_not_grow_with_the_store() {
    let mut measurements = Vec::new();
    for nodes in [256_u64, 1024] {
        let root = tempfile::tempdir().unwrap();
        let database = reset_family_fixture(&root.path().join("reset.db"), nodes);
        let selected = LogicalFamily::SchemaAndIndexes;
        let kept_root = family_root(&database, LogicalFamily::Graph);
        let kept_nodes = family_node_count(&database, LogicalFamily::Graph);
        let selected_nodes = family_node_count(&database, selected);
        assert!(
            selected_nodes > 0,
            "the family under reset has stored nodes"
        );

        let budget = accountant(64 * 1024 * 1024);
        let mut admission = database.admission(Some(Arc::clone(&budget)));
        let write = database.begin_write().unwrap();
        let (result, peak, _) = measured(|| {
            RedbPersistence::reset_content_identity_families_in_write(
                &write,
                &[selected],
                &mut admission,
            )
        });
        result.expect("an admitted family reset succeeds");
        let admitted = admission.peak();
        write.commit().unwrap();
        eprintln!(
            "family reset oracle: nodes={nodes} stored_nodes={selected_nodes} \
             measured_peak={peak} admitted_peak={admitted}"
        );
        // The keys the reset holds are admitted before they are cloned, so the
        // accountant reports a real charge rather than nothing at all.
        assert!(
            admitted > 0,
            "the reset admitted nothing for the keys it held: nodes={nodes}"
        );
        // And the admission is not a phantom: it never charges for memory the
        // process did not actually hold.
        assert!(
            admitted <= peak,
            "the reset charges for memory it never held: nodes={nodes} \
             admitted={admitted} measured={peak}"
        );
        assert_eq!(
            family_node_count(&database, selected),
            0,
            "every selected node is deleted"
        );
        assert_eq!(
            family_root(&database, selected),
            encode_root(&empty_root(selected)),
            "the selected family's root is the canonical empty one"
        );
        assert_eq!(
            family_root(&database, LogicalFamily::Graph),
            kept_root,
            "an unselected family's root is untouched"
        );
        assert_eq!(
            family_node_count(&database, LogicalFamily::Graph),
            kept_nodes,
            "an unselected family keeps every node"
        );
        drop(admission);
        assert_eq!(budget.usage().used, 0, "the reset returns its admission");
        measurements.push((nodes, peak, admitted));
    }

    let (small_nodes, small_peak, small_admitted) = measurements[0];
    let (large_nodes, large_peak, large_admitted) = measurements[1];
    // Four times the proof nodes must not cost four times the memory: the
    // deletion walks the family in bounded batches inside the same write
    // transaction.
    assert!(
        large_peak <= small_peak.saturating_mul(2),
        "the reset's peak grows with the store instead of staying bounded: \
         {small_nodes} nodes peaked at {small_peak}, {large_nodes} nodes at {large_peak}"
    );
    assert!(
        large_admitted <= small_admitted.saturating_mul(2),
        "the reset's admission grows with the store: {small_nodes} nodes admitted \
         {small_admitted}, {large_nodes} nodes admitted {large_admitted}"
    );
}

/// The batch charge is real: a budget that cannot admit one batch refuses with
/// the existing typed error, the transaction is abandoned, and not one stored
/// byte of the proof changes.
#[test]
#[serial]
fn a_refused_family_reset_changes_no_stored_byte_and_leaves_nothing_admitted() {
    let root = tempfile::tempdir().unwrap();
    let database = reset_family_fixture(&root.path().join("refused-reset.db"), 512);
    let selected = LogicalFamily::SchemaAndIndexes;
    let before_root = family_root(&database, selected);
    let before_nodes = family_node_count(&database, selected);

    let budget = accountant(64);
    let mut admission = database.admission(Some(Arc::clone(&budget)));
    let write = database.begin_write().unwrap();
    let refused = RedbPersistence::reset_content_identity_families_in_write(
        &write,
        &[selected],
        &mut admission,
    );
    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "a reset that cannot admit its batch is the typed budget refusal: {refused:?}"
    );
    drop(write);
    drop(admission);
    assert_eq!(
        budget.usage().used,
        0,
        "a refused reset leaves nothing admitted"
    );
    assert_eq!(family_root(&database, selected), before_root);
    assert_eq!(family_node_count(&database, selected), before_nodes);
}
