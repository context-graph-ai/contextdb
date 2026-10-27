//! Borrowed native header, transaction, ownership and captured-root guards.
use super::support::*;
use contextdb_core::{Value, VectorIndexRef, VectorPartitionKey, Wallclock};
use contextdb_engine::Database;
use std::sync::{Arc, Mutex};

fn index() -> VectorIndexRef {
    VectorIndexRef::new("items", "v")
}
fn tuple() -> VectorPartitionKey {
    VectorPartitionKey::unpartitioned()
}
fn store(path: &std::path::Path, dimensions: usize) -> Database {
    let db = open_caller_driven(path);
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({dimensions}) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95) IVF (LISTS = 4, PROBES = 4, REFINE_FACTOR = 64.00) WITH (training_samples = 1, training_iterations = 1)) HISTORY ALL"),&empty()).unwrap();
    for id in 0..64 {
        db.execute(
            "INSERT INTO items VALUES ($id,$v)",
            &params([
                ("id", Value::Int64(id)),
                ("v", Value::Vector(unit_vector(id as u64, dimensions))),
            ]),
        )
        .unwrap();
    }
    drive_until_serving(&db);
    db
}
#[derive(Clone, Copy)]
struct Pending {
    op: NativeSealOperation,
    key: NativeSealPage,
    sealed: bool,
}
#[derive(Clone, Copy)]
struct Operation {
    id: NativeSealOperation,
    drained: bool,
    closed: u16,
    fingerprinted: u16,
    roots: [[u8; 32]; 16],
    scoped: bool,
    fingerprints: usize,
}
#[derive(Clone, Copy)]
struct Object {
    instance: u64,
    kind: NativeSealObjectKind,
    layout: std::alloc::Layout,
    owner: usize,
    adapter: usize,
}
#[derive(Clone, Copy)]
struct Expected {
    kind: u8,
    ordinal: u64,
    first: Option<u64>,
    count: u32,
}
struct Capture {
    capture: Option<u64>,
    ended: bool,
    events: u64,
    pending: [Option<Pending>; 2048],
    ops: [Option<Operation>; 8],
    objects: [Option<Object>; 16],
    owner: usize,
    bound: bool,
    global_empty: bool,
    commits: u64,
    aborts: u64,
    headers: u64,
    opened: u64,
    last_opened: [Option<NativeSealPage>; 16],
    live_windows: usize,
    peak_windows: usize,
    expected: [Option<Expected>; 3],
    sealed_seen: [Option<(NativeSealPage, NativeSealOperation)>; 3],
    opened_seen: [Option<NativeSealPage>; 3],
}
impl Capture {
    fn new(owner: usize, expected: [Option<Expected>; 3]) -> Self {
        Self {
            capture: None,
            ended: false,
            events: 0,
            pending: [None; 2048],
            ops: [None; 8],
            objects: [None; 16],
            owner,
            bound: false,
            global_empty: false,
            commits: 0,
            aborts: 0,
            headers: 0,
            opened: 0,
            last_opened: [None; 16],
            live_windows: 0,
            peak_windows: 0,
            expected,
            sealed_seen: [None; 3],
            opened_seen: [None; 3],
        }
    }
    fn op(&mut self, id: NativeSealOperation) -> usize {
        if let Some(n) = self.ops.iter().position(|o| o.is_some_and(|x| x.id == id)) {
            return n;
        }
        let n = self
            .ops
            .iter()
            .position(Option::is_none)
            .expect("fixed operation recorder overflow");
        self.ops[n] = Some(Operation {
            id,
            drained: false,
            closed: 0,
            fingerprinted: 0,
            roots: [[0; 32]; 16],
            scoped: false,
            fingerprints: 0,
        });
        n
    }
    fn header(
        &mut self,
        key: NativeSealPage,
        header: &[u8; 192],
        payload: &[u8; 3904],
        operation: Option<NativeSealOperation>,
    ) {
        assert_eq!(&header[..4], b"CIVF");
        assert_eq!(u16::from_le_bytes(header[4..6].try_into().unwrap()), 1);
        assert_eq!(header[6], key.kind);
        assert_eq!(header[7], 0);
        assert_eq!(
            u64::from_le_bytes(header[72..80].try_into().unwrap()),
            key.generation
        );
        assert_eq!(
            u64::from_le_bytes(header[96..104].try_into().unwrap()),
            key.ordinal
        );
        assert_eq!(
            u32::from_le_bytes(header[116..120].try_into().unwrap()),
            3904
        );
        assert_eq!(&header[120..152], blake3::hash(payload).as_bytes());
        assert_eq!(&header[184..192], &[0; 8]);
        for n in 0..3 {
            if let Some(expected) = self.expected[n] {
                let first = u64::from_le_bytes(header[104..112].try_into().unwrap());
                let count = u32::from_le_bytes(header[112..116].try_into().unwrap());
                if expected.kind != key.kind
                    || expected.ordinal != key.ordinal
                    || count != expected.count
                {
                    continue;
                }
                if let Some(expected_first) = expected.first {
                    if first != expected_first {
                        continue;
                    }
                } else {
                    let slots = [0, 24, 48].map(|offset| {
                        u64::from_le_bytes(payload[offset..offset + 8].try_into().unwrap())
                    });
                    if first != slots[0]
                        || slots[0].checked_add(2) != Some(slots[1])
                        || slots[1].checked_add(2) != Some(slots[2])
                    {
                        continue;
                    }
                }
                if let Some(operation) = operation {
                    self.sealed_seen[n] = Some((key, operation));
                } else {
                    self.opened_seen[n] = Some(key);
                }
            }
        }
    }
    fn event(&mut self, event: NativeSealEvent<'_>) {
        match event {
            NativeSealEvent::CaptureBegan {
                capture,
                attempted,
                delivered,
                producers_open,
                overflow,
                reentry,
            } => {
                assert!(self.capture.replace(capture).is_none());
                assert_eq!((attempted, delivered, producers_open), (0, 0, 0));
                assert!(!overflow && !reentry);
                return;
            }
            NativeSealEvent::CaptureEnded {
                capture,
                attempted,
                delivered,
                producers_open,
                overflow,
                reentry,
            } => {
                assert_eq!(self.capture, Some(capture));
                assert!(!self.ended);
                self.ended = true;
                assert_eq!(attempted, delivered);
                assert_eq!(delivered, self.events);
                assert_eq!(producers_open, 0);
                assert!(!overflow && !reentry);
                return;
            }
            _ => {}
        }
        assert!(self.capture.is_some() && !self.ended);
        self.events += 1;
        match event {
            NativeSealEvent::MarkerInserted {
                operation,
                key,
                newly_inserted,
            } => {
                assert!(!self.bound);
                let op_index = self.op(operation);
                assert_eq!(
                    self.ops[op_index].unwrap().fingerprinted & (1 << (key.kind - 1)),
                    0,
                    "no payload/header mutation after this operation captured its kind root"
                );
                let existing = self
                    .pending
                    .iter()
                    .position(|p| p.is_some_and(|x| x.op == operation && x.key == key));
                assert_eq!(newly_inserted, existing.is_none());
                if existing.is_none() {
                    let n = self
                        .pending
                        .iter()
                        .position(Option::is_none)
                        .expect("fixed pending-key recorder overflow");
                    self.pending[n] = Some(Pending {
                        op: operation,
                        key,
                        sealed: false,
                    });
                }
            }
            NativeSealEvent::PayloadWritten { operation, key } => {
                assert!(!self.bound);
                let op_index = self.op(operation);
                assert_eq!(
                    self.ops[op_index].unwrap().fingerprinted & (1 << (key.kind - 1)),
                    0
                );
                let n = self
                    .pending
                    .iter()
                    .position(|p| p.is_some_and(|x| x.op == operation && x.key == key))
                    .expect("marker precedes payload");
                self.pending[n].as_mut().unwrap().sealed = false;
            }
            NativeSealEvent::HeaderSealed {
                operation,
                key,
                header,
                payload,
            } => {
                assert!(!self.bound);
                let op_index = self.op(operation);
                assert_eq!(
                    self.ops[op_index].unwrap().fingerprinted & (1 << (key.kind - 1)),
                    0
                );
                let n = self
                    .pending
                    .iter()
                    .position(|p| p.is_some_and(|x| x.op == operation && x.key == key))
                    .expect("header seals its actual live pending key");
                self.pending[n].as_mut().unwrap().sealed = true;
                self.headers += 1;
                self.header(key, header, payload, Some(operation));
            }
            NativeSealEvent::MarkerRemoved { operation, key } => {
                let n = self
                    .pending
                    .iter()
                    .position(|p| p.is_some_and(|x| x.op == operation && x.key == key))
                    .expect("remove exactly one existing marker");
                assert!(self.pending[n].unwrap().sealed);
                self.pending[n] = None;
            }
            NativeSealEvent::PageOpened {
                key,
                header,
                payload,
            } => {
                self.last_opened[(key.kind - 1) as usize] = Some(key);
                self.opened += 1;
                self.header(key, header, payload, None);
            }
            NativeSealEvent::OperationPrefixChecked {
                operation,
                after_drain,
                empty,
            } => {
                assert!(empty, "actual native cursor, not inferred marker balance");
                let n = self.op(operation);
                if after_drain {
                    assert!(!self.pending.iter().flatten().any(|p| p.op == operation));
                    self.ops[n].as_mut().unwrap().drained = true;
                }
            }
            NativeSealEvent::GlobalPendingChecked {
                before_final_binding,
                empty,
            } => {
                assert!(empty);
                if before_final_binding {
                    assert!(self.pending.iter().all(Option::is_none));
                    self.global_empty = true;
                }
            }
            NativeSealEvent::KindHandlesClosed { operation, kind } => {
                assert!((1..=16).contains(&kind));
                let n = self.op(operation);
                self.ops[n].as_mut().unwrap().closed |= 1 << (kind - 1);
            }
            NativeSealEvent::FamilyFingerprintCaptured {
                operation,
                kind,
                root,
            } => {
                assert!(!self.bound);
                let n = self.op(operation);
                let op = self.ops[n].as_mut().unwrap();
                assert!(op.drained);
                assert_ne!(op.closed & (1 << (kind - 1)), 0);
                op.fingerprinted |= 1 << (kind - 1);
                op.roots[(kind - 1) as usize] = root;
                op.fingerprints += 1;
                assert!(
                    op.fingerprints <= 3 * 16,
                    "root-directory bound is per operation"
                );
            }
            NativeSealEvent::ScopeWritten {
                operation,
                present,
                expected_roots,
            } => {
                assert!(!self.bound);
                let n = self.op(operation);
                let op = self.ops[n].as_mut().unwrap();
                assert!(op.drained);
                assert_ne!(op.fingerprinted, 0);
                for (kind, expected) in expected_roots.iter().enumerate() {
                    if op.fingerprinted & (1 << kind) != 0 {
                        assert_ne!(present & (1 << kind), 0);
                        assert_eq!(*expected, op.roots[kind]);
                    }
                }
                op.scoped = true;
            }
            NativeSealEvent::ConfigurationBound => {
                assert!(self.global_empty);
                assert!(self.pending.iter().all(Option::is_none));
                assert!(self.ops.iter().flatten().all(|op| op.scoped));
                self.bound = true;
            }
            NativeSealEvent::OuterCommitted { actual_commit_lsn } => {
                assert!(self.bound);
                assert!(actual_commit_lsn > 0);
                self.commits += 1;
            }
            NativeSealEvent::OuterAborted => {
                self.aborts += 1;
                self.pending.fill(None);
            }
            NativeSealEvent::ObjectAdmitted {
                instance,
                kind,
                layout,
                original_owner,
                adapter_owner,
                windows,
            } => {
                assert_eq!(original_owner, self.owner);
                assert_ne!(original_owner, 0);
                assert!(
                    !self
                        .objects
                        .iter()
                        .flatten()
                        .any(|o| o.instance == instance)
                );
                let n = self
                    .objects
                    .iter()
                    .position(Option::is_none)
                    .expect("fixed invoice recorder overflow");
                self.objects[n] = Some(Object {
                    instance,
                    kind,
                    layout,
                    owner: original_owner,
                    adapter: adapter_owner,
                });
                match kind {
                    NativeSealObjectKind::Scope => {
                        assert!(layout.size() > 1542);
                        assert!(windows.is_none());
                    }
                    NativeSealObjectKind::SealWorkspace => {
                        let windows =
                            windows.expect("actual checked ranges in enclosing allocation");
                        assert_eq!(windows.map(|w| w.layout.size()), [4096, 3904, 3904]);
                        assert!(layout.size() > 11904);
                        for (a, left) in windows.iter().enumerate() {
                            assert_eq!(left.offset % left.layout.align(), 0);
                            assert!(left.offset + left.layout.size() <= layout.size());
                            for right in &windows[a + 1..] {
                                assert!(
                                    left.offset + left.layout.size() <= right.offset
                                        || right.offset + right.layout.size() <= left.offset
                                );
                            }
                        }
                        self.live_windows += 3;
                        self.peak_windows = self.peak_windows.max(self.live_windows);
                        assert!(self.live_windows <= 3);
                    }
                }
            }
            NativeSealEvent::ObjectReleased {
                instance,
                kind,
                layout,
                original_owner,
                adapter_owner,
            } => {
                let n = self
                    .objects
                    .iter()
                    .position(|o| o.is_some_and(|x| x.instance == instance))
                    .expect("release pairs actual invoice instance");
                let old = self.objects[n].take().unwrap();
                assert_eq!(
                    (old.kind, old.layout, old.owner, old.adapter),
                    (kind, layout, original_owner, adapter_owner)
                );
                if kind == NativeSealObjectKind::SealWorkspace {
                    self.live_windows -= 3;
                }
            }
            NativeSealEvent::Other => {}
            NativeSealEvent::CaptureBegan { .. } | NativeSealEvent::CaptureEnded { .. } => {
                unreachable!()
            }
        }
    }
    fn complete(&self) {
        assert!(self.capture.is_some() && self.ended && self.events > 0);
        assert_eq!(self.live_windows, 0);
        assert!(self.objects.iter().all(Option::is_none));
    }
}
fn captured<T>(
    owner: usize,
    expected: [Option<Expected>; 3],
    operation: impl FnOnce() -> T,
) -> (T, Arc<Mutex<Capture>>) {
    let state = Arc::new(Mutex::new(Capture::new(owner, expected)));
    let sink = state.clone();
    let result = with_native_ivf_seal_observer_for_test(
        Arc::new(move |event| sink.lock().unwrap().event(event)),
        operation,
    );
    state.lock().unwrap().complete();
    (result, state)
}
fn e(kind: u8, ordinal: u64, first: u64, count: u32) -> Option<Expected> {
    Some(Expected {
        kind,
        ordinal,
        first: Some(first),
        count,
    })
}
#[test]
fn semantic_headers_cover_contiguous_padded_sparse_and_retained_record_boundaries() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    use NativeSemanticCase::*;
    let cases = [
        (
            CanonicalCrossing,
            1,
            [e(1, 0, 0, 326), e(1, 1, 326, 1), None],
        ),
        (
            CanonicalContinuation,
            2048,
            [e(1, 0, 0, 1), e(1, 1, 1, 0), e(1, 2, 1, 0)],
        ),
        (
            NonconsecutiveListRecords,
            8,
            [
                Some(Expected {
                    kind: 2,
                    ordinal: 0,
                    first: None,
                    count: 3,
                }),
                None,
                None,
            ],
        ),
        (
            SlotMetadataCrossing,
            8,
            [e(10, 0, 0, 98), e(10, 1, 98, 1), None],
        ),
        (
            DirectoryBoundary,
            8,
            [e(3, 0, 0, 162), e(3, 1, 162, 1), None],
        ),
        (DirectoryPresenceExtent, 8, [e(4, 0, 0, 2), None, None]),
        (
            VisibilityBoundary,
            8,
            [e(5, 0, 0, 31232), e(5, 1, 31232, 1), None],
        ),
        (
            CodebookCoordinates,
            977,
            [e(6, 0, 0, 1), e(6, 1, 0, 976), e(6, 2, 976, 1)],
        ),
        (
            AccumulatorBoundary,
            8,
            [e(7, 0, 0, 488), e(7, 1, 488, 1), None],
        ),
        (
            ReservoirBoundary,
            8,
            [e(8, 0, 0, 488), e(8, 1, 488, 1), None],
        ),
        (CatalogHeaderAndRun, 8, [e(9, 0, 0, 1), e(9, 7, 7, 1), None]),
        (RetainedTail, 8, [e(11, 0, 0, 488), e(11, 1, 488, 1), None]),
        (
            ListUsedBoundary,
            3888,
            [e(14, 0, 1, 488), e(14, 1, 489, 1), None],
        ),
        (
            ListLinksBoundary,
            3888,
            [e(12, 0, 1, 488), e(12, 1, 489, 1), None],
        ),
        (
            AllocatorBoundary,
            8,
            [e(13, 0, 0, 488), e(13, 1, 488, 1), None],
        ),
        (
            SparseRowVersions,
            8,
            [e(15, 0, 0, 2), e(15, 1, 97, 1), None],
        ),
        (SparseRowCopies, 8, [e(16, 0, 0, 2), e(16, 1, 162, 1), None]),
    ];
    for (case, dimensions, expected) in cases {
        let root = tempfile::tempdir().unwrap();
        let db = store(&root.path().join("headers.db"), dimensions);
        let before = db.snapshot();
        let rows = count(&db, "items");
        let owner = native_ivf_original_owner_for_test(&db);
        let mut setup = NativeSemanticSetup {
            dimensions,
            ..Default::default()
        };
        match case {
            CanonicalCrossing => setup.slots = 327,
            CanonicalContinuation => setup.slots = 1,
            NonconsecutiveListRecords => {
                setup.slots = 6;
                setup.lists = 2;
                setup.active_lists = 2;
            }
            SlotMetadataCrossing => setup.slots = 99,
            DirectoryBoundary | DirectoryPresenceExtent => setup.lists = 163,
            VisibilityBoundary => setup.slots = 31233,
            CodebookCoordinates => {
                setup.lists = 1;
                setup.active_lists = 1;
            }
            AccumulatorBoundary => {
                setup.lists = 489;
                setup.active_lists = 489;
            }
            ReservoirBoundary => {
                setup.slots = 489;
                setup.samples = 489;
            }
            CatalogHeaderAndRun => setup.retained_run = 7,
            RetainedTail => {
                setup.tail_head = 488;
                setup.tail_end = 489;
            }
            ListUsedBoundary | ListLinksBoundary => {
                setup.lists = 1;
                setup.allocated_list_pages = 489;
                setup.slots = 489;
            }
            AllocatorBoundary => setup.lists = 489,
            SparseRowVersions => {
                setup.sparse_records = [Some((0, 0, 1)), Some((96, 10, 3)), Some((97, 11, 1))]
            }
            SparseRowCopies => {
                setup.retained_run = 7;
                setup.sparse_records = [Some((0, 0, 1)), Some((161, 10, 3)), Some((162, 11, 1))];
            }
        }
        let cancellation = contextdb_core::read_contract::OwnerReadCancellation::default();
        let (result, state) = captured(owner, expected, || {
            drive_aborting_native_ivf_semantic_case_for_test(
                &db,
                &index(),
                &tuple(),
                case,
                setup,
                &cancellation,
            )
        });
        let drive = result.unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.commits, 0);
        assert_eq!(state.aborts, 1);
        assert!(state.headers > 0 && state.opened > 0);
        assert_eq!(state.peak_windows, 3);
        for (n, expected) in expected.iter().enumerate() {
            if let Some(expected) = expected {
                let key = drive.targets[n].expect("actual final target identity");
                assert_eq!((key.kind, key.ordinal), (expected.kind, expected.ordinal));
                assert_eq!(
                    state.sealed_seen[n],
                    Some((key, drive.final_operation)),
                    "final actual operation must seal the independently expected header for {case:?}"
                );
                assert_eq!(
                    state.opened_seen[n],
                    Some(key),
                    "actual admitted readback must expose that same target's independently expected header"
                );
            } else {
                assert!(drive.targets[n].is_none());
            }
        }
        assert_eq!(db.snapshot(), before);
        assert_eq!(
            count(&db, "items"),
            rows,
            "control-only drive never becomes committed authoritative state"
        );
    }
}
#[test]
fn two_native_kernel_commits_seal_in_one_atomic_authoritative_sql_commit() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("atomic-seal.db");
    let db = store(&path, 8);
    let before = db.snapshot();
    let pin = db.pin_snapshot(before).unwrap();
    let owner = native_ivf_original_owner_for_test(&db);
    let (_, state) = captured(owner, [None; 3], || {
        let tx = db.begin_or_panic();
        for id in [64, 65] {
            db.execute_in_tx(
                tx,
                "INSERT INTO items VALUES ($id,$v)",
                &params([
                    ("id", Value::Int64(id)),
                    ("v", Value::Vector(unit_vector(id as u64, 8))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    });
    {
        let state = state.lock().unwrap();
        assert_eq!(state.commits, 1);
        assert_eq!(state.aborts, 0);
        assert_eq!(state.ops.iter().flatten().count(), 2);
        assert!(state.headers > 0);
        assert_eq!(state.peak_windows, 3);
    }
    assert_eq!(
        db.execute_at_snapshot("SELECT COUNT(*) FROM items", &empty(), before)
            .unwrap()
            .rows()[0][0],
        Value::Int64(64)
    );
    assert_eq!(count(&db, "items"), 66);
    let query = params([("q", Value::Vector(unit_vector(64, 8)))]);
    let (found, work) = observe_search(&db, || {
        db.execute(
            "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 128",
            &query,
        )
        .unwrap()
    });
    assert!(work.clustered_route);
    let mut ids = int_ids(&found);
    assert_eq!(ids.len(), 66);
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids, (0..66).collect::<Vec<_>>());
    drop(pin);
    db.close().unwrap();
    drop(db);
    let db = open_caller_driven(&path);
    assert_eq!(count(&db, "items"), 66);
}
#[test]
fn every_fallible_native_seal_boundary_aborts_rows_roots_and_original_owner_scratch() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for point in [
        NativeSealFailurePoint::BeforeMarkerWrite,
        NativeSealFailurePoint::BeforePayloadWrite,
        NativeSealFailurePoint::BeforeHeaderSeal,
        NativeSealFailurePoint::BeforeKindFingerprint,
        NativeSealFailurePoint::BeforeScopeWrite,
        NativeSealFailurePoint::BeforeConfigurationBind,
        NativeSealFailurePoint::BeforeOuterCommit,
    ] {
        let root = tempfile::tempdir().unwrap();
        let db = store(&root.path().join("seal-abort.db"), 8);
        let snapshot = db.snapshot();
        let before =
            native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), snapshot).unwrap();
        let owner = native_ivf_original_owner_for_test(&db);
        let ((result, fired), state) = captured(owner, [None; 3], || {
            with_native_ivf_seal_failure_for_test(point, || {
                db.execute(
                    "INSERT INTO items VALUES (64,$v)",
                    &params([("v", Value::Vector(unit_vector(64, 8)))]),
                )
            })
        });
        assert!(fired && result.is_err());
        assert_eq!(state.lock().unwrap().commits, 0);
        assert_eq!(state.lock().unwrap().aborts, 1);
        assert_eq!(db.snapshot(), snapshot);
        assert_eq!(count(&db, "items"), 64);
        assert_eq!(
            native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), db.snapshot())
                .unwrap(),
            before
        );
        db.execute(
            "INSERT INTO items VALUES (64,$v)",
            &params([("v", Value::Vector(unit_vector(64, 8)))]),
        )
        .unwrap();
        assert_eq!(
            count(&db, "items"),
            65,
            "a fresh authoritative write starts from durable state"
        );
    }
}
fn checked_source(db: &Database, snapshot: contextdb_core::SnapshotId) -> ([u8; 32], u64, u64) {
    let mut digest = [0; 32];
    let mut entries = 0;
    let mut live = 0;
    visit_native_ivf_selected_source_for_test(db, &index(), &tuple(), snapshot, &mut |record| {
        let mut hash = blake3::Hasher::new();
        hash.update(record.key);
        hash.update(record.encoded);
        for (out, byte) in digest.iter_mut().zip(hash.finalize().as_bytes()) {
            *out ^= *byte;
        }
        entries += 1;
        live += u64::from(record.live);
    })
    .unwrap();
    (digest, entries, live)
}
#[test]
fn captured_native_roots_keep_frozen_basis_separate_from_actual_delta_coverage_and_read_snapshot() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = store(&root.path().join("coverage.db"), 8);
    let old = db.snapshot();
    let pin = db.pin_snapshot(old).unwrap();
    let initial =
        native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), old).unwrap();
    assert_eq!(initial.expected_roots, initial.actual_roots);
    assert_ne!(initial.present, 0);
    for sql in [
        "INSERT INTO items VALUES (64,$v)",
        "UPDATE items SET v = $v WHERE id = 7",
        "DELETE FROM items WHERE id = 9",
    ] {
        db.execute(sql, &params([("v", Value::Vector(unit_vector(64, 8)))]))
            .unwrap();
        let current =
            native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), db.snapshot())
                .unwrap();
        let (digest, entries, live) = checked_source(&db, db.snapshot());
        assert!(entries > 0);
        assert_eq!(
            (
                current.current_digest,
                current.current_entries,
                current.current_live_entries
            ),
            (digest, entries, live)
        );
        assert_eq!(current.expected_roots, current.actual_roots);
        assert_eq!(current.frozen_frontier, initial.frozen_frontier);
        assert_eq!(
            current.frozen_source_identity,
            initial.frozen_source_identity
        );
        assert!(current.current_frontier > current.frozen_frontier);
        assert_eq!(
            native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), old).unwrap(),
            initial
        );
    }
    let selected =
        native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), db.snapshot())
            .unwrap();
    db.execute("CREATE TABLE unrelated (id INT PRIMARY KEY)", &empty())
        .unwrap();
    db.execute("INSERT INTO unrelated VALUES (1)", &empty())
        .unwrap();
    let later = native_ivf_scope_roots_at_snapshot_for_test(&db, &index(), &tuple(), db.snapshot())
        .unwrap();
    assert!(later.snapshot_lsn > selected.current_frontier);
    assert_eq!(later.current_frontier, selected.current_frontier);
    assert_eq!(later.frozen_frontier, initial.frozen_frontier);
    assert_eq!(later.expected_roots, selected.expected_roots);
    assert_eq!(later.current_digest, selected.current_digest);
    let query = params([("q", Value::Vector(unit_vector(9, 8)))]);
    for mode in ["INDEXED", "EXACT"] {
        let sql = format!("SELECT id FROM items ORDER BY v <=> $q USE VECTOR {mode} LIMIT 128");
        let old_ids = int_ids(&db.execute_at_snapshot(&sql, &query, old).unwrap());
        assert_eq!(old_ids.len(), 64);
        assert!(old_ids.contains(&9));
        assert!(!old_ids.contains(&64));
        let ids = int_ids(&db.execute(&sql, &query).unwrap());
        assert_eq!(ids.len(), 64);
        assert!(!ids.contains(&9));
        assert!(ids.contains(&64));
    }
    drop(pin);
}

#[test]
fn real_one_list_page_489_search_loads_the_link_used_and_list_boundaries() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("list-489.db"));
    db.execute("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR(3888) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00) WITH (training_samples = 1, training_iterations = 1))",&empty()).unwrap();
    let tx = db.begin_or_panic();
    for id in 0..489 {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &params([
                ("id", Value::Int64(id)),
                ("v", Value::Vector(unit_vector(id as u64, 3888))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    drive_until_serving(&db);
    let owner = native_ivf_original_owner_for_test(&db);
    let (result, state) = captured(
        owner,
        [e(12, 1, 489, 1), e(14, 1, 489, 1), e(2, 488, 488, 1)],
        || {
            db.execute(
                "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 489",
                &params([("q", Value::Vector(unit_vector(488, 3888)))]),
            )
        },
    );
    let ids = int_ids(&result.unwrap());
    assert_eq!(ids.len(), 489);
    assert_eq!(ids.first(), Some(&488));
    let state = state.lock().unwrap();
    assert!(state.opened > 0);
    for n in 0..3 {
        assert!(
            state.opened_seen[n].is_some(),
            "the real healthy query loads every role at the 488/489 allocation boundary"
        );
    }
}

#[test]
fn actual_page_identity_damage_refuses_indexed_before_scoring_and_auto_uses_canonical_source() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for payload in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("actual-page-fault.db");
        let db = store(&path, 8);
        let owner = native_ivf_original_owner_for_test(&db);
        let query = params([("q", Value::Vector(unit_vector(7, 8)))]);
        let (healthy, state) = captured(owner, [None; 3], || {
            observe_search(&db, || {
                db.execute(
                    "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 64",
                    &query,
                )
                .unwrap()
            })
        });
        let (healthy, work) = healthy;
        assert!(work.clustered_route);
        let expected = int_ids(&healthy);
        let target = state.lock().unwrap().last_opened[5]
            .expect("healthy native query positively loads this actual codebook page");
        assert_eq!(target.kind, 6);
        corrupt_native_ivf_page_for_test(&db, target, payload, 8, 1).unwrap();
        db.close().unwrap();
        drop(db);
        let db = open_caller_driven(&path);
        let (result, work) = observe_search(&db, || {
            db.execute(
                "SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 64",
                &query,
            )
        });
        assert!(matches!(
            result,
            Err(contextdb_core::Error::StoreCorrupted { .. })
        ));
        assert_eq!(work.distance_evaluations(), 0);
        let (auto, work) = observe_search(&db, || {
            db.execute(
                "SELECT id FROM items ORDER BY v <=> $q USE VECTOR AUTO LIMIT 64",
                &query,
            )
            .unwrap()
        });
        assert_eq!(int_ids(&auto), expected);
        assert_eq!(work.route, Some(SearchRoute::Exact));
        assert_eq!(work.centroid_evaluations, 0);
        assert_eq!(work.quantized_evaluations, 0);
        assert_eq!(
            recovery_action(&db, "items", "v", &[]),
            Some(RecoveryAction::Rebuild)
        );
    }
}
#[test]
fn equal_schema_counters_on_different_index_names_never_share_native_authority() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let db = open_caller_driven(&root.path().join("index-names.db"));
    for table in ["alpha", "beta"] {
        db.execute(&format!("CREATE TABLE {table} (id INT PRIMARY KEY, v VECTOR(8) SEARCH_MODE INDEXED IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = 1.00))"),&empty()).unwrap();
        for id in 0..16 {
            db.execute(
                &format!("INSERT INTO {table} VALUES ($id,$v)"),
                &params([
                    ("id", Value::Int64(id)),
                    ("v", Value::Vector(unit_vector(id as u64, 8))),
                ]),
            )
            .unwrap();
        }
    }
    drive_until_serving(&db);
    let snapshot = db.snapshot();
    let a = native_ivf_scope_roots_at_snapshot_for_test(
        &db,
        &VectorIndexRef::new("alpha", "v"),
        &tuple(),
        snapshot,
    )
    .unwrap();
    let b = native_ivf_scope_roots_at_snapshot_for_test(
        &db,
        &VectorIndexRef::new("beta", "v"),
        &tuple(),
        snapshot,
    )
    .unwrap();
    assert_eq!(
        (a.table_generation, a.column_incarnation),
        (b.table_generation, b.column_incarnation),
        "fixture exercises the equal-counter identity collision"
    );
    assert_ne!(a.index_digest, b.index_digest);
    assert_eq!(a.expected_roots, a.actual_roots);
    assert_eq!(b.expected_roots, b.actual_roots);
    for table in ["alpha", "beta"] {
        let (result, work) = observe_search(&db, || {
            db.execute(
                &format!("SELECT id FROM {table} ORDER BY v <=> $q USE VECTOR INDEXED LIMIT 16"),
                &params([("q", Value::Vector(unit_vector(7, 8)))]),
            )
            .unwrap()
        });
        assert!(work.clustered_route);
        assert_eq!(int_ids(&result).first(), Some(&7));
    }
}
