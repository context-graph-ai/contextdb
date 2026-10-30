//! Independent guards of genuine native page ownership. These bind only to
//! actual producer events and returned page aliases; fixtures never create a payer.
#![cfg(feature = "test-seams")]
#![expect(
    clippy::result_large_err,
    reason = "Public errors retain inline typed refusal diagnostics."
)]
#[path = "native_page_public_ports/receipt.rs"]
mod receipt;
use contextdb_core::Value;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use redb::native_page_observations::{
    NativeColdReadAttemptForTest, NativePageCustodyEventForTest, NativePageObserverForTest,
    observe_native_pages_for_test,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};
const ROWS: usize = 8;
const DIMENSIONS: usize = contextdb_vector::ivf::storage::PAGE_BYTES / size_of::<f32>() + ROWS;
#[derive(Default)]
struct Observer {
    ledger: Mutex<receipt::Ledger>,
    keep: AtomicBool,
    aliases: Mutex<Vec<redb::CachePageForTest>>,
    cold_reads: AtomicUsize,
    fault: AtomicU8,
    phase_status: AtomicU8,
    fault_stage: AtomicU8,
    failed: AtomicUsize,
}
impl NativePageObserverForTest for Observer {
    fn observe(&self, event: NativePageCustodyEventForTest) {
        self.ledger.lock().unwrap().observe(event);
    }
    fn page_returned(&self, retain: &dyn Fn() -> redb::CachePageForTest) {
        if self.keep.swap(false, Ordering::SeqCst) {
            let page = retain();
            let backing = page.native_backing();
            let payment = page.native_payment();
            assert!(backing.layout.size() >= backing.payload_bytes);
            assert!(payment.source_receipt.is_some() || payment.fixed_cache_receipt.is_some());
            let before = page.strong_handles();
            let clone = page.clone();
            assert_eq!(clone.native_backing().allocation, backing.allocation);
            assert_eq!(clone.strong_handles(), before + 1);
            drop(clone);
            assert_eq!(page.strong_handles(), before);
            self.aliases.lock().unwrap().push(page);
        }
    }
    fn before_cold_read(
        &self,
        attempt: &NativeColdReadAttemptForTest,
    ) -> Option<redb::NativeColdReadError> {
        self.cold_reads.fetch_add(1, Ordering::SeqCst);
        let mode = self.fault.load(Ordering::SeqCst);
        if mode == 0 {
            return None;
        }
        self.fault_stage
            .store(self.phase_status.load(Ordering::SeqCst), Ordering::SeqCst);
        if mode == 1 {
            self.fault.store(0, Ordering::SeqCst);
            return Some(redb::NativeColdReadError::Io(std::io::Error::from(
                std::io::ErrorKind::Interrupted,
            )));
        }
        if mode == 3 {
            return Some(redb::NativeColdReadError::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof,
            )));
        }
        assert_eq!(mode, 2);
        Some(redb::NativeColdReadError::Corrupt(redb::NativeColdFault {
            reason: "injected persistent truncated native cold extent",
            page: attempt.offset.to_le_bytes(),
            offset: usize::try_from(attempt.offset).expect("actual native offset fits build host"),
            expected: attempt.len as u128,
            actual: attempt
                .len
                .checked_sub(1)
                .expect("native cold page is nonempty") as u128,
        }))
    }
    fn after_cold_read(
        &self,
        _: &NativeColdReadAttemptForTest,
        result: Result<(), &redb::NativeColdReadError>,
    ) {
        if result.is_err() {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }
}
fn empty() -> HashMap<String, Value> {
    HashMap::new()
}
fn open(path: &std::path::Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn vector(id: usize) -> Vec<f32> {
    (0..DIMENSIONS)
        .map(|c| (((id * 41 + c * 17 + 3) % 1009) as f32) / 1009.)
        .collect()
}
fn seed(db: &Database) {
    db.execute(&format!("CREATE TABLE items (id INT PRIMARY KEY, v VECTOR({DIMENSIONS}) SEARCH_MODE INDEXED SEARCH (TARGET_ACCURACY = 95) IVF (LISTS = 1, PROBES = 1, REFINE_FACTOR = {ROWS}.00)) HISTORY ALL"),&empty()).unwrap();
    let tx = db.begin_or_panic();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO items VALUES ($id,$v)",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("v".into(), Value::Vector(vector(id))),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    loop {
        let r = db.run_maintenance_cycle().unwrap();
        assert!(
            r.vector.first_failure.is_none(),
            "maintenance refuses: {:?}",
            r.vector
        );
        if r.vector.remaining_partitions == 0 && r.vector.ready_partitions > 0 {
            break;
        }
    }
}
fn search(db: &Database) -> contextdb_core::Result<contextdb_engine::QueryResult> {
    db.execute(
        &format!("SELECT id FROM items ORDER BY v <=> $q USE VECTOR INDEXED LIMIT {ROWS}"),
        &HashMap::from([("q".into(), Value::Vector(vector(ROWS + 3)))]),
    )
}
fn ring_capacity() -> usize {
    ROWS.checked_mul(DIMENSIONS)
        .unwrap()
        .checked_mul(size_of::<f32>())
        .unwrap()
}
#[test]
fn owned_page_rejection_and_shared_page_retirement_preserve_physical_custody() {
    let root = tempfile::tempdir().unwrap();
    let observer = Arc::new(Observer::default());
    let ((), capture) = observe_native_pages_for_test(observer.clone(), ring_capacity(), || {
        let db = open(&root.path().join("native-custody.db"));
        seed(&db);
        let cache = db.native_ivf_cache_observation_for_test().unwrap();
        cache.evict_unpinned_clean_pages_for_test();
        observer.keep.store(true, Ordering::SeqCst);
        assert_eq!(search(&db).unwrap().rows().len(), ROWS);
        let aliases = observer.aliases.lock().unwrap();
        assert_eq!(
            aliases.len(),
            1,
            "hold an actual producer-returned native page"
        );
        let alias = &aliases[0];
        let pointer = alias.native_backing().allocation;
        let digest = blake3::hash(alias.bytes());
        let payment = alias.native_payment();
        assert!(payment.source_receipt.is_some() || payment.fixed_cache_receipt.is_some());
        drop(aliases);
        cache.evict_unpinned_clean_pages_for_test();
        let aliases = observer.aliases.lock().unwrap();
        assert_eq!(aliases[0].native_backing().allocation, pointer);
        assert_eq!(
            blake3::hash(aliases[0].bytes()),
            digest,
            "a pinned page remains readable after cache pressure"
        );
        drop(aliases);
        // Page-specific unnamed=0 is checked at every real allocation,
        // payer transition and physical-free boundary by the ledger.
        // A whole-store census is separate from this page-custody witness.
        drop(cache);
        db.close().unwrap();
        drop(db);
    });
    capture.drain();
    assert!(!capture.completeness().overflow);
    assert!(
        capture.completeness().live_backings > 0,
        "the returned page alias genuinely retains native backing after database close"
    );
    let aliases = std::mem::take(&mut *observer.aliases.lock().unwrap());
    std::thread::spawn(move || drop(aliases)).join().unwrap();
    capture.drain();
    assert!(
        capture.close().complete(),
        "all late original-owner page/free/payment events are delivered"
    );
    observer.ledger.lock().unwrap().assert_all_released();
}

use contextdb_engine::vector_ivf_observations::{
    NativeIvfFaultBoundary, NativeIvfReadSetupPhase as SetupPhase, NativeIvfSealEvent,
    NativeIvfSealObserver, NativeIvfWorkPoint, with_native_ivf_seal_observer_for_test,
    with_native_ivf_work_observer_for_test,
};

#[derive(Clone, Copy, Debug)]
enum Api {
    Sql,
    Rust,
}
#[derive(Clone, Copy, Debug)]
enum Mode {
    Indexed,
    Auto,
    Exact,
}
impl Mode {
    fn sql(self) -> &'static str {
        match self {
            Self::Indexed => "INDEXED",
            Self::Auto => "AUTO",
            Self::Exact => "EXACT",
        }
    }
    fn rust(self) -> contextdb_core::VectorSearchMode {
        match self {
            Self::Indexed => contextdb_core::VectorSearchMode::Indexed,
            Self::Auto => contextdb_core::VectorSearchMode::Auto,
            Self::Exact => contextdb_core::VectorSearchMode::Exact,
        }
    }
}
#[derive(Clone, Copy, Debug)]
struct Route(Api, Mode);
const ROUTES: [Route; 6] = [
    Route(Api::Sql, Mode::Indexed),
    Route(Api::Sql, Mode::Auto),
    Route(Api::Sql, Mode::Exact),
    Route(Api::Rust, Mode::Indexed),
    Route(Api::Rust, Mode::Auto),
    Route(Api::Rust, Mode::Exact),
];
#[derive(Clone, Copy, Debug)]
enum Fault {
    OneShotIo,
    PersistentDamage,
    PersistentUnexpectedEof,
}
impl Fault {
    fn tag(self) -> u8 {
        match self {
            Self::OneShotIo => 1,
            Self::PersistentDamage => 2,
            Self::PersistentUnexpectedEof => 3,
        }
    }
}
fn phase_tag(phase: SetupPhase) -> u8 {
    match phase {
        SetupPhase::Authority => 1,
        SetupPhase::TupleScope => 2,
    }
}

struct Setup {
    observer: Arc<Observer>,
    target: SetupPhase,
    fault: Option<Fault>,
    entered: AtomicUsize,
    snapshot: Mutex<Option<redb::StorageSnapshotIdentity>>,
}
impl Setup {
    fn new(observer: Arc<Observer>, target: SetupPhase, fault: Option<Fault>) -> Self {
        Self {
            observer,
            target,
            fault,
            entered: AtomicUsize::new(0),
            snapshot: Mutex::new(None),
        }
    }
}
impl NativeIvfSealObserver for Setup {
    fn observe(&self, event: NativeIvfSealEvent<'_>) {
        match event {
            NativeIvfSealEvent::ReadSetupEntered { phase, snapshot } => {
                self.observer
                    .phase_status
                    .store(phase_tag(phase), Ordering::SeqCst);
                if phase == self.target && self.entered.fetch_add(1, Ordering::SeqCst) == 0 {
                    *self.snapshot.lock().unwrap() = Some(snapshot);
                    if let Some(fault) = self.fault {
                        self.observer.fault.store(fault.tag(), Ordering::SeqCst);
                    }
                }
            }
            NativeIvfSealEvent::ScopeOpened { .. } => {
                self.observer.phase_status.store(3, Ordering::SeqCst);
            }
            _ => {}
        }
    }
    fn before_boundary(&self, _: NativeIvfFaultBoundary<'_>) -> contextdb_core::Result<()> {
        Ok(())
    }
}
fn public_rows(db: &Database, route: Route) -> contextdb_core::Result<Vec<Vec<Value>>> {
    match route.0 {
        Api::Sql => Ok(db
            .execute(
                &format!(
                    "SELECT id FROM items ORDER BY v <=> $q USE VECTOR {} LIMIT {ROWS}",
                    route.1.sql()
                ),
                &HashMap::from([("q".into(), Value::Vector(vector(ROWS + 3)))]),
            )?
            .rows()
            .to_vec()),
        Api::Rust => {
            let mut query =
                contextdb_engine::SemanticQuery::new("items", "v", vector(ROWS + 3), ROWS);
            query.search_mode = Some(route.1.rust());
            Ok(db
                .semantic_search(query)?
                .into_iter()
                .map(|row| {
                    let Value::Int64(id) = row
                        .values
                        .get("id")
                        .expect("public Rust answer includes identity")
                    else {
                        panic!("public identity is an integer")
                    };
                    vec![Value::Int64(*id)]
                })
                .collect())
        }
    }
}

fn cold_case(route: Route, phase: SetupPhase, fault: Fault) -> (bool, Vec<String>) {
    use contextdb_core::Error;
    let label = format!("route={route:?} phase={phase:?} fault={fault:?}");
    let root = tempfile::tempdir().unwrap();
    let observer = Arc::new(Observer::default());
    let ((), capture) = observe_native_pages_for_test(observer.clone(), ring_capacity(), || {});
    let healthy = Arc::new(Setup::new(observer.clone(), phase, None));
    let (db, cache, expected) = capture.run(|| {
        let db = open(&root.path().join("native-cold.db"));
        seed(&db);
        let expected =
            with_native_ivf_seal_observer_for_test(healthy.clone(), || public_rows(&db, route))
                .unwrap();
        assert_eq!(expected.len(), ROWS);
        let mut identities = expected
            .iter()
            .map(|row| {
                let [Value::Int64(id)] = row.as_slice() else {
                    panic!("complete public identity")
                };
                *id
            })
            .collect::<Vec<_>>();
        identities.sort_unstable();
        assert_eq!(identities, (0..ROWS as i64).collect::<Vec<_>>());
        let cache = db.native_ivf_cache_observation_for_test().unwrap();
        cache.evict_unpinned_clean_pages_for_test(); // Cold preparation, never mandatory-reclaim proof.
        (db, cache, expected)
    });
    capture.drain();
    if healthy.entered.load(Ordering::SeqCst) == 0 {
        println!("NATIVE_PRIVATE_PHASE_ABSENT {label}");
        capture.run(|| {
            drop(cache);
            db.close().unwrap();
            drop(db);
        });
        capture.drain();
        assert!(capture.close().complete());
        observer.ledger.lock().unwrap().assert_all_released();
        return (false, Vec::new());
    }

    let builds = Arc::new(AtomicUsize::new(0));
    let mut failures = Vec::new();
    let attempts = match fault {
        Fault::OneShotIo => 1,
        Fault::PersistentDamage | Fault::PersistentUnexpectedEof => 2,
    };
    for attempt in 0..attempts {
        cache.evict_unpinned_clean_pages_for_test();
        observer.fault.store(0, Ordering::SeqCst);
        observer.fault_stage.store(0, Ordering::SeqCst);
        let armed = Arc::new(Setup::new(observer.clone(), phase, Some(fault)));
        let observed_builds = builds.clone();
        let before_failed = observer.failed.load(Ordering::SeqCst);
        let result = capture.run(|| {
            with_native_ivf_work_observer_for_test(
                Arc::new(move |point| {
                    if point == NativeIvfWorkPoint::BuildBatch {
                        observed_builds.fetch_add(1, Ordering::SeqCst);
                    }
                }),
                || {
                    with_native_ivf_seal_observer_for_test(armed.clone(), || {
                        public_rows(&db, route)
                    })
                },
            )
        });
        capture.drain(); // Outside every product lock, while this same source is still open.
        let failed = observer.failed.load(Ordering::SeqCst) - before_failed;
        if failed == 0 {
            failures.push(format!(
                "{label} attempt={attempt}: actual cold-error branch never reached"
            ));
        } else {
            observer
                .ledger
                .lock()
                .unwrap()
                .assert_failed_reads_released();
        }
        if armed.entered.load(Ordering::SeqCst) != 1 {
            failures.push(format!("{label} attempt={attempt}: terminal setup fault entered the selected constructor {} times", armed.entered.load(Ordering::SeqCst)));
        }
        if observer.fault_stage.load(Ordering::SeqCst) != phase_tag(phase) {
            failures.push(format!("{label} attempt={attempt}: fault did not occur inside the selected actual setup constructor (stage={})", observer.fault_stage.load(Ordering::SeqCst)));
        }
        match fault {
            Fault::OneShotIo => {
                if result.is_ok() {
                    failures.push(format!(
                        "{label}: one failed cold read must fail that query, actual={result:?}"
                    ));
                }
            }
            Fault::PersistentDamage | Fault::PersistentUnexpectedEof => {
                if !matches!(result, Err(Error::StoreCorrupted { .. })) {
                    failures.push(format!("{label}: persistent damage must return typed StoreCorrupted on every query, actual={result:?}"));
                }
            }
        }
        println!(
            "COLD_QUERY {label} attempt={attempt} actual={result:?} snapshot={:?}",
            *armed.snapshot.lock().unwrap()
        );
        drop(result);
        observer.fault.store(0, Ordering::SeqCst);
    }
    if matches!(fault, Fault::OneShotIo) {
        let observed_builds = builds.clone();
        let retried = capture.run(|| {
            with_native_ivf_work_observer_for_test(
                Arc::new(move |point| {
                    if point == NativeIvfWorkPoint::BuildBatch {
                        observed_builds.fetch_add(1, Ordering::SeqCst);
                    }
                }),
                || public_rows(&db, route),
            )
        });
        match retried {
            Ok(retried) => if retried != expected { failures.push(format!("{label}: retry changed the original complete answer")); },
            Err(error) => failures.push(format!("{label}: original retry-success assertion: next SAME-source query refused: {error:?}")),
        }
        capture.drain();
    }
    if builds.load(Ordering::SeqCst) != 0 {
        failures.push(format!(
            "{label}: cold fault entered hidden native build work"
        ));
    }
    {
        let ledger = observer.ledger.lock().unwrap();
        assert_eq!(
            ledger.failed_read_count(),
            observer.failed.load(Ordering::SeqCst)
        );
    }
    capture.run(|| {
        drop(cache);
        if let Err(error) = db.close() {
            failures.push(format!("{label}: close after read-only fault: {error:?}"));
        }
        drop(db);
    });
    capture.drain();
    assert!(
        capture.close().complete(),
        "every faulted native backing and genuine payer settles"
    );
    observer.ledger.lock().unwrap().assert_all_released();
    (true, failures)
}

#[test]
fn a_failed_page_read_leaves_nothing_resident() {
    let mut failures = Vec::new();
    for route in ROUTES {
        let mut exercised = false;
        for phase in [SetupPhase::Authority, SetupPhase::TupleScope] {
            let (covered, problems) = cold_case(route, phase, Fault::OneShotIo);
            exercised |= covered;
            failures.extend(problems);
        }
        assert!(
            exercised,
            "the real public route must exercise a native cold-read setup"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn persistent_native_cold_damage_is_typed_on_every_public_query_without_rebuild() {
    let mut failures = Vec::new();
    for route in ROUTES {
        let mut exercised = false;
        for phase in [SetupPhase::Authority, SetupPhase::TupleScope] {
            let (covered, problems) = cold_case(route, phase, Fault::PersistentDamage);
            exercised |= covered;
            failures.extend(problems);
        }
        assert!(
            exercised,
            "the real public route must exercise a native cold-read setup"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn persistent_native_unexpected_eof_is_typed_on_every_public_query_without_rebuild() {
    let mut failures = Vec::new();
    for route in ROUTES {
        let mut exercised = false;
        for phase in [SetupPhase::Authority, SetupPhase::TupleScope] {
            let (covered, problems) = cold_case(route, phase, Fault::PersistentUnexpectedEof);
            exercised |= covered;
            failures.extend(problems);
        }
        assert!(
            exercised,
            "the real public route must exercise a native cold-read setup"
        );
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
