//! Graph metadata authorization pays the reader's declared limits at each grant.
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailureDetail, ReadFailureLimit,
    ReadLimits,
};
use contextdb_core::{Principal, Value, Wallclock};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::memory_accounting::MemoryAccountant;
use contextdb_engine::plugin::CorePlugin;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;
const READER: &str = "metadata-reader";
const SQL: &str = "SELECT id FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source AND b.mark = 'wanted' COLUMNS(b.id AS id))";
struct Clock;
impl DeadlineClock for Clock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}
fn limits() -> ReadLimits {
    ReadLimits {
        result_rows: 64,
        result_bytes: 1 << 20,
        work: 1_000_000,
        active_ms: 1_000_000,
        memory: 8 << 20,
        cursor_page_rows: 1,
        cursor_page_bytes: 1 << 20,
        cursor_idle_ms: 1_000_000,
        cursor_lifetime_ms: 1_000_000,
    }
}
fn open(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            memory_limit: Some(64 << 20),
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}
fn grant(db: &Database, id: u128, principal: &str, acl: u128, width: usize) {
    db.execute(
        "INSERT INTO acl_grants VALUES ($id, 'Human', $principal, $acl, $ignored)",
        &HashMap::from([
            ("id".into(), Value::Uuid(Uuid::from_u128(id))),
            ("principal".into(), Value::Text(principal.into())),
            ("acl".into(), Value::Uuid(Uuid::from_u128(acl))),
            ("ignored".into(), Value::Text("x".repeat(width))),
        ]),
    )
    .unwrap();
}
fn seed(path: &Path, unrelated: u128) {
    let db = open(path);
    db.execute("CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID, ignored TEXT) SYNC OFF", &HashMap::new()).unwrap();
    db.execute("CREATE TABLE nodes (id UUID PRIMARY KEY, mark TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id), unused TEXT) SYNC OFF", &HashMap::new()).unwrap();
    db.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF", &HashMap::new()).unwrap();
    db.execute("BEGIN", &HashMap::new()).unwrap();
    for id in 0..unrelated {
        grant(&db, 100 + id, "other-reader", 10_000 + id, 32);
    }
    // A wide ignored field puts grant scratch above graph preparation; only
    // this one entitlement belongs to the reader at either population size.
    grant(&db, 99_999, READER, 7, 32_768);
    // The traversal anchor keeps an independent entitlement while target
    // grants are withdrawn and staged inside the reader transaction.
    grant(&db, 99_998, READER, 9, 32);
    for id in 1..=4 {
        db.execute(
            "INSERT INTO nodes VALUES ($id,'wanted',$acl,'fixed')",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(id))),
                (
                    "acl".into(),
                    Value::Uuid(Uuid::from_u128(if id == 1 {
                        9
                    } else if id == 4 {
                        8
                    } else {
                        7
                    })),
                ),
            ]),
        )
        .unwrap();
    }
    db.execute("COMMIT", &HashMap::new()).unwrap();
    // Scoped graph reads require actual relational edge metadata. A bare
    // adjacency entry is intentionally invisible to a non-admin principal.
    for id in 2..=4 {
        db.execute(
            "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS')",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(100_000 + id))),
                ("source".into(), Value::Uuid(Uuid::from_u128(1))),
                ("target".into(), Value::Uuid(Uuid::from_u128(id))),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
}
#[derive(Default)]
struct Samples {
    last: Option<bounded::TestWorkSource>,
    first_access_work: Option<u64>,
    wide: Option<(u64, u64)>,
    access_attempts: usize,
}
struct Probe {
    samples: Mutex<Samples>,
    touches: AtomicUsize,
    cancelled: AtomicUsize,
    token: Option<OwnerReadCancellation>,
}
impl Probe {
    fn new(token: Option<OwnerReadCancellation>) -> Self {
        Self {
            samples: Mutex::new(Samples::default()),
            touches: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            token,
        }
    }
}
impl bounded::ExecutionProbe for Probe {
    fn before_work(&self, source: bounded::TestWorkSource, work: u64) {
        let mut sample = self.samples.lock().unwrap();
        sample.last = Some(source);
        if source == bounded::TestWorkSource::AccessControl {
            sample.access_attempts += 1;
            sample.first_access_work.get_or_insert(work);
        }
    }
    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _: u64) {
        if touch == bounded::TestSourceTouch::AccessRow
            && self.samples.lock().unwrap().last == Some(bounded::TestWorkSource::AccessControl)
        {
            let prior = self.touches.fetch_add(1, Ordering::SeqCst);
            if prior == 0
                && let Some(token) = &self.token
            {
                token.cancel();
            }
        }
    }
    fn before_temporary_reservation(
        &self,
        source: bounded::TestWorkSource,
        requested: u64,
        held: u64,
    ) {
        if source == bounded::TestWorkSource::AccessControl && requested > 8192 {
            self.samples
                .lock()
                .unwrap()
                .wide
                .get_or_insert((requested, held));
        }
    }
    fn cancellation_observed(&self, _: u64) {
        self.cancelled.fetch_add(1, Ordering::SeqCst);
    }
}
fn request(probe: Arc<Probe>) -> bounded::BoundedReadRequest {
    let mut request = bounded::BoundedReadRequest::new(
        SQL,
        HashMap::from([("source".into(), Value::Uuid(Uuid::from_u128(1)))]),
        limits(),
        Arc::new(Clock),
    );
    request.probe = Some(probe);
    request
}
fn run(
    reader: Arc<Database>,
    request: &bounded::BoundedReadRequest,
    cursor: bool,
) -> Result<(Vec<Vec<Value>>, bounded::TestTelemetry), bounded::TestError> {
    if !cursor {
        return bounded::execute(&reader, request)
            .map(|result| (result.result.rows, result.telemetry));
    }
    let mut opened = bounded::open_cursor(reader, request)?;
    let mut rows = opened.first_page.rows;
    let mut more = opened.first_page.has_more;
    let mut telemetry = opened.telemetry;
    while more {
        let fetched = opened.cursor.fetch(None, OwnerReadCancellation::new())?;
        more = fetched.page.has_more;
        rows.extend(fetched.page.rows);
        // Each fetch reports that request's own work. Preserve the opening
        // grant lookup when a final empty page performs no authorization work.
        let page = fetched.telemetry;
        telemetry.work_units += page.work_units;
        telemetry.encoded_bytes += page.encoded_bytes;
        telemetry.peak_temporary_bytes = telemetry
            .peak_temporary_bytes
            .max(page.peak_temporary_bytes);
        telemetry.cancellation_observed |= page.cancellation_observed;
        for (source, units) in page.source_work {
            *telemetry.source_work.entry(source).or_default() += units;
        }
        for (source, bytes) in page.source_peak_temporary_bytes {
            let peak = telemetry
                .source_peak_temporary_bytes
                .entry(source)
                .or_default();
            *peak = (*peak).max(bytes);
        }
    }
    opened.cursor.close().unwrap();
    Ok((rows, telemetry))
}
fn expected() -> Vec<Vec<Value>> {
    vec![
        vec![Value::Uuid(Uuid::from_u128(2))],
        vec![Value::Uuid(Uuid::from_u128(3))],
    ]
}
fn refusal(error: bounded::TestError, limit: ReadFailureLimit, probe: &Probe) {
    let bounded::TestError::Refused(failure) = error else {
        panic!("typed request refusal required: {error:?}");
    };
    let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
        panic!("typed limit detail required: {failure:?}");
    };
    assert_eq!(detail.limit, limit);
    assert!(
        probe.touches.load(Ordering::SeqCst) > 0,
        "refusal must occur after real grant work"
    );
    assert_eq!(
        probe.samples.lock().unwrap().last,
        Some(bounded::TestWorkSource::AccessControl)
    );
}
#[test]
fn bounded_graph_where_metadata_authorization_charges_request_limits() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut population_peaks = Vec::new();
    for unrelated in [16_u128, 1024] {
        let directory = tempfile::tempdir().unwrap();
        let seed_path = directory.path().join("seed.redb");
        seed(&seed_path, unrelated);
        let mut peaks = Vec::new();
        for cursor in [false, true] {
            let mut boundaries: Option<(u64, u64)> = None;
            for arm in 0..4 {
                let path = directory.path().join(format!("read-{cursor}-{arm}.redb"));
                std::fs::copy(&seed_path, &path).unwrap();
                // Keep the caller-owned accountant alive after the database drops.
                let accountant = Arc::new(MemoryAccountant::with_budget(64 << 20));
                let db =
                    Database::open_with_config(&path, Arc::new(CorePlugin), accountant.clone())
                        .unwrap();
                db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
                let reader = Arc::new(db.scoped_with_constraints(
                    None,
                    None,
                    Some(Principal::Human(READER.into())),
                ));
                let token = OwnerReadCancellation::new();
                let probe = Arc::new(Probe::new((arm == 3).then(|| token.clone())));
                let mut request = request(probe.clone());
                request.cancellation = token;
                if arm == 1 {
                    request.limits.work = boundaries.unwrap().0;
                }
                if arm == 2 {
                    request.limits.memory = boundaries.unwrap().1;
                }
                let result = run(reader.clone(), &request, cursor);
                match arm {
                    0 => {
                        let (rows, telemetry) = result.unwrap();
                        assert_eq!(rows, expected());
                        assert!(
                            telemetry
                                .source_work
                                .get(&bounded::TestWorkSource::AccessControl)
                                .copied()
                                .unwrap_or(0)
                                > 0
                        );
                        assert!(probe.touches.load(Ordering::SeqCst) > 0);
                        let samples = probe.samples.lock().unwrap();
                        let (requested, held) =
                            samples.wide.expect("real wide grant scratch boundary");
                        boundaries =
                            Some((samples.first_access_work.unwrap() + 1, held + requested - 1));
                        peaks.push(telemetry.peak_temporary_bytes);
                    }
                    1 => refusal(result.unwrap_err(), ReadFailureLimit::Work, &probe),
                    2 => refusal(result.unwrap_err(), ReadFailureLimit::Memory, &probe),
                    3 => {
                        assert!(matches!(result, Err(bounded::TestError::Cancelled)));
                        assert_eq!(probe.touches.load(Ordering::SeqCst), 1);
                        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 1);
                    }
                    _ => unreachable!(),
                }
                drop(request);
                drop(reader);
                db.close().unwrap();
                drop(db);
                assert_eq!(
                    accountant.usage().used,
                    0,
                    "terminal reader and cache release every charged owner"
                );
            }
        }
        // The ordinary executor preserves exactly the same policy. A grant
        // staged by this reader affects only its own transaction and rollback
        // restores both admission and the independent withdrawal control.
        let path = directory.path().join("staged.redb");
        std::fs::copy(&seed_path, &path).unwrap();
        let db = open(&path);
        let reader =
            Arc::new(db.scoped_with_constraints(None, None, Some(Principal::Human(READER.into()))));
        let separate =
            db.scoped_with_constraints(None, None, Some(Principal::Human(READER.into())));
        assert_eq!(
            reader
                .execute(SQL, &request(Arc::new(Probe::new(None))).params)
                .unwrap()
                .rows,
            expected()
        );
        reader.execute("BEGIN", &HashMap::new()).unwrap();
        grant(&reader, 999_999, READER, 8, 32_768);
        let staged_expected = vec![vec![Value::Uuid(Uuid::from_u128(4))]];
        // Stage wide metadata on the source while the committed entitlement
        // permits this reader's write. Node 4 remains the committed denial
        // control; the staged grant changes read visibility, not write policy.
        reader
            .execute(
                "UPDATE nodes SET unused = $body WHERE id = $id",
                &HashMap::from([
                    ("body".into(), Value::Text("m".repeat(32768))),
                    ("id".into(), Value::Uuid(Uuid::from_u128(1))),
                ]),
            )
            .unwrap();
        reader
            .execute(
                "DELETE FROM acl_grants WHERE id = $id",
                &HashMap::from([("id".into(), Value::Uuid(Uuid::from_u128(99_999)))]),
            )
            .unwrap();
        assert_eq!(
            reader
                .execute(SQL, &request(Arc::new(Probe::new(None))).params)
                .unwrap()
                .rows,
            staged_expected
        );
        assert_eq!(
            separate
                .execute(SQL, &request(Arc::new(Probe::new(None))).params)
                .unwrap()
                .rows,
            expected()
        );
        for cursor in [false, true] {
            let probe = Arc::new(Probe::new(None));
            let (rows, telemetry) = run(reader.clone(), &request(probe.clone()), cursor).unwrap();
            assert_eq!(rows, staged_expected);
            assert!(
                telemetry
                    .source_work
                    .get(&bounded::TestWorkSource::AccessControl)
                    .copied()
                    .unwrap_or(0)
                    > 0
            );
            let samples = probe.samples.lock().unwrap();
            let work_limit = samples.first_access_work.expect("staged ACL work") + 1;
            let (extra, held) = samples.wide.expect("staged wide grant scratch");
            drop(samples);
            for (limit, work, memory) in [
                (ReadFailureLimit::Work, work_limit, limits().memory),
                (ReadFailureLimit::Memory, limits().work, held + extra - 1),
            ] {
                let probe = Arc::new(Probe::new(None));
                let mut constrained = request(probe.clone());
                constrained.limits.work = work;
                constrained.limits.memory = memory;
                refusal(
                    run(reader.clone(), &constrained, cursor).unwrap_err(),
                    limit,
                    &probe,
                );
            }
        }
        reader.execute("ROLLBACK", &HashMap::new()).unwrap();
        assert_eq!(
            reader
                .execute(SQL, &request(Arc::new(Probe::new(None))).params)
                .unwrap()
                .rows,
            expected()
        );
        drop(reader);
        drop(separate);
        db.close().unwrap();
        population_peaks.push(peaks);
    }
    assert_eq!(
        population_peaks[0], population_peaks[1],
        "grant growth changes counted work, never accumulated borrowed-row scratch"
    );
}
