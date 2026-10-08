//! Graph metadata predicates release each rejected binding's rows and obey
//! the caller's memory/work/cancellation budgets at the real metadata probes.
use contextdb_core::read_contract::{
    DeadlineClock, DeadlineWait, OwnerReadCancellation, ReadFailureDetail, ReadFailureLimit,
    ReadLimits,
};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::executor::bounded_read_test_support as bounded;
use contextdb_engine::{Database, DatabaseOpenOptions, MaintenancePolicy};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

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
        result_rows: 256,
        result_bytes: 1 << 20,
        work: 100_000,
        active_ms: 100_000,
        memory: 8 << 20,
        cursor_page_rows: 256,
        cursor_page_bytes: 1 << 20,
        cursor_idle_ms: 100_000,
        cursor_lifetime_ms: 1_000_000,
    }
}
struct CancelMetadata {
    cancellation: OwnerReadCancellation,
    metadata: AtomicUsize,
    observed: AtomicUsize,
}
impl bounded::ExecutionProbe for CancelMetadata {
    fn before_work(&self, _: bounded::TestWorkSource, _: u64) {}
    fn before_source_touch(&self, touch: bounded::TestSourceTouch, _: u64) {
        if touch == bounded::TestSourceTouch::AccessRow
            && self.metadata.fetch_add(1, Ordering::SeqCst) + 1 == 4
        {
            self.cancellation.cancel();
        }
    }
    fn cancellation_observed(&self, _: u64) {
        self.observed.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn bounded_graph_where_charge_is_independent_of_rejected_candidates() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let mut peaks = Vec::new();
    for rejected in [16_u128, 128] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("graph-predicate.redb");
        let db = Arc::new(
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    memory_limit: Some(64 << 20),
                    maintenance_policy: MaintenancePolicy::CallerDriven,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        db.execute(
            "CREATE TABLE nodes (id UUID PRIMARY KEY, mark TEXT, unused_body TEXT) SYNC OFF",
            &HashMap::new(),
        )
        .unwrap();
        db.execute("CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT) SYNC OFF", &HashMap::new()).unwrap();
        let source = Uuid::from_u128(1);
        for id in 1..=rejected + 3 {
            db.execute(
                "INSERT INTO nodes VALUES ($id,$mark,$body)",
                &HashMap::from([
                    ("id".into(), Value::Uuid(Uuid::from_u128(id))),
                    (
                        "mark".into(),
                        Value::Text(
                            if id == 2 || id == 3 {
                                "wanted"
                            } else {
                                "rejected"
                            }
                            .into(),
                        ),
                    ),
                    ("body".into(), Value::Text("x".repeat(8192))),
                ]),
            )
            .unwrap();
            if id > 1 {
                let tx = db.begin_or_panic();
                db.insert_edge(
                    tx,
                    source,
                    Uuid::from_u128(id),
                    "LINKS".into(),
                    HashMap::new(),
                )
                .unwrap();
                db.commit(tx).unwrap();
            }
        }
        let sql = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source AND b.mark = 'wanted' COLUMNS (b.id AS t))";
        let params = HashMap::from([("source".into(), Value::Uuid(source))]);
        let expected = vec![
            vec![Value::Uuid(Uuid::from_u128(2))],
            vec![Value::Uuid(Uuid::from_u128(3))],
        ];
        // Warm the fixed storage-cache owner before measuring transient charge.
        db.execute(sql, &params).unwrap();
        // A same-traversal control resolves only binding identities. Its
        // frontier has the same targets but does not load metadata predicates.
        let identity_sql = sql.replace("b.mark = 'wanted'", "b.id IN ($first, $second)");
        let mut identity_params = params.clone();
        identity_params.insert("first".into(), Value::Uuid(Uuid::from_u128(2)));
        identity_params.insert("second".into(), Value::Uuid(Uuid::from_u128(3)));
        let mut identity_peaks = Vec::new();
        let mut identity_request_peak = 0;
        for mode in 0..3 {
            let baseline = db.accountant().usage().used;
            db.accountant().reset_high_water_for_test();
            let rows = if mode == 0 {
                db.execute(&identity_sql, &identity_params).unwrap().rows
            } else {
                let mut request = bounded::BoundedReadRequest::new(
                    &identity_sql,
                    identity_params.clone(),
                    limits(),
                    Arc::new(Clock),
                );
                if mode == 1 {
                    let result = bounded::execute(&db, &request).unwrap();
                    identity_request_peak = result.telemetry.peak_temporary_bytes;
                    result.result.rows
                } else {
                    request.limits.cursor_page_rows = 1;
                    let mut opened = bounded::open_cursor(db.clone(), &request).unwrap();
                    let mut rows = opened.first_page.rows;
                    let mut more = opened.first_page.has_more;
                    while more {
                        let page = opened
                            .cursor
                            .fetch(None, OwnerReadCancellation::new())
                            .unwrap()
                            .page;
                        more = page.has_more;
                        rows.extend(page.rows);
                    }
                    opened.cursor.close().unwrap();
                    rows
                }
            };
            assert_eq!(rows, expected);
            identity_peaks.push(db.accountant().high_water_for_test() - baseline);
            assert_eq!(db.accountant().usage().used, baseline);
        }
        let baseline = db.accountant().usage().used;
        let mut arm = Vec::new();
        for mode in 0..3 {
            db.accountant().reset_high_water_for_test();
            let rows = if mode == 0 {
                db.execute(sql, &params).unwrap().rows
            } else {
                let mut request = bounded::BoundedReadRequest::new(
                    sql,
                    params.clone(),
                    limits(),
                    Arc::new(Clock),
                );
                if mode == 1 {
                    let result = bounded::execute(&db, &request).unwrap();
                    result.result.rows
                } else {
                    request.limits.cursor_page_rows = 1;
                    let mut opened = bounded::open_cursor(db.clone(), &request).unwrap();
                    let mut rows = opened.first_page.rows;
                    let mut more = opened.first_page.has_more;
                    while more {
                        let page = opened
                            .cursor
                            .fetch(None, OwnerReadCancellation::new())
                            .unwrap()
                            .page;
                        more = page.has_more;
                        rows.extend(page.rows);
                    }
                    opened.cursor.close().unwrap();
                    rows
                }
            };
            assert_eq!(
                rows, expected,
                "all graph operators preserve the exact two accepted bindings"
            );
            arm.push(
                db.accountant()
                    .high_water_for_test()
                    .checked_sub(baseline)
                    .unwrap(),
            );
            assert_eq!(
                db.accountant().usage().used,
                baseline,
                "predicate outputs and per-binding caches release exactly"
            );
        }
        let metadata_share = arm
            .iter()
            .zip(&identity_peaks)
            .map(|(metadata, control)| {
                metadata
                    .checked_sub(*control)
                    .expect("metadata control has no predicate payload")
            })
            .collect::<Vec<_>>();
        peaks.push(metadata_share);
        // Even the binding-only traversal must honor the caller's budget.
        // Use its measured request peak, so this refusal tests its own need
        // rather than a limit that stops before any graph preparation.
        assert!(identity_request_peak > 1);
        let mut control_request = bounded::BoundedReadRequest::new(
            &identity_sql,
            identity_params,
            limits(),
            Arc::new(Clock),
        );
        control_request.limits.memory = identity_request_peak - 1;
        let error = bounded::execute(&db, &control_request).unwrap_err();
        let bounded::TestError::Refused(failure) = error else {
            panic!("below traversal need must refuse typed: {error:?}");
        };
        let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
            panic!("traversal budget refusal must name its limit: {failure:?}");
        };
        assert_eq!(detail.limit, ReadFailureLimit::Memory);
        assert_eq!(db.accountant().usage().used, baseline);
        for (work, memory) in [(1, limits().memory), (limits().work, 1)] {
            let mut request =
                bounded::BoundedReadRequest::new(sql, params.clone(), limits(), Arc::new(Clock));
            request.limits.work = work;
            request.limits.memory = memory;
            let error = bounded::execute(&db, &request).unwrap_err();
            let bounded::TestError::Refused(failure) = error else {
                panic!("insufficient budget must return a typed refusal: {error:?}");
            };
            let ReadFailureDetail::OwnerLimitExceeded(detail) = failure.detail() else {
                panic!("budget refusal must identify the limit: {failure:?}");
            };
            assert_eq!(
                detail.limit,
                if work == 1 {
                    ReadFailureLimit::Work
                } else {
                    ReadFailureLimit::Memory
                }
            );
            assert_eq!(db.accountant().usage().used, baseline);
        }
        let cancellation = OwnerReadCancellation::new();
        let probe = Arc::new(CancelMetadata {
            cancellation: cancellation.clone(),
            metadata: AtomicUsize::new(0),
            observed: AtomicUsize::new(0),
        });
        let mut request = bounded::BoundedReadRequest::new(sql, params, limits(), Arc::new(Clock));
        request.cancellation = cancellation;
        request.probe = Some(probe.clone());
        assert!(
            matches!(
                bounded::execute(&db, &request),
                Err(bounded::TestError::Cancelled)
            ),
            "metadata pre-touch cancellation withdraws the statement"
        );
        assert_eq!(
            probe.metadata.load(Ordering::SeqCst),
            4,
            "withdrawal precedes any subsequent metadata touch"
        );
        assert_eq!(probe.observed.load(Ordering::SeqCst), 1);
        assert_eq!(
            db.accountant().usage().used,
            baseline,
            "cancelled metadata probe releases its transient owners"
        );
        db.close().unwrap();
    }
    for (small, grown) in peaks[0].iter().zip(&peaks[1]) {
        assert!(
            small.abs_diff(*grown) <= 256,
            "metadata predicate charge stays fixed across eager and both bounded operators: {peaks:?}"
        );
    }
}
