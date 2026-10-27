//! Small engine-only cost ratchets for persisted search and admitted import batches.
//! These count legacy engine doors; clustered-list distance work belongs to its
//! index observer. Re-measure these same production operations when that route changes.
use crate::primitive_work_support::{SearchWork, Work, observe_work};
use crate::resident_history_support::{empty, exec, insert_vector, open, query_vector};
use contextdb_core::{Value, Wallclock};
use contextdb_engine::writer_query_test_seams::observe as observe_writer;
use contextdb_engine::{
    Database,
    vector_observations::{VectorJournalReplayWork, observe_journal_replay},
};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Default)]
struct Builds(AtomicU64);
impl contextdb_vector::hnsw::HnswBuildObserver for Builds {
    fn before_hnsw_build(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
const DIM: usize = 8;
const BATCH: usize = 4;

// Fixed producer counts measured on these fixtures; improvements lower these bounds.
// Byte fields exclude the declared cache capacity and its native index;
// they retain the measured fixed controls and operation workspace allowances.
const SAVED_SEARCH_CEILING: Work = Work {
    search: SearchWork {
        decoded_rows: 32,
        decoded_vectors: 32,
        membership_records: 194,
        page_requests: 2_966,
        reader_admissions: 1,
        vector_body_accesses: 32,
        vectors_scored: 32,
        writer_budget_charges: 7,
    },
    // Backend hit/miss counts vary with cache order; page requests stay enforced.
    backend_bytes: None,
    backend_reads: None,
    reader_peak: Some(880),
    stored_vector_decodes: 32,
    fresh_dirty_pages: 0,
    fresh_dirty_page_bytes: 0,
    native_pages: None,
    native_page_bytes: 0,
    other_native_pages: 0,
    other_native_page_bytes: 0,
    writer_after: 8_480 + 5_254,
    writer_before: 8_480,
    writer_peak: 8_480 + 10_498,
};

const APPEND_CEILING: Work = Work {
    search: SearchWork {
        decoded_rows: 0,
        decoded_vectors: 0,
        membership_records: 0,
        page_requests: 2_203,
        reader_admissions: 1,
        vector_body_accesses: 0,
        vectors_scored: 0,
        writer_budget_charges: 1_099,
    },
    backend_bytes: Some(294_912),
    backend_reads: Some(71),
    reader_peak: Some(880),
    stored_vector_decodes: 0,
    fresh_dirty_pages: 0,
    fresh_dirty_page_bytes: 0,
    native_pages: None,
    native_page_bytes: 0,
    other_native_pages: 0,
    other_native_page_bytes: 0,
    writer_after: 8_480 + 10_508,
    writer_before: 8_480,
    writer_peak: 8_480 + 108_085,
};

const IMPORT_CEILING: Work = Work {
    search: SearchWork {
        decoded_rows: 0,
        decoded_vectors: 0,
        membership_records: 0,
        page_requests: 4_144,
        reader_admissions: 0,
        vector_body_accesses: 0,
        vectors_scored: 0,
        writer_budget_charges: 915,
    },
    backend_bytes: Some(69_632),
    backend_reads: Some(17),
    reader_peak: None,
    stored_vector_decodes: 4,
    fresh_dirty_pages: 0,
    fresh_dirty_page_bytes: 0,
    native_pages: None,
    native_page_bytes: 0,
    other_native_pages: 0,
    other_native_page_bytes: 0,
    writer_after: 8_480 + 10_508,
    writer_before: 8_480 + 10_508,
    writer_peak: 8_480 + 10_508 + 225_412,
};

fn create(db: &Database) {
    exec(
        db,
        "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(8) PARTITION_KEY (scope) MAX_PARTITIONS 2 SEARCH_MODE INDEXED AUTO_INDEX_AT 1 HNSW (M = 8, EF_CONSTRUCTION = 32, EF_SEARCH = 512) CONSOLIDATION NONE) HISTORY ALL",
    );
}
fn maintain(db: &Database) {
    for _ in 0..128 {
        let report = db.run_maintenance_cycle().unwrap().vector;
        assert!(report.first_failure.is_none(), "{report:?}");
        if report.ready_partitions == 1 && report.remaining_partitions == 0 {
            return;
        }
    }
    panic!("ordinary maintenance must publish a saved generation");
}
fn insert_batch(db: &Database, start: usize) {
    let mut params = HashMap::new();
    let values = (0..BATCH)
        .map(|offset| {
            let id = start + offset;
            params.insert(format!("id{offset}"), Value::Int64(id as i64));
            params.insert(
                format!("v{offset}"),
                Value::Vector(crate::resident_history_support::vector(id, DIM)),
            );
            format!("($id{offset}, 0, $v{offset})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(&format!("INSERT INTO items VALUES {values}"), &params)
        .unwrap();
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn first_saved_search_stays_inside_its_primitive_ceiling() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("saved.db");
    let db = open(&path, Some(64 * 1024 * 1024));
    create(&db);
    for id in 0..32 {
        insert_vector(&db, "items", id, Some(0), DIM);
    }
    let builds = Arc::new(Builds::default());
    contextdb_vector::hnsw::with_hnsw_build_observer(builds.clone(), || maintain(&db));
    assert!(
        builds.0.load(Ordering::SeqCst) > 0,
        "maintenance constructs the saved index"
    );
    db.close().unwrap();
    drop(db);
    let db = open(&path, Some(64 * 1024 * 1024));
    let before = db
        .vector_store_for_test()
        .passive_activity_counters_for_test();
    let sql = "SELECT id FROM items ORDER BY v <=> $v USE VECTOR INDEXED LIMIT 32";
    let params = HashMap::from([(
        "v".into(),
        Value::Vector(crate::resident_history_support::vector(0, DIM)),
    )]);
    let query_builds = Arc::new(Builds::default());
    let ((result, query), work) = observe_work(&db, || {
        observe_writer(|| {
            contextdb_vector::hnsw::with_hnsw_build_observer(query_builds.clone(), || {
                db.execute(sql, &params)
            })
        })
    });
    assert_eq!(query_builds.0.load(Ordering::SeqCst), 0);
    let mut ids = result
        .unwrap()
        .rows()
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => id,
            _ => panic!("integer id"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ids.first(),
        Some(&0),
        "the identical saved vector ranks first"
    );
    ids.sort_unstable();
    assert_eq!(ids, (0..32).collect::<Vec<_>>());
    let after = db
        .vector_store_for_test()
        .passive_activity_counters_for_test();
    assert_eq!(before.hnsw_builds, after.hnsw_builds);
    assert_eq!(
        before.hnsw_repairs_or_compactions,
        after.hnsw_repairs_or_compactions
    );
    assert!(
        query.saved_graph_loads > 0 && query.saved_graph_bytes > 0,
        "{query:?}"
    );
    eprintln!(
        "PRIMITIVE {}",
        serde_json::json!({"fixture":"saved-first-32x8", "work":work, "saved_graph_loads":query.saved_graph_loads, "saved_graph_bytes":query.saved_graph_bytes})
    );
    work.assert_within(
        "first saved search",
        &SAVED_SEARCH_CEILING.with_declared_writer_cache(&db),
    );
    assert_eq!(query.saved_graph_loads, 1);
    assert!(
        query.saved_graph_bytes <= 239,
        "saved generation load bytes grew: {query:?}"
    );
    db.close().unwrap();
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn an_admitted_batch_stays_inside_its_primitive_ceiling_on_doubled_history() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut completed_writer = None;
    for history in [32, 64] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("append.db");
        let db = open(&path, Some(64 * 1024 * 1024));
        create(&db);
        for id in 0..16 {
            insert_vector(&db, "items", id, Some(0), DIM);
        }
        maintain(&db);
        for id in 16..history {
            insert_vector(&db, "items", id, Some(0), DIM);
        }
        assert!(db.persist_import_vector_tails().unwrap());
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        let (_, append) = observe_work(&db, || insert_batch(&db, 1000));
        let records = Arc::new(AtomicU64::new(0));
        let vectors = Arc::new(AtomicU64::new(0));
        let r = records.clone();
        let v = vectors.clone();
        let replay = observe_journal_replay(Arc::new(move |event| match event.work {
            VectorJournalReplayWork::Record { .. } => {
                r.fetch_add(1, Ordering::SeqCst);
            }
            VectorJournalReplayWork::Vector { .. } => {
                v.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }));
        let (absorbed, import) = observe_work(&db, || db.persist_import_vector_tails());
        drop(replay);
        assert!(absorbed.unwrap());
        assert_eq!(
            vectors.load(Ordering::SeqCst),
            BATCH as u64,
            "only newly admitted vectors are decoded for replay"
        );
        assert!(
            records.load(Ordering::SeqCst) <= BATCH as u64,
            "journal record loads must stay inside the admitted batch"
        );
        eprintln!(
            "PRIMITIVE {}",
            serde_json::json!({"fixture":format!("append-{history}-plus-{BATCH}x8"),"append":append,"import":import,"journal_records":records.load(Ordering::SeqCst),"journal_vectors":vectors.load(Ordering::SeqCst)})
        );
        let (append_pages, import_pages, append_read_pages, import_read_pages) = match history {
            32 => (71, 188, 44, 10),
            64 => (92, 382, 55, 16),
            _ => unreachable!("the fixture has two measured physical-work rows"),
        };
        append.assert_within(
            "four-row append",
            &APPEND_CEILING.with_native_page_invoices(
                &db,
                &append,
                append_pages,
                append_read_pages,
            ),
        );
        import.assert_within(
            "four-vector import",
            &IMPORT_CEILING.with_native_page_invoices(
                &db,
                &import,
                import_pages,
                import_read_pages,
            ),
        );
        assert_eq!(
            import.stored_vector_decodes, BATCH,
            "each newly admitted vector is decoded exactly once"
        );
        if let Some(retained) = completed_writer {
            assert_eq!(
                import.writer_after, retained,
                "doubling completed history retains no additional charged writer memory"
            );
        }
        completed_writer = Some(import.writer_after);
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        assert_eq!(
            exec(&db, "SELECT COUNT(*) FROM items"),
            vec![vec![Value::Int64((history + BATCH) as i64)]]
        );
        for id in 1000..1000 + BATCH {
            assert_eq!(
                query_vector(&db, "items", id, DIM, "EXACT"),
                vec![vec![Value::Int64(id as i64)]]
            );
        }
        assert_eq!(
            db.execute("SELECT id FROM items WHERE id = 9999", &empty())
                .unwrap()
                .rows()
                .len(),
            0
        );
        db.close().unwrap();
    }
}
