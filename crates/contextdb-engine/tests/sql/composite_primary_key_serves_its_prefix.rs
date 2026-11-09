use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn limits() -> ReadLimits {
    ReadLimits {
        work: 1_000,
        memory: 8 * 1024 * 1024,
        ..ReadLimits::default()
    }
}

fn assert_primary_key_prefix(result: &contextdb_engine::QueryResult) {
    assert_eq!(
        result.rows(),
        vec![
            vec![Value::Int64(0)],
            vec![Value::Int64(1)],
            vec![Value::Int64(2)],
        ],
        "the primary-key prefix returns this passage's three source links"
    );
    assert_eq!(
        result.trace().index_used.as_deref(),
        Some("__pk_context_id_passage_id_source_order")
    );
    assert_eq!(
        result.trace().predicates_pushed.len(),
        2,
        "both equality columns of the primary-key prefix are pushed"
    );
    assert_eq!(
        result.trace().rows_examined,
        3,
        "one passage lookup must not scan a 30,000-link context"
    );
}

#[test]
fn composite_primary_key_serves_its_prefix() {
    let db = Database::open_memory();
    db.execute(
        "CREATE TABLE passage_links (context_id INT NOT NULL, passage_id INT NOT NULL, \
         source_order INT NOT NULL, PRIMARY KEY (context_id, passage_id, source_order))",
        &empty(),
    )
    .unwrap();
    for first_passage in (0..10_000_i64).step_by(1_000) {
        let tx = db.begin().unwrap();
        for passage in first_passage..first_passage + 1_000 {
            for source_order in 0..3_i64 {
                db.execute_in_tx(
                    tx,
                    "INSERT INTO passage_links VALUES (7, $passage, $source_order)",
                    &HashMap::from([
                        ("passage".into(), Value::Int64(passage)),
                        ("source_order".into(), Value::Int64(source_order)),
                    ]),
                )
                .unwrap();
            }
        }
        db.commit(tx).unwrap();
    }
    let sql = "SELECT source_order FROM passage_links WHERE context_id = 7 AND passage_id = 4242";
    let ordinary = db.execute(sql, &empty()).unwrap();
    assert_primary_key_prefix(&ordinary);

    let bounded = bounded::execute(
        &db,
        &bounded::BoundedReadRequest::new(sql, empty(), limits(), Arc::new(FrozenClock)),
    )
    .expect("the bounded production kernel serves the same primary-key prefix");
    assert_primary_key_prefix(&bounded.result);
    assert_eq!(bounded.result.rows(), ordinary.rows());
    println!(
        "wall_search_pk ordinary={{index:{:?},pushed:{},examined:{},rows:{:?}}} bounded={{index:{:?},pushed:{},examined:{},rows:{:?}}}",
        ordinary.trace().index_used.as_deref(),
        ordinary.trace().predicates_pushed.len(),
        ordinary.trace().rows_examined,
        ordinary.rows(),
        bounded.result.trace().index_used.as_deref(),
        bounded.result.trace().predicates_pushed.len(),
        bounded.result.trace().rows_examined,
        bounded.result.rows(),
    );
}
