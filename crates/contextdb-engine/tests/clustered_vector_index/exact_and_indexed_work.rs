//! EXACT compares every allowed stored vector at full precision, so it
//! returns the full-precision order even where the int8 list records cannot
//! tell neighbours apart; INDEXED re-ranks its candidates at full precision
//! and does strictly less complete distance work than EXACT on the same
//! scope: centroid, int8, canonical and write-buffer comparisons all count.

use super::support::{
    cosine_distance, drive_until_serving, empty, int_ids, ivf, observe_search, open_caller_driven,
    params, serving, unit_vector,
};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;
use uuid::Uuid;

fn angle(radians: f64) -> Vec<f32> {
    vec![radians.cos() as f32, radians.sin() as f32]
}

/// Ranked ids, nearest first, by full-precision distance to `query`.
fn ranked(query: &[f32], rows: &[(i64, Vec<f32>)]) -> Vec<(f64, i64)> {
    let mut ranked = rows
        .iter()
        .map(|(id, vector)| (cosine_distance(query, vector), *id))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| left.partial_cmp(right).unwrap());
    ranked
}

/// The tie trap's committed rows and its bound query.
type TieTrap = (Vec<(i64, Vec<f32>)>, HashMap<String, Value>);

/// The tie trap: eight neighbours far finer than one int8 step, committed
/// under `search_mode`, with the fixture's full-precision order checked.
fn tie_trap(db: &Database, search_mode: &str) -> TieTrap {
    db.execute(
        &format!(
            "CREATE TABLE trap_items (id INTEGER PRIMARY KEY, embedding VECTOR(2) SEARCH_MODE {search_mode} {})",
            ivf(1, 1, 16)
        ),
        &empty(),
    )
    .expect("declare a one-list clustered index");

    // Eight neighbours 0.001 rad apart: far finer than one int8 step over a
    // list whose coordinates span the whole circle. Their ids interleave so
    // that neither ascending nor descending id order is the distance order.
    let tie_ids = [105_i64, 101, 108, 103, 106, 102, 107, 104];
    let mut rows = tie_ids
        .iter()
        .enumerate()
        .map(|(rank, id)| (*id, angle(0.5 + 0.001 * rank as f64)))
        .collect::<Vec<_>>();
    for (id, radians) in [(1, 2.0), (2, 3.0), (3, 4.0), (4, 5.0), (5, -0.9), (6, 1.6)] {
        rows.push((id, angle(radians)));
    }
    for (id, vector) in &rows {
        db.execute(
            "INSERT INTO trap_items (id, embedding) VALUES ($id, $embedding)",
            &params([
                ("id", Value::Int64(*id)),
                ("embedding", Value::Vector(vector.clone())),
            ]),
        )
        .expect("commit one vector");
    }
    let query = angle(0.45);
    let expected = ranked(&query, &rows);
    assert_eq!(
        expected[..3].iter().map(|(_, id)| *id).collect::<Vec<_>>(),
        vec![105, 101, 108],
        "the fixture's full-precision order"
    );
    for pair in expected[..4].windows(2) {
        assert!(
            pair[1].0 - pair[0].0 > 1e-6,
            "neighbouring distances are resolvable at full precision: {pair:?}"
        );
    }
    (rows, params([("query", Value::Vector(query))]))
}

#[test]
fn exact_returns_full_precision_order_where_int8_records_tie() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("trap.db"));
    let (_, bound) = tie_trap(&db, "AUTO");
    for mode in ["EXACT", "AUTO"] {
        let sql = format!(
            "SELECT id FROM trap_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3"
        );
        assert_eq!(
            int_ids(&db.execute(&sql, &bound).unwrap()),
            vec![105, 101, 108],
            "{mode} returns the full-precision order"
        );
    }
}

/// The native work count of EXACT and the successful INDEXED arm of the tie
/// trap; the shared answer stays active above.
#[test]
#[ignore = "index deferred to the follow-on plan"]
fn exact_counts_every_vector_and_indexed_reranks_the_tie_trap() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("trap.db"));
    let (rows, bound) = tie_trap(&db, "INDEXED");
    drive_until_serving(&db, 6, 1, 10);
    for mode in ["EXACT", "INDEXED"] {
        let sql = format!(
            "SELECT id FROM trap_items ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 3"
        );
        let (result, work) = observe_search(&db, || db.execute(&sql, &bound).unwrap());
        assert_eq!(
            int_ids(&result),
            vec![105, 101, 108],
            "{mode} returns the full-precision order"
        );
        if mode == "EXACT" {
            assert_eq!(
                work.full_precision_comparisons(),
                rows.len() as u64,
                "EXACT compares every stored vector exactly once at full precision: {work:?}"
            );
        } else {
            assert!(work.clustered_route, "INDEXED ran on the clustered index");
        }
    }
}

const DIMENSIONS: usize = 16;
const ROWS_PER_CONTEXT: i64 = 600;
const TAIL_ROWS: i64 = 5;

fn context(ordinal: u128) -> Uuid {
    Uuid::from_u128(0xE0_0000 + ordinal)
}

#[test]
#[ignore = "index deferred to the follow-on plan"]
fn exact_rescores_every_allowed_vector_and_indexed_does_strictly_less_work() {
    let root = tempfile::tempdir().expect("temporary store directory");
    let db = open_caller_driven(&root.path().join("work.db"));
    db.execute(
        &format!(
            "CREATE TABLE work_items (id INTEGER PRIMARY KEY, context_id UUID NOT NULL, embedding VECTOR({DIMENSIONS}) PARTITION_KEY (context_id) SEARCH_MODE INDEXED {})",
            ivf(16, 2, 20)
        ),
        &empty(),
    )
    .expect("declare the clustered index");
    let insert =
        |tx, id: i64, ordinal: u128| {
            db.execute_in_tx(
            tx,
            "INSERT INTO work_items (id, context_id, embedding) VALUES ($id, $context, $embedding)",
            &params([
                ("id", Value::Int64(id)),
                ("context", Value::Uuid(context(ordinal))),
                ("embedding", Value::Vector(unit_vector(id as u64, DIMENSIONS))),
            ]),
        )
        .expect("stage one row");
        };
    let tx = db.begin_or_panic();
    for ordinal in 0..2_u128 {
        for row in 0..ROWS_PER_CONTEXT {
            insert(tx, ordinal as i64 * ROWS_PER_CONTEXT + row, ordinal);
        }
    }
    db.commit(tx).expect("commit both Contexts");
    drive_until_serving(&db, 1200, 1, 10);
    for ordinal in 0..2_u128 {
        let generation = serving(
            &db,
            "work_items",
            "embedding",
            &[Value::Uuid(context(ordinal))],
        )
        .unwrap_or_else(|| panic!("Context {ordinal} has its own serving generation"));
        assert_eq!(
            generation.lists, 16,
            "each Context's generation has the declared lists"
        );
    }
    // Committed after the build: the change tail EXACT must also compare.
    let tx = db.begin_or_panic();
    for id in 5_000..5_000 + TAIL_ROWS {
        insert(tx, id, 0);
    }
    db.commit(tx).expect("commit the tail");

    let allowed = (0..ROWS_PER_CONTEXT)
        .chain(5_000..5_000 + TAIL_ROWS)
        .map(|id| (id, unit_vector(id as u64, DIMENSIONS)))
        .collect::<Vec<_>>();
    let query = unit_vector(77_777, DIMENSIONS);
    let expected = ranked(&query, &allowed)
        .into_iter()
        .take(10)
        .map(|(_, id)| id)
        .collect::<Vec<_>>();
    let bound = params([
        ("c0", Value::Uuid(context(0))),
        ("query", Value::Vector(query.clone())),
    ]);
    let search = |mode: &str| {
        let sql = format!(
            "SELECT id FROM work_items WHERE context_id = $c0 ORDER BY embedding <=> $query USE VECTOR {mode} LIMIT 10"
        );
        observe_search(&db, || db.execute(&sql, &bound).unwrap())
    };

    let (exact, exact_work) = search("EXACT");
    assert_eq!(
        int_ids(&exact),
        expected,
        "EXACT is the full-precision answer"
    );
    assert_eq!(
        exact_work.full_precision_comparisons(),
        allowed.len() as u64,
        "EXACT compares every allowed vector, built and tail, exactly once at full precision, \
         whether it sits in a list generation or the write buffer: {exact_work:?}"
    );

    let (_, indexed_work) = search("INDEXED");
    assert!(
        indexed_work.clustered_route,
        "INDEXED ran on the clustered index"
    );
    assert!(
        indexed_work.centroid_evaluations > 0,
        "INDEXED's centroid comparisons are observed and counted as distance work: {indexed_work:?}"
    );
    assert!(
        exact_work.buffer_evaluations <= TAIL_ROWS as u64,
        "each buffered vector is compared at most once: {exact_work:?}"
    );
    assert_eq!(
        indexed_work.buffer_evaluations, exact_work.buffer_evaluations,
        "both routes compare the capped buffer exactly, once per buffered vector"
    );
    assert!(
        indexed_work.distance_evaluations() < exact_work.distance_evaluations(),
        "INDEXED does strictly fewer distance evaluations than EXACT: indexed {indexed_work:?}, exact {exact_work:?}"
    );
    assert!(
        indexed_work.full_precision_comparisons() < exact_work.full_precision_comparisons(),
        "INDEXED re-ranks a bounded candidate set, not every vector: {indexed_work:?}"
    );

    // An uncommitted row is part of the allowed set inside its transaction.
    let tx = db.begin_or_panic();
    db.execute_in_tx(
        tx,
        "INSERT INTO work_items (id, context_id, embedding) VALUES (9999, $c0, $query)",
        &bound,
    )
    .expect("stage an overlay row");
    let overlay = db
        .execute_in_tx(
            tx,
            "SELECT id FROM work_items WHERE context_id = $c0 ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 1",
            &bound,
        )
        .expect("EXACT inside the transaction");
    assert_eq!(
        int_ids(&overlay),
        vec![9999],
        "EXACT sees the transaction's own row"
    );
    db.rollback(tx).expect("roll the overlay back");
}
