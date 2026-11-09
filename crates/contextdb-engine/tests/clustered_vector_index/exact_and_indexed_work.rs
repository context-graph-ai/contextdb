//! EXACT compares every allowed stored vector at full precision, so it
//! returns the full-precision order even where the int8 list records cannot
//! tell neighbours apart; INDEXED re-ranks its candidates at full precision
//! and does strictly less complete distance work than EXACT on the same
//! scope: centroid, int8, canonical and write-buffer comparisons all count.

use super::support::{cosine_distance, empty, int_ids, ivf, open_caller_driven, params};
use contextdb_core::Value;
use contextdb_engine::Database;
use std::collections::HashMap;

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
