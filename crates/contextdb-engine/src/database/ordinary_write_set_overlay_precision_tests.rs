//! The ordinary WriteSet overlay -- the branch of a transaction's vector search
//! taken when `capture_vector_write_set` finds no mutation-time owner -- ranks
//! committed and staged rows by canonical distance, as the captured overlay
//! does. Every in-transaction write registers that owner, so no public door
//! reaches the branch (a marker scan over the vector, sql_surface, acceptance
//! and precision suites found none). This test builds the owner-less write set
//! from real SQL writes and takes the owner out of it for the reads. Of the
//! in-transaction search doors only the Rust semantic search falls back to the
//! ordinary overlay; SQL runs through the bounded executor, which reads the
//! captured overlay alone.
use super::*;
use contextdb_core::VectorSearchMode;

const NEAR: [f32; 2] = [1.0, 0.0001];
const FAR: [f32; 2] = [1.0, 0.0002];

fn row_params(id: i64, vector: &[f32]) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Int64(id)),
        ("scope".to_string(), Value::Int64(id)),
        ("v".to_string(), Value::Vector(vector.to_vec())),
    ])
}

fn ids(result: &crate::QueryResult) -> Vec<Value> {
    result.rows().iter().map(|row| row[0].clone()).collect()
}

fn committed_answer(db: &Database, query: &[f32]) -> Vec<Value> {
    ids(&db
        .execute(
            "SELECT id FROM overlay ORDER BY v <=> $q USE VECTOR EXACT LIMIT 2",
            &HashMap::from([("q".to_string(), Value::Vector(query.to_vec()))]),
        )
        .unwrap())
}

#[test]
fn ordinary_write_set_overlay_ranks_committed_and_staged_rows_by_canonical_distance() {
    let query = [1.0_f32, 0.0];
    let expected = [Value::Int64(2), Value::Int64(1)];
    // (committed id and vector, staged id and vector): both directions of which
    // side holds the nearer row, one partition each, so LIMIT is global.
    for (committed, staged) in [((2, NEAR), (1, FAR)), ((1, FAR), (2, NEAR))] {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE overlay (id INT PRIMARY KEY, scope INT NOT NULL, tag TEXT, v VECTOR(2) PARTITION_KEY (scope))",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "INSERT INTO overlay (id, scope, v) VALUES ($id,$scope,$v)",
            &row_params(committed.0, &committed.1),
        )
        .unwrap();
        let before = committed_answer(&db, &query);
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "INSERT INTO overlay (id, scope, v) VALUES ($id,$scope,$v)",
            &row_params(staged.0, &staged.1),
        )
        .unwrap();
        // Replace the committed row inside the transaction: its vector moves
        // to the new row version.
        db.execute_in_tx(
            tx,
            "UPDATE overlay SET tag = $tag WHERE id = $id",
            &HashMap::from([
                ("id".to_string(), Value::Int64(committed.0)),
                ("tag".to_string(), Value::Text("moved".to_string())),
            ]),
        )
        .unwrap();
        let owner = db
            .tx_mgr
            .with_write_set(tx, |ws| std::mem::take(&mut ws.ordered_staged_rows))
            .unwrap();
        for limit in [1, 2] {
            for mode in [VectorSearchMode::Exact, VectorSearchMode::Auto] {
                let rust = db
                    .__semantic_search_in_tx_for_test(
                        tx,
                        SemanticQuery {
                            search_mode: Some(mode),
                            ..SemanticQuery::new("overlay", "v", query.to_vec(), limit)
                        },
                    )
                    .unwrap();
                assert_eq!(
                    rust.iter()
                        .map(|result| result.values.get("id").cloned().unwrap())
                        .collect::<Vec<_>>(),
                    expected[..limit].to_vec(),
                    "{mode:?}, committed={}, LIMIT {limit}",
                    committed.0
                );
            }
        }
        db.tx_mgr
            .with_write_set(tx, |ws| ws.ordered_staged_rows = owner)
            .unwrap();
        db.rollback(tx).unwrap();
        let after = committed_answer(&db, &query);
        assert_eq!(after, before, "rollback restores the committed answer");
        assert_eq!(after, vec![Value::Int64(committed.0)]);
    }
}
