use contextdb_core::{Direction, Value};
use contextdb_engine::Database;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::path::PathBuf;
use uuid::Uuid;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let db_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("missing database path argument")?;

    println!("WORKLOAD_START");

    let db = Database::open(&db_path)?;
    db.execute(
        "CREATE TABLE rss_items (id UUID PRIMARY KEY, name TEXT, embedding VECTOR(16))",
        &HashMap::new(),
    )?;

    let mut chain = Vec::<Uuid>::new();
    let mut batches = 0usize;
    for batch in 0..8usize {
        let tx = db.begin();
        for i in 0..250usize {
            let ordinal = batch * 250 + i;
            let node_id = Uuid::from_u128(ordinal as u128 + 1);
            let row_id = db.insert_row(
                tx,
                "rss_items",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(node_id)),
                    (
                        "name".to_string(),
                        Value::Text(format!("rss-item-{ordinal:04}")),
                    ),
                ]),
            )?;
            let vector = (0..16usize)
                .map(|j| ((ordinal + j) % 17) as f32 / 17.0)
                .collect::<Vec<_>>();
            db.insert_vector(tx, row_id, vector)?;
            if let Some(&previous) = chain.last() {
                db.insert_edge(
                    tx,
                    previous,
                    node_id,
                    "DEPENDS_ON".to_string(),
                    HashMap::new(),
                )?;
            }
            chain.push(node_id);
        }
        db.commit(tx)?;
        batches += 1;
    }

    let reopened = Database::open(&db_path)?;
    let snapshot = reopened.snapshot();
    let rows = reopened.scan("rss_items", snapshot)?;
    assert_eq!(rows.len(), 2000, "all relational rows must survive reopen");

    let bfs = reopened.query_bfs(Uuid::from_u128(1), None, Direction::Outgoing, 2, snapshot)?;
    assert!(
        !bfs.nodes.is_empty(),
        "graph traversal after reopen must return reachable nodes"
    );

    let vector_results = reopened.query_vector(&[0.0; 16], 5, None, snapshot)?;
    assert!(
        !vector_results.is_empty(),
        "vector query after reopen must return persisted rows"
    );

    println!(
        "WORKLOAD_DONE batches={batches} rows={} graph_nodes={} vector_hits={}",
        rows.len(),
        bfs.nodes.len(),
        vector_results.len()
    );
    Ok(())
}
