//! A volatile default in an edge's wider identity cannot identify a graph-only alias.
use contextdb_core::{Value, Wallclock};
use contextdb_engine::{Database, ReadSession};
use std::collections::HashMap;
use uuid::Uuid;

const SOURCE: Uuid = Uuid::from_u128(0xec01);
const TARGET: Uuid = Uuid::from_u128(0xec02);
const ROW: Uuid = Uuid::from_u128(0xec03);
const DDL: &str = "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, subtype_key TIMESTAMP NOT NULL DEFAULT NOW(), UNIQUE (source_id, target_id, edge_type, subtype_key))";
const INSERT: &str = "INSERT INTO edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, 'DEPENDS_ON')";
const PATTERN: &str = "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:DEPENDS_ON]->(b) WHERE a.id = $source COLUMNS (b.id AS target))";

#[test]
fn a_volatile_default_in_a_wider_edge_key_keeps_the_graph_and_row_bindings_distinct() {
    // Keep NOW deterministic, including when both writes share the same clock
    // value. A volatile expression still cannot stand in for a literal alias.
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let params = HashMap::from([
        ("id".to_owned(), Value::Uuid(ROW)),
        ("source".to_owned(), Value::Uuid(SOURCE)),
        ("target".to_owned(), Value::Uuid(TARGET)),
    ]);
    let expected = vec![vec![Value::Uuid(TARGET)]; 2];
    let mut counts = Vec::new();
    for on_disk in [false, true] {
        for graph_first in [true, false] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("volatile-identity.db");
            let db = if on_disk {
                Database::open(&path).unwrap()
            } else {
                Database::open_memory()
            };
            db.execute(DDL, &HashMap::new()).unwrap();
            let graph_write = || {
                let tx = db.begin().unwrap();
                assert!(
                    db.insert_edge(tx, SOURCE, TARGET, "DEPENDS_ON".into(), HashMap::new())
                        .unwrap(),
                    "the graph door creates its own edge"
                );
                db.commit(tx).unwrap();
            };
            let row_write = || {
                assert_eq!(
                    db.execute(INSERT, &params).unwrap().rows_affected(),
                    1,
                    "the row door creates its distinct wider-key edge"
                );
            };
            if graph_first {
                graph_write();
                row_write();
            } else {
                row_write();
                graph_write();
            }
            let label = format!("disk={on_disk}, graph_first={graph_first}");
            assert_eq!(
                db.execute("SELECT id, subtype_key FROM edges", &HashMap::new())
                    .unwrap()
                    .rows(),
                vec![vec![Value::Uuid(ROW), Value::Timestamp(1_700_000_000)]],
                "{label}: the row door evaluated the declared volatile default"
            );
            let rows = db.execute(PATTERN, &params).unwrap().rows().to_vec();
            counts.push((format!("{label}: SQL bindings"), rows.len()));
            assert!(
                rows.iter().all(|row| row == &expected[0]),
                "{label}: unexpected target"
            );
            counts.push((
                format!("{label}: edge_count"),
                db.edge_count(SOURCE, "DEPENDS_ON", db.snapshot()).unwrap(),
            ));
            db.close().unwrap();
            if on_disk {
                let session = ReadSession::open(&path).unwrap();
                assert_eq!(
                    session.route(),
                    contextdb_core::read_contract::ReadRoute::File
                );
                let rows = session.execute(PATTERN, &params).unwrap().rows().to_vec();
                counts.push((format!("{label}: file SQL bindings"), rows.len()));
                assert!(rows.iter().all(|row| row == &expected[0]));
                let cursor = session.open_cursor(PATTERN, &params).unwrap();
                counts.push((
                    format!("{label}: file cursor bindings"),
                    cursor.first_page().rows().len(),
                ));
                assert!(!cursor.first_page().has_more());
                assert!(
                    cursor
                        .first_page()
                        .rows()
                        .iter()
                        .all(|row| row == &expected[0])
                );
                drop(cursor);
                drop(session);
                let reopened = Database::open(&path).unwrap();
                counts.push((
                    format!("{label}: reopened edge_count"),
                    reopened
                        .edge_count(SOURCE, "DEPENDS_ON", reopened.snapshot())
                        .unwrap(),
                ));
                reopened.close().unwrap();
            }
        }
    }
    assert!(
        counts.iter().all(|(_, count)| *count == 2),
        "a volatile wider-key default leaves two distinct bindings through every door: {counts:?}"
    );
}
