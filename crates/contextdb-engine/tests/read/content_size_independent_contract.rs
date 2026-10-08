//! Persisted body sizes and the typed refusal for stores without persisted bodies.
use contextdb_core::read_contract::{ReadFailureDetail, ReadFailureKind};
use contextdb_core::{Error, Value};
use contextdb_engine::Database;
use std::collections::HashMap;

#[test]
fn memory_content_size_refuses_without_inventing_persisted_bytes() {
    let database = Database::open_memory();
    let params = HashMap::new();
    for populated in [false, true] {
        if populated {
            database
                .execute(
                    "CREATE TABLE memory_sizes (id INT PRIMARY KEY, payload TEXT, embedding VECTOR(3)) HISTORY ALL",
                    &params,
                )
                .expect("create memory-only content fixture");
            database
                .execute(
                    "INSERT INTO memory_sizes (id, payload, embedding) VALUES (1, $payload, $embedding)",
                    &HashMap::from([
                        ("payload".into(), Value::Text("persisted size is unavailable".into())),
                        ("embedding".into(), Value::Vector(vec![1.0, 2.0, 3.0])),
                    ]),
                )
                .expect("populate memory-only rows and vector bodies");
        }
        let refusal = database
            .execute("SHOW CONTENT_SIZE", &params)
            .expect_err("a memory-only store cannot report persisted committed body lengths");
        let Error::ReadFailure(failure) = refusal else {
            panic!("expected a typed unsupported read refusal, got {refusal:?}");
        };
        assert_eq!(failure.kind(), ReadFailureKind::OwnerRouteUnsupported);
        let ReadFailureDetail::OwnerRouteUnsupported(detail) = failure.detail() else {
            panic!("unsupported read refusal must name the unavailable inspection");
        };
        assert_eq!(
            detail.inspection,
            "SHOW CONTENT_SIZE has no persisted committed values in a memory-only store"
        );
    }
    let answer = database
        .execute("SELECT payload FROM memory_sizes WHERE id = 1", &params)
        .expect("typed size refusal leaves the store usable");
    assert_eq!(
        answer.rows,
        vec![vec![Value::Text("persisted size is unavailable".into())]]
    );
}
