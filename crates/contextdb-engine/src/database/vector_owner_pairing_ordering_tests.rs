//! D2b (owner-ruled 2026-08-06) — pin `partition_owned_vector_groups`'s
//! documented ordering invariant so it cannot silently drift before the next
//! sync wire-format bump carries each vector's own row identity.
//!
//! The Vigil story this guards: a served vector names its owner only by the
//! SENDER's row number, so the receiver pairs vector groups to rows
//! POSITIONALLY — "the n-th group carrying a given table, LSN, and liveness
//! belongs to the n-th row carrying the same" (see the doc comment on
//! `partition_owned_vector_groups`, `database.rs:36793-36831`). If a
//! look-alike batch's rows and vector groups ever land in different orders,
//! this positional pairing silently attaches the wrong face to the wrong
//! enrollment row. This is a PIN, not a feature test: it may already pass —
//! its job is to fail loudly the day someone changes the pairing rule
//! without updating every caller that relies on strict positional order.

use super::*;
use std::collections::HashMap;

const TABLE: &str = "enrollments";

fn row(natural_key_id: i64, label: &str, lsn: Lsn) -> RowChange {
    RowChange {
        table: TABLE.to_string(),
        natural_key: NaturalKey::from_pairs(vec![("id".to_string(), Value::Int64(natural_key_id))])
            .expect("natural key"),
        values: HashMap::from([
            ("id".to_string(), Value::Int64(natural_key_id)),
            ("label".to_string(), Value::Text(label.to_string())),
        ]),
        deleted: false,
        lsn,
        created_at: None,
    }
}

/// One single-vector group, carrying the SENDER's own row id — which the
/// receiver must never trust as identity, only as a group boundary marker.
fn vector_group(sender_row_id: u64, payload: [f32; 3], lsn: Lsn) -> VectorChange {
    VectorChange {
        index: VectorIndexRef::new(TABLE, "embedding".to_string()),
        row_id: RowId(sender_row_id),
        vector: payload.to_vec(),
        lsn,
    }
}

/// Every group is claimed, in strict input order, by the row at the same
/// position — reconstructing, group by group, that vector[i] belongs to
/// rows[i], never any other row in the fixture.
#[test]
fn owned_groups_pair_positionally_with_rows_sharing_table_lsn_and_liveness() {
    let lsn = Lsn(7);
    // Three enrollments land in this exact order.
    let rows = vec![
        row(1, "delivery driver", lsn),
        row(2, "family member", lsn),
        row(3, "neighbor", lsn),
    ];
    // Three look-alike faces the sender served alongside them, in the SAME
    // intended order — sender row ids are arbitrary and must not be read as
    // identity.
    let vectors = vec![
        vector_group(101, [1.0, 0.0, 0.0], lsn),
        vector_group(202, [0.0, 1.0, 0.0], lsn),
        vector_group(303, [0.0, 0.0, 1.0], lsn),
    ];

    let (owned, unowned_groups) = partition_owned_vector_groups(vectors.clone(), &rows);

    assert_eq!(
        unowned_groups, 0,
        "every group has a row in this apply to belong to"
    );
    assert_eq!(
        owned, vectors,
        "partition_owned_vector_groups must not reorder or drop a claimed group"
    );

    // Reconstruct the pairing a receiver would perform (`vector_row_group_end`
    // walked cursor by cursor, exactly as the per-row apply loop does), and
    // assert group i's payload is the one the sender intended for rows[i].
    let expected_payload_per_row: HashMap<&str, [f32; 3]> = HashMap::from([
        ("delivery driver", [1.0, 0.0, 0.0]),
        ("family member", [0.0, 1.0, 0.0]),
        ("neighbor", [0.0, 0.0, 1.0]),
    ]);
    let mut cursor = 0usize;
    for expected_row in &rows {
        let end = vector_row_group_end(&owned, cursor);
        assert!(
            end > cursor,
            "row {:?} must claim a nonempty group",
            expected_row.natural_key
        );
        let group = &owned[cursor..end];
        let label = match expected_row.values.get("label") {
            Some(Value::Text(label)) => label.as_str(),
            other => panic!("fixture row missing its label: {other:?}"),
        };
        let expected = expected_payload_per_row[label];
        for member in group {
            assert_eq!(
                member.vector, expected,
                "row {label:?} must reconstruct with the vector the sender served \
                 alongside it, never a neighboring row's — got {:?}, expected {expected:?}",
                member.vector
            );
        }
        cursor = end;
    }
    assert_eq!(
        cursor,
        owned.len(),
        "every owned vector must be claimed by exactly one row"
    );
}

/// A page that re-reads history can serve a vector group whose row this
/// apply does not carry (the row was served at an earlier or later page).
/// That group must be reported as skipped, never guessed onto whichever row
/// happens to sit at the cursor.
#[test]
fn a_group_beyond_the_row_count_for_its_key_is_reported_unowned_not_guessed() {
    let lsn = Lsn(9);
    // Only two rows for (TABLE, lsn, live) in THIS apply...
    let rows = vec![row(1, "alice", lsn), row(2, "bob", lsn)];
    // ...but three vector groups were served for that same key (the third
    // belongs to a row this apply does not carry).
    let vectors = vec![
        vector_group(11, [1.0, 0.0, 0.0], lsn),
        vector_group(12, [0.0, 1.0, 0.0], lsn),
        vector_group(13, [0.0, 0.0, 1.0], lsn),
    ];

    let (owned, unowned_groups) = partition_owned_vector_groups(vectors.clone(), &rows);

    assert_eq!(
        unowned_groups, 1,
        "the third group has no row in this apply to belong to, and must be \
         reported rather than paired with alice's or bob's row"
    );
    assert_eq!(
        owned,
        vec![vectors[0].clone(), vectors[1].clone()],
        "only the first two groups (one per available row, in order) are owned; \
         the trailing group must not be guessed onto an unrelated row"
    );
}
