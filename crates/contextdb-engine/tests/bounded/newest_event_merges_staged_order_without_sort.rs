//! Acceptance: "Binding the full equality prefix and ordering with LIMIT 1 examines equal entry
//! counts at 10 and 20,010 rows, beside neighbouring streams. Staged rows remain visible; index
//! order avoids a separate sort inside and outside transactions."
//!
//! The parent fixture already proves committed and one-staged-row counts and trace sort elision.
//! This extension measures the actual overlay sort, with older staged rows and a staged delete.

use super::*;
use contextdb_core::Wallclock;
use contextdb_engine::writer_query_test_seams::observe;

#[test]
fn newest_event_merges_staged_order_without_sort() {
    let _clock = Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let context = stream_id(1);
    let stream = stream_id(2);
    let mut observed = Vec::new();
    let mut deleted_only = Vec::new();
    for committed in [SMALL_STREAM_ROWS, LARGE_STREAM_ROWS] {
        let database = new_database();
        seed_stream(&database, context, stream, committed);
        seed_stream(&database, context, stream_id(3), NEIGHBOUR_ROWS);
        let (outside, outside_work) = observe(|| newest(&database, context, stream));
        asserted_newest(&database, &outside, committed - 1, "committed lookup");
        let tx = database.begin_or_panic();
        // Insert in reverse index order so a sort cannot be hidden by already ordered input.
        for (id, sequence, payload) in [
            (stream_id(100), committed + 1, "staged-newest"),
            (stream_id(101), -2, "staged-older"),
            (stream_id(102), -1, "staged-older"),
        ] {
            database.execute_in_tx(tx,
                "INSERT INTO events VALUES ($id, $context_id, $connector_id, $source_instance_ref, $seq, $payload)",
                &params([
                    ("id", Value::Uuid(id)), ("context_id", Value::Uuid(context)),
                    ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                    ("source_instance_ref", Value::Text(stream.to_string())),
                    ("seq", Value::Int64(sequence)), ("payload", Value::Text(payload.to_owned())),
                ]),
            ).unwrap();
        }
        database
            .execute_in_tx(
                tx,
                "DELETE FROM events WHERE id = $id",
                &params([("id", Value::Uuid(event_id(stream, committed - 1)))]),
            )
            .unwrap();
        let (inside, inside_work) = observe(|| {
            database
                .execute_in_tx(
                    tx,
                    newest_sql(),
                    &params([
                        ("context_id", Value::Uuid(context)),
                        ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                        ("source_instance_ref", Value::Text(stream.to_string())),
                    ]),
                )
                .unwrap()
        });
        assert_eq!(
            inside.rows,
            vec![vec![
                Value::Int64(committed + 1),
                Value::Text("staged-newest".to_owned())
            ]]
        );
        assert!(
            inside.trace.sort_elided,
            "the declared index supplies the transactional order"
        );
        // A staged delete and inserts must remain private to this transaction.
        asserted_newest(
            &database,
            &newest(&database, context, stream),
            committed - 1,
            "concurrent committed lookup",
        );
        database.rollback(tx).unwrap();
        let deleted = database.begin_or_panic();
        database
            .execute_in_tx(
                deleted,
                "DELETE FROM events WHERE id = $id",
                &params([("id", Value::Uuid(event_id(stream, committed - 1)))]),
            )
            .unwrap();
        let (answer, work) = observe(|| {
            database
                .execute_in_tx(
                    deleted,
                    newest_sql(),
                    &params([
                        ("context_id", Value::Uuid(context)),
                        ("connector_id", Value::Text(CONNECTOR_ID.to_owned())),
                        ("source_instance_ref", Value::Text(stream.to_string())),
                    ]),
                )
                .unwrap()
        });
        asserted_newest(
            &database,
            &answer,
            committed - 2,
            "delete-only staged lookup",
        );
        assert!(answer.trace.sort_elided);
        assert_eq!(work.staged_sort_calls, 0);
        deleted_only.push(answer.trace.rows_examined);
        asserted_newest(
            &database,
            &newest(&database, context, stream),
            committed - 1,
            "the staged deletion remains private",
        );
        database.rollback(deleted).unwrap();
        observed.push((
            outside.trace.rows_examined,
            inside.trace.rows_examined,
            outside_work,
            inside_work,
        ));
    }
    assert_eq!(
        observed[0].0, observed[1].0,
        "committed lookup ignores stream depth and neighbours"
    );
    assert_eq!(
        observed[0].1, observed[1].1,
        "staged lookup ignores stream depth and neighbours"
    );
    assert_eq!(
        deleted_only[0], deleted_only[1],
        "delete-only newest lookup ignores doubled completed history"
    );
    for (_, _, outside, inside) in observed {
        assert_eq!(
            outside.staged_sort_calls, 0,
            "committed lookup performs no overlay sort"
        );
        assert_eq!(
            inside.staged_sort_calls, 0,
            "transactional newest lookup must merge staged index order without a separate sort; observed={inside:?}"
        );
    }
}
