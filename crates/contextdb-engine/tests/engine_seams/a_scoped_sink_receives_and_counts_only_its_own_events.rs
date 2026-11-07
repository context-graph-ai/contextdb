//! A sink's scope is declared on the engine and honoured by the engine's
//! dispatch: once a sink's registration is scoped, no event outside that
//! scope is handed to it or counted as delivered for it, including events the
//! dispatcher had already picked up under the previous registration.

use contextdb_core::Value;
use contextdb_core::types::ContextId;
use contextdb_engine::{Database, SinkEvent};
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, mpsc};
use uuid::Uuid;

const CONTEXT_A: u128 = 0xA;
const CONTEXT_B: u128 = 0xB;

fn declared_store(path: &std::path::Path) -> Database {
    let database = Database::open(path).expect("open the store");
    for statement in [
        "CREATE TABLE invs (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID)",
        "CREATE EVENT TYPE inv_inserted WHEN INSERT ON invs",
        "CREATE SINK scoped TYPE callback",
        "CREATE ROUTE inv_to_scoped EVENT inv_inserted TO scoped",
    ] {
        database
            .execute(statement, &HashMap::new())
            .expect("declare the event route");
    }
    database
}

fn insert(database: &Database, id: u128, context: u128) {
    database
        .execute(
            "INSERT INTO invs (id, context_id) VALUES ($id, $ctx)",
            &HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::from_u128(id))),
                ("ctx".to_owned(), Value::Uuid(Uuid::from_u128(context))),
            ]),
        )
        .expect("commit an event row");
}

fn event_id(event: &SinkEvent) -> u128 {
    match event.row_values.get("id") {
        Some(Value::Uuid(id)) => id.as_u128(),
        other => panic!("event without its row id: {other:?}"),
    }
}

#[test]
fn narrowing_a_sinks_scope_while_a_batch_is_in_flight_hands_it_no_event_outside_the_new_scope() {
    const IN_SCOPE: u128 = 1;
    const OUT_OF_SCOPE: u128 = 2;
    const LATER_IN_SCOPE: u128 = 3;
    let directory = tempfile::tempdir().expect("scratch directory");
    let database = declared_store(&directory.path().join("events.db"));
    insert(&database, IN_SCOPE, CONTEXT_A);
    insert(&database, OUT_OF_SCOPE, CONTEXT_B);

    // (registration, event id) for every callback invocation.
    let handed: Arc<Mutex<Vec<(&'static str, u128)>>> = Arc::default();
    let (reached, reached_rx) = mpsc::channel::<()>();
    let (release, release_rx) = mpsc::channel::<()>();
    let reached = Mutex::new(Some(reached));
    let release_rx = Mutex::new(release_rx);
    let unscoped_log = Arc::clone(&handed);
    // The first registration sees every context. Its first delivery is held
    // so the dispatcher's batch, already holding both events, stays in flight.
    database
        .register_sink("scoped", None, move |event| {
            unscoped_log
                .lock()
                .expect("delivery log")
                .push(("unscoped", event_id(event)));
            if let Some(reached) = reached.lock().expect("reached sender").take() {
                let _ = reached.send(());
                let _ = release_rx.lock().expect("release receiver").recv();
            }
            Ok(())
        })
        .expect("register the sink");
    reached_rx
        .recv()
        .expect("the dispatcher hands the first event of its batch to the sink");

    // The scoped registration reports when it is handed the event committed
    // after the scope was declared; delivery is in queue order, so by then the
    // batch that was in flight has finished.
    let (later_delivered, later_delivered_rx) = mpsc::channel::<()>();
    let later_delivered = Mutex::new(later_delivered);
    let scoped_log = Arc::clone(&handed);
    database
        .__debug_register_sink_with_constraints_for_test(
            "scoped",
            Some(BTreeSet::from([ContextId::new(Uuid::from_u128(CONTEXT_A))])),
            None,
            None,
            move |event| {
                let id = event_id(event);
                scoped_log
                    .lock()
                    .expect("delivery log")
                    .push(("scoped", id));
                if id == LATER_IN_SCOPE {
                    let _ = later_delivered.lock().expect("later sender").send(());
                }
                Ok(())
            },
        )
        .expect("scope the sink to context A");
    let declared_at = handed.lock().expect("delivery log").len();
    insert(&database, LATER_IN_SCOPE, CONTEXT_A);

    drop(release);
    later_delivered_rx
        .recv()
        .expect("the scoped sink is handed the in-scope event committed after its scope");
    let after_scope: Vec<_> = handed.lock().expect("delivery log")[declared_at..].to_vec();
    assert!(
        !after_scope.iter().any(|(_, id)| *id == OUT_OF_SCOPE),
        "once the sink is scoped to context A, no context-B event may be handed to it; after the \
         scope was declared it was handed {after_scope:?}"
    );
    assert_eq!(
        database.sink_metrics("scoped").delivered,
        2,
        "only the two in-scope events count as delivered"
    );
}
