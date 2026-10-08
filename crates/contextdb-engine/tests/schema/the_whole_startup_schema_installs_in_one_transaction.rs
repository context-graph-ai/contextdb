//! A startup migration must publish every declared schema family together.

use contextdb_core::{Lsn, Value};
use contextdb_engine::{Database, sync_types::DdlChange};
use std::collections::HashMap;
use uuid::Uuid;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

// Supplemental family fixture. The complete, source-extracted cg startup is tested below;
// cg installs no sinks or routes, and this additional fixture preserves those existing assertions.
const STARTUP_SCHEMA: &[&str] = &[
    "CREATE TABLE startup_contexts (id UUID PRIMARY KEY)",
    "CREATE TABLE startup_rank_weights (id UUID PRIMARY KEY, passage_id UUID NOT NULL, weight REAL NOT NULL)",
    "CREATE INDEX startup_rank_weights_passage ON startup_rank_weights(passage_id)",
    "CREATE TABLE startup_passages (id UUID PRIMARY KEY, context_id UUID NOT NULL REFERENCES startup_contexts(id), embedding VECTOR(2) RANK_POLICY (JOIN startup_rank_weights ON passage_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY startup_rank))",
    "CREATE TABLE startup_edges (id UUID PRIMARY KEY, source_id UUID NOT NULL REFERENCES startup_contexts(id), target_id UUID NOT NULL REFERENCES startup_contexts(id), edge_type TEXT NOT NULL) DAG('CITES')",
    "CREATE INDEX startup_passages_context ON startup_passages(context_id)",
    "CREATE TABLE startup_events (id UUID PRIMARY KEY, context_id UUID NOT NULL REFERENCES startup_contexts(id))",
    "CREATE EVENT TYPE startup_event WHEN INSERT ON startup_events",
    "CREATE TRIGGER startup_trigger ON startup_events WHEN INSERT INCLUDING SYNC",
    "CREATE SINK startup_sink TYPE callback",
    "CREATE ROUTE startup_route EVENT startup_event TO startup_sink",
];

fn install(db: &Database) -> contextdb_core::Result<()> {
    let tx = db.begin()?;
    for ddl in STARTUP_SCHEMA {
        db.execute_in_tx(tx, ddl, &empty())?;
    }
    db.commit(tx)
}

fn assert_absent(db: &Database, phase: &str) {
    for table in [
        "startup_contexts",
        "startup_rank_weights",
        "startup_passages",
        "startup_edges",
        "startup_events",
    ] {
        assert!(
            db.table_meta(table).is_none(),
            "{phase}: rollback must not publish {table}"
        );
    }
    assert!(
        db.list_triggers().is_empty(),
        "{phase}: rollback must not publish the trigger"
    );
    assert!(
        db.changes_since(Lsn(0))
            .expect("read the declared changes")
            .ddl
            .is_empty(),
        "{phase}: rollback must not publish a partial authored migration"
    );
}

fn assert_complete(db: &Database, phase: &str) {
    for table in [
        "startup_contexts",
        "startup_rank_weights",
        "startup_passages",
        "startup_edges",
        "startup_events",
    ] {
        assert!(
            db.table_meta(table).is_some(),
            "{phase}: commit must publish {table}"
        );
    }
    assert!(
        db.list_triggers()
            .iter()
            .any(|trigger| trigger.name == "startup_trigger"),
        "{phase}: commit must publish the trigger with the rest of the schema"
    );

    let changes = db.changes_since(Lsn(0)).expect("read the declared changes");
    assert_eq!(
        changes.ddl.len(),
        STARTUP_SCHEMA.len(),
        "{phase}: every startup declaration must retain one authored entry"
    );
    assert!(
        changes.ddl_lsn.iter().all(|lsn| *lsn == changes.ddl_lsn[0]),
        "{phase}: all startup declarations must retain one source transaction boundary"
    );
    let authored_kinds = changes
        .ddl
        .iter()
        .map(|ddl| match ddl {
            DdlChange::CreateTable { .. } => "table",
            DdlChange::CreateIndex { .. } => "index",
            DdlChange::CreateEventType { .. } => "event type",
            DdlChange::CreateTriggerIncludingSync { .. } => "trigger",
            DdlChange::CreateSink { .. } => "sink",
            DdlChange::CreateRoute { .. } => "route",
            other => panic!("{phase}: unexpected authored startup declaration: {other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        authored_kinds,
        [
            "table",
            "table",
            "index",
            "table",
            "table",
            "index",
            "table",
            "event type",
            "trigger",
            "sink",
            "route",
        ],
        "{phase}: the durable DDL log must preserve the authored whole-migration order"
    );
    assert!(
        changes
            .ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateIndex { .. })),
        "{phase}: the one transaction must contain its index declaration"
    );
    assert!(
        changes
            .ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateEventType { .. })),
        "{phase}: the one transaction must contain its event type declaration"
    );
    assert!(
        changes
            .ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateTriggerIncludingSync { .. })),
        "{phase}: the one transaction must contain its including-sync trigger"
    );
    assert!(
        changes
            .ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateSink { .. })),
        "{phase}: the one transaction must contain its sink declaration"
    );
    assert!(
        changes
            .ddl
            .iter()
            .any(|ddl| matches!(ddl, DdlChange::CreateRoute { .. })),
        "{phase}: the one transaction must contain its route declaration"
    );
}

fn assert_schema_components_are_usable(db: &Database, phase: &str, seed: u128) {
    support::initialize_fixture_callbacks(db);
    let context = Uuid::from_u128(seed);
    let second_context = Uuid::from_u128(seed + 1);
    let passage = Uuid::from_u128(seed + 2);
    let weight = Uuid::from_u128(seed + 3);
    let edge = Uuid::from_u128(seed + 4);
    let event = Uuid::from_u128(seed + 5);
    db.execute(
        "INSERT INTO startup_contexts VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(context))]),
    )
    .expect("the committed relational schema must accept a context row");
    db.execute(
        "INSERT INTO startup_contexts VALUES ($id)",
        &HashMap::from([("id".to_string(), Value::Uuid(second_context))]),
    )
    .expect("the committed relational schema must accept a second context row");
    db.execute(
        "INSERT INTO startup_passages VALUES ($id, $context_id, $embedding)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(passage)),
            ("context_id".to_string(), Value::Uuid(context)),
            ("embedding".to_string(), Value::Vector(vec![1.0, 0.0])),
        ]),
    )
    .expect("the committed vector schema must accept a passage row");
    db.execute(
        "INSERT INTO startup_rank_weights VALUES ($id, $passage_id, $weight)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(weight)),
            ("passage_id".to_string(), Value::Uuid(passage)),
            ("weight".to_string(), Value::Float64(1.0)),
        ]),
    )
    .expect("the committed rank-join schema must accept its joined row");
    db.execute(
        "INSERT INTO startup_edges VALUES ($id, $source_id, $target_id, 'CITES')",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(edge)),
            ("source_id".to_string(), Value::Uuid(context)),
            ("target_id".to_string(), Value::Uuid(second_context)),
        ]),
    )
    .expect("the committed DAG schema must accept an acyclic edge");
    db.execute(
        "INSERT INTO startup_events VALUES ($id, $context_id)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(event)),
            ("context_id".to_string(), Value::Uuid(context)),
        ]),
    )
    .expect("the committed event, trigger, sink, and route declarations must accept an event");

    assert!(
        db.column_has_covering_index("startup_passages", "context_id"),
        "{phase}: the committed context index must be usable"
    );
    let vector = db
        .execute(
            "SELECT id FROM startup_passages WHERE context_id = $context_id \
             ORDER BY embedding <=> $query USE RANK startup_rank LIMIT 1",
            &HashMap::from([
                ("context_id".to_string(), Value::Uuid(context)),
                ("query".to_string(), Value::Vector(vec![1.0, 0.0])),
            ]),
        )
        .expect("the committed vector and rank declarations must answer a ranked query");
    assert_eq!(
        vector.rows.len(),
        1,
        "{phase}: ranked vector query must return its row"
    );

    let events = db.event_bus_status();
    assert!(
        events
            .event_types
            .iter()
            .any(|event| event.name == "startup_event")
    );
    assert!(events.sinks.iter().any(|sink| sink.name == "startup_sink"));
    assert!(events.routes.iter().any(|route| {
        route.name == "startup_route"
            && route.event_type == "startup_event"
            && route.sink == "startup_sink"
    }));
}

#[test]
fn supplemental_schema_families_including_sinks_and_routes_install_atomically() {
    let root = tempfile::tempdir().expect("create temporary startup-schema root");
    let rollback_path = root.path().join("rollback.db");
    let rollback = Database::open(&rollback_path).expect("open rollback fixture");
    let tx = rollback
        .begin()
        .expect("begin rollback fixture transaction");
    for ddl in STARTUP_SCHEMA {
        rollback
            .execute_in_tx(tx, ddl, &empty())
            .expect("stage every startup declaration before the rollback");
    }
    rollback.rollback(tx).expect("roll back startup migration");
    assert_absent(&rollback, "before rollback-fixture reopen");
    rollback.close().expect("close rollback fixture");
    let reopened_rollback = Database::open(&rollback_path).expect("reopen rollback fixture");
    assert_absent(&reopened_rollback, "after rollback-fixture reopen");
    reopened_rollback
        .close()
        .expect("close reopened rollback fixture");

    let committed_path = root.path().join("committed.db");
    let committed = Database::open(&committed_path).expect("open commit fixture");
    install(&committed).expect("install the complete startup migration in one transaction");
    assert_complete(&committed, "before committed-fixture reopen");
    assert_schema_components_are_usable(&committed, "before committed-fixture reopen", 0x5000);
    committed.close().expect("close committed fixture");
    let reopened_committed = Database::open(&committed_path).expect("reopen committed fixture");
    assert_complete(&reopened_committed, "after committed-fixture reopen");
    assert_schema_components_are_usable(
        &reopened_committed,
        "after committed-fixture reopen",
        0x6000,
    );
    reopened_committed
        .close()
        .expect("close reopened committed fixture");
}

#[test]
fn a_late_invalid_declaration_rolls_back_the_complete_schema_stage() {
    let root = tempfile::tempdir().expect("create late-invalid startup-schema root");
    let path = root.path().join("late-invalid.db");
    let db = Database::open(&path).expect("open late-invalid fixture");
    let tx = db.begin().expect("begin late-invalid schema transaction");
    for ddl in STARTUP_SCHEMA {
        db.execute_in_tx(tx, ddl, &empty())
            .expect("stage the valid startup prefix");
    }
    db.execute_in_tx(
        tx,
        "CREATE ROUTE invalid_late_route EVENT missing_event TO startup_sink",
        &empty(),
    )
    .expect_err("the final declaration must fail validation");
    assert_absent(&db, "after the late declaration failure");
    db.rollback(tx).expect("roll back the failed migration");
    assert_absent(&db, "after late-invalid rollback");
    db.close().expect("close late-invalid fixture");
    let reopened = Database::open(&path).expect("reopen late-invalid fixture");
    assert_absent(&reopened, "after late-invalid reopen");
}

#[path = "../fixtures/schema_wall/mod.rs"]
mod support;

#[test]
fn the_whole_startup_schema_installs_in_one_transaction() {
    let expected = support::reference(support::pinned::LOCAL);
    let authored = expected
        .changes_since(Lsn(0))
        .expect("read the declared changes")
        .ddl;
    assert_eq!(authored.len(), support::pinned::LOCAL.len());
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("complete-cg-startup.db");
    let db = Database::open(&path).unwrap();
    let tx = db.begin().unwrap();
    for (ordinal, ddl) in support::pinned::LOCAL.iter().enumerate() {
        db.execute_in_tx(tx, ddl, &empty()).unwrap_or_else(|error| {
            panic!("SCHEMA_FIRST_FAILURE phase=whole-startup-stage ordinal={ordinal}: {error}; DDL={ddl}")
        });
    }
    support::assert_no_schema(&db);
    db.rollback(tx).unwrap();
    support::assert_no_schema(&db);
    db.close().unwrap();
    let reopened = Database::open(&path).unwrap();
    support::assert_no_schema(&reopened);
    support::install(&reopened, support::pinned::LOCAL);
    support::assert_full_schema(&reopened, &expected);
    support::assert_one_ordered_unit(&reopened, Lsn(0), &authored);
    support::seed_cg_artifacts(&reopened, 0x7000);
    support::assert_cg_artifacts(&reopened, 0x7000);
    reopened.close().unwrap();
    let committed = Database::open(&path).unwrap();
    support::assert_full_schema(&committed, &expected);
    support::assert_one_ordered_unit(&committed, Lsn(0), &authored);
    support::assert_cg_artifacts(&committed, 0x7000);
    committed.close().unwrap();
    let reader = contextdb_engine::ReadSession::open(&path).unwrap();
    assert_eq!(
        reader.route(),
        contextdb_core::read_contract::ReadRoute::File
    );
    support::assert_cg_artifact_queries(|sql, p| reader.execute(sql, p).unwrap().rows, 0x7000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_complete_pinned_startup_reaches_a_second_edge_in_author_order() {
    let root = tempfile::tempdir().unwrap();
    let source = std::sync::Arc::new(Database::open(root.path().join("source.db")).unwrap());
    let second = std::sync::Arc::new(Database::open(root.path().join("second.db")).unwrap());
    let receiver = std::sync::Arc::new(Database::open(root.path().join("hub.db")).unwrap());
    let hub = support::Hub::start(receiver.clone(), "full-cg-startup").await;
    let a = hub.client(source.clone());
    let b = hub.client(second.clone());
    hub.bind(&a).await;
    hub.bind(&b).await;
    let source_before = source.current_lsn();
    let receiver_before = receiver.current_lsn();
    let second_before = second.current_lsn();
    support::install(&source, support::pinned::LOCAL);
    support::initialize_fixture_callbacks(&source);
    let authored = source
        .changes_since(source_before)
        .expect("read the declared changes")
        .ddl;
    assert_eq!(authored.len(), support::pinned::LOCAL.len());
    support::assert_one_ordered_unit(&source, source_before, &authored);
    a.push()
        .await
        .expect("push the complete authenticated source migration");
    let source_lsn = source
        .changes_since(source_before)
        .expect("read the declared changes")
        .ddl_lsn[0];
    assert!(
        support::assert_authored_wire(
            &hub.broker,
            &authored,
            source_lsn,
            source
                .sync_incarnation_for_test(&contextdb_core::TenantId::from("full-cg-startup"))
                .unwrap()
        ) > 0,
        "the entire authored transaction must cross the actual authenticated wire"
    );
    b.pull_default()
        .await
        .expect("second edge receives one complete schema unit");
    support::initialize_fixture_callbacks(&second);
    a.pull_default()
        .await
        .expect("source converges after hub acceptance");
    let expected = support::reference(support::pinned::LOCAL);
    for (phase, db, before) in [
        ("source", &source, source_before),
        ("receiver", &receiver, receiver_before),
        ("second edge", &second, second_before),
    ] {
        support::assert_schema_contains(db, &expected);
        support::assert_contains_one_ordered_unit(db, before, &authored, phase);
    }
    let before = second
        .changes_since(second_before)
        .expect("read the declared changes");
    a.push().await.expect("repeat source sync");
    b.pull_default().await.expect("repeat receiver sync");
    assert_eq!(
        second
            .changes_since(second_before)
            .expect("read the declared changes")
            .ddl,
        before.ddl,
        "retry adds no duplicate prefix"
    );
    assert_eq!(
        second
            .changes_since(second_before)
            .expect("read the declared changes")
            .ddl_lsn,
        before.ddl_lsn
    );
    drop(a);
    drop(b);
    hub.stop().await;
}

#[test]
fn the_complete_bound_tenant_startup_stages_without_rewriting_its_declarations() {
    // Exact tenant-policy branch, including the real event delivery manifest.
    // This is local transactional schema authoring; the authenticated binding
    // and full migration transport are exercised by the second-edge test.
    let root = tempfile::tempdir().unwrap();
    let db = Database::open(root.path().join("tenant-startup.db")).unwrap();
    let expected = support::reference(support::pinned::TENANT_HUB);
    support::install(&db, support::pinned::TENANT_HUB);
    support::assert_full_schema(&db, &expected);
    support::assert_one_ordered_unit(
        &db,
        Lsn(0),
        &expected
            .changes_since(Lsn(0))
            .expect("read the declared changes")
            .ddl,
    );
    db.close().unwrap();
}

#[cfg(unix)]
mod source_crash {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::path::PathBuf;
    use std::process::{Child, Command, Stdio};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    const CHILD_PATH: &str = "CONTEXTDB_FULL_SCHEMA_SOURCE_CHILD_PATH";
    const CHILD_BOUNDARY: &str = "CONTEXTDB_FULL_SCHEMA_SOURCE_CHILD_BOUNDARY";
    const MARKER: &str = "FULL_SCHEMA_SOURCE_BOUNDARY";

    use support::committed_identity as identity;

    #[test]
    fn full_schema_source_crash_child() {
        let Some(path) = std::env::var_os(CHILD_PATH) else {
            // Normal enumeration is a real positive control, never an empty
            // child-only success. The parent invokes the full fixture below.
            let db = Database::open_memory();
            support::install(
                &db,
                &["CREATE TABLE source_fence_control (id INTEGER PRIMARY KEY)"],
            );
            assert!(db.table_meta("source_fence_control").is_some());
            return;
        };
        let boundary = std::env::var(CHILD_BOUNDARY).unwrap();
        let db = Arc::new(Database::open(PathBuf::from(path)).unwrap());
        let pause = match boundary.as_str() {
            "before-durable" => db.pause_before_local_schema_durable_commit_for_test(),
            "after-durable-before-publication" => db.pause_before_local_schema_publish_for_test(),
            "between-publication-components" => {
                db.pause_between_local_publication_components_for_test()
            }
            other => panic!("unknown local startup-schema boundary {other}"),
        };
        let _writer = std::thread::spawn({
            let db = db.clone();
            move || support::install(&db, support::pinned::LOCAL)
        });
        assert!(
            pause.wait_until_reached(Duration::from_secs(30)),
            "the actual complete startup did not reach its source boundary; preceding writer error is the prerequisite failure"
        );
        // The commit holds the schema-publication write gate at both pause
        // points, so a public inspector correctly blocks until publication.
        // The parent kills here and proves the durable result after reopen.
        println!("{MARKER} {boundary}");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }

    struct KillOnDrop(Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn crash(boundary: &str, committed: bool) {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("source.db");
        Database::open(&path).unwrap().close().unwrap();
        let before = identity(&path);
        let module = module_path!().split_once("::").unwrap().1;
        let mut child = KillOnDrop(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    &format!("{module}::full_schema_source_crash_child"),
                    "--nocapture",
                ])
                .env(CHILD_PATH, &path)
                .env(CHILD_BOUNDARY, boundary)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        );
        let output = child.0.stdout.take().unwrap();
        let (send, receive) = mpsc::channel();
        let pump = std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                let line = line.expect("read source crash receipt");
                if let Some(at) = line.find(MARKER) {
                    let _ = send.send(line[at..].to_string());
                    return;
                }
            }
        });
        assert_eq!(
            receive
                .recv_timeout(Duration::from_secs(60))
                .expect("real source boundary receipt, not an arbitrary kill"),
            format!("{MARKER} {boundary}")
        );
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        pump.join().unwrap();
        // Redb may require ordinary writable crash recovery before a File
        // reader can inspect its committed image. Recovery is part of reopen.
        Database::open(&path).unwrap().close().unwrap();
        let after = identity(&path);
        let reopened = Database::open(&path).unwrap();
        if committed {
            let expected = support::reference(support::pinned::LOCAL);
            support::assert_full_schema(&reopened, &expected);
            support::assert_one_ordered_unit(
                &reopened,
                Lsn(0),
                &expected
                    .changes_since(Lsn(0))
                    .expect("read the declared changes")
                    .ddl,
            );
            assert_ne!(
                after, before,
                "complete committed schema changes content identity"
            );
        } else {
            support::assert_no_schema(&reopened);
            assert_eq!(
                after, before,
                "a killed pre-durable source publishes no content identity"
            );
            support::install(&reopened, support::pinned::LOCAL);
            support::assert_one_ordered_unit(
                &reopened,
                Lsn(0),
                &support::reference(support::pinned::LOCAL)
                    .changes_since(Lsn(0))
                    .expect("read the declared changes")
                    .ddl,
            );
        }
        support::seed_cg_artifacts(&reopened, 0x8000);
        support::assert_cg_artifacts(&reopened, 0x8000);
        reopened.close().unwrap();
        let reader = contextdb_engine::ReadSession::open(&path).unwrap();
        assert_eq!(
            reader.route(),
            contextdb_core::read_contract::ReadRoute::File
        );
        support::assert_cg_artifact_queries(|sql, p| reader.execute(sql, p).unwrap().rows, 0x8000);
        drop(reader);
        let recovered = identity(&path);
        assert_eq!(
            identity(&path),
            recovered,
            "repeated read-only reopen has one stable verified content identity"
        );
    }

    #[test]
    fn full_startup_source_crash_before_durability_publishes_nothing() {
        crash("before-durable", false);
    }

    #[test]
    fn full_startup_source_crash_after_durability_recovers_every_artifact() {
        crash("after-durable-before-publication", true);
    }

    /// The local door's own BETWEEN-PUBLICATION-COMPONENTS boundary: the
    /// prepared schema image is already live in this process's memory and the
    /// custody and lineage deltas of the same commit are not. The gate names
    /// every local publication boundary, not only the one before the first
    /// component -- a crash here must still leave the complete startup schema
    /// after a restart, derived from the durable image the dead process never
    /// got to finish publishing.
    #[test]
    fn full_startup_source_crash_between_publication_components_recovers_every_artifact() {
        crash("between-publication-components", true);
    }
}
