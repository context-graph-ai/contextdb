//! One door for what a store says about itself, whichever route answers.
//!
//! A caller asking "what tables are here" should not have to know whether the
//! store is an idle file, a writer in this process, or a writer over a
//! channel. So there is one door, and the answer it hands back is the same
//! body on every route -- the document that was published, not a
//! route-flavoured rendering of it.
//!
//! Where a paged answer left off rides beside that body rather than inside
//! it, because the body is the published document and the page is this
//! exchange's business. Offering a continuation to a question that answers in
//! one piece is refused: a caller that thinks it is resuming, and is really
//! starting over, reads the inventory twice and never finds out.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    OwnerReadLimits, OwnerRouteUnsupportedDetail, ReadFailure, ReadFailureDetail, ReadFailureKind,
    ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, DirectImageMetadataKind, MetadataBody, MetadataRequest,
    OwnerReadConfig, ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn seed(database: &Database) {
    for statement in [
        "CREATE TABLE documents (id INTEGER PRIMARY KEY, body TEXT)",
        "CREATE TABLE owners (id INTEGER PRIMARY KEY, name TEXT)",
        // Something the store does on its own, so the event inventory has a
        // schedule in it to be right or wrong about.
        "CREATE SCHEDULE compact_documents EVERY '7 HOURS' TX (compact_cb)",
        "CREATE SCHEDULE vacuum_owners EVERY '3 HOURS' TX (vacuum_cb)",
        // Enough tables, with long enough names, that the inventory cannot
        // fit in one small page and has to be resumed.
        "CREATE TABLE quarterly_revenue_projections (id INTEGER PRIMARY KEY)",
        "CREATE TABLE customer_support_transcripts (id INTEGER PRIMARY KEY)",
        "CREATE TABLE inventory_reconciliation_log (id INTEGER PRIMARY KEY)",
        "CREATE TABLE marketing_attribution_events (id INTEGER PRIMARY KEY)",
        "CREATE TABLE shipment_tracking_snapshots (id INTEGER PRIMARY KEY)",
        "CREATE TABLE subscription_renewal_notices (id INTEGER PRIMARY KEY)",
        "CREATE TABLE warehouse_capacity_forecasts (id INTEGER PRIMARY KEY)",
        "CREATE TABLE payment_settlement_batches (id INTEGER PRIMARY KEY)",
    ] {
        database
            .execute(statement, &HashMap::new())
            .unwrap_or_else(|error| panic!("seed {statement}: {error}"));
    }
    database
        .execute(
            "INSERT INTO documents (id, body) VALUES ($id, $body)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(1)),
                ("body".to_owned(), Value::Text("kept".to_owned())),
            ]),
        )
        .expect("seed a document");
}

/// An idle store, so the reading route is the committed file.
fn idle_store(directory: &Path) -> PathBuf {
    let path = directory.join("metadata.db");
    let database = Database::open(&path).expect("open the metadata fixture writer");
    seed(&database);
    database.close().expect("the fixture writer closes cleanly");
    path
}

/// A live writer serving owner reads, so the reading route is that owner over
/// a channel.
fn served_store(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("served.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root),
                limits: OwnerReadLimits {
                    limits: ReadLimits::default(),
                    concurrency: 4,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads");
    seed(&database);
    (database, path)
}

fn events(body: &MetadataBody) -> contextdb_engine::DirectEventsStatus {
    match body {
        MetadataBody::EventsStatus { status, .. } => status.clone(),
        other => panic!("asked for the event inventory and got {other:?}"),
    }
}

fn table_names(body: &MetadataBody) -> Vec<String> {
    match body {
        MetadataBody::Tables { items, .. } => items.clone(),
        other => panic!("asked for the table inventory and got {other:?}"),
    }
}

#[test]
fn the_idle_file_answers_every_question_a_store_is_asked_about_itself() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle store");
    assert_eq!(session.route(), ReadRoute::File);

    let tables = session
        .metadata(MetadataRequest::Tables, None)
        .expect("the file answers the table inventory");
    assert!(
        table_names(&tables.body).contains(&"documents".to_owned()),
        "the inventory names the tables the store has: {:?}",
        tables.body
    );
    assert_eq!(
        tables.continuation, None,
        "an inventory that fits in one answer resumes nowhere"
    );

    let schema = session
        .metadata(
            MetadataRequest::Schema {
                table: "documents".to_owned(),
            },
            None,
        )
        .expect("the file answers a table schema");
    let MetadataBody::Schema { schema } = &schema.body else {
        panic!("asked for a schema and got {:?}", schema.body);
    };
    assert_eq!(schema.table, "documents");
    assert!(
        schema.columns.iter().any(|column| column.name == "body"),
        "the schema names the columns the table has"
    );

    let explained = session
        .metadata(
            MetadataRequest::Explain {
                sql: "SELECT id FROM documents".to_owned(),
                params: HashMap::new(),
            },
            None,
        )
        .expect("the file explains a statement");
    let MetadataBody::Explain { physical_plan, .. } = &explained.body else {
        panic!("asked for an explain and got {:?}", explained.body);
    };
    assert!(
        !physical_plan.is_empty(),
        "an explained statement names the plan the engine chose"
    );

    assert!(matches!(
        session
            .metadata(MetadataRequest::EventsStatus, None)
            .expect("the file answers the event inventory")
            .body,
        MetadataBody::EventsStatus { .. }
    ));
    assert!(matches!(
        session
            .metadata(MetadataRequest::MaintenanceStatus, None)
            .expect("the file answers the maintenance status")
            .body,
        MetadataBody::MaintenanceStatus { .. }
    ));

    // The state of a committed image is a question only a file can answer,
    // and this route is a file.
    for kind in [
        DirectImageMetadataKind::Sync,
        DirectImageMetadataKind::ChangeLog,
        DirectImageMetadataKind::Configuration,
    ] {
        assert!(
            matches!(
                session
                    .metadata(MetadataRequest::ImageState { kind }, None)
                    .expect("the file answers its own image state")
                    .body,
                MetadataBody::ImageState { .. }
            ),
            "the file answers image state {kind:?}"
        );
    }
}

#[test]
fn the_owner_answers_the_same_questions_in_the_same_words() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "served-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);

    let tables = session
        .metadata(MetadataRequest::Tables, None)
        .expect("the owner answers the table inventory");
    assert!(
        table_names(&tables.body).contains(&"documents".to_owned()),
        "the owner names the tables it has: {:?}",
        tables.body
    );

    let schema = session
        .metadata(
            MetadataRequest::Schema {
                table: "documents".to_owned(),
            },
            None,
        )
        .expect("the owner answers a table schema");
    let MetadataBody::Schema { schema } = &schema.body else {
        panic!("asked for a schema and got {:?}", schema.body);
    };
    assert_eq!(schema.table, "documents");
    assert!(
        schema.columns.iter().any(|column| column.name == "body"),
        "the schema that travelled the channel kept its columns"
    );

    let explained = session
        .metadata(
            MetadataRequest::Explain {
                sql: "SELECT id FROM documents".to_owned(),
                params: HashMap::new(),
            },
            None,
        )
        .expect("the owner explains a statement");
    assert!(matches!(explained.body, MetadataBody::Explain { .. }));

    assert!(matches!(
        session
            .metadata(MetadataRequest::EventsStatus, None)
            .expect("the owner answers the event inventory")
            .body,
        MetadataBody::EventsStatus { .. }
    ));
    assert!(matches!(
        session
            .metadata(MetadataRequest::MaintenanceStatus, None)
            .expect("the owner answers the maintenance status")
            .body,
        MetadataBody::MaintenanceStatus { .. }
    ));

    // An owner is not a committed file: its state is still moving, and the
    // channel carries no request for one. The question is refused by name
    // rather than answered with an empty state a caller would believe.
    assert_eq!(
        owner_route_image_state_refusal(&session, DirectImageMetadataKind::Sync).kind(),
        ReadFailureKind::OwnerRouteUnsupported,
    );

    drop(session);
    database.close().expect("the writer closes cleanly");
}

#[test]
fn both_routes_describe_the_same_store_the_same_way() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "parity-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let asked_the_owner = {
        let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
            ReadSession::open(&path)
        })
        .expect("a live owner is reachable");
        assert_eq!(session.route(), ReadRoute::Owner);
        let tables = session
            .metadata(MetadataRequest::Tables, None)
            .expect("the owner answers");
        let schema = session
            .metadata(
                MetadataRequest::Schema {
                    table: "documents".to_owned(),
                },
                None,
            )
            .expect("the owner answers");
        let events = session
            .metadata(MetadataRequest::EventsStatus, None)
            .expect("the owner answers");
        (tables.body, schema.body, events.body)
    };

    // The same store, now idle, asked the same questions.
    database.close().expect("the writer closes cleanly");
    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the store is still readable once its owner is gone");
    assert_eq!(session.route(), ReadRoute::File);

    let asked_the_file = (
        session
            .metadata(MetadataRequest::Tables, None)
            .expect("the file answers")
            .body,
        session
            .metadata(
                MetadataRequest::Schema {
                    table: "documents".to_owned(),
                },
                None,
            )
            .expect("the file answers")
            .body,
        session
            .metadata(MetadataRequest::EventsStatus, None)
            .expect("the file answers")
            .body,
    );

    assert_eq!(
        table_names(&asked_the_owner.0),
        table_names(&asked_the_file.0),
        "one store, one inventory, whoever answers"
    );
    assert_eq!(
        asked_the_owner.1, asked_the_file.1,
        "one store, one schema document, whoever answers"
    );

    // A store's schedules are part of what it does on its own. An owner that
    // reported none of them would be indistinguishable from a store that has
    // none, which is the answer a caller would believe.
    let owner_events = events(&asked_the_owner.2);
    let file_events = events(&asked_the_file.2);
    assert!(
        !file_events.schedules.is_empty(),
        "the fixture declares schedules, so there is something to be right about"
    );
    let mut owner_schedules = owner_events.schedules.clone();
    let mut file_schedules = file_events.schedules.clone();
    owner_schedules.sort_by(|left, right| left.name.cmp(&right.name));
    file_schedules.sort_by(|left, right| left.name.cmp(&right.name));
    assert_eq!(
        owner_schedules, file_schedules,
        "one store, one schedule inventory, whoever answers"
    );
    assert_eq!(
        owner_events.event_types, file_events.event_types,
        "one store, one set of event types, whoever answers"
    );
    assert_eq!(
        owner_events.sinks, file_events.sinks,
        "one store, one set of sinks, whoever answers"
    );
    assert_eq!(
        owner_events.routes, file_events.routes,
        "one store, one set of routes, whoever answers"
    );
}

/// A Context-, scope-label-, or principal-constrained handle is promised
/// a redacted `.explain` for its own query -- the same count and route its
/// own bound query reports locally. The owner socket honors that for the
/// OWNER-READ route (`owner_read_service_contract`). This proof is about the
/// door every OTHER reader uses: `ReadSession::metadata`, whichever route
/// answers it -- the live owner over its read-session channel, or the
/// committed file once nobody owns the store. Both plan a `.explain` through
/// `MetadataRequest::Explain { sql, params }`
/// (`direct_file_reader.rs`'s `DirectMetadataRequest::Explain`), which now
/// carries the caller's own binding alongside the statement, and
/// `read_session.rs`'s own `owner_explain` plans the statement with that same
/// binding -- so a constrained handle asking why ITS query routes as it does
/// gets the same bound answer, however it is routed.
#[test]
fn the_owner_route_and_the_file_route_both_explain_a_bound_partition_key_the_way_the_owner_plans_it_locally()
 {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "bound-explain-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    database
        .execute(
            "CREATE TABLE bound_docs (\
                id UUID PRIMARY KEY, \
                scope_id TEXT NOT NULL, \
                embedding VECTOR(3) PARTITION_KEY (scope_id) \
                    MAX_PARTITIONS 8 SEARCH_MODE AUTO\
            )",
            &HashMap::new(),
        )
        .expect("declare a partitioned AUTO-mode vector column");
    for (offset, scope, vector) in [
        (0_u128, "alpha-scope", vec![1.0_f32, 0.0, 0.0]),
        (1_u128, "alpha-scope", vec![0.9, 0.1, 0.0]),
        (2_u128, "bravo-scope", vec![0.0, 1.0, 0.0]),
        (3_u128, "bravo-scope", vec![0.0, 0.9, 0.1]),
    ] {
        database
            .execute(
                "INSERT INTO bound_docs (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Uuid(uuid::Uuid::from_u128(
                            0xB0B1_0000_0000_0000_0000_0000_0000_0000 + offset,
                        )),
                    ),
                    ("scope".to_owned(), Value::Text(scope.to_owned())),
                    ("embedding".to_owned(), Value::Vector(vector)),
                ]),
            )
            .expect("commit one fixture row");
    }

    let sql = "SELECT id FROM bound_docs WHERE scope_id = $scope \
               ORDER BY embedding <=> [1.0,0.0,0.0] USE VECTOR AUTO LIMIT 2";

    // Local ground truth: the same parameter-aware passive-planning path the
    // CLI's `.explain <sql>` command uses, with the key actually bound.
    let bound_params = HashMap::from([("scope".to_owned(), Value::Text("alpha-scope".to_owned()))]);
    let local_rendered =
        contextdb_engine::cli_render::render_explain(&database, sql, &bound_params)
            .expect("explaining a bound statement locally must succeed");
    let local_vector_search_line = local_rendered
        .lines()
        .find(|line| line.starts_with("vector_search "))
        .unwrap_or_else(|| {
            panic!("a vector-similarity SELECT discloses vector_search: {local_rendered}")
        })
        .to_owned();

    // The owner route: a constrained handle reaching the same live writer
    // over its read-session channel, asking to explain the identical bound
    // query, sending the same binding it bound locally.
    let owner_session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    let owner_explained = owner_session
        .metadata(
            MetadataRequest::Explain {
                sql: sql.to_owned(),
                params: bound_params.clone(),
            },
            None,
        )
        .expect("the owner explains the statement");
    let MetadataBody::Explain { vector_search, .. } = &owner_explained.body else {
        panic!("asked for an explain and got {:?}", owner_explained.body);
    };
    let owner_rendered = vector_search
        .as_ref()
        .expect("a vector-similarity SELECT reports a disclosure over the owner route too")
        .to_string();
    assert_eq!(
        owner_rendered, local_vector_search_line,
        "a constrained handle's `.explain` over the read-session owner route must report the \
         same bound count and route its own query reports locally: \
         local={local_vector_search_line:?} owner={owner_rendered:?}"
    );

    drop(owner_session);
    database.close().expect("the writer closes cleanly");

    // The direct-file route: nobody owns the store any more, so the reader
    // reads the committed file itself and must still answer the same way,
    // given the same binding.
    let file_session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the store is still readable once its owner is gone");
    assert_eq!(file_session.route(), ReadRoute::File);
    let file_explained = file_session
        .metadata(
            MetadataRequest::Explain {
                sql: sql.to_owned(),
                params: bound_params.clone(),
            },
            None,
        )
        .expect("the file explains the statement");
    let MetadataBody::Explain { vector_search, .. } = &file_explained.body else {
        panic!("asked for an explain and got {:?}", file_explained.body);
    };
    let file_rendered = vector_search
        .as_ref()
        .expect("a vector-similarity SELECT reports a disclosure over the direct-file route too")
        .to_string();
    assert_eq!(
        file_rendered, local_vector_search_line,
        "the direct-file route must also report the bound count and route a constrained \
         handle's own query reports locally: local={local_vector_search_line:?} \
         file={file_rendered:?}"
    );
}

#[test]
fn a_live_owner_returns_the_same_bounded_redacted_vector_trace_as_local_execution() {
    let directory = tempfile::TempDir::new().expect("task-scoped trace directory");
    let runtime_root = secure_runtime_root(directory.path(), "vector-trace-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());
    database
        .execute(
            "CREATE TABLE trace_docs (\
                id UUID PRIMARY KEY, \
                scope TEXT NOT NULL, \
                embedding VECTOR(3) PARTITION_KEY (scope) \
                    MAX_PARTITIONS 4 SEARCH_MODE EXACT\
            )",
            &HashMap::new(),
        )
        .expect("declare the vector trace fixture");
    for (id, scope, embedding) in [
        (1_u128, "alpha", vec![1.0, 0.0, 0.0]),
        (2_u128, "alpha", vec![0.9, 0.1, 0.0]),
        (3_u128, "hidden-value", vec![0.0, 1.0, 0.0]),
    ] {
        database
            .execute(
                "INSERT INTO trace_docs (id, scope, embedding) VALUES ($id, $scope, $embedding)",
                &HashMap::from([
                    ("id".to_owned(), Value::Uuid(uuid::Uuid::from_u128(id))),
                    ("scope".to_owned(), Value::Text(scope.to_owned())),
                    ("embedding".to_owned(), Value::Vector(embedding)),
                ]),
            )
            .expect("seed one trace vector");
    }
    let sql = "SELECT id FROM trace_docs WHERE scope = $scope \
               ORDER BY embedding <=> $query USE VECTOR EXACT LIMIT 2";
    let params = HashMap::from([
        ("scope".to_owned(), Value::Text("alpha".to_owned())),
        ("query".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0])),
    ]);
    let local = database
        .execute(sql, &params)
        .expect("the live writer executes the vector query");
    let local_trace = local
        .trace
        .vector_search
        .clone()
        .expect("local execution publishes its bounded vector disclosure");
    let rendered = local_trace.to_string();
    assert!(rendered.contains("partition=<redacted:1>"));
    assert!(!rendered.contains("alpha"));
    assert!(!rendered.contains("hidden-value"));

    let owner =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a second reader reaches the live owner");
    assert_eq!(owner.route(), ReadRoute::Owner);
    let owner_result = owner
        .execute(sql, &params)
        .expect("the live owner returns the executed query");
    assert_eq!(
        owner_result.trace.vector_search.as_ref(),
        Some(&local_trace),
        "the canonical owner result restores the exact bounded and redacted vector trace"
    );
    drop(owner);

    database.close().expect("close the vector trace writer");
    let file =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("the committed file remains readable");
    assert_eq!(file.route(), ReadRoute::File);
    let file_result = file
        .execute(sql, &params)
        .expect("the committed file executes the same query");
    assert_eq!(file_result.trace.vector_search.as_ref(), Some(&local_trace));
}

#[test]
fn a_continuation_is_refused_by_a_question_that_never_issues_one() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let path = idle_store(directory.path());
    let session = ReadSession::open(&path).expect("open the idle store");

    for request in [
        MetadataRequest::Schema {
            table: "documents".to_owned(),
        },
        MetadataRequest::Explain {
            sql: "SELECT id FROM documents".to_owned(),
            params: HashMap::new(),
        },
        MetadataRequest::MaintenanceStatus,
        MetadataRequest::ImageState {
            kind: DirectImageMetadataKind::Sync,
        },
    ] {
        let refused = session.metadata(request.clone(), Some("somewhere"));
        let Err(Error::ReadFailure(failure)) = &refused else {
            panic!("offering {request:?} a continuation was answered with {refused:?}");
        };
        assert_eq!(
            failure.kind(),
            ReadFailureKind::InvalidContinuation,
            "a continuation nobody issued is refused as one: {refused:?}"
        );
    }

    // The paged kinds do hand continuations out -- and each hands out its
    // own. A token one of them issued means a place in ITS inventory and
    // nothing at all in the other's, and a token nobody issued means nothing
    // anywhere; both used to start the caller over at the beginning or hand
    // back an empty page, neither of which a caller can tell from a real
    // answer.
    for request in [MetadataRequest::Tables, MetadataRequest::EventsStatus] {
        let refused = session.metadata(request.clone(), Some("documents"));
        let Err(Error::ReadFailure(failure)) = &refused else {
            panic!("offering {request:?} a token nobody issued was answered with {refused:?}");
        };
        assert_eq!(
            failure.kind(),
            ReadFailureKind::InvalidContinuation,
            "a token nobody issued is refused rather than read as the beginning: {refused:?}"
        );
    }

    // A ceiling small enough that the table inventory HAS to stop early, so
    // there is a real issued token to misuse -- a conditional check here
    // would quietly test nothing the day the fixture fits in one page.
    let paging = ReadSession::open_with_options(&path, paging_options())
        .expect("open the idle store under a paging ceiling");
    let issued = paging
        .metadata(MetadataRequest::Tables, None)
        .expect("the table inventory answers")
        .continuation
        .expect("this ceiling stops the inventory early, so it issues a token");

    let crossed = paging.metadata(MetadataRequest::EventsStatus, Some(&issued));
    let Err(Error::ReadFailure(failure)) = &crossed else {
        panic!("a table continuation offered to the event inventory answered {crossed:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::InvalidContinuation,
        "a continuation belongs to the question that issued it: {crossed:?}"
    );
    paging
        .metadata(MetadataRequest::Tables, Some(&issued))
        .expect("the question that issued it resumes from it");
}

#[test]
fn a_document_that_does_not_fit_the_caller_s_byte_ceiling_says_what_would_fit_it() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "ceiling-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    // A ceiling far too small for any real schema document. The owner's
    // answer is not "no", it is the number that would make it yes.
    let options = ReadSessionOptions {
        limits: ReadLimits {
            result_bytes: 64,
            // A page can never be larger than the whole answer, so the page
            // ceiling comes down with it.
            cursor_page_bytes: 64,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    };
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, options)
    })
    .expect("a live owner is reachable");

    let refused = session.metadata(
        MetadataRequest::Schema {
            table: "documents".to_owned(),
        },
        None,
    );
    let Err(Error::ReadFailure(failure)) = &refused else {
        panic!("a document past the ceiling was answered with {refused:?}");
    };
    assert_eq!(
        failure.kind(),
        ReadFailureKind::OwnerLimitExceeded,
        "the ceiling that stopped it is the answer: {refused:?}"
    );
    let contextdb_core::read_contract::ReadFailureDetail::OwnerLimitExceeded(detail) =
        failure.detail()
    else {
        panic!("the refusal carries no ceiling: {refused:?}");
    };
    let required = detail
        .required
        .as_ref()
        .expect("a caller is told the number that would fit their answer");
    assert!(
        required.required_bytes > 64,
        "the number offered is larger than the ceiling that refused: {required:?}"
    );

    drop(session);
    database.close().expect("the writer closes cleanly");
}

/// The whole table inventory, one page at a time, as a sequence of pages.
///
/// A caller that cannot hold the whole inventory reads it exactly this way:
/// ask, take the page, hand the continuation back, until there is nothing
/// left to resume from.
fn drain_the_table_inventory(session: &ReadSession) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut resume: Option<String> = None;
    loop {
        let answer = session
            .metadata(MetadataRequest::Tables, resume.as_deref())
            .expect("a page of the table inventory");
        pages.push(table_names(&answer.body));
        match answer.continuation {
            Some(next) => resume = Some(next),
            None => break,
        }
        assert!(
            pages.len() < 100,
            "a paged inventory must finish: {pages:?}"
        );
    }
    pages
}

/// A byte ceiling too small to hold the fixture's whole table inventory, so
/// reading it has to page.
fn paging_options() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: ReadLimits {
            result_bytes: 256,
            cursor_page_bytes: 256,
            ..ReadLimits::default()
        },
        ..ReadSessionOptions::default()
    }
}

#[test]
fn a_table_inventory_too_big_for_one_answer_pages_the_same_way_on_both_routes() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "paging-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let owner_pages = {
        let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
            ReadSession::open_with_options(&path, paging_options())
        })
        .expect("a live owner is reachable");
        assert_eq!(session.route(), ReadRoute::Owner);
        drain_the_table_inventory(&session)
    };
    assert!(
        owner_pages.len() > 1,
        "the fixture's inventory does not fit one 256-byte answer, so it pages: {owner_pages:?}"
    );

    // The same store, now idle, read the same way.
    database.close().expect("the writer closes cleanly");
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, paging_options())
    })
    .expect("the store is still readable once its owner is gone");
    assert_eq!(session.route(), ReadRoute::File);
    let file_pages = drain_the_table_inventory(&session);

    assert_eq!(
        owner_pages, file_pages,
        "one store, one inventory, cut at the same places whoever answers"
    );

    // And paging loses nothing: every table is read exactly once.
    let mut all: Vec<String> = file_pages.concat();
    let read_once = all.len();
    all.sort();
    all.dedup();
    assert_eq!(
        all.len(),
        read_once,
        "a resumed page never repeats an item it already published"
    );
    let whole =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("open the same store with room for the whole answer");
    let mut unpaged = table_names(
        &whole
            .metadata(MetadataRequest::Tables, None)
            .expect("the whole inventory in one answer")
            .body,
    );
    unpaged.sort();
    assert_eq!(
        all, unpaged,
        "reading the inventory a page at a time reads the same inventory"
    );
}

#[test]
fn a_continuation_from_one_route_resumes_the_same_place_on_the_other() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "cross-route-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    // Where the owner said it stopped.
    let (first_page, resume) = {
        let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
            ReadSession::open_with_options(&path, paging_options())
        })
        .expect("a live owner is reachable");
        let answer = session
            .metadata(MetadataRequest::Tables, None)
            .expect("the owner's first page");
        (
            table_names(&answer.body),
            answer.continuation.expect("the owner has more to give"),
        )
    };

    // Handed to the same store as an idle file, it resumes from exactly
    // there -- a continuation names a place in the inventory, not a place in
    // one route's bookkeeping.
    database.close().expect("the writer closes cleanly");
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&path, paging_options())
    })
    .expect("the store is still readable once its owner is gone");
    let resumed = session
        .metadata(MetadataRequest::Tables, Some(&resume))
        .expect("the file resumes where the owner stopped");
    let resumed_names = table_names(&resumed.body);
    assert!(
        !resumed_names.is_empty(),
        "resuming produces the rest of the inventory"
    );
    // The token itself is the route-neutral handle, not something a caller
    // reads; what it names is the last item the first page published, so the
    // resumed page has to start strictly after THAT.
    let stopped_after = first_page
        .last()
        .cloned()
        .expect("the first page published something");
    for name in &resumed_names {
        assert!(
            !first_page.contains(name),
            "{name} was already published before the continuation"
        );
        assert!(
            name.as_str() > stopped_after.as_str(),
            "a resumed page starts strictly after the item it resumed from"
        );
    }
    assert!(
        resume.ends_with(&stopped_after),
        "the continuation names the item it stopped after: {resume}"
    );
}

/// The stable word the refusal carries for the inspection the live owner's
/// channel does not answer.
const IMAGE_STATE_INSPECTION: &str = "image_state";

/// Asks the owner for a committed image's state and returns the refusal it
/// owes, so both the parity arm above and the pin below read the same thing.
fn owner_route_image_state_refusal(
    session: &ReadSession,
    kind: DirectImageMetadataKind,
) -> ReadFailure {
    let refused = session.metadata(MetadataRequest::ImageState { kind }, None);
    match refused {
        Err(Error::ReadFailure(failure)) => failure,
        other => panic!("the owner refuses an image-state question as a read failure: {other:?}"),
    }
}

/// A refusal has to say what was refused. "Not implemented" on its own leaves
/// a caller with no idea which question failed or where to ask it instead, so
/// asking a live owner for a committed image's state names the inspection the
/// owner route cannot answer and points at the route that can: a direct file
/// session, once no writer holds the store. The file route answering the same
/// three questions is the control -- without it, a reader that had simply
/// stopped answering image-state everywhere would satisfy the refusal.
#[test]
fn an_image_state_question_over_the_owner_route_names_what_it_cannot_answer() {
    let directory = tempfile::TempDir::new().expect("task-scoped metadata directory");
    let runtime_root = secure_runtime_root(directory.path(), "unsupported-runtime");
    let (database, path) = served_store(directory.path(), runtime_root.clone());

    let session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(&path))
            .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);

    for kind in [
        DirectImageMetadataKind::Sync,
        DirectImageMetadataKind::ChangeLog,
        DirectImageMetadataKind::Configuration,
    ] {
        let failure = owner_route_image_state_refusal(&session, kind);
        assert_eq!(
            failure.kind(),
            ReadFailureKind::OwnerRouteUnsupported,
            "an image-state question the owner route cannot answer is refused by its own kind"
        );
        assert_eq!(
            failure.detail(),
            &ReadFailureDetail::OwnerRouteUnsupported(OwnerRouteUnsupportedDetail {
                inspection: IMAGE_STATE_INSPECTION.to_owned(),
            }),
            "the refusal names the inspection it could not answer, for {kind:?}"
        );
        let message = failure.to_string();
        assert!(
            message.contains("image state"),
            "the refusal says which question was refused: {message}"
        );
        assert!(
            message.contains("file"),
            "the refusal says a direct file session answers it: {message}"
        );
    }

    drop(session);
    database.close().expect("the writer closes cleanly");

    // The control: the same store, with no writer holding it, answers every
    // one of those questions.
    let file = ReadSession::open(&path).expect("open the closed store as a file");
    assert_eq!(file.route(), ReadRoute::File);
    for kind in [
        DirectImageMetadataKind::Sync,
        DirectImageMetadataKind::ChangeLog,
        DirectImageMetadataKind::Configuration,
    ] {
        assert!(
            matches!(
                file.metadata(MetadataRequest::ImageState { kind }, None)
                    .expect("the file answers its own image state")
                    .body,
                MetadataBody::ImageState { .. }
            ),
            "the file route still answers image state {kind:?}"
        );
    }
}
