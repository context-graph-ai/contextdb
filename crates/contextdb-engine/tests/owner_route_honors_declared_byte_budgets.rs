//! What a reader is allowed to receive is the budget its session declared.
//!
//! `result_bytes` and `cursor_page_bytes` are the thresholds a caller and a
//! writer state out loud, and they are the only ones a reader can be refused
//! by. The four-mebibyte local frame is an internal fact about how the owner's
//! channel moves bytes -- ordinary results already cut themselves into as many
//! frames as they need, so the frame ceiling never reaches a caller as a
//! refusal there.
//!
//! Cursor pages and metadata answers travel in one frame apiece, so a page or
//! an inspection answer that is legitimately larger than that frame -- and
//! well inside the budget the session declared -- dies mid-session as
//! `invalid_channel_data` on the owner route while the very same question
//! answers cleanly from the committed file. Same store, same declared budget,
//! two different answers depending on who is holding the store.
//!
//! These journeys pin the contract: the declared budget governs, both routes
//! publish the same bytes, and a payload that really is over the declared
//! budget still refuses by naming that budget.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_core::read_contract::{
    OwnerReadLimits, ReadFailureKind, ReadFailureLimit, ReadLimits, ReadRoute,
};
use contextdb_core::{Error, Value};
use contextdb_engine::read_contract::encode_cursor_page;
use contextdb_engine::{
    Database, DatabaseOpenOptions, MetadataBody, MetadataRequest, OwnerReadConfig, ReadSession,
    ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

const MIB: u64 = 1024 * 1024;
/// Declared page and result budgets, deliberately above the local frame so a
/// legitimate answer between the two sizes exists at all.
const DECLARED_BYTES: u64 = 8 * MIB;
/// Rows sized so one cursor page lands between the frame ceiling and the
/// declared budget.
const WIDE_ROW_BYTES: usize = 1024 * 1024;
const WIDE_ROWS: usize = 5;
/// One payload the declared budget genuinely refuses.
const OVER_BUDGET_BYTES: usize = 9 * 1024 * 1024;
/// Table names long enough that the store's own inventory of itself cannot
/// fit in a single local frame either.
const LONG_NAME_BYTES: usize = 1_500_000;
const LONG_NAMED_TABLES: usize = 4;

const WIDE_TABLE: &str = "wide_pages";
const OVER_TABLE: &str = "over_budget_rows";
const SELECT_WIDE: &str = "SELECT body FROM wide_pages ORDER BY id";
const SELECT_OVER: &str = "SELECT body FROM over_budget_rows ORDER BY id";

fn declared_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 500,
        result_bytes: DECLARED_BYTES,
        work: 50_000,
        active_ms: 60_000,
        memory: 96 * MIB,
        cursor_page_rows: 100,
        cursor_page_bytes: DECLARED_BYTES,
        cursor_idle_ms: 300_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

fn declared_session() -> ReadSessionOptions {
    ReadSessionOptions {
        limits: declared_limits(),
        ..ReadSessionOptions::default()
    }
}

fn secure_runtime_root(directory: &Path, name: &str) -> PathBuf {
    let root = directory.join(name);
    std::fs::create_dir(&root).expect("create the task-scoped owner runtime root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped owner runtime root");
    root
}

fn filler(bytes: usize) -> String {
    "w".repeat(bytes)
}

/// A legal identifier of the requested length, distinct per index.
fn long_table_name(index: usize) -> String {
    let suffix = format!("_{index}");
    format!(
        "inventory_{}{suffix}",
        "n".repeat(LONG_NAME_BYTES - suffix.len() - "inventory_".len())
    )
}

/// What the published table inventory costs in item bytes -- the measurement
/// that decides whether this fixture crosses the local frame at all.
fn inventory_bytes(body: &MetadataBody) -> usize {
    match body {
        MetadataBody::Tables { items, .. } => items.iter().map(String::len).sum(),
        other => panic!("asked for the table inventory and got {other:?}"),
    }
}

/// A store whose rows are wide enough that one page of them cannot fit a
/// single local frame, plus one row wider than the declared budget itself,
/// plus a table whose complete schema answer is larger than that budget.
fn seed(database: &Database) {
    database
        .execute(
            &format!("CREATE TABLE {WIDE_TABLE} (id INTEGER PRIMARY KEY, body TEXT)"),
            &HashMap::new(),
        )
        .expect("create the wide-row table");
    for row in 0..WIDE_ROWS {
        database
            .execute(
                &format!("INSERT INTO {WIDE_TABLE} (id, body) VALUES ($id, $body)"),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(row as i64)),
                    ("body".to_owned(), Value::Text(filler(WIDE_ROW_BYTES))),
                ]),
            )
            .expect("store one wide row");
    }
    database
        .execute(
            &format!("CREATE TABLE {OVER_TABLE} (id INTEGER PRIMARY KEY, body TEXT)"),
            &HashMap::new(),
        )
        .expect("create the over-budget table");
    database
        .execute(
            &format!("INSERT INTO {OVER_TABLE} (id, body) VALUES ($id, $body)"),
            &HashMap::from([
                ("id".to_owned(), Value::Int64(0)),
                ("body".to_owned(), Value::Text(filler(OVER_BUDGET_BYTES))),
            ]),
        )
        .expect("store the over-budget row");
    for index in 0..LONG_NAMED_TABLES {
        database
            .execute(
                &format!(
                    "CREATE TABLE {} (id INTEGER PRIMARY KEY)",
                    long_table_name(index)
                ),
                &HashMap::new(),
            )
            .expect("create a table whose name alone weighs on the inventory");
    }
}

/// A writer serving owner reads under the same declared budgets the reader
/// states, so the effective ceiling is the declared one on both sides.
fn served_store(directory: &Path, runtime_root: PathBuf) -> (Database, PathBuf) {
    let path = directory.join("budgets.db");
    let database = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root),
                limits: OwnerReadLimits {
                    limits: declared_limits(),
                    concurrency: 4,
                },
                ..OwnerReadConfig::default()
            },
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open a writer that serves owner reads under the declared budgets");
    seed(&database);
    (database, path)
}

fn owner_session(runtime_root: &Path, path: &Path) -> ReadSession {
    let session = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::open_with_options(path, declared_session())
    })
    .expect("a live owner is reachable");
    assert_eq!(session.route(), ReadRoute::Owner);
    session
}

fn file_session(runtime_root: &Path, path: &Path) -> ReadSession {
    let session = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::open_with_options(path, declared_session())
    })
    .expect("the committed file is readable once nobody holds it");
    assert_eq!(session.route(), ReadRoute::File);
    session
}

fn refusal_kind(error: &Error) -> ReadFailureKind {
    match error {
        Error::ReadFailure(failure) => failure.kind(),
        other => panic!("expected a typed read refusal, got {other:?}"),
    }
}

/// A cursor page between the local frame and the declared page budget is the
/// owner's to serve, and it is the same page the committed file publishes.
#[test]
fn a_cursor_page_inside_the_declared_page_budget_is_served_over_the_owner_route() {
    let directory = tempfile::TempDir::new().expect("task-scoped byte-budget directory");
    let owner_runtime = secure_runtime_root(directory.path(), "owner-runtime");
    let (database, path) = served_store(directory.path(), owner_runtime.clone());

    let over_the_channel = {
        let session = owner_session(&owner_runtime, &path);
        let cursor = session.open_cursor(SELECT_WIDE, &HashMap::new());
        cursor.map(|cursor| cursor.first_page().clone())
    };

    drop(database);
    let file_runtime = secure_runtime_root(directory.path(), "file-runtime");
    let from_the_file = {
        let session = file_session(&file_runtime, &path);
        session
            .open_cursor(SELECT_WIDE, &HashMap::new())
            .expect("the committed file serves the same page")
            .first_page()
            .clone()
    };

    let file_bytes = encode_cursor_page(&from_the_file).expect("encode the file route's page");
    assert!(
        u64::try_from(file_bytes.len()).expect("page size fits") > 4 * MIB,
        "the fixture must produce a page larger than one local frame; it produced {} bytes",
        file_bytes.len()
    );
    assert!(
        u64::try_from(file_bytes.len()).expect("page size fits") <= DECLARED_BYTES,
        "the fixture must stay inside the declared page budget; it produced {} bytes",
        file_bytes.len()
    );

    let over_the_channel = over_the_channel.unwrap_or_else(|error| {
        panic!(
            "the owner must serve a page its session declared room for; it refused with {error:?}"
        )
    });
    assert_eq!(
        encode_cursor_page(&over_the_channel).expect("encode the owner route's page"),
        file_bytes,
        "both routes publish the same page bytes"
    );
}

/// The same contract for the inspection answers that ship in one piece: a
/// complete metadata answer inside the declared result budget is served, and
/// it is the answer the committed file gives.
#[test]
fn a_metadata_answer_inside_the_declared_result_budget_is_served_over_the_owner_route() {
    let directory = tempfile::TempDir::new().expect("task-scoped byte-budget directory");
    let owner_runtime = secure_runtime_root(directory.path(), "owner-runtime");
    let (database, path) = served_store(directory.path(), owner_runtime.clone());

    let over_the_channel = {
        let session = owner_session(&owner_runtime, &path);
        session
            .metadata(MetadataRequest::Tables, None)
            .map(|answer| answer.body)
    };

    drop(database);
    let file_runtime = secure_runtime_root(directory.path(), "file-runtime");
    let from_the_file = {
        let session = file_session(&file_runtime, &path);
        session
            .metadata(MetadataRequest::Tables, None)
            .expect("the committed file answers the table inventory")
            .body
    };

    let published = inventory_bytes(&from_the_file);
    assert!(
        u64::try_from(published).expect("inventory size fits") > 4 * MIB,
        "the fixture must produce an inventory larger than one local frame; it produced {published} bytes"
    );
    assert!(
        u64::try_from(published).expect("inventory size fits") <= DECLARED_BYTES,
        "the fixture must stay inside the declared result budget; it produced {published} bytes"
    );

    let over_the_channel = over_the_channel.unwrap_or_else(|error| {
        panic!(
            "the owner must answer an inspection its session declared room for; it refused with \
             {error:?}"
        )
    });
    assert_eq!(
        over_the_channel, from_the_file,
        "both routes describe the store with the same body"
    );
}

/// The control: a payload that really is over the declared budget keeps
/// refusing by naming that budget, never as a malformed frame.
#[test]
fn a_page_over_the_declared_budget_still_refuses_by_naming_the_budget() {
    let directory = tempfile::TempDir::new().expect("task-scoped byte-budget directory");
    let owner_runtime = secure_runtime_root(directory.path(), "owner-runtime");
    let (database, path) = served_store(directory.path(), owner_runtime.clone());

    let refusal = {
        let session = owner_session(&owner_runtime, &path);
        session
            .open_cursor(SELECT_OVER, &HashMap::new())
            .err()
            .expect("a row wider than the declared page budget cannot be paged")
    };

    assert_eq!(
        refusal_kind(&refusal),
        ReadFailureKind::OwnerLimitExceeded,
        "an over-budget page is a declared-ceiling refusal, not a channel-data one: {refusal:?}"
    );
    let Error::ReadFailure(failure) = &refusal else {
        unreachable!("the kind assertion above proves this is a typed refusal");
    };
    assert!(
        format!("{:?}", failure.detail())
            .contains(&format!("{:?}", ReadFailureLimit::CursorPageBytes)),
        "the refusal names the page-byte budget it crossed: {failure:?}"
    );

    database.close().expect("the writer closes cleanly");
}
