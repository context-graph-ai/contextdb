//! Row visibility answers the same way on every read route.
//!
//! An access-controlled table declares `acl_id UUID ACL REFERENCES
//! acl_grants(acl_id)`. Authorization for such a table is a property of the
//! table, not an axis a reader opts into: a handle that narrowed itself by
//! context or scope and named no principal that can hold grants is refused the
//! table rather than served its rows unfiltered. A table that declares no ACL
//! column is unaffected and keeps narrowing exactly by the axes it declares.

use contextdb_core::read_contract::{ReadLimits, ReadRoute};
use contextdb_core::{ContextId, Error, Principal, Value};
use contextdb_engine::{Database, QueryResult, ReadSession, ReadSessionOptions};
use std::collections::{BTreeSet, HashMap};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const CONTROLLED_TABLE: &str = "parity_controlled_rows";
const PLAIN_TABLE: &str = "parity_plain_rows";

struct ParityFixture {
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
    allowed_context: ContextId,
    principal: Principal,
    granted_here: String,
    granted_elsewhere: String,
    ungranted_here: String,
    plain_here: String,
    plain_elsewhere: String,
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create the task-scoped read runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped read runtime root");
    root
}

/// Identities derived from the declared read policy so the fixture carries no
/// invented constants.
fn identity_seed() -> u128 {
    let limits = ReadLimits::default();
    u128::from(limits.result_rows)
        .saturating_mul(1_000_003)
        .saturating_add(u128::from(limits.result_bytes))
}

fn parity_fixture() -> ParityFixture {
    let directory = tempfile::TempDir::new().expect("task-scoped route-parity directory");
    let path = directory.path().join("route-parity.db");
    let seed = identity_seed();
    let allowed_context = uuid::Uuid::from_u128(seed.saturating_add(1));
    let foreign_context = uuid::Uuid::from_u128(seed.saturating_add(2));
    let granted_acl = uuid::Uuid::from_u128(seed.saturating_add(3));
    let ungranted_acl = uuid::Uuid::from_u128(seed.saturating_add(4));
    let principal_name = format!("route-parity-reader-{}", ReadLimits::default().work);
    let principal = Principal::Agent(principal_name.clone());

    let admin = Database::open(&path).expect("create the file-backed route-parity fixture");
    admin
        .execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &HashMap::new(),
        )
        .expect("the administrative owner creates the grant table");
    admin
        .execute(
            &format!(
                "CREATE TABLE {CONTROLLED_TABLE} (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the access-controlled table");
    admin
        .execute(
            &format!(
                "CREATE TABLE {PLAIN_TABLE} (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the table without an ACL declaration");
    admin
        .execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
            &HashMap::from([
                (
                    "id".to_owned(),
                    Value::Uuid(uuid::Uuid::from_u128(seed.saturating_add(5))),
                ),
                ("principal".to_owned(), Value::Text(principal_name)),
                ("acl".to_owned(), Value::Uuid(granted_acl)),
            ]),
        )
        .expect("the administrative owner grants one ACL to the reader");

    let granted_here = "granted-here".to_owned();
    let granted_elsewhere = "granted-elsewhere".to_owned();
    let ungranted_here = "ungranted-here".to_owned();
    for (id, context, acl, payload) in [
        (0i64, allowed_context, granted_acl, granted_here.clone()),
        (1, foreign_context, granted_acl, granted_elsewhere.clone()),
        (2, allowed_context, ungranted_acl, ungranted_here.clone()),
    ] {
        admin
            .execute(
                &format!(
                    "INSERT INTO {CONTROLLED_TABLE} (id, context_id, acl_id, payload) VALUES ($id, $context, $acl, $payload)"
                ),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("context".to_owned(), Value::Uuid(context)),
                    ("acl".to_owned(), Value::Uuid(acl)),
                    ("payload".to_owned(), Value::Text(payload)),
                ]),
            )
            .expect("the administrative owner inserts an access-controlled row");
    }

    let plain_here = "plain-here".to_owned();
    let plain_elsewhere = "plain-elsewhere".to_owned();
    for (id, context, payload) in [
        (0i64, allowed_context, plain_here.clone()),
        (1, foreign_context, plain_elsewhere.clone()),
    ] {
        admin
            .execute(
                &format!(
                    "INSERT INTO {PLAIN_TABLE} (id, context_id, payload) VALUES ($id, $context, $payload)"
                ),
                &HashMap::from([
                    ("id".to_owned(), Value::Int64(id)),
                    ("context".to_owned(), Value::Uuid(context)),
                    ("payload".to_owned(), Value::Text(payload)),
                ]),
            )
            .expect("the administrative owner inserts a row in the unrestricted table");
    }
    admin
        .close()
        .expect("release the idle store for the direct route");

    ParityFixture {
        directory,
        path,
        allowed_context: ContextId::new(allowed_context),
        principal,
        granted_here,
        granted_elsewhere,
        ungranted_here,
        plain_here,
        plain_elsewhere,
    }
}

fn payloads(result: QueryResult) -> BTreeSet<String> {
    result
        .rows
        .into_iter()
        .map(|row| match row.as_slice() {
            [Value::Text(payload)] => payload.clone(),
            other => panic!("payload projection returned {other:?}"),
        })
        .collect()
}

fn select_payloads(table: &str) -> String {
    format!("SELECT payload FROM {table} ORDER BY id")
}

fn owner(fixture: &ParityFixture) -> Database {
    Database::open(&fixture.path).expect("reopen the route-parity store as its owner")
}

/// Route 3: the store is closed, so the session selects the direct file
/// backend and answers from the committed image.
fn direct_payloads(fixture: &ParityFixture, table: &str) -> BTreeSet<String> {
    let runtime_root = secure_runtime_root(&fixture.directory, &format!("direct-{table}"));
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&fixture.path, ReadSessionOptions::default())
    })
    .expect("an idle store selects the direct route");
    assert_eq!(session.route(), ReadRoute::File);
    session
        .execute(&select_payloads(table), &HashMap::new())
        .map(payloads)
        .expect("the direct route answers the bounded query")
}

fn assert_principal_required(error: &Error) {
    assert!(
        matches!(error, Error::PrincipalRequired { table } if table == CONTROLLED_TABLE),
        "the access-controlled table reported {error:?}"
    );
}

#[test]
fn a_context_only_handle_is_refused_an_access_controlled_table_on_both_live_routes() {
    let fixture = parity_fixture();
    let owner = owner(&fixture);
    let constrained =
        owner.scoped_with_constraints(Some(BTreeSet::from([fixture.allowed_context])), None, None);

    let ordinary = constrained
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .expect_err("an ordinary read of an access-controlled table names no principal");
    assert_principal_required(&ordinary);

    let bounded = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-owner session from the constrained handle")
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .expect_err("a bounded read of an access-controlled table names no principal");
    assert_principal_required(&bounded);

    owner.close().expect("close the route-parity owner");
}

#[test]
fn a_system_principal_is_refused_an_access_controlled_table_on_both_live_routes() {
    let fixture = parity_fixture();
    let owner = owner(&fixture);
    let constrained = owner.scoped_with_constraints(None, None, Some(Principal::System));

    let ordinary = constrained
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .expect_err("a principal that cannot hold grants reads no access-controlled row");
    assert_principal_required(&ordinary);

    let bounded = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-owner session from the constrained handle")
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .expect_err("a principal that cannot hold grants reads no access-controlled row");
    assert_principal_required(&bounded);

    owner.close().expect("close the route-parity owner");
}

#[test]
fn a_granting_principal_sees_the_same_rows_on_both_live_routes() {
    let fixture = parity_fixture();
    let owner = owner(&fixture);
    let constrained = owner.scoped_with_constraints(None, None, Some(fixture.principal.clone()));
    let granted = BTreeSet::from([
        fixture.granted_here.clone(),
        fixture.granted_elsewhere.clone(),
    ]);

    let ordinary = constrained
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the ordinary route answers the granted rows");
    let bounded = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-owner session from the constrained handle")
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the bounded route answers the granted rows");

    assert_eq!(ordinary, granted);
    assert_eq!(bounded, granted);
    assert!(!ordinary.contains(&fixture.ungranted_here));
    assert!(!bounded.contains(&fixture.ungranted_here));

    owner.close().expect("close the route-parity owner");
}

#[test]
fn a_context_and_principal_handle_hides_the_foreign_context_and_ungranted_rows_on_both_live_routes()
{
    let fixture = parity_fixture();
    let owner = owner(&fixture);
    let constrained = owner.scoped_with_constraints(
        Some(BTreeSet::from([fixture.allowed_context])),
        None,
        Some(fixture.principal.clone()),
    );
    let expected = BTreeSet::from([fixture.granted_here.clone()]);

    let ordinary = constrained
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the ordinary route answers the narrowed granted rows");
    let bounded = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-owner session from the constrained handle")
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the bounded route answers the narrowed granted rows");

    assert_eq!(ordinary, expected);
    assert_eq!(bounded, expected);

    owner.close().expect("close the route-parity owner");
}

#[test]
fn a_table_without_an_acl_declaration_narrows_by_context_alone_on_every_route() {
    let fixture = parity_fixture();
    let direct = direct_payloads(&fixture, PLAIN_TABLE);
    assert_eq!(
        direct,
        BTreeSet::from([fixture.plain_here.clone(), fixture.plain_elsewhere.clone()]),
        "the direct route reads the committed image as the store's own owner"
    );

    let owner = owner(&fixture);
    let constrained =
        owner.scoped_with_constraints(Some(BTreeSet::from([fixture.allowed_context])), None, None);
    let expected = BTreeSet::from([fixture.plain_here.clone()]);

    let ordinary = constrained
        .execute(&select_payloads(PLAIN_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the ordinary route narrows the unrestricted table by context");
    let bounded = constrained
        .read_session(ReadLimits::default())
        .expect("derive a bounded live-owner session from the constrained handle")
        .execute(&select_payloads(PLAIN_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the bounded route narrows the unrestricted table by context");

    assert_eq!(ordinary, expected);
    assert_eq!(bounded, expected);

    owner.close().expect("close the route-parity owner");
}

#[test]
fn the_direct_route_reads_a_closed_store_as_its_owner_with_no_declared_narrowing() {
    let fixture = parity_fixture();
    let direct = direct_payloads(&fixture, CONTROLLED_TABLE);
    assert_eq!(
        direct,
        BTreeSet::from([
            fixture.granted_here.clone(),
            fixture.granted_elsewhere.clone(),
            fixture.ungranted_here.clone(),
        ]),
        "a direct read of a closed store declares no principal and no narrowing, so it answers as the owner does"
    );

    let owner = owner(&fixture);
    let administrative = owner
        .execute(&select_payloads(CONTROLLED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the administrative owner reads every access-controlled row");
    assert_eq!(direct, administrative);
    owner.close().expect("close the route-parity owner");
}
