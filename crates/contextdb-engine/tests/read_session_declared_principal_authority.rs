//! A read session that declares WHO it reads as is answered as that identity,
//! or told plainly that it cannot be.
//!
//! Contexts and scope labels are sets, so a reader asking for more than the
//! writer holds is answered the intersection and the declaration can only take
//! rows away. Two principals are not sets: `Agent("service")` and
//! `Agent("tenant-a")` hold whatever grants each was given, and neither
//! identity is inside the other, so there is no intersection to serve and
//! "keep the writer's" is not a narrowing -- it hands the reader every row the
//! WRITER's grants open up, which is strictly more than the reader declared.
//!
//! So the principal axis has its own rule, and these are its proofs. A writer
//! that named no principal HONORS the reader's declaration, exactly as the
//! direct-file route does on the committed image. A writer that named the same
//! principal serves it unchanged. A writer that named a DIFFERENT principal
//! refuses the session at admission with a refusal the reader can read --
//! never a silently widened view. A reader that declares no principal at all
//! keeps reading under the writer's own, which is what it does today.

use contextdb_core::read_contract::{ReadFailureKind, ReadLimits, ReadRoute};
use contextdb_core::{ContextId, Error, Principal, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, QueryResult, ReadSession, ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const PRINCIPAL_TABLE: &str = "principal_rows";

/// Identities derived from the declared read policy so the fixture carries no
/// invented constants.
fn identity_seed() -> u128 {
    let limits = ReadLimits::default();
    u128::from(limits.result_rows)
        .saturating_mul(1_000_033)
        .saturating_add(u128::from(limits.cursor_page_rows))
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create the task-scoped read runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped read runtime root");
    root
}

/// Two identities over one access-controlled table: the identity a writer is
/// opened as holds both grants, and the identity a reader declares holds one
/// of them.
struct PrincipalFixture {
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
    /// The principal the serving writer is opened as; holds both grants.
    writer_principal: Principal,
    /// The principal a read session declares; holds the shared grant only.
    reader_principal: Principal,
    here: ContextId,
    /// Rows under the grant BOTH identities hold.
    shared_rows: BTreeSet<String>,
    /// Rows under the grant only the writer's identity holds.
    writer_only_rows: BTreeSet<String>,
    /// The shared-grant row inside the near context.
    shared_here: String,
    /// The writer-only-grant row inside the near context.
    writer_only_here: String,
}

impl PrincipalFixture {
    /// Every row in the table, which is what the writer's own identity reads.
    fn every_row(&self) -> BTreeSet<String> {
        self.shared_rows
            .union(&self.writer_only_rows)
            .cloned()
            .collect()
    }

    /// What the near context holds under the writer's own identity.
    fn every_row_here(&self) -> BTreeSet<String> {
        BTreeSet::from([self.shared_here.clone(), self.writer_only_here.clone()])
    }
}

fn principal_fixture() -> PrincipalFixture {
    let directory = tempfile::TempDir::new().expect("task-scoped declared-principal directory");
    let path = directory.path().join("declared-principal.db");
    let seed = identity_seed();
    let limits = ReadLimits::default();
    let here = uuid::Uuid::from_u128(seed.saturating_add(1));
    let elsewhere = uuid::Uuid::from_u128(seed.saturating_add(2));
    let shared_acl = uuid::Uuid::from_u128(seed.saturating_add(3));
    let writer_only_acl = uuid::Uuid::from_u128(seed.saturating_add(4));
    let writer_name = format!("service-{}", limits.work);
    let reader_name = format!("tenant-a-{}", limits.work);

    let admin = Database::open(&path).expect("create the file-backed declared-principal fixture");
    admin
        .execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &HashMap::new(),
        )
        .expect("the administrative owner creates the grant table");
    admin
        .execute(
            &format!(
                "CREATE TABLE {PRINCIPAL_TABLE} (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the access-controlled table");

    // The writer's identity holds both grants; the reader's identity holds the
    // shared one alone, so every writer-only row is a row the reader declared
    // itself out of.
    for (grant, principal_id, acl) in [
        (5u128, writer_name.clone(), shared_acl),
        (6, writer_name.clone(), writer_only_acl),
        (7, reader_name.clone(), shared_acl),
    ] {
        admin
            .execute(
                "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) VALUES ($id, 'Agent', $principal, $acl)",
                &HashMap::from([
                    (
                        "id".to_owned(),
                        Value::Uuid(uuid::Uuid::from_u128(seed.saturating_add(grant))),
                    ),
                    ("principal".to_owned(), Value::Text(principal_id)),
                    ("acl".to_owned(), Value::Uuid(acl)),
                ]),
            )
            .expect("the administrative owner records one access-list grant");
    }

    let shared_here = "shared-grant-here".to_owned();
    let shared_elsewhere = "shared-grant-elsewhere".to_owned();
    let writer_only_here = "writer-grant-here".to_owned();
    let writer_only_elsewhere = "writer-grant-elsewhere".to_owned();
    for (id, context, acl, payload) in [
        (0i64, here, shared_acl, shared_here.clone()),
        (1, elsewhere, shared_acl, shared_elsewhere.clone()),
        (2, here, writer_only_acl, writer_only_here.clone()),
        (3, elsewhere, writer_only_acl, writer_only_elsewhere.clone()),
    ] {
        admin
            .execute(
                &format!(
                    "INSERT INTO {PRINCIPAL_TABLE} (id, context_id, acl_id, payload) VALUES ($id, $context, $acl, $payload)"
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
    admin
        .close()
        .expect("release the idle store so the direct route may read it");

    PrincipalFixture {
        directory,
        path,
        writer_principal: Principal::Agent(writer_name),
        reader_principal: Principal::Agent(reader_name),
        here: ContextId::new(here),
        shared_rows: BTreeSet::from([shared_here.clone(), shared_elsewhere]),
        writer_only_rows: BTreeSet::from([writer_only_here.clone(), writer_only_elsewhere]),
        shared_here,
        writer_only_here,
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

fn select_payloads() -> String {
    format!("SELECT payload FROM {PRINCIPAL_TABLE} ORDER BY id")
}

/// The declaration a read session makes about who it reads as.
fn declaring(
    contexts: Option<BTreeSet<ContextId>>,
    principal: Option<Principal>,
) -> ReadSessionOptions {
    ReadSessionOptions {
        contexts,
        principal,
        ..ReadSessionOptions::default()
    }
}

/// A writer that serves owner reads out of a task-scoped runtime root.
fn owner_writer(
    fixture: &PrincipalFixture,
    runtime_root: &std::path::Path,
    principal: Option<Principal>,
) -> Database {
    Database::open_with_options(
        &fixture.path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            principal,
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("open the writer that serves the declared-principal reads")
}

/// Route 2: a live writer holds the store, so the session reaches it over the
/// local owner channel.
fn owner_session(
    fixture: &PrincipalFixture,
    runtime_root: &std::path::Path,
    options: ReadSessionOptions,
) -> ReadSession {
    let session = ReadSession::with_runtime_directory_for_test(runtime_root, || {
        ReadSession::open_with_options(&fixture.path, options.clone())
    })
    .expect("a live writer selects the owner route");
    assert_eq!(session.route(), ReadRoute::Owner);
    session
}

/// Route 3: the store is idle, so the session answers from the committed image
/// through the direct-file backend.
fn direct_session(
    fixture: &PrincipalFixture,
    name: &str,
    options: ReadSessionOptions,
) -> ReadSession {
    let runtime_root = secure_runtime_root(&fixture.directory, &format!("direct-{name}"));
    let session = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(&fixture.path, options.clone())
    })
    .expect("an idle store selects the direct route");
    assert_eq!(session.route(), ReadRoute::File);
    session
}

fn answered(session: &ReadSession, route: ReadRoute) -> BTreeSet<String> {
    session
        .execute(&select_payloads(), &HashMap::new())
        .map(payloads)
        .unwrap_or_else(|error| panic!("the {route:?} route refused the declared read: {error:?}"))
}

/// The refusals that describe a channel that is not there, a store that is not
/// there, or a writer that is busy. A declaration the owner will not serve is
/// none of those, and answering it with one of them tells the reader to retry
/// something that will never succeed.
fn misdirecting_kinds() -> [ReadFailureKind; 11] {
    [
        ReadFailureKind::OwnerNotRunning,
        ReadFailureKind::OwnerNotServing,
        ReadFailureKind::OwnerUserMismatch,
        ReadFailureKind::OwnerMismatch,
        ReadFailureKind::OwnerAtCapacity,
        ReadFailureKind::OwnerTimeout,
        ReadFailureKind::OwnerDisconnected,
        ReadFailureKind::InvalidChannelData,
        ReadFailureKind::LocalProtocolMismatch,
        ReadFailureKind::StoreNotFound,
        ReadFailureKind::DirectReadRequiresWriter,
    ]
}

#[test]
fn an_unprincipaled_writer_serves_a_declared_principal_its_own_grants_and_no_more() {
    let fixture = principal_fixture();
    let runtime_root = secure_runtime_root(&fixture.directory, "honored-runtime");
    let writer = owner_writer(&fixture, &runtime_root, None);

    let observed = answered(
        &owner_session(
            &fixture,
            &runtime_root,
            declaring(None, Some(fixture.reader_principal.clone())),
        ),
        ReadRoute::Owner,
    );

    assert_eq!(
        observed, fixture.shared_rows,
        "the owner route answered a session declaring {:?} rows outside that identity's grants",
        fixture.reader_principal,
    );
    for hidden in &fixture.writer_only_rows {
        assert!(
            !observed.contains(hidden),
            "the owner route leaked {hidden}, a row only {:?} is granted, to a session declaring {:?}",
            fixture.writer_principal,
            fixture.reader_principal,
        );
    }

    writer
        .close()
        .expect("close the unprincipaled declared-principal writer");
}

#[test]
fn both_routes_answer_the_same_declared_principal_the_same_rows() {
    let fixture = principal_fixture();
    let declaration = declaring(None, Some(fixture.reader_principal.clone()));

    let from_the_file = answered(
        &direct_session(&fixture, "parity", declaration.clone()),
        ReadRoute::File,
    );

    let runtime_root = secure_runtime_root(&fixture.directory, "parity-runtime");
    let writer = owner_writer(&fixture, &runtime_root, None);
    let over_the_channel = answered(
        &owner_session(&fixture, &runtime_root, declaration),
        ReadRoute::Owner,
    );
    writer.close().expect("close the route-parity writer");

    assert_eq!(
        from_the_file, fixture.shared_rows,
        "the direct route answered the declared principal rows outside its grants",
    );
    assert_eq!(
        over_the_channel, from_the_file,
        "the same declared principal answers the same rows whether or not a writer is live",
    );
}

#[test]
fn a_declaration_naming_another_principal_than_the_writer_is_refused_at_admission() {
    let fixture = principal_fixture();
    let runtime_root = secure_runtime_root(&fixture.directory, "refusal-runtime");
    let writer = owner_writer(
        &fixture,
        &runtime_root,
        Some(fixture.writer_principal.clone()),
    );

    let outcome = ReadSession::with_runtime_directory_for_test(&runtime_root, || {
        ReadSession::open_with_options(
            &fixture.path,
            declaring(None, Some(fixture.reader_principal.clone())),
        )
    });
    let failure = match outcome {
        Ok(served) => {
            let rows = served
                .execute(&select_payloads(), &HashMap::new())
                .map(payloads);
            panic!(
                "the owner route served a session declaring {:?} against a writer opened as {:?} instead of refusing it; it answered {rows:?}",
                fixture.reader_principal, fixture.writer_principal,
            )
        }
        Err(Error::ReadFailure(failure)) => failure,
        Err(other) => panic!(
            "a declaration the owner will not serve owes a typed read refusal; observed {other:?}"
        ),
    };
    assert!(
        !misdirecting_kinds().contains(&failure.kind()),
        "the refusal for a conflicting declared principal must name that conflict, not {:?}",
        failure.kind(),
    );
    let stated = failure.to_string();
    assert!(
        stated.to_lowercase().contains("principal"),
        "the refusal a reader reads must say the principal it declared is the reason: {stated}",
    );

    // The writer keeps serving: the declaration was refused, not the channel.
    assert_eq!(
        answered(
            &owner_session(&fixture, &runtime_root, ReadSessionOptions::default()),
            ReadRoute::Owner,
        ),
        fixture.every_row(),
        "a refused declaration must not stop the owner answering the next reader",
    );

    writer.close().expect("close the refusing writer");
}

#[test]
fn a_declaration_naming_the_writers_own_principal_is_served_unchanged() {
    let fixture = principal_fixture();
    let runtime_root = secure_runtime_root(&fixture.directory, "equal-runtime");
    let writer = owner_writer(
        &fixture,
        &runtime_root,
        Some(fixture.writer_principal.clone()),
    );

    assert_eq!(
        answered(
            &owner_session(
                &fixture,
                &runtime_root,
                declaring(None, Some(fixture.writer_principal.clone())),
            ),
            ReadRoute::Owner,
        ),
        fixture.every_row(),
        "declaring the identity the writer already reads as is a no-op, not a refusal",
    );

    writer
        .close()
        .expect("close the same-principal declared-principal writer");
}

#[test]
fn a_declaration_that_names_no_principal_keeps_reading_under_the_writers_own() {
    let fixture = principal_fixture();
    let runtime_root = secure_runtime_root(&fixture.directory, "undeclared-runtime");
    let writer = owner_writer(
        &fixture,
        &runtime_root,
        Some(fixture.writer_principal.clone()),
    );

    assert_eq!(
        answered(
            &owner_session(&fixture, &runtime_root, ReadSessionOptions::default()),
            ReadRoute::Owner,
        ),
        fixture.every_row(),
        "a session that declares nothing reads as the writer, exactly as it does today",
    );
    assert_eq!(
        answered(
            &owner_session(
                &fixture,
                &runtime_root,
                declaring(Some(BTreeSet::from([fixture.here.clone()])), None),
            ),
            ReadRoute::Owner,
        ),
        fixture.every_row_here(),
        "declaring a context and no principal narrows the context and leaves the identity alone",
    );

    writer
        .close()
        .expect("close the undeclared-principal writer");
}
