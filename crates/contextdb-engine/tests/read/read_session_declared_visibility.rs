//! A read session declares the contexts, scopes and principal it reads as,
//! and the engine — not the caller's SQL — decides which rows it may see.
//!
//! A writable open already says who it is reading as: `DatabaseOpenOptions`
//! carries `contexts`, `scope_labels` and `principal`, and the row gate hides
//! everything outside them. A read session had no way to say the same thing,
//! so every read session read the whole store and a consumer that wanted a
//! narrowed view had to filter the SQL itself. Filtering SQL cannot be made
//! safe: `scope = 'mine' OR 1 = 1`, `NOT (scope = 'mine')`, an IN-list, a
//! LIKE, a subquery — each of them re-widens a query an analyzer believed it
//! had narrowed, and the rows come back.
//!
//! So `ReadSessionOptions` carries the same three declarations, they reach the
//! same row gate, and an out-of-scope row is INVISIBLE whatever shape asks for
//! it, on the direct-file route and on the live-owner route alike, across
//! cursor pages as well as whole results. Declaring nothing keeps today's
//! behavior exactly: an undeclared session reads as it always has.

use contextdb_core::read_contract::{CursorPage, ReadLimits, ReadRoute};
use contextdb_core::{ContextId, Principal, ScopeLabel, Value};
use contextdb_engine::{
    Database, DatabaseOpenOptions, OwnerReadConfig, QueryResult, ReadSession, ReadSessionOptions,
};
use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

const DECLARED_TABLE: &str = "declared_rows";
const GRANTED_TABLE: &str = "granted_rows";

/// The cursor page size these proofs read with, taken from the public route
/// vocabulary so no page count is an invented constant.
fn page_rows() -> usize {
    [ReadRoute::File, ReadRoute::Owner].len()
}

/// Enough row groups that a constrained cursor must skip hidden rows both
/// inside a page and between two fetches.
fn row_groups() -> usize {
    page_rows().saturating_mul(2).saturating_add(1)
}

/// Identities derived from the declared read policy so the fixture carries no
/// invented constants.
fn identity_seed() -> u128 {
    let limits = ReadLimits::default();
    u128::from(limits.result_rows)
        .saturating_mul(1_000_003)
        .saturating_add(u128::from(limits.result_bytes))
}

fn secure_runtime_root(directory: &tempfile::TempDir, name: &str) -> std::path::PathBuf {
    let root = directory.path().join(name);
    std::fs::create_dir(&root).expect("create the task-scoped read runtime root");
    #[cfg(unix)]
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure the task-scoped read runtime root");
    root
}

struct DeclaredFixture {
    directory: tempfile::TempDir,
    path: std::path::PathBuf,
    allowed_context: ContextId,
    foreign_context: ContextId,
    allowed_scope: ScopeLabel,
    forbidden_scope: ScopeLabel,
    principal: Principal,
    /// `declared_rows` payloads inside both the allowed context and the
    /// allowed scope.
    inside_both_axes: BTreeSet<String>,
    /// `declared_rows` payloads a context declaration hides (foreign context,
    /// allowed scope).
    outside_the_context: BTreeSet<String>,
    /// `declared_rows` payloads a scope declaration hides (allowed context,
    /// forbidden scope).
    outside_the_scope: BTreeSet<String>,
    /// `declared_rows` payloads both declarations hide.
    outside_both_axes: BTreeSet<String>,
    granted_here: String,
    granted_elsewhere: String,
    ungranted_here: String,
}

impl DeclaredFixture {
    /// What a session declaring the allowed scope alone may see.
    fn inside_the_scope(&self) -> BTreeSet<String> {
        self.inside_both_axes
            .union(&self.outside_the_context)
            .cloned()
            .collect()
    }

    /// What a session declaring the allowed context alone may see.
    fn inside_the_context(&self) -> BTreeSet<String> {
        self.inside_both_axes
            .union(&self.outside_the_scope)
            .cloned()
            .collect()
    }

    /// Every row in the table, which is what an undeclared session reads.
    fn every_declared_payload(&self) -> BTreeSet<String> {
        self.inside_the_scope()
            .union(&self.inside_the_context())
            .cloned()
            .chain(self.outside_both_axes.iter().cloned())
            .collect()
    }

    fn every_granted_payload(&self) -> BTreeSet<String> {
        BTreeSet::from([
            self.granted_here.clone(),
            self.granted_elsewhere.clone(),
            self.ungranted_here.clone(),
        ])
    }
}

fn declared_fixture() -> DeclaredFixture {
    let directory = tempfile::TempDir::new().expect("task-scoped declared-visibility directory");
    let path = directory.path().join("declared-visibility.db");
    let seed = identity_seed();
    let limits = ReadLimits::default();
    let allowed_context = uuid::Uuid::from_u128(seed.saturating_add(1));
    let foreign_context = uuid::Uuid::from_u128(seed.saturating_add(2));
    let granted_acl = uuid::Uuid::from_u128(seed.saturating_add(3));
    let ungranted_acl = uuid::Uuid::from_u128(seed.saturating_add(4));
    let allowed_scope_name = format!("declared-{}", limits.cursor_page_rows);
    let forbidden_scope_name = format!("undeclared-{}", limits.cursor_page_rows);
    let principal_name = format!("declared-reader-{}", limits.work);
    let principal = Principal::Agent(principal_name.clone());

    let admin = Database::open(&path).expect("create the file-backed declared-visibility fixture");
    admin
        .execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
            &HashMap::new(),
        )
        .expect("the administrative owner creates the grant table");
    admin
        .execute(
            &format!(
                "CREATE TABLE {DECLARED_TABLE} (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, scope TEXT SCOPE_LABEL_READ ('{allowed_scope_name}','{forbidden_scope_name}') WRITE ('{allowed_scope_name}','{forbidden_scope_name}'), payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the context- and scope-bearing table");
    admin
        .execute(
            &format!(
                "CREATE TABLE {GRANTED_TABLE} (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)"
            ),
            &HashMap::new(),
        )
        .expect("the administrative owner creates the access-controlled table");
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
        .expect("the administrative owner grants one access list to the declared principal");

    let mut inside_both_axes = BTreeSet::new();
    let mut outside_the_context = BTreeSet::new();
    let mut outside_the_scope = BTreeSet::new();
    let mut outside_both_axes = BTreeSet::new();
    for group in 0..row_groups() {
        for (offset, context, scope, payload, bucket) in [
            (
                0usize,
                allowed_context,
                allowed_scope_name.clone(),
                format!("inside-both-{group}"),
                &mut inside_both_axes,
            ),
            (
                1,
                allowed_context,
                forbidden_scope_name.clone(),
                format!("outside-scope-{group}"),
                &mut outside_the_scope,
            ),
            (
                2,
                foreign_context,
                allowed_scope_name.clone(),
                format!("outside-context-{group}"),
                &mut outside_the_context,
            ),
            (
                3,
                foreign_context,
                forbidden_scope_name.clone(),
                format!("outside-both-{group}"),
                &mut outside_both_axes,
            ),
        ] {
            let id = group.saturating_mul(4).saturating_add(offset);
            admin
                .execute(
                    &format!(
                        "INSERT INTO {DECLARED_TABLE} (id, context_id, scope, payload) VALUES ($id, $context, $scope, $payload)"
                    ),
                    &HashMap::from([
                        (
                            "id".to_owned(),
                            Value::Int64(i64::try_from(id).expect("fixture row id fits an i64")),
                        ),
                        ("context".to_owned(), Value::Uuid(context)),
                        ("scope".to_owned(), Value::Text(scope)),
                        ("payload".to_owned(), Value::Text(payload.clone())),
                    ]),
                )
                .expect("the administrative owner inserts a declared-visibility row");
            bucket.insert(payload);
        }
    }

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
                    "INSERT INTO {GRANTED_TABLE} (id, context_id, acl_id, payload) VALUES ($id, $context, $acl, $payload)"
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

    DeclaredFixture {
        directory,
        path,
        allowed_context: ContextId::new(allowed_context),
        foreign_context: ContextId::new(foreign_context),
        allowed_scope: ScopeLabel::new(allowed_scope_name),
        forbidden_scope: ScopeLabel::new(forbidden_scope_name),
        principal,
        inside_both_axes,
        outside_the_context,
        outside_the_scope,
        outside_both_axes,
        granted_here,
        granted_elsewhere,
        ungranted_here,
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

fn page_payloads(page: &CursorPage) -> BTreeSet<String> {
    page.rows
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Text(payload)] => payload.clone(),
            other => panic!("cursor payload projection returned {other:?}"),
        })
        .collect()
}

/// The declaration a read session makes about who it reads as.
fn declaring(
    contexts: Option<BTreeSet<ContextId>>,
    scope_labels: Option<BTreeSet<ScopeLabel>>,
    principal: Option<Principal>,
) -> ReadSessionOptions {
    ReadSessionOptions {
        contexts,
        scope_labels,
        principal,
        ..ReadSessionOptions::default()
    }
}

/// Route 3: the store is idle, so the session answers from the committed
/// image through the direct-file backend.
fn direct_session(
    fixture: &DeclaredFixture,
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

/// A writer that serves owner reads out of a task-scoped runtime root.
fn owner_writer(
    fixture: &DeclaredFixture,
    runtime_root: &std::path::Path,
    options: DatabaseOpenOptions,
) -> Database {
    Database::open_with_options(
        &fixture.path,
        DatabaseOpenOptions {
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.to_path_buf()),
                ..OwnerReadConfig::default()
            },
            ..options
        },
    )
    .expect("open the writer that serves the declared-visibility reads")
}

/// Route 2: a live writer holds the store, so the session reaches it over the
/// local owner channel.
fn owner_session(
    fixture: &DeclaredFixture,
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

fn select_payloads(table: &str) -> String {
    format!("SELECT payload FROM {table} ORDER BY id")
}

/// The query shapes a consumer-side SQL analyzer cannot be trusted with. Each
/// one asks for rows outside the declaration in a different way; the pair is
/// the SQL and the payloads the engine may answer with when the allowed scope
/// alone is declared.
fn adversarial_shapes(fixture: &DeclaredFixture) -> Vec<(String, BTreeSet<String>)> {
    let allowed = &fixture.allowed_scope.0;
    let forbidden = &fixture.forbidden_scope.0;
    let inside = fixture.inside_the_scope();
    vec![
        (select_payloads(DECLARED_TABLE), inside.clone()),
        (
            format!(
                "SELECT payload FROM {DECLARED_TABLE} WHERE scope = '{allowed}' OR 1 = 1 ORDER BY id"
            ),
            inside.clone(),
        ),
        (
            format!(
                "SELECT payload FROM {DECLARED_TABLE} WHERE NOT (scope = '{allowed}') ORDER BY id"
            ),
            BTreeSet::new(),
        ),
        (
            format!(
                "SELECT payload FROM {DECLARED_TABLE} WHERE scope IN ('{allowed}','{forbidden}') ORDER BY id"
            ),
            inside.clone(),
        ),
        (
            format!("SELECT payload FROM {DECLARED_TABLE} WHERE payload LIKE '%' ORDER BY id"),
            inside.clone(),
        ),
        (
            format!(
                "SELECT payload FROM {DECLARED_TABLE} WHERE id IN (SELECT id FROM {DECLARED_TABLE}) ORDER BY id"
            ),
            inside,
        ),
    ]
}

fn assert_shape_answers(
    session: &ReadSession,
    fixture: &DeclaredFixture,
    route: ReadRoute,
) -> Vec<BTreeSet<String>> {
    let mut answers = Vec::new();
    for (sql, expected) in adversarial_shapes(fixture) {
        let observed = session
            .execute(&sql, &HashMap::new())
            .map(payloads)
            .unwrap_or_else(|error| panic!("{route:?} route refused `{sql}`: {error:?}"));
        assert_eq!(
            observed, expected,
            "`{sql}` answered rows outside the declared scope on the {route:?} route"
        );
        for hidden in fixture
            .outside_the_scope
            .iter()
            .chain(&fixture.outside_both_axes)
        {
            assert!(
                !observed.contains(hidden),
                "`{sql}` leaked the out-of-scope row {hidden} on the {route:?} route"
            );
        }
        answers.push(observed);
    }
    answers
}

#[test]
fn a_declared_scope_hides_out_of_scope_rows_from_every_query_shape_on_the_direct_route() {
    let fixture = declared_fixture();
    let session = direct_session(
        &fixture,
        "scope-shapes",
        declaring(
            None,
            Some(BTreeSet::from([fixture.allowed_scope.clone()])),
            None,
        ),
    );
    assert_shape_answers(&session, &fixture, ReadRoute::File);
}

#[test]
fn a_declared_context_and_principal_hide_the_same_rows_a_writable_handle_hides() {
    let fixture = declared_fixture();

    let by_context = direct_session(
        &fixture,
        "context",
        declaring(
            Some(BTreeSet::from([fixture.allowed_context.clone()])),
            None,
            None,
        ),
    )
    .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
    .map(payloads)
    .expect("the direct route answers the declared context");
    assert_eq!(by_context, fixture.inside_the_context());

    let by_both_axes = direct_session(
        &fixture,
        "context-and-scope",
        declaring(
            Some(BTreeSet::from([fixture.allowed_context.clone()])),
            Some(BTreeSet::from([fixture.allowed_scope.clone()])),
            None,
        ),
    )
    .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
    .map(payloads)
    .expect("the direct route answers the declared context and scope");
    assert_eq!(by_both_axes, fixture.inside_both_axes);

    let by_principal = direct_session(
        &fixture,
        "principal",
        declaring(None, None, Some(fixture.principal.clone())),
    )
    .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
    .map(payloads)
    .expect("the direct route answers the rows the declared principal is granted");
    assert_eq!(
        by_principal,
        BTreeSet::from([
            fixture.granted_here.clone(),
            fixture.granted_elsewhere.clone()
        ]),
    );
    assert!(!by_principal.contains(&fixture.ungranted_here));

    let by_context_and_principal = direct_session(
        &fixture,
        "context-and-principal",
        declaring(
            Some(BTreeSet::from([fixture.allowed_context.clone()])),
            None,
            Some(fixture.principal.clone()),
        ),
    )
    .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
    .map(payloads)
    .expect("the direct route narrows the granted rows by the declared context");
    assert_eq!(
        by_context_and_principal,
        BTreeSet::from([fixture.granted_here.clone()])
    );
}

#[test]
fn the_live_owner_route_answers_a_declaration_exactly_as_the_direct_route_does() {
    let fixture = declared_fixture();
    let scope_only = declaring(
        None,
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        None,
    );
    let both_axes = declaring(
        Some(BTreeSet::from([fixture.allowed_context.clone()])),
        Some(BTreeSet::from([fixture.allowed_scope.clone()])),
        None,
    );
    let by_principal = declaring(None, None, Some(fixture.principal.clone()));

    let direct_shapes = assert_shape_answers(
        &direct_session(&fixture, "parity-scope", scope_only.clone()),
        &fixture,
        ReadRoute::File,
    );
    let direct_both = direct_session(&fixture, "parity-both", both_axes.clone())
        .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the direct route answers both declared axes");
    let direct_granted = direct_session(&fixture, "parity-principal", by_principal.clone())
        .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the direct route answers the declared principal");

    let runtime_root = secure_runtime_root(&fixture.directory, "parity-owner-runtime");
    let writer = owner_writer(&fixture, &runtime_root, DatabaseOpenOptions::default());

    let owner_shapes = assert_shape_answers(
        &owner_session(&fixture, &runtime_root, scope_only),
        &fixture,
        ReadRoute::Owner,
    );
    let owner_both = owner_session(&fixture, &runtime_root, both_axes)
        .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the owner route answers both declared axes");
    let owner_granted = owner_session(&fixture, &runtime_root, by_principal)
        .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the owner route answers the declared principal");

    assert_eq!(
        owner_shapes, direct_shapes,
        "the same declaration answers the same rows on both read routes"
    );
    assert_eq!(owner_both, direct_both);
    assert_eq!(owner_both, fixture.inside_both_axes);
    assert_eq!(owner_granted, direct_granted);
    assert_eq!(
        owner_granted,
        BTreeSet::from([
            fixture.granted_here.clone(),
            fixture.granted_elsewhere.clone()
        ]),
    );

    writer
        .close()
        .expect("close the declared-visibility writer");
}

#[test]
fn a_reader_never_widens_past_the_narrowing_the_writer_itself_was_opened_with() {
    let fixture = declared_fixture();
    let runtime_root = secure_runtime_root(&fixture.directory, "no-widening-runtime");
    let writer = owner_writer(
        &fixture,
        &runtime_root,
        DatabaseOpenOptions {
            contexts: Some(BTreeSet::from([fixture.allowed_context.clone()])),
            scope_labels: Some(BTreeSet::from([fixture.allowed_scope.clone()])),
            ..DatabaseOpenOptions::default()
        },
    );

    let asking_for_every_context_and_scope = owner_session(
        &fixture,
        &runtime_root,
        declaring(
            Some(BTreeSet::from([
                fixture.allowed_context.clone(),
                fixture.foreign_context.clone(),
            ])),
            Some(BTreeSet::from([
                fixture.allowed_scope.clone(),
                fixture.forbidden_scope.clone(),
            ])),
            None,
        ),
    )
    .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
    .map(payloads)
    .expect("the owner route answers a reader asking for more than the writer holds");
    assert_eq!(
        asking_for_every_context_and_scope, fixture.inside_both_axes,
        "a reader asking for a broader set than the writer holds is answered the intersection"
    );

    let declaring_nothing = owner_session(&fixture, &runtime_root, ReadSessionOptions::default())
        .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the owner route answers a reader that declares nothing");
    assert_eq!(
        declaring_nothing, fixture.inside_both_axes,
        "declaring nothing inherits the writer's own narrowing rather than widening past it"
    );

    writer.close().expect("close the narrowed writer");
}

#[test]
fn cursor_pages_under_a_declaration_never_yield_a_row_outside_it() {
    let fixture = declared_fixture();
    let limits = ReadLimits {
        cursor_page_rows: u64::try_from(page_rows()).expect("page row count fits the read policy"),
        ..ReadLimits::default()
    };
    let options = ReadSessionOptions {
        limits,
        ..declaring(
            Some(BTreeSet::from([fixture.allowed_context.clone()])),
            Some(BTreeSet::from([fixture.allowed_scope.clone()])),
            None,
        )
    };
    let hidden = fixture
        .outside_the_context
        .union(&fixture.outside_the_scope)
        .cloned()
        .chain(fixture.outside_both_axes.iter().cloned())
        .collect::<BTreeSet<String>>();

    let runtime_root = secure_runtime_root(&fixture.directory, "cursor-owner-runtime");
    let writer = owner_writer(&fixture, &runtime_root, DatabaseOpenOptions::default());
    let over_the_channel = drain_cursor(
        &owner_session(&fixture, &runtime_root, options.clone()),
        &hidden,
        ReadRoute::Owner,
    );
    writer.close().expect("close the cursor-paging writer");

    let from_the_file = drain_cursor(
        &direct_session(&fixture, "cursor", options),
        &hidden,
        ReadRoute::File,
    );

    assert_eq!(from_the_file, fixture.inside_both_axes);
    assert_eq!(over_the_channel, fixture.inside_both_axes);
}

/// Read every page of a constrained cursor, failing on the first page that
/// carries a row the declaration hides.
fn drain_cursor(
    session: &ReadSession,
    hidden: &BTreeSet<String>,
    route: ReadRoute,
) -> BTreeSet<String> {
    let mut cursor = session
        .open_cursor(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .expect("open a bounded cursor under the declared visibility");
    let mut seen = page_payloads(cursor.first_page());
    let mut has_more = cursor.first_page().has_more;
    let mut pages = 1usize;
    assert_no_hidden_row(&seen, hidden, route, pages);

    while has_more {
        let page = cursor
            .fetch(NonZeroUsize::new(page_rows()))
            .expect("the constrained cursor produces its next page");
        pages = pages.saturating_add(1);
        let payloads = page_payloads(&page);
        assert_no_hidden_row(&payloads, hidden, route, pages);
        seen.extend(payloads);
        has_more = page.has_more;
    }
    cursor.close().expect("close the constrained cursor");
    assert!(
        pages > 1,
        "the fixture must span more than one page for paging to prove anything",
    );
    seen
}

fn assert_no_hidden_row(
    observed: &BTreeSet<String>,
    hidden: &BTreeSet<String>,
    route: ReadRoute,
    page: usize,
) {
    let leaked = observed.intersection(hidden).collect::<Vec<_>>();
    assert!(
        leaked.is_empty(),
        "page {page} on the {route:?} route carried rows outside the declaration: {leaked:?}"
    );
}

#[test]
fn a_session_that_declares_nothing_reads_exactly_what_it_reads_today() {
    let fixture = declared_fixture();

    let by_default = direct_session(
        &fixture,
        "undeclared-default",
        ReadSessionOptions::default(),
    );
    let declared_rows = by_default
        .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("an undeclared direct session reads the committed image as its owner");
    let granted_rows = by_default
        .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("an undeclared direct session reads the access-controlled table as its owner");
    assert_eq!(declared_rows, fixture.every_declared_payload());
    assert_eq!(granted_rows, fixture.every_granted_payload());

    let explicitly_undeclared =
        direct_session(&fixture, "undeclared-explicit", declaring(None, None, None));
    assert_eq!(
        explicitly_undeclared
            .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
            .map(payloads)
            .expect("declaring None on every axis reads what the default reads"),
        declared_rows,
    );
    assert_eq!(
        explicitly_undeclared
            .execute(&select_payloads(GRANTED_TABLE), &HashMap::new())
            .map(payloads)
            .expect("declaring None on every axis reads what the default reads"),
        granted_rows,
    );

    let owner =
        Database::open(&fixture.path).expect("reopen the store as its administrative owner");
    let administrative = owner
        .execute(&select_payloads(DECLARED_TABLE), &HashMap::new())
        .map(payloads)
        .expect("the administrative owner reads every row");
    assert_eq!(
        declared_rows, administrative,
        "an undeclared session answers exactly what the store's own owner answers"
    );
    owner.close().expect("close the administrative owner");
}
