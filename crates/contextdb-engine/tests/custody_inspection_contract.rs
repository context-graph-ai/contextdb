//! Metadata inspection follows the actual caller scope and leaves durable state unchanged.

use contextdb_core::{ContextId, Error, TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_server::sync_client::ApplicationTablePolicyExpectation;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{SyncClient, SyncServer, peer_dial_spec};
use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const T0: u64 = 1_700_000_000_000;

const ROOT_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE \
     DELIVERY MANIFEST OVER record_parts";
const MEMBER_CLAUSES: &str = "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE";

// Statement 19: Both conflicting sources and their incumbent use the declared application schema.
const ROOT_DDL: &str = "CREATE TABLE records (id UUID PRIMARY KEY, body TEXT) \
    SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE DELIVERY MANIFEST OVER record_parts";
const MEMBER_DDL: &str = "CREATE TABLE record_parts \
    (id UUID PRIMARY KEY, records_id UUID REFERENCES records(id), body TEXT) \
    SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE";

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

async fn within<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .expect("a sync exchange must complete within 60s")
}

fn bind_spec(identity_path: &Path) -> String {
    format!("iroh:?identity={}", identity_path.display())
}

/// The exact binary this lane built. Cargo hands it to a test target of the
/// package that declares it; every other target reads the same value out of the
/// environment the gate exports. A binary found by guessing at a target
/// directory is not admissible here, so there is no third branch.
fn cli_binary() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_contextdb") {
        return PathBuf::from(path);
    }
    match std::env::var_os("CARGO_BIN_EXE_contextdb") {
        Some(path) => PathBuf::from(path),
        None => panic!(
            "this case runs the contextdb command-line binary and must be told which \
             one: build it and export CARGO_BIN_EXE_contextdb with its exact path"
        ),
    }
}

/// Runs one command-line session over `path` and returns `(stdout, stderr)`.
/// Machine output is one complete document per line on stdout; everything else
/// is on stderr.
fn run_cli(path: &Path, input: &str, write: bool) -> (String, String) {
    let mut command = Command::new(cli_binary());
    command.arg("--json").arg(path);
    if write {
        command.arg("--write");
    }
    let mut child = command
        .env_remove("CARGO_BIN_EXE_contextdb")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn the contextdb command line");
    child
        .stdin
        .as_mut()
        .expect("command-line input")
        .write_all(input.as_bytes())
        .expect("write the session's input");
    drop(child.stdin.take());
    let output = child
        .wait_with_output()
        .expect("the command-line session ends");
    assert!(
        output.status.success(),
        "CLI read failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    (
        String::from_utf8(output.stdout).expect("machine output is text"),
        String::from_utf8(output.stderr).expect("diagnostic output is text"),
    )
}

/// Ordinary named query-result documents emitted by scripted JSON mode.
fn result_documents(stdout: &str) -> Vec<serde_json::Value> {
    stdout
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).expect("every stdout line is JSON")
        })
        .filter_map(|value| value.get("result").cloned())
        .collect()
}

fn shown(db: &Database, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let result = db
        .execute(sql, &p())
        .unwrap_or_else(|err| panic!("{sql} must answer: {err}"));
    let rows = result
        .rows
        .iter()
        .map(|row| row.iter().map(|cell| format!("{cell:?}")).collect())
        .collect();
    (result.columns.clone(), rows)
}

/// Everything an inspection must leave exactly where it found it: the log
/// position, both sync watermarks, the pull cursor, and the rendered bindings.
fn untouchable_state(db: &Database, tenant: &str) -> String {
    let tenant = TenantId::from(tenant);
    format!(
        "{:?}|{:?}|{:?}|{:?}|{:?}",
        db.current_lsn(),
        db.persisted_sync_watermarks(&tenant),
        db.persisted_sync_pull_cursor(&tenant),
        shown(db, "SHOW SYNC BINDINGS"),
        db.__delivery_metadata_bytes_for_test().unwrap(),
    )
}

struct Hub {
    db: Arc<Database>,
    ticket: String,
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn stop(self) {
        self.stop.store(true, Ordering::SeqCst);
        within(self.task).await.expect("hub stops cleanly");
    }
}

async fn start_hub(root: &Path, tenant: &str) -> Hub {
    let identity_path = root.join("hub.db.fabric-identity.key");
    let endpoint = IrohServer::bind(&bind_spec(&identity_path))
        .await
        .expect("bind the authoritative hub endpoint");
    let ticket = endpoint.ticket();
    let db = Arc::new(Database::open(root.join("hub.db")).expect("open the hub store"));
    let server = Arc::new(SyncServer::new(
        db.clone(),
        &endpoint,
        TenantId::from(tenant),
    ));
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    Hub {
        db,
        ticket,
        stop,
        task,
    }
}

// Statement 19: Administrative SHOW and CLI metadata reads are complete, constrained safely, and read-only.
#[tokio::test]
async fn the_three_show_statements_and_their_meta_commands_are_metadata_only_refuse_a_constrained_handle_and_render_json()
 {
    let _clock = Wallclock::test_clock_guard(|| T0);
    let root = tempfile::tempdir().expect("temporary test directory");
    let tenant = "inspection-is-a-read";
    let hub_path = root.path().join("hub.db");
    let edge_path = root.path().join("edge.db");
    let hub = start_hub(root.path(), tenant).await;

    hub.db
        .__seed_tenant_table_policies_for_test(
            TenantId::from(tenant),
            ApplicationTablePolicyExpectation::new()
                .expect_table("records", ROOT_CLAUSES)
                .expect("the root fixture uses canonical declaration clauses")
                .expect_table("record_parts", MEMBER_CLAUSES)
                .expect("the member fixture uses canonical declaration clauses"),
        )
        .expect("install durable declaration prerequisites for inspection");

    // Statement 19: Install matching hub tables before either fixture binding; inspection
    // does not depend on the separate arriving-DDL policy adoption target.
    hub.db.execute(ROOT_DDL, &p()).unwrap();
    hub.db.execute(MEMBER_DDL, &p()).unwrap();

    // Statement 19: Prepare both actual verdict kinds before exercising inspection.
    // An enrolled edge that has bound, written a unit and had it answered, so
    // all three questions have something real to answer with.
    let edge_identity = root.path().join("edge.db.fabric-identity.key");
    let edge = Arc::new(Database::open(&edge_path).expect("open the edge store"));
    let client = SyncClient::new(
        edge.clone(),
        &peer_dial_spec(&hub.ticket, &edge_identity),
        TenantId::from(tenant),
    );
    within(
        client.__seed_application_table_policy_binding_for_test(
            ApplicationTablePolicyExpectation::new()
                .expect_table("records", ROOT_CLAUSES)
                .expect("the root expectation is written in the declaration's clause words")
                .expect_table("record_parts", MEMBER_CLAUSES)
                .expect("the member expectation is written in the declaration's clause words"),
        ),
    )
    .await
    .expect("the edge binds both declared tables");
    // Statement 19: The accepted source uses the same policy as the real incumbent.
    edge.execute(ROOT_DDL, &p())
        .expect("the edge installs the bound root table");
    edge.execute(MEMBER_DDL, &p())
        .expect("the edge installs the bound member table");

    let root_key = uuid::Uuid::new_v4();
    let tx = edge.begin().expect("open the writing transaction");
    edge.execute_in_tx(
        tx,
        "INSERT INTO records (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(root_key)),
            ("body".to_string(), Value::Text("root row".to_string())),
        ]),
    )
    .expect("write the root row");
    client
        .__stage_delivery_manifest_for_test(
            tx,
            contextdb_engine::DeliveryManifest {
                root_table: "records",
                root_key: contextdb_engine::sync_types::NaturalKey::single(
                    "id".into(),
                    Value::Uuid(root_key),
                ),
                members: Vec::new(),
            },
        )
        .expect("canonical empty-member source registration");
    edge.commit(tx).expect("commit the unit");

    within(client.__seed_delivery_outcome_for_test(
        "records",
        &contextdb_engine::sync_types::NaturalKey::single("id".into(), Value::Uuid(root_key)),
        contextdb_engine::DeliveryOutcomeKind::Accepted,
        None,
    ))
    .await
    .expect("canonical committed outcome before inspection");

    // Statement 19: A second edge loses a real same-key keep-first conflict. The existing
    // prerequisite seam verifies the differing incumbent and commits a signed refusal;
    // it cannot manufacture this outcome without an actual conflicting hub row.
    let refused_path = root.path().join("refused.db");
    let refused = Arc::new(Database::open(&refused_path).unwrap());
    let refused_client = SyncClient::new(
        refused.clone(),
        &peer_dial_spec(
            &hub.ticket,
            &root.path().join("refused.db.fabric-identity.key"),
        ),
        TenantId::from(tenant),
    );
    within(
        refused_client.__seed_application_table_policy_binding_for_test(
            ApplicationTablePolicyExpectation::new()
                .expect_table("records", ROOT_CLAUSES)
                .unwrap()
                .expect_table("record_parts", MEMBER_CLAUSES)
                .unwrap(),
        ),
    )
    .await
    .unwrap();
    // Statement 19: Only row content differs on the refused source, never its bound policy.
    refused.execute(ROOT_DDL, &p()).unwrap();
    refused.execute(MEMBER_DDL, &p()).unwrap();
    let key = contextdb_engine::sync_types::NaturalKey::single("id".into(), Value::Uuid(root_key));
    let tx = refused.begin().unwrap();
    refused
        .execute_in_tx(
            tx,
            "INSERT INTO records (id, body) VALUES ($id, 'refused root row')",
            &HashMap::from([("id".into(), Value::Uuid(root_key))]),
        )
        .unwrap();
    refused_client
        .__stage_delivery_manifest_for_test(
            tx,
            contextdb_engine::DeliveryManifest {
                root_table: "records",
                root_key: key.clone(),
                members: Vec::new(),
            },
        )
        .unwrap();
    refused.commit(tx).unwrap();
    within(refused_client.__seed_delivery_outcome_for_test(
        "records",
        &key,
        contextdb_engine::DeliveryOutcomeKind::Refused,
        Some("keep_first_refused"),
    ))
    .await
    .expect("a genuine conflicting incumbent produces the refusal fixture");
    // Statement 19: Observe the real persisted terminal through the existing read adapter,
    // keeping inspection independent of the separate public delivery_outcome target.
    let refusals = refused.__delivery_prerequisite_wires_for_test().unwrap();
    assert_eq!(refusals.len(), 1);
    assert_eq!(refusals[0].outcome_for_test(), Some("refused"));
    assert_eq!(refusals[0].cause_for_test(), Some("keep_first_refused"));
    assert_eq!(
        hub.db
            .execute(
                "SELECT body FROM records WHERE id=$id",
                &HashMap::from([("id".into(), Value::Uuid(root_key))])
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("root row".into())]],
        "Statement 19: refusal leaves the actual incumbent unchanged"
    );
    eprintln!(
        "Statement 19 fixture: committed genuine keep_first_refused terminal; incumbent unchanged"
    );

    // Statement 19: Keep the original policy assertions after both real outcomes are prepared.
    let (policy_columns, policy_rows) = shown(&hub.db, "SHOW TENANT TABLE POLICY");
    for name in [
        "table",
        "version",
        "digest",
        "sync_direction",
        "sync_conflict",
        "manifest_tables",
        "edge_discard",
    ] {
        assert!(policy_columns.iter().any(|column| column == name));
    }
    assert_eq!(policy_rows.len(), 2, "both declared tables render");

    // ---- metadata only: repeat every question and compare what it touched --
    // Statement 19: Repeated reads preserve both accepted and refused durable metadata.
    for db in [hub.db.as_ref(), edge.as_ref(), refused.as_ref()] {
        let before = untouchable_state(db, tenant);
        let first_policy = shown(db, "SHOW TENANT TABLE POLICY");
        let first_bindings = shown(db, "SHOW SYNC BINDINGS");
        let first_outcomes = shown(db, "SHOW DELIVERY OUTCOMES FOR records");
        let between = untouchable_state(db, tenant);
        let second_policy = shown(db, "SHOW TENANT TABLE POLICY");
        let second_bindings = shown(db, "SHOW SYNC BINDINGS");
        let second_outcomes = shown(db, "SHOW DELIVERY OUTCOMES FOR records");
        let after = untouchable_state(db, tenant);

        assert_eq!(
            (&before, &between),
            (&before, &before),
            "asking the three questions moved a watermark, a cursor, a binding or \
             the log"
        );
        assert_eq!(
            (&after, &between),
            (&after, &after),
            "asking them a second time moved something the first time did not"
        );
        assert_eq!(
            (first_policy, first_bindings, first_outcomes),
            (second_policy, second_bindings, second_outcomes),
            "the same question asked twice must give the same answer"
        );
    }

    let (outcome_columns, hub_outcome_rows) = shown(&hub.db, "SHOW DELIVERY OUTCOMES FOR records");
    for name in [
        "root_table",
        "root_key",
        "unit_digest",
        "outcome",
        "cause",
        "hub_node_id",
        "hub_incarnation",
        "edge_node_id",
        "edge_incarnation",
    ] {
        assert!(outcome_columns.iter().any(|column| column == name));
    }
    let outcome_column = outcome_columns
        .iter()
        .position(|column| column == "outcome")
        .expect("outcome column renders");
    let cause_column = outcome_columns
        .iter()
        .position(|column| column == "cause")
        .expect("cause column renders");
    let edge_column = outcome_columns
        .iter()
        .position(|column| column == "edge_node_id")
        .expect("edge identity column renders");
    assert_eq!(
        hub_outcome_rows.len(),
        2,
        "Statement 19: the hub renders one terminal outcome for each edge"
    );
    let accepted = hub_outcome_rows
        .iter()
        .find(|row| row[outcome_column] == "Text(\"accepted\")")
        .expect("the hub renders the accepted edge outcome");
    let rejected = hub_outcome_rows
        .iter()
        .find(|row| row[outcome_column] == "Text(\"refused\")")
        .expect("the hub renders the refused edge outcome");
    assert_eq!(accepted[cause_column], "Null");
    assert_eq!(rejected[cause_column], "Text(\"keep_first_refused\")");
    assert_ne!(
        accepted[edge_column], rejected[edge_column],
        "Statement 19: hub outcomes retain the two submitting edge identities"
    );
    for row in &hub_outcome_rows {
        assert!(
            !row.iter().any(|cell| cell.contains("root row")),
            "Statement 19: hub outcome metadata contains no row content: {row:?}"
        );
    }
    let (_, outcome_rows) = shown(&edge, "SHOW DELIVERY OUTCOMES FOR records");
    assert_eq!(
        outcome_rows.len(),
        1,
        "the edge reads back exactly the one answer it was given"
    );
    assert_eq!(
        outcome_rows[0][outcome_columns.iter().position(|c| c == "outcome").unwrap()],
        "Text(\"accepted\")",
        "the rendered verdict is one of the closed words"
    );
    assert!(
        !outcome_rows[0].iter().any(|cell| cell.contains("root row")),
        "no rendered cell carries a row's content: {:?}",
        outcome_rows[0]
    );

    // A table whose paging is asked for renders the page, not the whole set.
    assert!(
        shown(&edge, "SHOW DELIVERY OUTCOMES FOR records LIMIT 1 OFFSET 1")
            .1
            .is_empty(),
        "paging past the single answer renders nothing"
    );

    within(client.shutdown()).await;
    drop(client);

    assert_custody_read_limits(&hub_path, &edge_path);

    // ---- a narrowed handle is refused, not answered partially ---------------
    let constrained = [
        hub.db
            .scoped_with_contexts(BTreeSet::from([ContextId::new(uuid::Uuid::new_v4())])),
        hub.db.scoped_with_constraints(
            None,
            Some(BTreeSet::from([contextdb_core::ScopeLabel::new("private")])),
            None,
        ),
        hub.db.scoped_with_constraints(
            None,
            None,
            Some(contextdb_core::Principal::Agent("reader".into())),
        ),
    ];
    for narrowed in &constrained {
        for sql in [
            "SHOW TENANT TABLE POLICY",
            "SHOW SYNC BINDINGS",
            "SHOW DELIVERY OUTCOMES FOR records",
        ] {
            match narrowed.execute(sql, &p()) {
                Err(Error::ReadFailure(failure)) => assert_eq!(failure.kind(),
                    contextdb_core::read_contract::ReadFailureKind::ConstrainedHandleInspectionRefused),
                other => panic!("{sql} must refuse every constrained handle: {other:?}"),
            }
        }
    }
    drop(constrained);
    // Statement 19: Release the refused CLI store after its authenticated fixture exchanges.
    within(refused_client.shutdown()).await;
    drop(refused_client);
    drop(refused);
    drop(edge);
    hub.stop().await;

    // Statement 19: the released file route enforces the same ceilings as the live owner.
    assert_custody_read_limits(&hub_path, &edge_path);

    // ---- the command line, on a session that has no sync client at all ------
    // No `--tenant-id`, so this session never starts one. The three questions
    // are store reads and must answer anyway.
    let (stdout, stderr) = run_cli(
        &hub_path,
        ".sync policy\n.sync bindings\n.sync outcomes FOR records\n",
        false,
    );
    assert!(
        !stderr.to_ascii_lowercase().contains("sync not configured"),
        "a store read must not be turned away for want of a sync client: {stderr:?}"
    );

    let results = result_documents(&stdout);
    assert_eq!(
        results.len(),
        3,
        "each meta-command emits one ordinary result"
    );
    let policy = &results[0];
    let tables = policy["rows"]
        .as_array()
        .expect("the rendered policy carries its tables");
    assert_eq!(tables.len(), 2, "both declared tables render");
    let records = tables
        .iter()
        .find(|table| table["table"] == serde_json::json!("records"))
        .expect("the root table renders");
    assert_eq!(records["version"], serde_json::json!(1));
    assert_eq!(records["tenant_id"], serde_json::json!(tenant));
    assert_eq!(records["sync_direction"], serde_json::json!("push_only"));
    assert_eq!(records["sync_conflict"], serde_json::json!("keep_first"));
    assert_eq!(records["immutable"], serde_json::json!(true));
    assert_eq!(
        records["manifest_tables"],
        serde_json::json!(["record_parts"])
    );
    assert_eq!(records["edge_discard"], serde_json::json!("after_outcome"));
    assert_eq!(
        records["digest"]
            .as_str()
            .expect("the digest renders as text")
            .len(),
        64,
        "the rendered digest is the whole canonical digest"
    );

    let (edge_stdout, edge_stderr) = run_cli(
        &edge_path,
        ".sync bindings\n.sync outcomes FOR records\n",
        false,
    );
    assert!(
        !edge_stderr
            .to_ascii_lowercase()
            .contains("sync not configured"),
        "the edge's own store read must answer too: {edge_stderr:?}"
    );
    let edge_results = result_documents(&edge_stdout);
    assert_eq!(
        edge_results.len(),
        2,
        "both edge reads emit ordinary results"
    );
    let bindings = &edge_results[0];
    assert_eq!(
        bindings["rows"]
            .as_array()
            .expect("the rendered bindings carry their tables")
            .len(),
        2,
        "both bound tables render"
    );

    let outcomes = &edge_results[1];
    assert_eq!(
        outcomes["rows"]
            .as_array()
            .expect("the rendered outcomes carry their entries")
            .len(),
        1
    );
    assert_eq!(
        outcomes["rows"][0]["outcome"],
        serde_json::json!("accepted")
    );
    assert_eq!(outcomes["rows"][0]["cause"], serde_json::Value::Null);
    assert!(
        outcomes["rows"][0].get("cause").is_some(),
        "cause is present even when null"
    );
    assert!(
        !edge_stdout.contains("root row"),
        "no rendered document carries a row's content"
    );

    // Statement 19: Observe the actual CLI JSON cause, including presence, exact spelling
    // and a nonempty value. Accepted/null above remains a separate required observation.
    let (refused_stdout, _) = run_cli(&refused_path, ".sync outcomes FOR records\n", false);
    let refused_results = result_documents(&refused_stdout);
    assert_eq!(refused_results.len(), 1);
    let refused_rows = refused_results[0]["rows"].as_array().unwrap();
    assert_eq!(refused_rows.len(), 1);
    assert_eq!(refused_rows[0]["outcome"], serde_json::json!("refused"));
    let cause = refused_rows[0]
        .get("cause")
        .expect("the refusal JSON includes the cause field")
        .as_str()
        .expect("a refused outcome has a textual cause");
    assert!(!cause.is_empty());
    assert_eq!(cause, "keep_first_refused");
    assert_eq!(
        Some(cause),
        refusals[0].cause_for_test(),
        "CLI renders the persisted refusal's actual cause"
    );
    for output in [&stdout, &edge_stdout, &refused_stdout] {
        assert!(
            !output.contains("root row"),
            "Statement 19: metadata never discloses either row body"
        );
    }

    // Asking from the command line changed nothing either.
    let reopened_edge = Database::open(&edge_path).expect("reopen the edge store");
    let after_cli = untouchable_state(&reopened_edge, tenant);
    drop(reopened_edge);
    let (_, _) = run_cli(&edge_path, ".sync outcomes FOR records\n", false);
    let reopened_edge = Database::open(&edge_path).expect("reopen the edge store");
    assert_eq!(
        untouchable_state(&reopened_edge, tenant),
        after_cli,
        "asking from the command line moved a watermark, a cursor or a binding"
    );
}

// Statement 19: custody SHOW uses ordinary row and byte ceilings, on both read routes.
fn assert_custody_read_limits(hub: &Path, edge: &Path) {
    use contextdb_core::read_contract::{ReadFailureKind, ReadLimits};
    use contextdb_engine::{ReadSession, ReadSessionOptions};
    for limits in [
        ReadLimits {
            result_rows: 1,
            cursor_page_rows: 1,
            ..ReadLimits::default()
        },
        ReadLimits {
            result_bytes: 1,
            cursor_page_bytes: 1,
            ..ReadLimits::default()
        },
    ] {
        for (path, sql) in [
            (hub, "SHOW TENANT TABLE POLICY"),
            (edge, "SHOW SYNC BINDINGS"),
            (hub, "SHOW DELIVERY OUTCOMES FOR records"),
        ] {
            let reader = ReadSession::open_with_options(
                path,
                ReadSessionOptions {
                    limits,
                    ..Default::default()
                },
            )
            .unwrap();
            match reader.execute(sql, &p()) {
                Err(Error::ReadFailure(failure)) => assert_eq!(
                    failure.kind(),
                    ReadFailureKind::OwnerLimitExceeded,
                    "{sql}: {failure}"
                ),
                other => panic!(
                    "{sql} must enforce the ordinary result ceiling without returning a prefix: {other:?}"
                ),
            }
        }
    }
}
