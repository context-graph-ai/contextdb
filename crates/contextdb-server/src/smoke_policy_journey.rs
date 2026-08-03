//! Fixed installed-release convergence and refusal journeys. Every exchange
//! uses the production ticketed-Iroh endpoint and durable database/identity
//! files; the only child process is the installed product CLI.

use contextdb_core::{TenantId, Value};
use contextdb_engine::database::{DeleteObligationInspection, SnapshotInspector};
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::{Database, SyncClient, SyncServer};
use contextdb_server::{FabricIdentity, PeerEndpoint, peer_bind_spec, peer_dial_spec};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const TWO_WAY_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
    HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP FIRST";
const PUSH_ONLY_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
    RETAIN 0 SECONDS SYNC SAFE SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST";
const PULL_ONLY_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
    SYNC PULL ONLY SYNC CONFLICT KEEP FIRST";
const LATEST_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) \
    HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST";

struct Hub {
    db: Arc<Database>,
    endpoint: PeerEndpoint,
    ticket: String,
    node_id: String,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn start(root: &Path, tenant: &str, ddl: Option<&str>) -> Result<Self, String> {
        std::fs::create_dir_all(root)
            .map_err(|_| "cannot create policy verifier hub directory".to_string())?;
        let db_path = root.join("hub.db");
        let identity_path = root.join("hub.identity");
        let endpoint = PeerEndpoint::bind(&peer_bind_spec(&identity_path))
            .await
            .map_err(|_| "cannot bind policy verifier hub".to_string())?;
        let ticket = endpoint.ticket();
        let node_id = endpoint.node_id();
        let db = Arc::new(
            Database::open(&db_path)
                .map_err(|_| "cannot open policy verifier hub database".to_string())?,
        );
        if let Some(ddl) = ddl
            && db.table_meta("notes").is_none()
        {
            db.execute(ddl, &HashMap::new())
                .map_err(|_| "cannot declare policy verifier hub table".to_string())?;
        }
        let server = SyncServer::new(db.clone(), &endpoint, TenantId::from(tenant));
        let shutdown = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let shutdown = shutdown.clone();
            async move { server.run_until(shutdown).await }
        });
        Ok(Self {
            db,
            endpoint,
            ticket,
            node_id,
            shutdown,
            task,
        })
    }

    async fn stop(self) -> Result<(), String> {
        self.shutdown.store(true, Ordering::SeqCst);
        tokio::time::timeout(Duration::from_secs(30), self.task)
            .await
            .map_err(|_| "policy verifier hub did not stop".to_string())?
            .map_err(|_| "policy verifier hub task failed".to_string())?;
        self.endpoint.close().await;
        self.db
            .close()
            .map_err(|_| "policy verifier hub database did not close".to_string())
    }
}

struct Edge {
    db: Arc<Database>,
    client: SyncClient,
    identity: PathBuf,
    node_id: String,
}

fn open_edge(
    root: &Path,
    name: &str,
    ticket: &str,
    tenant: &str,
    ddl: &str,
) -> Result<Edge, String> {
    let directory = root.join(name);
    std::fs::create_dir_all(&directory)
        .map_err(|_| "cannot create policy verifier edge directory".to_string())?;
    let db_path = directory.join("edge.db");
    let identity = directory.join("edge.identity");
    let node_id = FabricIdentity::load_or_generate(&identity)
        .map_err(|_| "cannot load policy verifier edge identity".to_string())?
        .node_id();
    let db = Arc::new(
        Database::open(&db_path)
            .map_err(|_| "cannot open policy verifier edge database".to_string())?,
    );
    if db.table_meta("notes").is_none() {
        db.execute(ddl, &HashMap::new())
            .map_err(|_| "cannot declare policy verifier edge table".to_string())?;
    }
    let client = SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, &identity),
        TenantId::from(tenant),
    );
    Ok(Edge {
        db,
        client,
        identity,
        node_id,
    })
}

fn put(db: &Database, id: Uuid, body: &str) -> Result<(), String> {
    db.execute(
        "INSERT INTO notes (id, body) VALUES ($id, $body)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .map_err(|_| "policy verifier insert failed".to_string())?;
    Ok(())
}

fn update(db: &Database, id: Uuid, body: &str) -> Result<(), String> {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .map_err(|_| "policy verifier update failed".to_string())?;
    Ok(())
}

fn delete(db: &Database, id: Uuid) -> Result<(), String> {
    db.execute(
        "DELETE FROM notes WHERE id = $id",
        &HashMap::from([("id".to_string(), Value::Uuid(id))]),
    )
    .map_err(|_| "policy verifier delete failed".to_string())?;
    Ok(())
}

fn body(db: &Database, id: Uuid) -> Result<Option<String>, String> {
    let rows = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|_| "policy verifier row read failed".to_string())?
        .rows;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [Value::Text(body)] => Ok(Some(body.clone())),
            _ => Err("policy verifier body has the wrong type".to_string()),
        },
        _ => Err("policy verifier key returned multiple rows".to_string()),
    }
}

fn exact_conflict(
    result: &impl serde::Serialize,
    id: Uuid,
    mutation_kind: &str,
    winner_node_id: &str,
    winner_position: u64,
) -> Result<JsonValue, String> {
    let rendered = serde_json::to_value(result)
        .map_err(|_| "cannot serialize policy verifier result".to_string())?;
    let conflicts = rendered["conflicts"]
        .as_array()
        .ok_or_else(|| "policy verifier result has no conflict list".to_string())?;
    if conflicts.len() != 1 {
        return Err("one refused mutation must yield one conflict".to_string());
    }
    let conflict = conflicts[0].clone();
    let expected_key = serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(id)))
        .map_err(|_| "cannot serialize policy verifier key".to_string())?;
    if conflict["table"] != "notes"
        || conflict["natural_key"] != expected_key
        || conflict["mutation_kind"] != mutation_kind
        || conflict["winning_author_node_id"] != winner_node_id
        || conflict["hub_acceptance_position"].as_u64() != Some(winner_position)
    {
        return Err(format!(
            "refusal omitted exact table/key/kind/winner/position: {conflict}"
        ));
    }
    Ok(conflict)
}

fn ensure_body(db: &Database, id: Uuid, expected: Option<&str>, place: &str) -> Result<(), String> {
    if body(db, id)?.as_deref() != expected {
        return Err(format!("{place} has the wrong row value"));
    }
    Ok(())
}

fn inspect_delete_obligation(
    db_path: &Path,
    artifact: &Path,
    id: Uuid,
) -> Result<DeleteObligationInspection, String> {
    let db = Database::open(db_path)
        .map_err(|_| "cannot open delete-obligation database".to_string())?;
    db.export_snapshot(artifact)
        .map_err(|_| "cannot export delete-obligation snapshot".to_string())?;
    db.close()
        .map_err(|_| "cannot close delete-obligation database".to_string())?;
    let inspector = SnapshotInspector::open(artifact)
        .map_err(|_| "cannot open delete-obligation snapshot".to_string())?;
    let report = inspector
        .inspect_key(
            "notes",
            &NaturalKey::single("id".to_string(), Value::Uuid(id)),
            &[],
        )
        .map_err(|_| "cannot inspect delete-obligation snapshot".to_string())?;
    inspector
        .close()
        .map_err(|_| "cannot close delete-obligation snapshot".to_string())?;
    report
        .lineage
        .map(|lineage| lineage.delete_obligation)
        .ok_or_else(|| "delete obligation has no durable lineage record".to_string())
}

pub async fn run(root: &Path, cli: &Path) -> Result<(), String> {
    if !root.is_dir() {
        return Err("policy verifier root must already exist".to_string());
    }
    if !cli.is_file() {
        return Err("policy verifier needs the installed CLI path".to_string());
    }
    run_two_way(root, cli).await?;
    run_push_only_write(root).await?;
    run_push_only_delete(root, cli).await?;
    run_pull_only(root).await?;
    run_offline_delete_accepted(root, cli).await?;
    run_offline_delete_refused(root, cli).await?;
    println!("{}", json!({"event":"policy_journeys_complete"}));
    Ok(())
}

async fn run_two_way(root: &Path, cli: &Path) -> Result<(), String> {
    let case = root.join("two-way");
    let tenant = "installed-two-way";
    let hub = Hub::start(&case.join("hub"), tenant, Some(TWO_WAY_DDL)).await?;
    let id = Uuid::from_u128(0x11111111111141118111111111111111);
    let winner = open_edge(&case, "winner", &hub.ticket, tenant, TWO_WAY_DDL)?;
    let loser = open_edge(&case, "loser", &hub.ticket, tenant, TWO_WAY_DDL)?;
    put(&winner.db, id, "hub-accepted-first")?;
    put(&loser.db, id, "offline-loser")?;
    let accepted = winner
        .client
        .push()
        .await
        .map_err(|_| "winning two-way push failed".to_string())?;
    let winner_position = accepted.new_lsn.0;
    let winner_node_id = winner.node_id.clone();
    let loser_node_id = loser.node_id.clone();
    let hub_node_id = hub.node_id.clone();
    winner.client.shutdown().await;
    loser.client.shutdown().await;
    drop(winner.client);
    drop(loser.client);
    hub.stop().await?;

    let hub = Hub::start(&case.join("hub"), tenant, Some(TWO_WAY_DDL)).await?;
    if hub.node_id != hub_node_id {
        return Err("hub identity changed across restart".to_string());
    }
    let winner_client = SyncClient::new(
        winner.db.clone(),
        &peer_dial_spec(&hub.ticket, &winner.identity),
        TenantId::from(tenant),
    );
    let loser_client = SyncClient::new(
        loser.db.clone(),
        &peer_dial_spec(&hub.ticket, &loser.identity),
        TenantId::from(tenant),
    );
    let refusal = loser_client
        .push()
        .await
        .map_err(|_| "losing two-way push did not return a result".to_string())?;
    let first_conflict = exact_conflict(&refusal, id, "edit", &winner_node_id, winner_position)?;
    winner_client
        .pull_default()
        .await
        .map_err(|_| "winning edge pull failed".to_string())?;
    loser_client
        .pull_default()
        .await
        .map_err(|_| "losing edge pull failed".to_string())?;
    ensure_body(&hub.db, id, Some("hub-accepted-first"), "hub")?;
    ensure_body(&winner.db, id, Some("hub-accepted-first"), "winning edge")?;
    ensure_body(&loser.db, id, Some("hub-accepted-first"), "losing edge")?;

    update(&loser.db, id, "loser-after-restart")?;
    let repeated = loser_client
        .push()
        .await
        .map_err(|_| "repeat losing push failed".to_string())?;
    let repeated_conflict =
        exact_conflict(&repeated, id, "edit", &winner_node_id, winner_position)?;
    if repeated_conflict["hub_acceptance_position"] != first_conflict["hub_acceptance_position"] {
        return Err("winner position changed across hub restart".to_string());
    }
    loser_client
        .pull_default()
        .await
        .map_err(|_| "repeat losing edge did not reconcile".to_string())?;
    ensure_body(
        &loser.db,
        id,
        Some("hub-accepted-first"),
        "repeat losing edge",
    )?;

    let hub_local_id = Uuid::from_u128(0x22222222222242228222222222222222);
    put(&hub.db, hub_local_id, "hub-local-winner")?;
    let hub_local_position = hub.db.current_lsn().0;
    let third = open_edge(&case, "third", &hub.ticket, tenant, TWO_WAY_DDL)?;
    put(&third.db, hub_local_id, "edge-loser")?;
    let hub_local = third
        .client
        .push()
        .await
        .map_err(|_| "hub-local refusal push failed".to_string())?;
    let hub_local_conflict = exact_conflict(
        &hub_local,
        hub_local_id,
        "edit",
        &hub_node_id,
        hub_local_position,
    )?;
    third
        .client
        .pull_default()
        .await
        .map_err(|_| "hub-local losing edge did not reconcile".to_string())?;
    ensure_body(
        &hub.db,
        hub_local_id,
        Some("hub-local-winner"),
        "hub-local winner",
    )?;
    ensure_body(
        &third.db,
        hub_local_id,
        Some("hub-local-winner"),
        "hub-local losing edge",
    )?;
    let (manual_conflict, auto_conflict) =
        run_installed_cli_refusals(&case, cli, &hub, tenant).await?;
    println!(
        "{}",
        json!({
            "event":"two_way_keep_first",
            "hub_node_id":hub_node_id,
            "winner_node_id":winner_node_id,
            "loser_node_id":loser_node_id,
            "winner_position":winner_position,
            "conflict":first_conflict,
            "hub_local_conflict":hub_local_conflict,
            "manual_cli_conflict":manual_conflict,
            "auto_sync_conflict":auto_conflict,
            "final_body":"hub-accepted-first",
        })
    );
    winner_client.shutdown().await;
    loser_client.shutdown().await;
    third.client.shutdown().await;
    hub.stop().await
}

async fn run_installed_cli_refusals(
    case: &Path,
    cli: &Path,
    hub: &Hub,
    tenant: &str,
) -> Result<(JsonValue, JsonValue), String> {
    let manual_id = Uuid::from_u128(0x66666666666646668666666666666666);
    put(&hub.db, manual_id, "manual-hub-winner")?;
    let manual_position = hub.db.current_lsn().0;
    let manual_dir = case.join("manual-cli");
    std::fs::create_dir_all(&manual_dir)
        .map_err(|_| "cannot create manual CLI verifier directory".to_string())?;
    let manual_db = manual_dir.join("edge.db");
    {
        let db = Database::open(&manual_db)
            .map_err(|_| "cannot open manual CLI verifier database".to_string())?;
        db.execute(TWO_WAY_DDL, &HashMap::new())
            .map_err(|_| "cannot declare manual CLI verifier table".to_string())?;
        put(&db, manual_id, "manual-edge-loser")?;
        db.close()
            .map_err(|_| "cannot close manual CLI verifier database".to_string())?;
    }
    let manual = run_cli(
        cli,
        &manual_db,
        Some(&hub.ticket),
        tenant,
        ".sync push\n.sync pull\n.quit\n",
        60_000,
    )?;
    if !manual.0.success() {
        return Err("manual installed CLI refusal journey failed".to_string());
    }
    let manual_conflict = find_cli_conflict(
        manual.1.lines().chain(manual.2.lines()),
        manual_id,
        "edit",
        &hub.node_id,
        manual_position,
    )?;
    let manual_edge = Database::open(&manual_db)
        .map_err(|_| "cannot reopen manual CLI verifier database".to_string())?;
    ensure_body(
        &manual_edge,
        manual_id,
        Some("manual-hub-winner"),
        "manual CLI edge",
    )?;
    manual_edge
        .close()
        .map_err(|_| "cannot close manual CLI verification read".to_string())?;

    let auto_id = Uuid::from_u128(0x77777777777747778777777777777777);
    put(&hub.db, auto_id, "auto-hub-winner")?;
    let auto_position = hub.db.current_lsn().0;
    let auto_dir = case.join("auto-cli");
    std::fs::create_dir_all(&auto_dir)
        .map_err(|_| "cannot create auto CLI verifier directory".to_string())?;
    let auto_db = auto_dir.join("edge.db");
    let setup = run_cli(
        cli,
        &auto_db,
        Some(&hub.ticket),
        tenant,
        ".sync pull\n.quit\n",
        60_000,
    )?;
    if !setup.0.success() {
        return Err("auto CLI setup pull failed".to_string());
    }
    {
        let db = Database::open(&auto_db)
            .map_err(|_| "cannot open auto CLI database offline".to_string())?;
        delete(&db, auto_id)?;
        db.close()
            .map_err(|_| "cannot close auto CLI database offline".to_string())?;
    }

    let mut child = Command::new(cli)
        .arg(&auto_db)
        .arg("--json")
        .env("CONTEXTDB_SYNC_ENDPOINT", &hub.ticket)
        .env("CONTEXTDB_TENANT_ID", tenant)
        .env("CONTEXTDB_SYNC_DEBOUNCE_MS", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| "cannot spawn installed auto-sync CLI".to_string())?;
    let (reports, readers) = start_output_readers(&mut child)?;
    let trigger_id = Uuid::from_u128(0x88888888888848888888888888888888);
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "auto-sync CLI stdin is unavailable".to_string())?;
    writeln!(
        stdin,
        "INSERT INTO notes (id, body) VALUES ('{trigger_id}', 'auto-sync-trigger');"
    )
    .map_err(|_| "cannot trigger installed auto-sync".to_string())?;
    stdin
        .flush()
        .map_err(|_| "cannot flush installed auto-sync input".to_string())?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut observed_lines = 0_u64;
    let mut closed_streams = 0_u8;
    let auto_conflict = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (stream, line) = reports
            .recv_timeout(remaining)
            .map_err(|_| {
                format!(
                    "auto-sync refusal was not reported after {observed_lines} output lines and {closed_streams} closed streams"
                )
            })?;
        let Some(line) = line else {
            let _ = stream;
            closed_streams = closed_streams.saturating_add(1);
            continue;
        };
        let _ = stream;
        observed_lines = observed_lines.saturating_add(1);
        if let Ok(conflict) = find_cli_conflict(
            std::iter::once(line.as_str()),
            auto_id,
            "delete",
            &hub.node_id,
            auto_position,
        ) {
            break conflict;
        }
    };
    stdin
        .write_all(b".sync pull\n.quit\n")
        .map_err(|_| "cannot reconcile and stop installed auto-sync CLI".to_string())?;
    drop(stdin);
    let status = wait_for_exit(&mut child)?;
    if !status.success() {
        return Err(format!("installed auto-sync CLI exited with {status}"));
    }
    close_output_readers(readers)?;
    let auto_edge =
        Database::open(&auto_db).map_err(|_| "cannot reopen auto-sync CLI database".to_string())?;
    ensure_body(
        &auto_edge,
        auto_id,
        Some("auto-hub-winner"),
        "auto-sync CLI edge",
    )?;
    auto_edge
        .close()
        .map_err(|_| "cannot close auto-sync verification read".to_string())?;
    Ok((manual_conflict, auto_conflict))
}

fn find_cli_conflict<'a>(
    lines: impl IntoIterator<Item = &'a str>,
    id: Uuid,
    mutation_kind: &str,
    author: &str,
    position: u64,
) -> Result<JsonValue, String> {
    let id = id.to_string();
    for line in lines {
        let Ok(document) = serde_json::from_str::<JsonValue>(line) else {
            continue;
        };
        let mut leaves = Vec::new();
        json_leaves(&document, &mut leaves);
        let rendered = document.to_string().to_ascii_lowercase();
        if (rendered.contains("conflict") || rendered.contains("refus"))
            && leaves.iter().any(|value| value.as_str() == Some("notes"))
            && leaves
                .iter()
                .any(|value| value.as_str() == Some(id.as_str()))
            && leaves
                .iter()
                .any(|value| value.as_str() == Some(mutation_kind))
            && leaves.iter().any(|value| value.as_str() == Some(author))
            && leaves.iter().any(|value| value.as_u64() == Some(position))
        {
            return Ok(document);
        }
    }
    Err("installed CLI omitted the complete typed refusal".to_string())
}

async fn run_push_only_write(root: &Path) -> Result<(), String> {
    let case = root.join("push-only-write");
    let tenant = "installed-push-only-write";
    let hub = Hub::start(&case.join("hub"), tenant, Some(PUSH_ONLY_DDL)).await?;
    let id = Uuid::from_u128(0x33333333333343338333333333333333);
    let winner = open_edge(&case, "winner", &hub.ticket, tenant, PUSH_ONLY_DDL)?;
    put(&winner.db, id, "camera-hub-winner")?;
    let accepted = winner
        .client
        .push()
        .await
        .map_err(|_| "push-only winning push failed".to_string())?;
    let loser_directory = case.join("loser");
    std::fs::create_dir_all(&loser_directory)
        .map_err(|_| "cannot create cloned-identity loser directory".to_string())?;
    std::fs::copy(&winner.identity, loser_directory.join("edge.identity"))
        .map_err(|_| "cannot clone the camera-box identity fixture".to_string())?;
    let loser = open_edge(&case, "loser", &hub.ticket, tenant, PUSH_ONLY_DDL)?;
    if loser.node_id != winner.node_id {
        return Err("cloned camera boxes did not authenticate under one identity".to_string());
    }
    put(&loser.db, id, "camera-refused-local")?;
    let source_lsn = loser.db.current_lsn();
    let refusal = loser
        .client
        .push()
        .await
        .map_err(|_| "push-only refusal did not return a result".to_string())?;
    let conflict = exact_conflict(&refusal, id, "edit", &winner.node_id, accepted.new_lsn.0)?;
    if loser
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect push-only pending state".to_string())?
    {
        return Err("push-only refused write remained pending".to_string());
    }
    ensure_body(&hub.db, id, Some("camera-hub-winner"), "push-only hub")?;
    ensure_body(
        &loser.db,
        id,
        Some("camera-refused-local"),
        "push-only loser",
    )?;
    let retained = loser
        .db
        .point_lookup(
            "notes",
            "id",
            &Value::Uuid(id),
            loser.db.snapshot_at(source_lsn),
        )
        .map_err(|_| "push-only retained-history lookup failed".to_string())?
        .ok_or_else(|| "push-only refused version is absent from retained history".to_string())?;
    if retained.values.get("body") != Some(&Value::Text("camera-refused-local".to_string())) {
        return Err("push-only retained history has the wrong refused value".to_string());
    }
    let repeat = loser
        .client
        .push()
        .await
        .map_err(|_| "push-only repeat push failed".to_string())?;
    if repeat.applied_rows != 0 || repeat.skipped_rows != 0 || !repeat.conflicts.is_empty() {
        return Err("push-only refused write was resent".to_string());
    }
    let pruning = loser
        .db
        .run_pruning_cycle_checked()
        .map_err(|_| "push-only pruning cycle failed".to_string())?;
    if pruning.blocked_count != 0 {
        return Err(format!(
            "terminal push-only refusal still blocks SYNC SAFE pruning: {:?}",
            pruning.blocked
        ));
    }
    println!(
        "{}",
        json!({
            "event":"push_only_write_refused",
            "cloned_node_id":winner.node_id,
            "conflict":conflict,
            "hub_body":"camera-hub-winner",
            "local_history_body":"camera-refused-local",
            "pending":false,
            "pruning_blocked":pruning.blocked_count,
        })
    );
    winner.client.shutdown().await;
    loser.client.shutdown().await;
    hub.stop().await
}

async fn run_push_only_delete(root: &Path, cli: &Path) -> Result<(), String> {
    let case = root.join("push-only-delete");
    std::fs::create_dir_all(&case)
        .map_err(|_| "cannot create push-only delete directory".to_string())?;
    let tenant = "installed-push-only-delete";
    let hub = Hub::start(&case.join("hub"), tenant, Some(PUSH_ONLY_DDL)).await?;
    let id = Uuid::from_u128(0x44444444444444448444444444444444);
    let winner = open_edge(&case, "winner", &hub.ticket, tenant, PUSH_ONLY_DDL)?;
    put(&winner.db, id, "delete-hub-winner")?;
    let accepted = winner
        .client
        .push()
        .await
        .map_err(|_| "push-only delete winner push failed".to_string())?;

    let deleting_dir = case.join("deleting");
    std::fs::create_dir_all(&deleting_dir)
        .map_err(|_| "cannot create deleting edge directory".to_string())?;
    let deleting_db = deleting_dir.join("edge.db");
    std::fs::copy(&winner.identity, deleting_dir.join("edge.identity"))
        .map_err(|_| "cannot clone deleting camera-box identity fixture".to_string())?;
    let sql = format!(
        "{PUSH_ONLY_DDL};\nINSERT INTO notes (id, body) VALUES ('{id}', 'delete-hub-winner');\nDELETE FROM notes WHERE id = '{id}';\n.quit\n"
    );
    let output = run_cli(cli, &deleting_db, None, tenant, &sql, 60_000)?;
    if !output.0.success() {
        return Err("offline deleting CLI process failed".to_string());
    }
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, PUSH_ONLY_DDL)?;
    if !deleting
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect reopened delete obligation".to_string())?
    {
        return Err("reopened push-only delete was not pending".to_string());
    }
    let refusal = deleting
        .client
        .push()
        .await
        .map_err(|_| "push-only delete refusal did not return a result".to_string())?;
    let conflict = exact_conflict(&refusal, id, "delete", &winner.node_id, accepted.new_lsn.0)?;
    if deleting
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect retired delete".to_string())?
    {
        return Err("push-only refused delete remained pending".to_string());
    }
    deleting.client.shutdown().await;
    drop(deleting.client);
    deleting
        .db
        .close()
        .map_err(|_| "cannot close push-only refused-delete database".to_string())?;
    drop(deleting.db);
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, PUSH_ONLY_DDL)?;
    if deleting
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect durable push-only refusal".to_string())?
    {
        return Err("push-only refused delete returned after restart".to_string());
    }
    ensure_body(&deleting.db, id, None, "push-only deleting edge")?;
    ensure_body(
        &hub.db,
        id,
        Some("delete-hub-winner"),
        "push-only delete hub",
    )?;
    println!(
        "{}",
        json!({
            "event":"push_only_delete_refused",
            "conflict":conflict,
            "edge_body":null,
            "hub_body":"delete-hub-winner",
            "pending":false,
        })
    );
    winner.client.shutdown().await;
    deleting.client.shutdown().await;
    hub.stop().await
}

async fn run_pull_only(root: &Path) -> Result<(), String> {
    let case = root.join("pull-only");
    let tenant = "installed-pull-only";
    let hub = Hub::start(&case.join("hub"), tenant, Some(PULL_ONLY_DDL)).await?;
    let id = Uuid::from_u128(0x55555555555545558555555555555555);
    put(&hub.db, id, "authoritative-hub-value")?;
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, PULL_ONLY_DDL)?;
    put(&edge.db, id, "local-edit-that-must-not-leak")?;
    let outbound = edge
        .client
        .push()
        .await
        .map_err(|_| "pull-only outbound probe failed".to_string())?;
    if outbound.applied_rows != 0 || outbound.skipped_rows != 0 || !outbound.conflicts.is_empty() {
        return Err("pull-only local edit leaked into outbound sync".to_string());
    }
    ensure_body(
        &hub.db,
        id,
        Some("authoritative-hub-value"),
        "pull-only hub",
    )?;
    edge.client
        .pull_default()
        .await
        .map_err(|_| "pull-only pull failed".to_string())?;
    ensure_body(
        &edge.db,
        id,
        Some("authoritative-hub-value"),
        "pull-only edge",
    )?;
    println!(
        "{}",
        json!({
            "event":"pull_only_overwrite",
            "before":"local-edit-that-must-not-leak",
            "after":"authoritative-hub-value",
            "outbound_rows":0,
        })
    );
    edge.client.shutdown().await;
    hub.stop().await
}

async fn run_offline_delete_accepted(root: &Path, cli: &Path) -> Result<(), String> {
    let case = root.join("offline-delete-accepted");
    let tenant = "installed-delete-accepted";
    let hub = Hub::start(&case.join("hub"), tenant, Some(LATEST_DDL)).await?;
    let id = Uuid::from_u128(0x99999999999949998999999999999999);
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, LATEST_DDL)?;
    let observer = open_edge(&case, "observer", &hub.ticket, tenant, LATEST_DDL)?;
    put(&deleting.db, id, "accepted-offline-delete")?;
    deleting
        .client
        .push()
        .await
        .map_err(|_| "accepted-delete seed push failed".to_string())?;
    observer
        .client
        .pull_default()
        .await
        .map_err(|_| "accepted-delete observer seed pull failed".to_string())?;
    let deleting_db_path = case.join("deleting").join("edge.db");
    deleting.client.shutdown().await;
    drop(deleting.client);
    deleting
        .db
        .close()
        .map_err(|_| "cannot close deleting edge before process restart".to_string())?;
    drop(deleting.db);

    let deleted = run_cli(
        cli,
        &deleting_db_path,
        None,
        tenant,
        &format!("DELETE FROM notes WHERE id = '{id}';\n.quit\n"),
        60_000,
    )?;
    if !deleted.0.success() {
        return Err("offline accepted-delete process failed".to_string());
    }
    let pending = inspect_delete_obligation(&deleting_db_path, &case.join("pending.snapshot"), id)?;
    if pending != DeleteObligationInspection::Pending {
        return Err("offline accepted delete was not pending after process exit".to_string());
    }

    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, LATEST_DDL)?;
    deleting
        .client
        .push()
        .await
        .map_err(|_| "restarted accepted delete push failed".to_string())?;
    observer
        .client
        .pull_default()
        .await
        .map_err(|_| "observer did not receive accepted delete".to_string())?;
    deleting
        .client
        .pull_default()
        .await
        .map_err(|_| "later pull after accepted delete failed".to_string())?;
    ensure_body(&hub.db, id, None, "accepted-delete hub")?;
    ensure_body(&deleting.db, id, None, "accepted-delete reopened edge")?;
    ensure_body(&observer.db, id, None, "accepted-delete observer")?;
    deleting.client.shutdown().await;
    drop(deleting.client);
    deleting
        .db
        .close()
        .map_err(|_| "cannot close accepted-delete edge for second reopen".to_string())?;
    drop(deleting.db);
    let accepted =
        inspect_delete_obligation(&deleting_db_path, &case.join("accepted.snapshot"), id)?;
    if accepted != DeleteObligationInspection::Accepted {
        return Err("hub-accepted delete did not retain its durable marker".to_string());
    }
    let reopened = open_edge(&case, "deleting", &hub.ticket, tenant, LATEST_DDL)?;
    reopened
        .client
        .pull_default()
        .await
        .map_err(|_| "second reopened deleting edge could not pull".to_string())?;
    ensure_body(&reopened.db, id, None, "second reopened deleting edge")?;
    println!(
        "{}",
        json!({
            "event":"offline_delete_accepted",
            "before_restart":"pending",
            "after_hub":"accepted",
            "hub_present":false,
            "deleting_edge_present":false,
            "observer_present":false,
        })
    );
    observer.client.shutdown().await;
    reopened.client.shutdown().await;
    hub.stop().await
}

async fn run_offline_delete_refused(root: &Path, cli: &Path) -> Result<(), String> {
    let case = root.join("offline-delete-refused");
    let tenant = "installed-delete-refused";
    let hub = Hub::start(&case.join("hub"), tenant, Some(TWO_WAY_DDL)).await?;
    let id = Uuid::from_u128(0xaaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa);
    let winner = open_edge(&case, "winner", &hub.ticket, tenant, TWO_WAY_DDL)?;
    put(&winner.db, id, "refused-delete-winner")?;
    let accepted = winner
        .client
        .push()
        .await
        .map_err(|_| "refused-delete winner push failed".to_string())?;
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, TWO_WAY_DDL)?;
    deleting
        .client
        .pull_default()
        .await
        .map_err(|_| "refused-delete seed pull failed".to_string())?;
    let deleting_db_path = case.join("deleting").join("edge.db");
    deleting.client.shutdown().await;
    drop(deleting.client);
    deleting
        .db
        .close()
        .map_err(|_| "cannot close refused-delete edge before process restart".to_string())?;
    drop(deleting.db);
    let deleted = run_cli(
        cli,
        &deleting_db_path,
        None,
        tenant,
        &format!("DELETE FROM notes WHERE id = '{id}';\n.quit\n"),
        60_000,
    )?;
    if !deleted.0.success() {
        return Err("offline refused-delete process failed".to_string());
    }
    let pending = inspect_delete_obligation(&deleting_db_path, &case.join("pending.snapshot"), id)?;
    if pending != DeleteObligationInspection::Pending {
        return Err("offline refused delete was not pending after process exit".to_string());
    }
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, TWO_WAY_DDL)?;
    let refusal = deleting
        .client
        .push()
        .await
        .map_err(|_| "two-way delete refusal did not return a result".to_string())?;
    let conflict = exact_conflict(&refusal, id, "delete", &winner.node_id, accepted.new_lsn.0)?;
    if deleting
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect refused-delete resend state".to_string())?
    {
        return Err("two-way refused delete remained pending".to_string());
    }
    deleting.client.shutdown().await;
    drop(deleting.client);
    deleting
        .db
        .close()
        .map_err(|_| "cannot close refused-delete database after adjudication".to_string())?;
    drop(deleting.db);
    let deleting = open_edge(&case, "deleting", &hub.ticket, tenant, TWO_WAY_DDL)?;
    if deleting
        .client
        .has_pending_push_changes()
        .map_err(|_| "cannot inspect durable two-way refusal".to_string())?
    {
        return Err("two-way refused delete returned after restart".to_string());
    }
    deleting
        .client
        .pull_default()
        .await
        .map_err(|_| "two-way refused delete did not reconcile".to_string())?;
    ensure_body(
        &deleting.db,
        id,
        Some("refused-delete-winner"),
        "refused deleting edge",
    )?;
    ensure_body(
        &hub.db,
        id,
        Some("refused-delete-winner"),
        "refused-delete hub",
    )?;
    println!(
        "{}",
        json!({
            "event":"offline_delete_refused",
            "before_restart":"pending",
            "after_hub":"terminal_refusal",
            "conflict":conflict,
            "final_body":"refused-delete-winner",
            "pending":false,
        })
    );
    winner.client.shutdown().await;
    deleting.client.shutdown().await;
    hub.stop().await
}

fn run_cli(
    cli: &Path,
    db: &Path,
    ticket: Option<&str>,
    tenant: &str,
    input: &str,
    debounce_ms: u64,
) -> Result<(ExitStatus, String, String), String> {
    let mut command = Command::new(cli);
    command
        .arg(db)
        .arg("--json")
        .env_remove("CONTEXTDB_SYNC_ENDPOINT")
        .env_remove("CONTEXTDB_TENANT_ID")
        .env_remove("CONTEXTDB_SYNC_DEBOUNCE_MS");
    if let Some(ticket) = ticket {
        command
            .env("CONTEXTDB_SYNC_ENDPOINT", ticket)
            .env("CONTEXTDB_TENANT_ID", tenant)
            .env("CONTEXTDB_SYNC_DEBOUNCE_MS", debounce_ms.to_string());
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|_| "cannot spawn installed CLI".to_string())?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| "installed CLI stdin is unavailable".to_string())?
        .write_all(input.as_bytes())
        .map_err(|_| "cannot write installed CLI input".to_string())?;
    drop(child.stdin.take());
    wait_with_output(child)
}

fn wait_with_output(mut child: Child) -> Result<(ExitStatus, String, String), String> {
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "installed CLI stdout is unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "installed CLI stderr is unavailable".to_string())?;
    let stdout = std::thread::spawn(move || read_all(stdout));
    let stderr = std::thread::spawn(move || read_all(stderr));
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_| "cannot poll installed CLI".to_string())?
        {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("installed CLI did not exit within its bound".to_string());
        }
        std::thread::yield_now();
    };
    let stdout = stdout
        .join()
        .map_err(|_| "installed CLI stdout reader failed".to_string())??;
    let stderr = stderr
        .join()
        .map_err(|_| "installed CLI stderr reader failed".to_string())??;
    Ok((status, stdout, stderr))
}

fn read_all(mut reader: impl Read) -> Result<String, String> {
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .map_err(|_| "cannot read installed CLI output".to_string())?;
    Ok(output)
}

fn json_leaves<'a>(document: &'a JsonValue, leaves: &mut Vec<&'a JsonValue>) {
    match document {
        JsonValue::Array(values) => {
            for value in values {
                json_leaves(value, leaves);
            }
        }
        JsonValue::Object(values) => {
            for value in values.values() {
                json_leaves(value, leaves);
            }
        }
        leaf => leaves.push(leaf),
    }
}

type ChildOutput = (&'static str, Option<String>);

fn start_line_reader<R: BufRead + Send + 'static>(
    mut reader: R,
    stream: &'static str,
    tx: mpsc::Sender<ChildOutput>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let rendered = line.trim_end().to_string();
                    if tx.send((stream, Some(rendered))).is_err() {
                        return;
                    }
                }
                Err(_) => break,
            }
        }
        let _ = tx.send((stream, None));
    })
}

fn start_output_readers(
    child: &mut Child,
) -> Result<
    (
        mpsc::Receiver<ChildOutput>,
        Vec<std::thread::JoinHandle<()>>,
    ),
    String,
> {
    let (sender, receiver) = mpsc::channel();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "auto-sync CLI stdout is unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "auto-sync CLI stderr is unavailable".to_string())?;
    let stdout = start_line_reader(BufReader::new(stdout), "stdout", sender.clone());
    let stderr = start_line_reader(BufReader::new(stderr), "stderr", sender);
    Ok((receiver, vec![stdout, stderr]))
}

fn wait_for_exit(child: &mut Child) -> Result<ExitStatus, String> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child
            .try_wait()
            .map_err(|_| "cannot poll installed auto-sync CLI".to_string())?
        {
            return Ok(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("installed auto-sync CLI did not exit within its bound".to_string());
        }
        std::thread::yield_now();
    }
}

fn close_output_readers(readers: Vec<std::thread::JoinHandle<()>>) -> Result<(), String> {
    for reader in readers {
        reader
            .join()
            .map_err(|_| "installed auto-sync output reader failed".to_string())?;
    }
    Ok(())
}
