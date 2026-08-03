//! Fixed installed-release journeys for authoritative, permanent erasure.
//! They use durable files, public SQL, ContextDB-owned media storage, and the
//! production ticketed-Iroh path; no arbitrary SQL or transport controls are
//! exposed by the verifier command.

use contextdb_core::{Error, RowId, TenantId, Value, VectorIndexRef};
use contextdb_engine::composite_store::ChangeLogEntry;
use contextdb_engine::database::{
    BlobInspection, DeleteObligationInspection, KeyInspection, SnapshotInspector,
};
use contextdb_engine::protocol::{
    MessageType, PushRequest, PushResponse, WireChangeSet, WirePurgeChange, WirePushError, decode,
    encode,
};
use contextdb_engine::subjects::push_subject;
use contextdb_engine::sync_types::NaturalKey;
use contextdb_engine::transport::client_transport;
use contextdb_engine::work_ledger::{
    BlobHash, InputRef, JobSpec, MovementPolicy, install_work_ledger_schema, submit_job,
};
use contextdb_engine::{ApplyResult, BlobStore, Database, SyncClient, SyncServer};
use contextdb_server::{FabricIdentity, PeerEndpoint, peer_bind_spec, peer_dial_spec};
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use uuid::Uuid;

const NOTES_DDL: &str = "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT, embedding VECTOR(3)) \
    HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST";
const PUSH_ONLY_DDL: &str = "CREATE TABLE push_only_notes (id UUID PRIMARY KEY, body TEXT) \
    HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP LATEST";

struct Hub {
    db: Arc<Database>,
    endpoint: PeerEndpoint,
    directory: PathBuf,
    ticket: String,
    node_id: String,
    shutdown: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Hub {
    async fn start(directory: &Path, tenant: &str, ddl: Option<&str>) -> Result<Self, String> {
        std::fs::create_dir_all(directory)
            .map_err(|_| "cannot create purge verifier hub directory".to_string())?;
        let endpoint = PeerEndpoint::bind(&peer_bind_spec(&directory.join("hub.identity")))
            .await
            .map_err(|_| "cannot bind purge verifier hub".to_string())?;
        let ticket = endpoint.ticket();
        let node_id = endpoint.node_id();
        let db = Arc::new(
            Database::open(directory.join("hub.db"))
                .map_err(|_| "cannot open purge verifier hub database".to_string())?,
        );
        if let Some(ddl) = ddl {
            let table = if ddl == PUSH_ONLY_DDL {
                "push_only_notes"
            } else {
                "notes"
            };
            if db.table_meta(table).is_none() {
                db.execute(ddl, &HashMap::new())
                    .map_err(|_| "cannot declare purge verifier hub table".to_string())?;
            }
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
            directory: directory.to_path_buf(),
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
            .map_err(|_| "purge verifier hub did not stop".to_string())?
            .map_err(|_| "purge verifier hub task failed".to_string())?;
        self.endpoint.close().await;
        self.db
            .close()
            .map_err(|_| "purge verifier hub database did not close".to_string())
    }
}

struct Edge {
    db: Arc<Database>,
    client: SyncClient,
    identity: PathBuf,
}

fn open_edge(
    root: &Path,
    name: &str,
    ticket: &str,
    tenant: &str,
    ddl: Option<&str>,
) -> Result<Edge, String> {
    let directory = root.join(name);
    std::fs::create_dir_all(&directory)
        .map_err(|_| "cannot create purge verifier edge directory".to_string())?;
    let identity = directory.join("edge.identity");
    let db = Arc::new(
        Database::open(directory.join("edge.db"))
            .map_err(|_| "cannot open purge verifier edge database".to_string())?,
    );
    if let Some(ddl) = ddl {
        let table = if ddl == PUSH_ONLY_DDL {
            "push_only_notes"
        } else {
            "notes"
        };
        if db.table_meta(table).is_none() {
            db.execute(ddl, &HashMap::new())
                .map_err(|_| "cannot declare purge verifier edge table".to_string())?;
        }
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
    })
}

async fn within<F: std::future::Future>(future: F) -> Result<F::Output, String> {
    tokio::time::timeout(Duration::from_secs(60), future)
        .await
        .map_err(|_| "purge verifier transport operation timed out".to_string())
}

fn put_note(db: &Database, id: Uuid, body: &str, vector: Vec<f32>) -> Result<(), String> {
    db.execute(
        "INSERT INTO notes (id, body, embedding) VALUES ($id, $body, $embedding)",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
            ("embedding".to_string(), Value::Vector(vector)),
        ]),
    )
    .map_err(|_| "cannot insert purge verifier note".to_string())?;
    Ok(())
}

fn edit_note(db: &Database, id: Uuid, body: &str) -> Result<(), String> {
    db.execute(
        "UPDATE notes SET body = $body WHERE id = $id",
        &HashMap::from([
            ("id".to_string(), Value::Uuid(id)),
            ("body".to_string(), Value::Text(body.to_string())),
        ]),
    )
    .map_err(|_| "cannot edit purge verifier note".to_string())?;
    Ok(())
}

fn note_body(db: &Database, id: Uuid) -> Result<Option<String>, String> {
    let rows = db
        .execute(
            "SELECT body FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|_| "cannot read purge verifier note".to_string())?
        .rows;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [Value::Text(body)] => Ok(Some(body.clone())),
            _ => Err("purge verifier note has the wrong body type".to_string()),
        },
        _ => Err("purge verifier note key returned multiple rows".to_string()),
    }
}

fn note_row_id(db: &Database, id: Uuid) -> Result<RowId, String> {
    let rows = db
        .scan("notes", db.snapshot())
        .map_err(|_| "cannot scan purge verifier notes".to_string())?
        .into_iter()
        .filter(|row| row.values.get("id") == Some(&Value::Uuid(id)))
        .collect::<Vec<_>>();
    if rows.len() != 1 {
        return Err("purge verifier expected one exact note".to_string());
    }
    Ok(rows[0].row_id)
}

fn work_job_ids(db: &Database) -> Result<Vec<String>, String> {
    db.execute(
        "SELECT job_id FROM work_jobs ORDER BY job_id",
        &HashMap::new(),
    )
    .map_err(|_| "cannot read purge verifier work jobs".to_string())?
    .rows
    .into_iter()
    .map(|row| match row.as_slice() {
        [Value::Text(job_id)] => Ok(job_id.clone()),
        _ => Err("purge verifier work job has the wrong key type".to_string()),
    })
    .collect()
}

fn inspect_snapshot_key(
    inspector: &SnapshotInspector,
    table: &str,
    column: &str,
    value: Value,
) -> Result<KeyInspection, String> {
    inspector
        .inspect_key(table, &NaturalKey::single(column.to_string(), value), &[])
        .map_err(|_| "cannot inspect purge key state".to_string())
}

fn export_key(
    db: &Database,
    artifact: &Path,
    table: &str,
    column: &str,
    value: Value,
) -> Result<KeyInspection, String> {
    db.export_snapshot(artifact)
        .map_err(|_| "cannot export purge inspection snapshot".to_string())?;
    let inspector = SnapshotInspector::open(artifact)
        .map_err(|_| "cannot open purge inspection snapshot".to_string())?;
    let report = inspect_snapshot_key(&inspector, table, column, value)?;
    inspector
        .close()
        .map_err(|_| "cannot close purge key inspector".to_string())?;
    Ok(report)
}

fn export_blob(db: &Database, artifact: &Path, hash: &BlobHash) -> Result<BlobInspection, String> {
    db.export_snapshot(artifact)
        .map_err(|_| "cannot export blob inspection snapshot".to_string())?;
    let inspector = SnapshotInspector::open(artifact)
        .map_err(|_| "cannot open blob inspection snapshot".to_string())?;
    let report = inspector
        .inspect_blob(hash.as_bytes())
        .map_err(|_| "cannot inspect ContextDB-owned media".to_string())?;
    inspector
        .close()
        .map_err(|_| "cannot close blob inspector".to_string())?;
    Ok(report)
}

fn require_purged_key(report: &KeyInspection, place: &str) -> Result<u64, String> {
    if report.total_retained_versions != 0 || !report.retained_versions.is_empty() {
        return Err(format!("{place} still retains purged row history"));
    }
    let lineage = report
        .lineage
        .as_ref()
        .ok_or_else(|| format!("{place} has no permanent purge lineage"))?;
    if lineage.delete_obligation != DeleteObligationInspection::Purged {
        return Err(format!("{place} lineage is not permanently purged"));
    }
    lineage
        .purge_frontier_lsn
        .filter(|frontier| *frontier > 0)
        .ok_or_else(|| format!("{place} has no nonzero purge frontier"))
}

fn require_blob_absent(report: &BlobInspection, place: &str) -> Result<u64, String> {
    if report.active_generation.is_some()
        || report.manifest.is_some()
        || report.partial.is_some()
        || report.tag_roles.servable
        || report.tag_roles.fetch_protection
        || report.last_purge_lsn == 0
    {
        return Err(format!(
            "{place} still exposes ContextDB-owned media after purge"
        ));
    }
    Ok(report.last_purge_lsn)
}

fn require_clean_setup_sync(result: &ApplyResult, operation: &str) -> Result<(), String> {
    if !result.conflicts.is_empty() || result.skipped_rows != 0 {
        return Err(format!(
            "{operation} did not cleanly apply setup rows before purge: {} conflicts, {} skipped",
            result.conflicts.len(),
            result.skipped_rows
        ));
    }
    Ok(())
}

fn exact_purge_conflict(
    result: &impl serde::Serialize,
    table: &str,
    column: &str,
    value: Value,
    mutation: Option<&str>,
) -> Result<JsonValue, String> {
    let rendered =
        serde_json::to_value(result).map_err(|_| "cannot serialize purge refusal".to_string())?;
    let conflicts = rendered["conflicts"]
        .as_array()
        .ok_or_else(|| "purge refusal has no conflict list".to_string())?;
    if conflicts.len() != 1 {
        return Err("one stale lineage must yield one visible refusal".to_string());
    }
    let conflict = conflicts[0].clone();
    let key = serde_json::to_value(NaturalKey::single(column.to_string(), value))
        .map_err(|_| "cannot serialize purge refusal key".to_string())?;
    let meaning = conflict.to_string().to_ascii_lowercase();
    if conflict["table"] != table
        || conflict["natural_key"] != key
        || mutation.is_some_and(|mutation| conflict["mutation_kind"] != mutation)
        || !meaning.contains("purge")
        || !meaning.contains("lineage")
    {
        return Err(format!("stale-lineage refusal is incomplete: {conflict}"));
    }
    Ok(conflict)
}

pub async fn run(root: &Path) -> Result<(), String> {
    if !root.is_dir() {
        return Err("purge verifier root must already exist".to_string());
    }
    run_copy_erasure(root).await?;
    run_edge_authority(root).await?;
    run_stale_and_fresh(root).await?;
    run_recreated_generation(root).await?;
    run_push_only(root).await?;
    run_shared_blob(root).await?;
    run_pre_purge_restore(root).await?;
    run_wrong_hub(root).await?;
    run_forged_push(root).await?;
    println!("{}", json!({"event":"purge_journeys_complete"}));
    Ok(())
}

async fn run_copy_erasure(root: &Path) -> Result<(), String> {
    let case = root.join("copy-erasure");
    let tenant = "installed-purge-copy-erasure";
    let hub = Hub::start(&case.join("hub"), tenant, Some(NOTES_DDL)).await?;
    let edge_a = open_edge(&case, "edge-a", &hub.ticket, tenant, Some(NOTES_DDL))?;
    let edge_b = open_edge(&case, "edge-b", &hub.ticket, tenant, Some(NOTES_DDL))?;
    for db in [&hub.db, &edge_a.db, &edge_b.db] {
        install_work_ledger_schema(db)
            .map_err(|_| "cannot install purge verifier work ledger".to_string())?;
    }
    let selected = Uuid::from_u128(0xc1111111111141118111111111111111);
    let selected_two = Uuid::from_u128(0xc2222222222242228222222222222222);
    let survivor = Uuid::from_u128(0xc3333333333343338333333333333333);
    put_note(
        &edge_a.db,
        selected,
        "selected-secret-before-edit",
        vec![1.0, 0.0, 0.0],
    )?;
    let initial_push = within(edge_a.client.push())
        .await?
        .map_err(|_| "cannot publish initial selected purge row at hub".to_string())?;
    require_clean_setup_sync(&initial_push, "initial selected-row push")?;
    let initial_pull = within(edge_b.client.pull_default())
        .await?
        .map_err(|_| "cannot publish initial selected purge row at second edge".to_string())?;
    require_clean_setup_sync(&initial_pull, "initial selected-row pull")?;
    edit_note(&edge_a.db, selected, "selected-secret-after-edit")?;
    put_note(
        &edge_a.db,
        selected_two,
        "second-selected-secret",
        vec![0.0, 0.0, 1.0],
    )?;
    put_note(
        &edge_a.db,
        survivor,
        "unrelated-survivor",
        vec![0.0, 1.0, 0.0],
    )?;

    let media = b"installed distributed-detection clip";
    let hash = BlobHash::of(media);
    let submitter = FabricIdentity::load_or_generate(&edge_a.identity)
        .map_err(|_| "cannot load media submitter identity".to_string())?
        .node_id();
    let job = JobSpec::builder("selected-detection-job", "media.detect", "clip", &submitter)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(1_700_000_000_000)
        .build();
    submit_job(&edge_a.db, &job, &[] as &[&[u8]])
        .map_err(|_| "cannot submit blob-referencing detection job".to_string())?;
    let policy = MovementPolicy {
        auto_propagate: true,
    };
    let hub_store = BlobStore::new(hub.db.clone(), policy, hub.directory.join("hub.identity"));
    let edge_a_store = BlobStore::new(edge_a.db.clone(), policy, edge_a.identity.clone());
    let edge_b_store = BlobStore::new(edge_b.db.clone(), policy, edge_b.identity.clone());
    for store in [&hub_store, &edge_a_store, &edge_b_store] {
        if store
            .ingest_bytes(media)
            .map_err(|_| "cannot ingest ContextDB-owned media copy".to_string())?
            != hash
        {
            return Err("media ingest returned the wrong content address".to_string());
        }
    }

    let final_push = within(edge_a.client.push())
        .await?
        .map_err(|_| "cannot seed purge copy classes at hub".to_string())?;
    require_clean_setup_sync(&final_push, "purge copy-class push")?;
    let final_pull = within(edge_b.client.pull_default())
        .await?
        .map_err(|_| "cannot seed purge copy classes at second edge".to_string())?;
    require_clean_setup_sync(&final_pull, "purge copy-class pull")?;
    let selected_rows = [
        note_row_id(&hub.db, selected)?,
        note_row_id(&edge_a.db, selected)?,
        note_row_id(&edge_b.db, selected)?,
    ];
    let selected_two_rows = [
        note_row_id(&hub.db, selected_two)?,
        note_row_id(&edge_a.db, selected_two)?,
        note_row_id(&edge_b.db, selected_two)?,
    ];
    let survivor_rows = [
        note_row_id(&hub.db, survivor)?,
        note_row_id(&edge_a.db, survivor)?,
        note_row_id(&edge_b.db, survivor)?,
    ];
    for (ordinal, (place, db)) in [
        ("hub", &hub.db),
        ("edge-a", &edge_a.db),
        ("edge-b", &edge_b.db),
    ]
    .iter()
    .enumerate()
    {
        let before_history = export_key(
            db,
            &case.join(format!("before-history-{ordinal}.snapshot")),
            "notes",
            "id",
            Value::Uuid(selected),
        )?;
        if before_history.total_retained_versions < 2 {
            return Err(format!(
                "{place} did not receive superseded selected history before purge"
            ));
        }
    }
    let expected_jobs = vec!["selected-detection-job".to_string()];
    for (place, db) in [
        ("hub", &hub.db),
        ("edge-a", &edge_a.db),
        ("edge-b", &edge_b.db),
    ] {
        if work_job_ids(db)? != expected_jobs {
            return Err(format!(
                "{place} did not hold the detection-job reference before purge"
            ));
        }
    }
    let before_blob = export_blob(&hub.db, &case.join("before-blob.snapshot"), &hash)?;
    if before_blob.active_generation.is_none()
        || before_blob.manifest.is_none()
        || !before_blob.tag_roles.servable
    {
        return Err("hub did not hold a servable media copy before purge".to_string());
    }

    hub.db
        .execute("BEGIN", &HashMap::new())
        .map_err(|_| "cannot begin caller transaction before purge refusal".to_string())?;
    let in_tx = hub
        .db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .expect_err("PURGE inside a caller transaction must be refused");
    if !matches!(in_tx, Error::PurgeRequiresStandaloneExecution) {
        return Err("PURGE inside a transaction returned the wrong typed error".to_string());
    }
    hub.db
        .execute("ROLLBACK", &HashMap::new())
        .map_err(|_| "cannot roll back refused purge transaction".to_string())?;
    if note_body(&hub.db, selected)?.as_deref() != Some("selected-secret-after-edit") {
        return Err("refused in-transaction purge changed the selected row".to_string());
    }

    let stale_tx = hub
        .db
        .begin()
        .map_err(|_| "cannot begin pre-purge descendant transaction".to_string())?;
    hub.db
        .execute_in_tx(
            stale_tx,
            "UPDATE notes SET body = 'staged-descendant' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot stage pre-purge descendant".to_string())?;
    let control_tx = hub
        .db
        .begin()
        .map_err(|_| "cannot begin unpurged control transaction".to_string())?;
    hub.db
        .execute_in_tx(
            control_tx,
            "UPDATE notes SET body = 'unrelated-survivor-committed' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(survivor))]),
        )
        .map_err(|_| "cannot stage unpurged control edit".to_string())?;

    let purged_rows = hub
        .db
        .execute(
            "PURGE FROM notes WHERE id IN ($selected, $selected_two)",
            &HashMap::from([
                ("selected".to_string(), Value::Uuid(selected)),
                ("selected_two".to_string(), Value::Uuid(selected_two)),
            ]),
        )
        .map_err(|_| "cannot execute authoritative multi-row purge".to_string())?;
    if purged_rows.rows_affected != 2 {
        return Err("multi-row purge did not commit both selected rows".to_string());
    }
    let stale_commit = hub
        .db
        .commit(stale_tx)
        .expect_err("transaction predating purge must not commit its descendant");
    if !matches!(stale_commit, Error::PurgeCausalityFence { .. }) {
        return Err("pre-purge descendant commit returned the wrong typed fence".to_string());
    }
    let _ = hub.db.rollback(stale_tx);
    hub.db
        .commit(control_tx)
        .map_err(|_| "purge incorrectly blocked an unrelated transaction".to_string())?;
    let purged_job = hub
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'selected-detection-job'",
            &HashMap::new(),
        )
        .map_err(|_| "cannot purge selected detection job".to_string())?;
    if purged_job.rows_affected != 1 {
        return Err("detection-job purge did not select exactly one job".to_string());
    }
    within(edge_a.client.pull_default())
        .await?
        .map_err(|_| "first edge did not receive authoritative purge".to_string())?;
    within(edge_b.client.pull_default())
        .await?
        .map_err(|_| "second edge did not receive authoritative purge".to_string())?;

    let participants = [
        (
            "hub",
            &hub.db,
            selected_rows[0],
            selected_two_rows[0],
            survivor_rows[0],
        ),
        (
            "edge-a",
            &edge_a.db,
            selected_rows[1],
            selected_two_rows[1],
            survivor_rows[1],
        ),
        (
            "edge-b",
            &edge_b.db,
            selected_rows[2],
            selected_two_rows[2],
            survivor_rows[2],
        ),
    ];
    let mut frontiers = Vec::new();
    let mut second_frontiers = Vec::new();
    let mut job_frontiers = Vec::new();
    let mut blob_frontiers = Vec::new();
    for (ordinal, (place, db, selected_row, selected_two_row, survivor_row)) in
        participants.iter().enumerate()
    {
        if note_body(db, selected)?.is_some()
            || note_body(db, selected_two)?.is_some()
            || note_body(db, survivor)?.as_deref() != Some("unrelated-survivor-committed")
            || !work_job_ids(db)?.is_empty()
        {
            return Err(format!("{place} has the wrong post-purge row set"));
        }
        let selected_hits = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[1.0, 0.0, 0.0],
                16,
                None,
                db.snapshot(),
            )
            .map_err(|_| format!("cannot inspect post-purge vector state at {place}"))?;
        if selected_hits.iter().any(|(row, _)| row == selected_row) {
            return Err(format!("{place} ANN still returns the purged vector"));
        }
        let selected_two_hits = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[0.0, 0.0, 1.0],
                16,
                None,
                db.snapshot(),
            )
            .map_err(|_| format!("cannot inspect second post-purge vector at {place}"))?;
        if selected_two_hits
            .iter()
            .any(|(row, _)| row == selected_two_row)
        {
            return Err(format!(
                "{place} ANN still returns the second purged vector"
            ));
        }
        let survivor_hit = db
            .query_vector(
                VectorIndexRef::new("notes", "embedding"),
                &[0.0, 1.0, 0.0],
                1,
                None,
                db.snapshot(),
            )
            .map_err(|_| format!("cannot inspect survivor vector at {place}"))?;
        if survivor_hit.first().map(|hit| hit.0) != Some(*survivor_row) {
            return Err(format!("{place} lost the unrelated survivor vector"));
        }
        let key = export_key(
            db,
            &case.join(format!("after-key-{ordinal}.snapshot")),
            "notes",
            "id",
            Value::Uuid(selected),
        )?;
        frontiers.push(require_purged_key(&key, place)?);
        let second_key = export_key(
            db,
            &case.join(format!("after-second-key-{ordinal}.snapshot")),
            "notes",
            "id",
            Value::Uuid(selected_two),
        )?;
        second_frontiers.push(require_purged_key(&second_key, place)?);
        let job_key = export_key(
            db,
            &case.join(format!("after-job-key-{ordinal}.snapshot")),
            "work_jobs",
            "job_id",
            Value::Text("selected-detection-job".to_string()),
        )?;
        job_frontiers.push(require_purged_key(&job_key, place)?);
        let blob = export_blob(
            db,
            &case.join(format!("after-blob-{ordinal}.snapshot")),
            &hash,
        )?;
        blob_frontiers.push(require_blob_absent(&blob, place)?);
    }

    let post_purge = case.join("post-purge.snapshot");
    hub.db
        .export_snapshot(&post_purge)
        .map_err(|_| "cannot export post-purge backup".to_string())?;
    let backup_inspector = SnapshotInspector::open(&post_purge)
        .map_err(|_| "cannot inspect post-purge backup".to_string())?;
    for (name, table, column, value) in [
        ("first selected row", "notes", "id", Value::Uuid(selected)),
        (
            "second selected row",
            "notes",
            "id",
            Value::Uuid(selected_two),
        ),
        (
            "detection-job reference",
            "work_jobs",
            "job_id",
            Value::Text("selected-detection-job".to_string()),
        ),
    ] {
        let report = inspect_snapshot_key(&backup_inspector, table, column, value)?;
        require_purged_key(&report, &format!("post-purge backup {name}"))?;
    }
    let backup_blob = backup_inspector
        .inspect_blob(hash.as_bytes())
        .map_err(|_| "cannot inspect post-purge backup media".to_string())?;
    require_blob_absent(&backup_blob, "post-purge backup")?;
    backup_inspector
        .close()
        .map_err(|_| "cannot close post-purge backup inspector".to_string())?;
    let restored =
        Database::open(&post_purge).map_err(|_| "cannot reopen post-purge backup".to_string())?;
    if note_body(&restored, selected)?.is_some()
        || note_body(&restored, selected_two)?.is_some()
        || note_body(&restored, survivor)?.as_deref() != Some("unrelated-survivor-committed")
        || !work_job_ids(&restored)?.is_empty()
    {
        return Err("post-purge backup restored removed data".to_string());
    }
    restored
        .close()
        .map_err(|_| "cannot close post-purge backup".to_string())?;

    println!(
        "{}",
        json!({
            "event":"purge_copy_erasure",
            "standalone_transaction_refusal":"typed",
            "pre_purge_transaction_commit":"causality_fenced",
            "unrelated_transaction":"committed",
            "multi_row_atomic_commit":2,
            "participants":["hub","edge-a","edge-b"],
            "copy_classes":["live_row","retained_history","vector_ann","work_ledger_reference","blob_manifest","blob_bytes","protection_tags"],
            "purge_frontiers":frontiers,
            "second_purge_frontiers":second_frontiers,
            "work_job_purge_frontiers":job_frontiers,
            "blob_purge_frontiers":blob_frontiers,
            "post_purge_backup_absent":true,
        })
    );
    drop(hub_store);
    drop(edge_a_store);
    drop(edge_b_store);
    edge_a.client.shutdown().await;
    edge_b.client.shutdown().await;
    hub.stop().await
}

async fn run_recreated_generation(root: &Path) -> Result<(), String> {
    let case = root.join("recreated-generation");
    let tenant = "installed-purge-recreated-generation";
    let hub = Hub::start(&case.join("hub"), tenant, Some(NOTES_DDL)).await?;
    let source = open_edge(&case, "old-source", &hub.ticket, tenant, Some(NOTES_DDL))?;
    let purged = Uuid::from_u128(0xdaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa);
    let old_control = Uuid::from_u128(0xdbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb);
    let new_hub = Uuid::from_u128(0xdccccccccccc4ccc8ccccccccccccccc);
    let new_edge = Uuid::from_u128(0xdddddddddddd4ddd8ddddddddddddddd);
    put_note(
        &source.db,
        purged,
        "removed-generation-purged-row",
        vec![1.0, 0.0, 0.0],
    )?;
    put_note(
        &source.db,
        old_control,
        "removed-generation-control-row",
        vec![0.0, 1.0, 0.0],
    )?;
    let seeded = within(source.client.push())
        .await?
        .map_err(|_| "cannot seed old table generation".to_string())?;
    require_clean_setup_sync(&seeded, "old-generation seed push")?;
    if note_body(&hub.db, purged)?.is_none() || note_body(&hub.db, old_control)?.is_none() {
        return Err("old-generation seed did not reach the hub".to_string());
    }
    let old_snapshot = case.join("old-generation.snapshot");
    source
        .db
        .export_snapshot(&old_snapshot)
        .map_err(|_| "cannot export authenticated old table generation".to_string())?;
    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(purged))]),
        )
        .map_err(|_| "cannot purge old table generation".to_string())?;
    hub.db
        .execute("DROP TABLE notes", &HashMap::new())
        .map_err(|_| "cannot drop old table generation".to_string())?;
    hub.db
        .execute(NOTES_DDL, &HashMap::new())
        .map_err(|_| "cannot create replacement table generation".to_string())?;
    put_note(
        &hub.db,
        new_hub,
        "new-generation-hub-row",
        vec![0.0, 0.0, 1.0],
    )?;
    source.client.shutdown().await;
    drop(source.client);
    source
        .db
        .close()
        .map_err(|_| "cannot close old-generation source".to_string())?;
    drop(source.db);

    let restored = Arc::new(
        Database::open(&old_snapshot)
            .map_err(|_| "cannot reopen old table generation".to_string())?,
    );
    edit_note(&restored, purged, "old-generation-purged-lineage-replay")?;
    edit_note(
        &restored,
        old_control,
        "old-generation-removed-lineage-replay",
    )?;
    let client = SyncClient::new(
        restored.clone(),
        &peer_dial_spec(&hub.ticket, &source.identity),
        TenantId::from(tenant),
    );
    let replay = within(client.push())
        .await?
        .map_err(|_| "old-generation replay did not return a result".to_string())?;
    let rendered = serde_json::to_value(&replay)
        .map_err(|_| "cannot inspect old-generation refusal".to_string())?;
    let conflicts = rendered["conflicts"]
        .as_array()
        .ok_or_else(|| "old-generation replay has no conflict list".to_string())?;
    if conflicts.len() != 2 {
        return Err("both old-generation rows were not visibly refused".to_string());
    }
    let purged_key =
        serde_json::to_value(NaturalKey::single("id".to_string(), Value::Uuid(purged)))
            .map_err(|_| "cannot serialize old purged key".to_string())?;
    let control_key = serde_json::to_value(NaturalKey::single(
        "id".to_string(),
        Value::Uuid(old_control),
    ))
    .map_err(|_| "cannot serialize old generation key".to_string())?;
    let purged_conflict = conflicts
        .iter()
        .find(|conflict| conflict["natural_key"] == purged_key)
        .ok_or_else(|| "purged old-generation row has no refusal".to_string())?;
    let generation_conflict = conflicts
        .iter()
        .find(|conflict| conflict["natural_key"] == control_key)
        .ok_or_else(|| "unpurged old-generation row has no refusal".to_string())?;
    let generation_meaning = generation_conflict.to_string().to_ascii_lowercase();
    if purged_conflict["table"] != "notes"
        || purged_conflict["mutation_kind"] != "edit"
        || purged_conflict["reason"] != "purged_lineage"
        || generation_conflict["table"] != "notes"
        || generation_conflict["mutation_kind"] != "edit"
        || generation_conflict["reason"] != "removed_generation"
        || !generation_meaning.contains("generation")
        || !generation_meaning.contains("removed")
    {
        return Err("old table generation returned incomplete refusals".to_string());
    }
    if note_body(&hub.db, purged)?.is_some() || note_body(&hub.db, old_control)?.is_some() {
        return Err("old table generation replay reached the hub".to_string());
    }
    within(client.pull_default())
        .await?
        .map_err(|_| "restored source could not adopt replacement generation".to_string())?;
    if note_body(&restored, purged)?.is_some()
        || note_body(&restored, old_control)?.is_some()
        || note_body(&restored, new_hub)?.as_deref() != Some("new-generation-hub-row")
    {
        return Err("restored source did not converge to the replacement generation".to_string());
    }
    put_note(
        &restored,
        new_edge,
        "new-generation-edge-row",
        vec![1.0, 1.0, 0.0],
    )?;
    let accepted = within(client.push())
        .await?
        .map_err(|_| "new-generation row did not sync".to_string())?;
    let accepted_json = serde_json::to_value(&accepted)
        .map_err(|_| "cannot inspect new-generation result".to_string())?;
    if accepted_json["conflicts"]
        .as_array()
        .is_none_or(|items| !items.is_empty())
        || note_body(&hub.db, new_edge)?.as_deref() != Some("new-generation-edge-row")
    {
        return Err("replacement generation refused new data".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_recreated_generation",
            "purged_old_row_refused":true,
            "unpurged_old_row_refused_by_generation":true,
            "new_hub_row_synced_down":true,
            "new_edge_row_synced_up":true,
        })
    );
    client.shutdown().await;
    restored
        .close()
        .map_err(|_| "cannot close replacement-generation source".to_string())?;
    hub.stop().await
}

fn sync_state(db: &Database, artifact: &Path) -> Result<JsonValue, String> {
    db.export_snapshot(artifact)
        .map_err(|_| "cannot export purge state fingerprint".to_string())?;
    let inspector = SnapshotInspector::open(artifact)
        .map_err(|_| "cannot open purge state fingerprint".to_string())?;
    let state = inspector
        .inspect_sync_apply_state()
        .map_err(|_| "cannot inspect purge state fingerprint".to_string())?;
    inspector
        .close()
        .map_err(|_| "cannot close purge state fingerprint".to_string())?;
    serde_json::to_value(state).map_err(|_| "cannot serialize purge state fingerprint".to_string())
}

fn require_hub_authority_error(error: Error, hub_node_id: &str) -> Result<(), String> {
    match error {
        Error::PurgeRequiresAuthoritativeHub {
            hub_node_id: actual,
        } if actual == hub_node_id => Ok(()),
        _ => Err("edge PURGE returned the wrong typed authority error".to_string()),
    }
}

async fn run_edge_authority(root: &Path) -> Result<(), String> {
    let case = root.join("edge-authority");
    let tenant = "installed-purge-edge-authority";
    let hub = Hub::start(&case.join("hub"), tenant, Some(NOTES_DDL)).await?;
    let id = Uuid::from_u128(0xd1111111111141118111111111111111);
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, Some(NOTES_DDL))?;
    put_note(
        &edge.db,
        id,
        "edge-local-data-before-first-connection",
        vec![1.0, 0.0, 0.0],
    )?;
    let before = sync_state(&edge.db, &case.join("before-first-refusal.snapshot"))?;
    let first = edge
        .db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect_err("configured edge must refuse purge before first connection");
    require_hub_authority_error(first, &hub.node_id)?;
    let after = sync_state(&edge.db, &case.join("after-first-refusal.snapshot"))?;
    if before != after
        || note_body(&edge.db, id)?.as_deref() != Some("edge-local-data-before-first-connection")
    {
        return Err("edge authority refusal mutated local state".to_string());
    }
    edge.client.shutdown().await;
    drop(edge.client);
    edge.db
        .close()
        .map_err(|_| "cannot close authority-refusal edge".to_string())?;
    drop(edge.db);
    let restarted = open_edge(&case, "edge", &hub.ticket, tenant, Some(NOTES_DDL))?;
    let restart_before = sync_state(&restarted.db, &case.join("before-restart-refusal.snapshot"))?;
    let second = restarted
        .db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .expect_err("restarted configured edge must still refuse purge");
    require_hub_authority_error(second, &hub.node_id)?;
    let restart_after = sync_state(&restarted.db, &case.join("after-restart-refusal.snapshot"))?;
    if restart_before != restart_after
        || note_body(&restarted.db, id)?.as_deref()
            != Some("edge-local-data-before-first-connection")
    {
        return Err("restarted edge authority refusal mutated local state".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_edge_authority",
            "authoritative_hub_node_id":hub.node_id,
            "before_first_connection":"refused_without_mutation",
            "after_restart":"refused_without_mutation",
        })
    );
    restarted.client.shutdown().await;
    hub.stop().await
}

async fn run_stale_and_fresh(root: &Path) -> Result<(), String> {
    let case = root.join("stale-and-fresh");
    let tenant = "installed-purge-stale-and-fresh";
    let hub = Hub::start(&case.join("hub"), tenant, Some(NOTES_DDL)).await?;
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, Some(NOTES_DDL))?;
    let selected = Uuid::from_u128(0xd2222222222242228222222222222222);
    let independent = Uuid::from_u128(0xd3333333333343338333333333333333);
    put_note(&edge.db, selected, "old-lineage", vec![1.0, 0.0, 0.0])?;
    put_note(
        &edge.db,
        independent,
        "independent-record",
        vec![0.0, 1.0, 0.0],
    )?;
    within(edge.client.push())
        .await?
        .map_err(|_| "cannot seed stale-lineage fixture".to_string())?;
    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot purge stale-lineage fixture".to_string())?;
    edit_note(&edge.db, selected, "offline-descendant-must-not-return")?;
    let refused = within(edge.client.push())
        .await?
        .map_err(|_| "stale descendant push did not return a result".to_string())?;
    let conflict =
        exact_purge_conflict(&refused, "notes", "id", Value::Uuid(selected), Some("edit"))?;
    if note_body(&hub.db, selected)?.is_some() {
        return Err("hub accepted an offline descendant of purged data".to_string());
    }
    within(edge.client.pull_default())
        .await?
        .map_err(|_| "edge could not apply authoritative purge".to_string())?;
    if note_body(&edge.db, selected)?.is_some()
        || note_body(&edge.db, independent)?.as_deref() != Some("independent-record")
        || note_body(&hub.db, independent)?.as_deref() != Some("independent-record")
    {
        return Err("purge damaged independent data or left the stale descendant".to_string());
    }
    put_note(
        &edge.db,
        selected,
        "explicit-fresh-same-key",
        vec![0.0, 0.0, 1.0],
    )?;
    let fresh = within(edge.client.push())
        .await?
        .map_err(|_| "fresh same-key creation did not sync".to_string())?;
    let fresh_json = serde_json::to_value(&fresh)
        .map_err(|_| "cannot inspect fresh-lineage result".to_string())?;
    if fresh_json["conflicts"]
        .as_array()
        .is_none_or(|items| !items.is_empty())
        || note_body(&hub.db, selected)?.as_deref() != Some("explicit-fresh-same-key")
    {
        return Err("explicit fresh same-key data remained in the purged lineage".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_stale_and_fresh",
            "stale_descendant_conflict":conflict,
            "stale_removed":true,
            "fresh_same_key":"accepted",
            "independent_record":"unchanged",
        })
    );
    edge.client.shutdown().await;
    hub.stop().await
}

fn push_only_body(db: &Database, id: Uuid) -> Result<Option<String>, String> {
    let rows = db
        .execute(
            "SELECT body FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(id))]),
        )
        .map_err(|_| "cannot read push-only purge row".to_string())?
        .rows;
    match rows.as_slice() {
        [] => Ok(None),
        [row] => match row.as_slice() {
            [Value::Text(body)] => Ok(Some(body.clone())),
            _ => Err("push-only purge row has the wrong type".to_string()),
        },
        _ => Err("push-only purge key returned multiple rows".to_string()),
    }
}

async fn run_push_only(root: &Path) -> Result<(), String> {
    let case = root.join("push-only");
    let tenant = "installed-purge-push-only";
    let hub = Hub::start(&case.join("hub"), tenant, Some(PUSH_ONLY_DDL)).await?;
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, Some(PUSH_ONLY_DDL))?;
    let selected = Uuid::from_u128(0xd4444444444444448444444444444444);
    let sentinel = Uuid::from_u128(0xd5555555555545558555555555555555);
    hub.db
        .execute(
            "INSERT INTO push_only_notes (id, body) VALUES ($id, 'hub-only-sentinel')",
            &HashMap::from([("id".to_string(), Value::Uuid(sentinel))]),
        )
        .map_err(|_| "cannot insert push-only hub sentinel".to_string())?;
    edge.db
        .execute(
            "INSERT INTO push_only_notes (id, body) VALUES ($id, 'edge-originated-secret')",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot insert push-only purge row".to_string())?;
    within(edge.client.push())
        .await?
        .map_err(|_| "cannot push one-way purge row".to_string())?;
    if push_only_body(&hub.db, selected)?.as_deref() != Some("edge-originated-secret")
        || push_only_body(&edge.db, sentinel)?.is_some()
    {
        return Err("ordinary push-only direction was not established".to_string());
    }
    let stale = case.join("stale-edge.snapshot");
    edge.db
        .export_snapshot(&stale)
        .map_err(|_| "cannot capture stale push-only edge".to_string())?;
    hub.db
        .execute(
            "PURGE FROM push_only_notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot purge push-only row at hub".to_string())?;
    within(edge.client.pull_default())
        .await?
        .map_err(|_| "push-only edge did not receive purge".to_string())?;
    if push_only_body(&hub.db, selected)?.is_some()
        || push_only_body(&edge.db, selected)?.is_some()
        || push_only_body(&edge.db, sentinel)?.is_some()
    {
        return Err("purge plane violated push-only row direction or left data".to_string());
    }
    let hub_key = export_key(
        &hub.db,
        &case.join("hub-push-only-purge.snapshot"),
        "push_only_notes",
        "id",
        Value::Uuid(selected),
    )?;
    let edge_key = export_key(
        &edge.db,
        &case.join("edge-push-only-purge.snapshot"),
        "push_only_notes",
        "id",
        Value::Uuid(selected),
    )?;
    require_purged_key(&hub_key, "push-only hub")?;
    require_purged_key(&edge_key, "push-only edge")?;

    edge.client.shutdown().await;
    drop(edge.client);

    let stale_db = Arc::new(
        Database::open(&stale).map_err(|_| "cannot reopen stale push-only edge".to_string())?,
    );
    stale_db
        .execute(
            "UPDATE push_only_notes SET body = 'stale-offline-descendant' WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot author stale push-only descendant".to_string())?;
    let stale_client = SyncClient::new(
        stale_db.clone(),
        &peer_dial_spec(&hub.ticket, &edge.identity),
        TenantId::from(tenant),
    );
    let refusal = within(stale_client.push())
        .await?
        .map_err(|_| "stale push-only replay did not return a result".to_string())?;
    let conflict = exact_purge_conflict(
        &refusal,
        "push_only_notes",
        "id",
        Value::Uuid(selected),
        Some("edit"),
    )?;
    if push_only_body(&hub.db, selected)?.is_some() {
        return Err("stale push-only replay resurrected purged data".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_push_only_delivery",
            "ordinary_hub_row_pulled_down":false,
            "purge_delivered":true,
            "hub_tombstone":true,
            "edge_tombstone":true,
            "stale_replay_conflict":conflict,
        })
    );
    stale_client.shutdown().await;
    stale_db
        .close()
        .map_err(|_| "cannot close stale push-only source".to_string())?;
    hub.stop().await
}

async fn run_shared_blob(root: &Path) -> Result<(), String> {
    let case = root.join("shared-blob");
    let tenant = "installed-purge-shared-blob";
    let hub = Hub::start(&case.join("hub"), tenant, None).await?;
    install_work_ledger_schema(&hub.db)
        .map_err(|_| "cannot install shared-blob work ledger".to_string())?;
    let bytes = b"shared distributed-detection media";
    let hash = BlobHash::of(bytes);
    let store = BlobStore::new(
        hub.db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        hub.directory.join("hub.identity"),
    );
    if store
        .ingest_bytes(bytes)
        .map_err(|_| "cannot ingest shared media".to_string())?
        != hash
    {
        return Err("shared media returned the wrong content address".to_string());
    }
    for job_id in ["selected-job", "remaining-job"] {
        let job = JobSpec::builder(job_id, "media.detect", "clip", &hub.node_id)
            .input_refs(vec![InputRef::blob_ref(hash.clone())])
            .submitted_at_ms(1_700_000_000_000)
            .build();
        submit_job(&hub.db, &job, &[] as &[&[u8]])
            .map_err(|_| "cannot submit shared-media job".to_string())?;
    }
    let first = hub
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'selected-job'",
            &HashMap::new(),
        )
        .map_err(|_| "cannot purge one shared-media referent".to_string())?;
    let report = serde_json::to_string(&first.rows)
        .map_err(|_| "cannot serialize shared-media purge report".to_string())?;
    if first.rows_affected != 1 || !report.contains("remaining-job") {
        return Err("shared-media purge did not name the surviving referent".to_string());
    }
    if work_job_ids(&hub.db)? != vec!["remaining-job".to_string()] {
        return Err("shared-media purge did not preserve exactly the surviving job".to_string());
    }
    let retained = export_blob(&hub.db, &case.join("shared-retained.snapshot"), &hash)?;
    if retained.active_generation.is_none()
        || retained.manifest.is_none()
        || !retained.tag_roles.servable
    {
        return Err(
            "shared-media purge destroyed bytes still referenced by a live job".to_string(),
        );
    }
    let final_purge = hub
        .db
        .execute(
            "PURGE FROM work_jobs WHERE job_id = 'remaining-job'",
            &HashMap::new(),
        )
        .map_err(|_| "cannot purge final shared-media referent".to_string())?;
    if final_purge.rows_affected != 1 || !work_job_ids(&hub.db)?.is_empty() {
        return Err("final shared-media purge did not remove the remaining job".to_string());
    }
    let destroyed = export_blob(&hub.db, &case.join("shared-destroyed.snapshot"), &hash)?;
    require_blob_absent(&destroyed, "shared-media hub")?;
    println!(
        "{}",
        json!({
            "event":"purge_shared_media",
            "surviving_referent":"remaining-job",
            "bytes_retained_while_shared":true,
            "bytes_destroyed_after_final_referent":true,
        })
    );
    drop(store);
    hub.stop().await
}

async fn run_pre_purge_restore(root: &Path) -> Result<(), String> {
    let case = root.join("pre-purge-restore");
    let tenant = "installed-purge-pre-backup";
    let hub = Hub::start(&case.join("original-hub"), tenant, Some(NOTES_DDL)).await?;
    let original_hub_id = hub.node_id.clone();
    let original_identity = hub.directory.join("hub.identity");
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, Some(NOTES_DDL))?;
    let selected = Uuid::from_u128(0xd6666666666646668666666666666666);
    put_note(
        &edge.db,
        selected,
        "operator-backup-still-holds-this",
        vec![1.0, 0.0, 0.0],
    )?;
    within(edge.client.push())
        .await?
        .map_err(|_| "cannot seed pre-purge backup".to_string())?;
    let pre_purge = case.join("pre-purge.snapshot");
    hub.db
        .export_snapshot(&pre_purge)
        .map_err(|_| "cannot export pre-purge operator backup".to_string())?;
    hub.db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "cannot purge before backup restore".to_string())?;
    within(edge.client.pull_default())
        .await?
        .map_err(|_| "edge did not retain purge before hub restore".to_string())?;
    let retained = export_key(
        &edge.db,
        &case.join("edge-retained-frontier.snapshot"),
        "notes",
        "id",
        Value::Uuid(selected),
    )?;
    let expected_frontier = require_purged_key(&retained, "pre-backup peer")?;
    let expected_lineage_fingerprint = retained
        .lineage
        .as_ref()
        .ok_or_else(|| "pre-backup peer lost its permanent purge lineage".to_string())?
        .lineage_fingerprint
        .clone();
    edge.client.shutdown().await;
    drop(edge.client);
    edge.db
        .close()
        .map_err(|_| "cannot close peer before restored-hub rebind".to_string())?;
    drop(edge.db);
    hub.stop().await?;

    let restored_dir = case.join("restored-hub");
    std::fs::create_dir_all(&restored_dir)
        .map_err(|_| "cannot create restored hub directory".to_string())?;
    std::fs::copy(&pre_purge, restored_dir.join("hub.db"))
        .map_err(|_| "cannot restore the pre-purge database file".to_string())?;
    std::fs::copy(&original_identity, restored_dir.join("hub.identity"))
        .map_err(|_| "cannot restore the authoritative hub identity".to_string())?;
    let restored = Hub::start(&restored_dir, tenant, Some(NOTES_DDL)).await?;
    if restored.node_id != original_hub_id
        || note_body(&restored.db, selected)?.as_deref() != Some("operator-backup-still-holds-this")
    {
        return Err(
            "pre-purge restore did not preserve the expected hub identity/data".to_string(),
        );
    }
    let edge = open_edge(&case, "edge", &restored.ticket, tenant, Some(NOTES_DDL))?;
    let refusal = within(edge.client.pull_default())
        .await?
        .expect_err("peer with a permanent purge must fence a restored pre-purge lineage");
    let refusal = match refusal {
        Error::PurgeCausalityFence {
            table,
            key,
            lineage_root,
            frontier,
        } => {
            let expected_key = NaturalKey::single("id".to_string(), Value::Uuid(selected));
            let actual_fingerprint = blake3::hash(lineage_root.as_bytes()).to_hex().to_string();
            if table != "notes"
                || key != expected_key.pairs()
                || lineage_root.is_empty()
                || frontier.0 != expected_frontier
                || actual_fingerprint != expected_lineage_fingerprint
            {
                return Err(format!(
                    "restored pre-purge lineage fence did not match the peer's durable purge: \
                     table={table}, key={key:?}, frontier={}, lineage_fingerprint={actual_fingerprint}",
                    frontier.0
                ));
            }
            json!({
                "type":"purge_causality_fence",
                "table":table,
                "natural_key":expected_key,
                "lineage_fingerprint":actual_fingerprint,
                "frontier":frontier.0,
            })
        }
        other => {
            return Err(format!(
                "restored pre-purge lineage returned the wrong refusal: {other}"
            ));
        }
    };
    if note_body(&edge.db, selected)?.is_some()
        || note_body(&restored.db, selected)?.as_deref() != Some("operator-backup-still-holds-this")
    {
        return Err("peer accepted the resurrected pre-purge backup lineage".to_string());
    }
    restored
        .db
        .execute(
            "PURGE FROM notes WHERE id = $id",
            &HashMap::from([("id".to_string(), Value::Uuid(selected))]),
        )
        .map_err(|_| "operator could not reissue purge after restore".to_string())?;
    within(edge.client.pull_default())
        .await?
        .map_err(|_| "peer could not receive reissued purge".to_string())?;
    if note_body(&restored.db, selected)?.is_some() || note_body(&edge.db, selected)?.is_some() {
        return Err("reissued purge did not remove restored data".to_string());
    }
    for (ordinal, (place, db)) in [("restored hub", &restored.db), ("peer", &edge.db)]
        .iter()
        .enumerate()
    {
        let reissued = export_key(
            db,
            &case.join(format!("reissued-purge-{ordinal}.snapshot")),
            "notes",
            "id",
            Value::Uuid(selected),
        )?;
        require_purged_key(&reissued, place)?;
    }
    println!(
        "{}",
        json!({
            "event":"purge_pre_backup_restore",
            "restored_hub_identity":original_hub_id,
            "peer_refusal":refusal,
            "peer_remained_absent":true,
            "operator_reissued_purge":true,
        })
    );
    edge.client.shutdown().await;
    restored.stop().await
}

async fn run_wrong_hub(root: &Path) -> Result<(), String> {
    let case = root.join("wrong-hub");
    let tenant = "installed-purge-wrong-hub";
    let good = Hub::start(&case.join("good"), tenant, Some(NOTES_DDL)).await?;
    let wrong = Hub::start(&case.join("wrong"), tenant, Some(NOTES_DDL)).await?;
    let edge = open_edge(&case, "edge", &good.ticket, tenant, Some(NOTES_DDL))?;
    let selected = Uuid::from_u128(0xd7777777777747778777777777777777);
    put_note(&edge.db, selected, "bound-hub-data", vec![1.0, 0.0, 0.0])?;
    within(edge.client.push())
        .await?
        .map_err(|_| "cannot establish authoritative hub binding".to_string())?;
    edge.client.shutdown().await;
    drop(edge.client);
    let wrong_client = SyncClient::new(
        edge.db.clone(),
        &peer_dial_spec(&wrong.ticket, &edge.identity),
        TenantId::from(tenant),
    );
    within(wrong_client.ensure_connected())
        .await?
        .map_err(|_| "cannot authenticate wrong-hub fixture".to_string())?;
    let before = sync_state(&edge.db, &case.join("before-wrong-hub.snapshot"))?;
    let error = within(wrong_client.pull_default())
        .await?
        .expect_err("non-bound hub must be refused before pull mutation");
    let diagnostic = error.to_string();
    let meaning = diagnostic.to_ascii_lowercase();
    if !matches!(error, Error::SchemaInvalid { .. })
        || !meaning.contains("pull")
        || !meaning.contains("push")
        || !meaning.contains("purge")
        || !meaning.contains("authorit")
        || !diagnostic.contains(&good.node_id)
    {
        return Err("wrong hub returned an incomplete authority refusal".to_string());
    }
    let after = sync_state(&edge.db, &case.join("after-wrong-hub.snapshot"))?;
    if before != after || note_body(&edge.db, selected)?.as_deref() != Some("bound-hub-data") {
        return Err("wrong hub mutated the bound edge".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_wrong_hub_refused",
            "bound_hub_node_id":good.node_id,
            "wrong_hub_node_id":wrong.node_id,
            "state_unchanged":true,
        })
    );
    wrong_client.shutdown().await;
    good.stop().await?;
    wrong.stop().await
}

async fn run_forged_push(root: &Path) -> Result<(), String> {
    let case = root.join("forged-push");
    let tenant = "installed-purge-forged-push";
    let tenant_id = TenantId::from(tenant);
    let hub = Hub::start(&case.join("hub"), tenant, Some(NOTES_DDL)).await?;
    let edge = open_edge(&case, "edge", &hub.ticket, tenant, Some(NOTES_DDL))?;
    within(edge.client.push())
        .await?
        .map_err(|_| "cannot seed forged-push schema".to_string())?;
    let selected = Uuid::from_u128(0xd8888888888848888888888888888888);
    let sibling = Uuid::from_u128(0xd9999999999949998999999999999999);
    put_note(
        &hub.db,
        selected,
        "must-survive-forged-purge",
        vec![1.0, 0.0, 0.0],
    )?;
    put_note(
        &edge.db,
        sibling,
        "ordinary-sibling-must-not-partially-apply",
        vec![0.0, 1.0, 0.0],
    )?;
    let push_watermark = edge.client.push_watermark();
    let mut changeset = WireChangeSet::from(edge.db.changes_since(push_watermark));
    if changeset.rows.is_empty() {
        return Err("forged-push fixture has no ordinary sibling row".to_string());
    }
    changeset.purges.push(WirePurgeChange::default());
    let request = PushRequest {
        changeset,
        incarnation: edge
            .db
            .production_smoke_sync_incarnation(&tenant_id)
            .map_err(|_| "cannot read forged-push database incarnation".to_string())?,
    };
    let bytes = encode(MessageType::PushRequest, &request)
        .map_err(|_| "cannot encode fixed forged-push request".to_string())?;
    let hub_watermark_before = hub
        .db
        .persisted_sync_applied_push_watermark(&tenant_id)
        .map_err(|_| "cannot read hub watermark before forged push".to_string())?;
    let hub_lsn_before = hub.db.current_lsn();
    let hub_state_before = sync_state(&hub.db, &case.join("before-forged-push.snapshot"))?;
    edge.client.shutdown().await;
    drop(edge.client);
    let transport = client_transport(&peer_dial_spec(&hub.ticket, &edge.identity));
    let reply = within(transport.request(&push_subject(tenant), bytes, Duration::from_secs(30)))
        .await?
        .map_err(|_| "fixed forged-push request failed at transport".to_string())?;
    let envelope = decode(&reply).map_err(|_| "cannot decode forged-push response".to_string())?;
    if envelope.message_type != MessageType::PushResponse {
        return Err("forged-push response has the wrong message type".to_string());
    }
    let response: PushResponse = rmp_serde::from_slice(&envelope.payload)
        .map_err(|_| "cannot decode typed forged-push response".to_string())?;
    match response.application_error {
        Some(WirePushError::PurgeRequiresAuthoritativeHub { hub_node_id })
            if hub_node_id == hub.node_id => {}
        _ => return Err("forged push returned the wrong authority refusal".to_string()),
    }
    if response.result.is_some()
        || response.error.is_some()
        || note_body(&hub.db, selected)?.as_deref() != Some("must-survive-forged-purge")
        || note_body(&hub.db, sibling)?.is_some()
        || hub
            .db
            .persisted_sync_applied_push_watermark(&tenant_id)
            .map_err(|_| "cannot read hub watermark after forged push".to_string())?
            != hub_watermark_before
    {
        return Err("forged push partially mutated hub payload or progress".to_string());
    }
    let hub_state_after = sync_state(&hub.db, &case.join("after-forged-push.snapshot"))?;
    let contact_changes = hub.db.change_log_since(hub_lsn_before);
    if !contact_changes.iter().all(|entry| {
        matches!(
            entry,
            ChangeLogEntry::RowInsert { table, .. } | ChangeLogEntry::RowDelete { table, .. }
                if table == "work_node_contacts"
        )
    }) {
        return Err(
            "forged push changed durable state beyond its authenticated peer contact".to_string(),
        );
    }
    // A terminal authority refusal can return before push admission records a
    // contact. Even an admitted contact can be a same-millisecond idempotent
    // update. With no durable contact entry, the entire inspection must match.
    if contact_changes.is_empty() {
        if hub_state_before != hub_state_after {
            return Err("forged push mutated durable state without a peer-contact row".to_string());
        }
    } else {
        let mut hub_state_before_unaffected = hub_state_before;
        let mut hub_state_after_unaffected = hub_state_after;
        for state in [
            &mut hub_state_before_unaffected,
            &mut hub_state_after_unaffected,
        ] {
            let Some(fields) = state.as_object_mut() else {
                return Err("cannot normalize authenticated-apply fingerprint".to_string());
            };
            for field in [
                "digest",
                "current_lsn",
                "retained_row_versions",
                "index_postings",
                "change_log_entries",
                "commit_index_entries",
            ] {
                fields.remove(field);
            }
        }
        if hub_state_before_unaffected != hub_state_after_unaffected {
            return Err(
                "forged push mutated durable state outside the peer-contact row".to_string(),
            );
        }
    }
    let selected_key = export_key(
        &hub.db,
        &case.join("forged-selected.snapshot"),
        "notes",
        "id",
        Value::Uuid(selected),
    )?;
    if selected_key
        .lineage
        .as_ref()
        .and_then(|lineage| lineage.purge_frontier_lsn)
        .is_some()
    {
        return Err("forged push installed a purge frontier".to_string());
    }
    println!(
        "{}",
        json!({
            "event":"purge_forged_push_refused",
            "authoritative_hub_node_id":hub.node_id,
            "typed_authority_error":true,
            "conflict_winner_fields":false,
            "ordinary_sibling_applied":false,
            "payload_and_progress_unchanged":true,
        })
    );
    transport.shutdown().await.ok();
    hub.stop().await
}
