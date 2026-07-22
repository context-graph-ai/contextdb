//! The media-transfer demo caller: a minimal product-neutral layer over the
//! work ledger's `blob_ref` reference kind and the blob_ref resolver
//! (`contextdb_server::blob_resolver::BlobService`). It demonstrates
//! real-machine media-transfer scenarios: version-gate
//! evidence, entitlement/policy enforcement, hub blob-bytes isolation, and
//! tamper detection.
//!
//! This binary is a CALLER of the public library surface — the resolver and
//! the work ledger know nothing about it — the same shape as the sibling
//! `examples/work_fabric_demo.rs`. It names four roles, one per
//! subcommand:
//!
//! - `holder`: ingests a file, submits a `blob_ref` job, serves the blob.
//! - `consumer`: pulls the job, claims it, discovers the holder through the
//!   fabric peer directory, and resolves the bytes node-to-node.
//! - `rogue`: forges a LOCAL claim (to pass its own client-side pre-check)
//!   and asks the holder directly, proving the holder's own ledger — not the
//!   caller's local mirror — is the security boundary.
//! - `scan-hub`: scans every table in the hub's OWN data root for the marker
//!   string, proving the hub carries coordination rows (job/claim) but never
//!   blob bytes.
//! - `backend-evidence`: prints the fetch backend's dependency line as
//!   version-gate evidence.
//!
//! Every node derives its identity from the fabric identity file next to its
//! database (`<db>.fabric-identity.key`) — the same file the sync transport
//! uses, so the ledger, the resolver, and the transport all agree on who
//! this machine is.

use clap::{Parser, Subcommand};
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::peer_directory::{install_peer_directory_schema, lookup_peer_ticket};
use contextdb_engine::work_ledger::{
    self as ledger, BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, REF_KIND_BLOB_REF,
    insert_claim, install_work_ledger_schema, job_snapshot, submit_job,
};
use contextdb_server::blob_resolver::{BlobService, ResolveError};
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{FabricIdentity, SyncClient};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser)]
#[command(
    name = "media_transfer_fabric_demo",
    about = "Demo caller for the media plane's blob_ref resolver"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Holder role: ingest a file, submit a blob_ref job, and serve it.
    Holder {
        /// Database path (the fabric identity lives at <db>.fabric-identity.key).
        #[arg(long = "db-path")]
        db: String,
        /// Sync endpoint: the hub's enrollment ticket / endpoint spec.
        #[arg(long = "sync-endpoint")]
        endpoint: String,
        /// Tenant id shared by every node in the fabric.
        #[arg(long = "tenant-id")]
        tenant: String,
        /// The file to serve as the demo "clip".
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        job_id: String,
        /// The holder's movement policy: "on" auto-propagates to any
        /// entitled claimant, "off" forbids the blob from ever leaving.
        #[arg(long, default_value = "on")]
        policy: String,
        /// Arm the tamper hook: serve bytes that do NOT match the reference
        /// hash, under the same claimed name.
        #[arg(long, default_value_t = false)]
        tamper: bool,
        /// Relay for the blob-serving endpoint: "none" (default, direct only —
        /// contextdb never contacts a relay unless asked), "n0" (n0's free
        /// public relays), or a self-hosted relay URL. The choice rides in the
        /// serve ticket, so a consumer that cannot reach this holder directly
        /// (e.g. across NAT with no shared VPN) is bridged through it.
        #[arg(long, default_value = "none")]
        relay: String,
    },
    /// Consumer role: pull the job, claim it, discover the holder through
    /// the fabric peer directory, and resolve the blob node-to-node.
    Consumer {
        #[arg(long = "db-path")]
        db: String,
        #[arg(long = "sync-endpoint")]
        endpoint: String,
        #[arg(long = "tenant-id")]
        tenant: String,
        #[arg(long)]
        job_id: String,
    },
    /// Rogue role: forge a LOCAL claim (this node's own ledger mirror only)
    /// and ask the holder directly — the holder's ledger must refuse it.
    Rogue {
        #[arg(long = "db-path")]
        db: String,
        #[arg(long = "sync-endpoint")]
        endpoint: String,
        #[arg(long = "tenant-id")]
        tenant: String,
        #[arg(long)]
        job_id: String,
        #[arg(long)]
        hash: String,
        #[arg(long)]
        holder_ticket: String,
    },
    /// Scan every table in the HUB's own data root for the marker string,
    /// in raw / lowercase-hex / standard-base64 encodings.
    ScanHub {
        /// The hub's OWN database path (run this on/against the hub, not an
        /// edge — it opens the hub's data root directly, no sync).
        #[arg(long = "db-path")]
        db: String,
        #[arg(long)]
        marker: String,
    },
    /// Print the fetch backend's dependency evidence for the version gate.
    BackendEvidence,
}

fn wall_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Exit code for a scenario whose observed outcome DEVIATED from its intent —
/// a legitimate resolve that failed, or a rogue resolve that unexpectedly
/// succeeded. Kept distinct from the `2` used at the setup/operational-error
/// sites above so a caller/CI can tell "the scenario ran but its security /
/// transfer outcome was wrong" apart from "the demo could not even start".
const EXIT_OUTCOME_DEVIATION: i32 = 3;

/// The outcome of a resolve-style scenario, tagged by which role produced it
/// and therefore what result was INTENDED. The legitimate consumer is meant to
/// succeed; the rogue is meant to be refused. `scenario_exit_code` turns this
/// into a process exit code so a caller can branch on whether the scenario met
/// its intent, not merely on whether a line was printed.
enum ScenarioOutcome {
    /// The legitimate consumer resolved the bytes — the intended success.
    LegitResolveOk,
    /// The legitimate consumer's resolve failed (e.g. HolderUnreachable) — a
    /// real failure of the transfer path.
    LegitResolveErr,
    /// The rogue's resolve was refused (Unentitled / PolicyForbidden) — the
    /// DESIRED security outcome.
    RogueRefused,
    /// The rogue's resolve unexpectedly succeeded — a security hole.
    RogueUnexpectedOk,
    /// The rogue check could not complete because of an INFRA failure
    /// (holder unreachable, local store unavailable, transfer aborted, …)
    /// rather than a security refusal — so the check proved nothing. Counted
    /// as a deviation so an infra failure is never masked as "security worked".
    RogueInconclusive,
}

/// Map a scenario outcome to a process exit code: `0` when the observed
/// outcome matched intent (legit resolve succeeded, or rogue was refused),
/// otherwise `EXIT_OUTCOME_DEVIATION`.
fn scenario_exit_code(outcome: &ScenarioOutcome) -> i32 {
    match outcome {
        // Outcome matched intent: the transfer worked, or the rogue was
        // refused as it should be.
        ScenarioOutcome::LegitResolveOk | ScenarioOutcome::RogueRefused => 0,
        // Outcome deviated from intent: a real transfer failure, a rogue that
        // got through (a security hole), or a rogue check that could not
        // complete because of an infra failure (so it proved nothing).
        ScenarioOutcome::LegitResolveErr
        | ScenarioOutcome::RogueUnexpectedOk
        | ScenarioOutcome::RogueInconclusive => EXIT_OUTCOME_DEVIATION,
    }
}

/// Print a human-readable final pass/fail line for `outcome`, then exit the
/// process with its outcome-aware code. Never returns.
fn finish_scenario(outcome: ScenarioOutcome) -> ! {
    let code = scenario_exit_code(&outcome);
    if code == 0 {
        println!("scenario=PASS");
    } else {
        println!("scenario=FAIL (outcome deviated from intent)");
    }
    std::process::exit(code);
}

/// Open `db_path`, install the two schemas this demo touches, load/generate
/// this node's fabric identity, and build a `SyncClient` against `endpoint`
/// (appending this node's identity file the same way `work_fabric_demo`
/// does, unless the operator already supplied one).
fn open_node(
    db_path: &str,
    endpoint: &str,
    tenant: &str,
) -> (Arc<Database>, SyncClient, String, PathBuf) {
    let identity_path = PathBuf::from(format!("{db_path}.fabric-identity.key"));
    let identity = FabricIdentity::load_or_generate(&identity_path).expect("identity");
    let node_id = identity.node_id();
    eprintln!("[demo] node id: {node_id}");

    let db = Arc::new(Database::open(db_path).unwrap_or_else(|err| {
        eprintln!("cannot open {db_path}: {err}");
        eprintln!(
            "(one process per store: if another demo command is running against this \
             database, stop it first)"
        );
        std::process::exit(2);
    }));
    install_work_ledger_schema(&db).expect("install work ledger schema");
    install_peer_directory_schema(&db).expect("install peer directory schema");

    let endpoint = if endpoint.contains('?') && !endpoint.contains("identity=") {
        format!("{endpoint}&identity={}", identity_path.display())
    } else {
        endpoint.to_string()
    };
    let client = SyncClient::new(
        db.clone(),
        &endpoint,
        contextdb_core::TenantId::from(tenant),
    );
    (db, client, node_id, identity_path)
}

/// Extract the `blob_ref` hash carried by a job's input refs. A media-plane
/// job carries exactly one; this is demo-layer reading of a public
/// `JobSnapshot`, never a reach into ledger internals.
fn blob_hash_of_job(input_refs: &[InputRef]) -> Option<BlobHash> {
    input_refs.iter().find_map(|input_ref| {
        if input_ref.kind != REF_KIND_BLOB_REF {
            return None;
        }
        let hex = input_ref.detail.get("hash")?.as_str()?;
        BlobHash::from_hex(hex).ok()
    })
}

/// First ~16 bytes of `bytes`, decoded lossily as UTF-8 — the marker this
/// demo prints back to the owner. `marker_found` is true when the decode
/// carries no replacement characters (i.e. it reads as real text, not
/// truncated/binary noise).
fn leading_marker(bytes: &[u8]) -> (String, bool) {
    let take = bytes.len().min(16);
    let text = String::from_utf8_lossy(&bytes[..take]).into_owned();
    let found = !text.is_empty() && !text.contains('\u{fffd}');
    (text, found)
}

/// Hand-rolled standard base64 (no crate dependency justified for one demo
/// scan) — RFC 4648 standard alphabet, with padding.
fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(ALPHABET[((n >> 18) & 0x3f) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3f) as usize] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[((n >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[(n & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    out
}

fn hex_encode_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// A `Value` cell, rendered the way it would need to render to actually
/// *contain* readable marker text — text and JSON verbatim; everything else
/// via its debug form (numeric/blob types can't smuggle a text marker, but
/// the scan stays honest by not special-casing them out).
fn cell_as_text(value: &Value) -> String {
    match value {
        Value::Text(s) => s.clone(),
        Value::Json(j) => j.to_string(),
        Value::Uuid(u) => u.to_string(),
        other => format!("{other:?}"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    match args.command {
        Command::Holder {
            db,
            endpoint,
            tenant,
            file,
            job_id,
            policy,
            tamper,
            relay,
        } => {
            run_holder(
                &db, &endpoint, &tenant, &file, &job_id, &policy, tamper, &relay,
            )
            .await
        }
        Command::Consumer {
            db,
            endpoint,
            tenant,
            job_id,
        } => run_consumer(&db, &endpoint, &tenant, &job_id).await,
        Command::Rogue {
            db,
            endpoint,
            tenant,
            job_id,
            hash,
            holder_ticket,
        } => run_rogue(&db, &endpoint, &tenant, &job_id, &hash, &holder_ticket).await,
        Command::ScanHub { db, marker } => run_scan_hub(&db, &marker),
        Command::BackendEvidence => run_backend_evidence(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_holder(
    db_path: &str,
    endpoint: &str,
    tenant: &str,
    file: &PathBuf,
    job_id: &str,
    policy: &str,
    tamper: bool,
    relay: &str,
) {
    let (db, client, node_id, identity_path) = open_node(db_path, endpoint, tenant);
    let auto_propagate = policy != "off";
    let movement = MovementPolicy { auto_propagate };
    let holder = BlobService::new(db.clone(), movement, identity_path.clone());

    let hash = holder.ingest_file(file).unwrap_or_else(|err| {
        eprintln!("ingest failed: {err}");
        std::process::exit(2);
    });
    println!("hash={}", hash.to_hex());

    let spec = JobSpec::builder(job_id, "media.demo", "live-smoke", node_id.clone())
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .provenance(Some(
            serde_json::json!({ "origin": "media_transfer_fabric_demo" }),
        ))
        .submitted_at_ms(wall_now_ms())
        .build();
    submit_job(&db, &spec, &[] as &[&[u8]]).expect("submit blob_ref job");
    match client.push().await {
        Ok(result) => eprintln!(
            "[holder] submitted {job_id}: pushed (applied {}, skipped {})",
            result.applied_rows, result.skipped_rows
        ),
        Err(err) => eprintln!("[holder] submitted {job_id}: hub unreachable ({err})"),
    }

    if tamper {
        let original = std::fs::read(file).expect("read file for tamper");
        let mut wrong = original.clone();
        for byte in wrong.iter_mut() {
            *byte ^= 0xff;
        }
        holder
            .serve_wrong_bytes_for_test(&hash, &wrong)
            .expect("arm tamper hook");
        eprintln!("[holder] tamper hook armed: will serve corrupted bytes under {hash}");
    }

    // The blob-serving endpoint uses its OWN identity, distinct from this
    // node's sync-client identity: a node runs two iroh endpoints (one dialing
    // the hub, one serving blobs), and giving them the same key makes the
    // serve endpoint undiallable (same node id, ambiguous dial-by-key). The
    // holder authorizes a fetch against the CONSUMER's authenticated id, not
    // its own serve id, so a separate serve identity is transparent — the peer
    // directory still maps this node's ledger id to the serve ticket below.
    let serve_identity = PathBuf::from(format!("{db_path}.serve-identity.key"));
    // The relay choice rides in the serve ticket; "none" (default) keeps the
    // no-third-party posture — contextdb contacts a relay only when asked.
    let relay_suffix = if relay == "none" {
        String::new()
    } else {
        format!("&relay={relay}")
    };
    let serve_spec = format!("iroh:?identity={}{relay_suffix}", serve_identity.display());
    let serve_endpoint = IrohServer::bind(&serve_spec).await.unwrap_or_else(|err| {
        eprintln!("cannot bind serving endpoint: {err}");
        std::process::exit(2);
    });
    eprintln!("[holder] serving endpoint bound (relay={relay})");
    holder.serve_on(&serve_endpoint);
    let serve_ticket = serve_endpoint.ticket();

    // serve_on already registers this node's OWN ticket in the peer
    // directory; push it to the hub so a consumer's pull can discover it.
    match client.push().await {
        Ok(result) => eprintln!(
            "[holder] peer directory pushed (applied {}, skipped {})",
            result.applied_rows, result.skipped_rows
        ),
        Err(err) => eprintln!("[holder] peer directory push failed: {err}"),
    }

    println!("holder_node={node_id}");
    println!("serve_ticket={serve_ticket}");
    println!("hash={}", hash.to_hex());
    println!("policy={}", if auto_propagate { "on" } else { "off" });
    println!("tamper={tamper}");

    // The holder's own ledger mirror is what its `serve_on` verdict
    // function reads at fetch time (the entitlement + policy gates) — so
    // this node must keep learning about claims consumers make
    // AFTER it started serving, not just the state at submit time. A
    // long-running holder pulls periodically for exactly this reason.
    let pull_client = client;
    let pulling = tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            if let Err(err) = pull_client.pull_default().await {
                eprintln!("[holder] background pull failed: {err}");
            }
        }
    });

    eprintln!("[holder] serving (pulling claims every 2s); Ctrl-C to stop");
    let _ = tokio::signal::ctrl_c().await;
    eprintln!("[holder] shutting down");
    pulling.abort();
    serve_endpoint.close().await;
}

async fn run_consumer(db_path: &str, endpoint: &str, tenant: &str, job_id: &str) {
    let (db, client, node_id, identity_path) = open_node(db_path, endpoint, tenant);

    match client.pull_default().await {
        Ok(result) => eprintln!(
            "[consumer] pulled (applied {}, skipped {})",
            result.applied_rows, result.skipped_rows
        ),
        Err(err) => {
            eprintln!("[consumer] pull failed: {err}");
            std::process::exit(2);
        }
    }

    let Some(snapshot) = job_snapshot(&db, job_id).expect("job snapshot read") else {
        eprintln!("[consumer] job {job_id} not present after pull");
        std::process::exit(2);
    };
    let submitter_node_id = snapshot.submitter_node_id.clone();
    let Some(hash) = blob_hash_of_job(&snapshot.input_refs) else {
        eprintln!("[consumer] job {job_id} carries no blob_ref input");
        std::process::exit(2);
    };

    let now = wall_now_ms();
    let lease_ms = now + 10 * 60_000;
    let attempt = ledger::next_attempt(&db, job_id).expect("next attempt");
    match insert_claim(&db, job_id, attempt, &node_id, lease_ms, now).expect("insert claim") {
        ClaimInsert::Inserted => eprintln!("[consumer] claimed {job_id}#{attempt}"),
        ClaimInsert::AlreadyHeld { holder } if holder == node_id => {
            eprintln!("[consumer] already holds the claim for {job_id}#{attempt}")
        }
        ClaimInsert::AlreadyHeld { holder } => {
            eprintln!(
                "[consumer] claim {job_id}#{attempt} already held by {holder}; continuing anyway"
            )
        }
    }
    match client.push().await {
        Ok(result) => eprintln!(
            "[consumer] claim pushed (applied {}, skipped {})",
            result.applied_rows, result.skipped_rows
        ),
        Err(err) => eprintln!("[consumer] claim push failed: {err}"),
    }

    let ticket = lookup_peer_ticket(&db, &submitter_node_id).expect("peer directory lookup");
    println!(
        "directory_lookup: asked={submitter_node_id} got={}",
        ticket.as_deref().unwrap_or("<none>")
    );
    let Some(ticket) = ticket else {
        eprintln!("[consumer] no directory entry for holder {submitter_node_id}");
        std::process::exit(2);
    };

    let consumer = BlobService::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        identity_path,
    );
    // The holder authorizes against ITS OWN ledger, which learns of this
    // consumer's just-pushed claim only on its next background pull (a couple
    // of seconds). A transient `Unentitled` here means "the holder hasn't
    // synced the claim yet", NOT a real refusal — so retry a bounded number
    // of times before believing it. A genuine refusal (rogue / policy) stays
    // Unentitled/PolicyForbidden across every attempt and surfaces after the
    // budget. Every other error is terminal and surfaces immediately.
    let mut sink: Vec<u8> = Vec::new();
    let mut last_err: Option<ResolveError> = None;
    for attempt in 0..6 {
        sink.clear();
        match consumer.resolve_blob_ref(&hash, &ticket, &mut sink).await {
            Ok(bytes) => {
                let received_hash = BlobHash::of(&sink);
                let (marker, marker_found) = leading_marker(&sink);
                println!(
                    "resolve=OK bytes={bytes} received_hash={} marker=\"{marker}\" marker_found={marker_found}",
                    received_hash.to_hex()
                );
                finish_scenario(ScenarioOutcome::LegitResolveOk);
            }
            Err(ResolveError::Unentitled) => {
                eprintln!(
                    "[consumer] not yet entitled on the holder (attempt {}); waiting for the holder to pull the claim",
                    attempt + 1
                );
                last_err = Some(ResolveError::Unentitled);
                tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
            }
            Err(err) => {
                println!("resolve=ERR {err}");
                finish_scenario(ScenarioOutcome::LegitResolveErr);
            }
        }
    }
    println!(
        "resolve=ERR {}",
        last_err.unwrap_or(ResolveError::Unentitled)
    );
    finish_scenario(ScenarioOutcome::LegitResolveErr);
}

async fn run_rogue(
    db_path: &str,
    endpoint: &str,
    tenant: &str,
    job_id: &str,
    hash_hex: &str,
    holder_ticket: &str,
) {
    let (db, _client, node_id, identity_path) = open_node(db_path, endpoint, tenant);
    let hash = BlobHash::from_hex(hash_hex).unwrap_or_else(|err| {
        eprintln!("bad --hash: {err}");
        std::process::exit(2);
    });

    // Forge a job + claim entirely LOCAL to this node's own ledger mirror —
    // enough to pass this node's own client-side pre-check
    // (`any_node_holds_claim_for_blob`), but invisible to the holder, whose
    // OWN ledger is the actual security boundary.
    let spec = JobSpec::builder(job_id, "media.demo", "live-smoke-rogue", node_id.clone())
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .provenance(Some(
            serde_json::json!({ "origin": "media_transfer_fabric_demo/rogue" }),
        ))
        .submitted_at_ms(wall_now_ms())
        .build();
    submit_job(&db, &spec, &[] as &[&[u8]]).expect("submit forged job");
    let now = wall_now_ms();
    insert_claim(&db, job_id, 1, &node_id, now + 10 * 60_000, now).expect("insert forged claim");
    eprintln!("[rogue] forged a local job+claim for {job_id} under its own node id {node_id}");
    // Deliberately never pushed to the hub — a rogue's forged rows must
    // never even leave this box to attempt the deception.

    let rogue = BlobService::new(
        db,
        MovementPolicy {
            auto_propagate: true,
        },
        identity_path,
    );
    let mut sink: Vec<u8> = Vec::new();
    match rogue
        .resolve_blob_ref(&hash, holder_ticket, &mut sink)
        .await
    {
        Ok(bytes) => {
            println!("rogue_resolve=OK (UNEXPECTED) bytes={bytes}");
            finish_scenario(ScenarioOutcome::RogueUnexpectedOk);
        }
        // Only an entitlement/policy refusal is the intended security outcome.
        Err(err @ (ResolveError::Unentitled | ResolveError::PolicyForbidden)) => {
            println!("rogue_resolve=ERR {err}");
            finish_scenario(ScenarioOutcome::RogueRefused);
        }
        // Any other error is an INFRA failure, not a refusal — the check
        // proved nothing, so it must not read as "security worked".
        Err(err) => {
            println!("rogue_resolve=ERR {err} (inconclusive: infra failure, not a refusal)");
            finish_scenario(ScenarioOutcome::RogueInconclusive);
        }
    }
}

fn run_scan_hub(db_path: &str, marker: &str) {
    let db = Database::open(db_path).unwrap_or_else(|err| {
        eprintln!("cannot open hub db {db_path}: {err}");
        std::process::exit(2);
    });

    let marker_hex = hex_encode_lower(marker.as_bytes());
    let marker_b64 = base64_encode(marker.as_bytes());

    let mut marker_found = false;
    let mut has_job_rows = false;
    let mut has_claim_rows = false;

    for table in db.table_names() {
        let sql = format!("SELECT * FROM {table}");
        let Ok(result) = db.execute(&sql, &std::collections::HashMap::new()) else {
            eprintln!("[scan-hub] could not scan table {table}; skipping");
            continue;
        };
        if table == "work_jobs" && !result.rows.is_empty() {
            has_job_rows = true;
        }
        if table == "work_claims" && !result.rows.is_empty() {
            has_claim_rows = true;
        }
        for row in &result.rows {
            for value in row {
                let text = cell_as_text(value);
                if text.contains(marker) || text.contains(&marker_hex) || text.contains(&marker_b64)
                {
                    eprintln!("[scan-hub] marker hit in table {table}: {text}");
                    marker_found = true;
                }
            }
        }
    }

    println!("hub_marker_found={marker_found}");
    println!("hub_has_job_rows={has_job_rows}");
    println!("hub_has_claim_rows={has_claim_rows}");
}

fn run_backend_evidence() {
    let tree = std::process::Command::new("cargo")
        .args(["tree", "-p", "contextdb-server", "-i", "iroh-blobs"])
        .output();
    if let Ok(output) = tree
        && output.status.success()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        if let Some(version) = stdout
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().find(|tok| tok.starts_with('v')))
        {
            println!("fetch_backend=iroh-blobs {}", &version[1..]);
            return;
        }
    }

    // Fall back to the workspace lockfile: two directories up from this
    // crate's manifest is the workspace root.
    let lockfile = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock");
    let Ok(contents) = std::fs::read_to_string(&lockfile) else {
        eprintln!("could not run `cargo tree` or read {}", lockfile.display());
        std::process::exit(2);
    };
    let mut lines = contents.lines();
    while let Some(line) = lines.next() {
        if line.trim() == "name = \"iroh-blobs\""
            && let Some(version_line) = lines.next()
            && let Some(version) = version_line.split('"').nth(1)
        {
            println!("fetch_backend=iroh-blobs {version}");
            return;
        }
    }
    eprintln!("iroh-blobs not found in {}", lockfile.display());
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;

    // The legitimate consumer resolve is expected to SUCCEED; a printed
    // `resolve=ERR ...` (e.g. HolderUnreachable) is a real failure, so it must
    // exit with the distinct non-zero deviation code, not 0.
    #[test]
    fn legit_resolve_failure_exits_nonzero() {
        assert_eq!(
            scenario_exit_code(&ScenarioOutcome::LegitResolveErr),
            EXIT_OUTCOME_DEVIATION
        );
        assert_ne!(scenario_exit_code(&ScenarioOutcome::LegitResolveErr), 0);
    }

    // A rogue resolve that is REFUSED is the desired security outcome; it must
    // keep the demo's exit at 0 (a refusal is not a demo failure).
    #[test]
    fn rogue_refused_exits_zero() {
        assert_eq!(scenario_exit_code(&ScenarioOutcome::RogueRefused), 0);
    }

    // A rogue resolve that fails on INFRA (holder unreachable, local store
    // unavailable, …) is NOT a security refusal — the check proved nothing, so
    // it must exit non-zero rather than be masked as "security worked".
    #[test]
    fn rogue_infra_failure_is_not_counted_as_refused() {
        assert_eq!(
            scenario_exit_code(&ScenarioOutcome::RogueInconclusive),
            EXIT_OUTCOME_DEVIATION
        );
        assert_ne!(scenario_exit_code(&ScenarioOutcome::RogueInconclusive), 0);
    }

    // A rogue resolve that unexpectedly SUCCEEDS is a security hole and must
    // exit non-zero.
    #[test]
    fn rogue_unexpected_success_exits_nonzero() {
        assert_eq!(
            scenario_exit_code(&ScenarioOutcome::RogueUnexpectedOk),
            EXIT_OUTCOME_DEVIATION
        );
        assert_ne!(scenario_exit_code(&ScenarioOutcome::RogueUnexpectedOk), 0);
    }

    // The intended happy path (legit consumer resolves the bytes) stays exit 0.
    #[test]
    fn legit_resolve_success_exits_zero() {
        assert_eq!(scenario_exit_code(&ScenarioOutcome::LegitResolveOk), 0);
    }

    // The deviation code is distinct from the setup/operational-error code (2).
    #[test]
    fn deviation_code_is_distinct_from_setup_error() {
        assert_ne!(EXIT_OUTCOME_DEVIATION, 0);
        assert_ne!(EXIT_OUTCOME_DEVIATION, 2);
    }
}
