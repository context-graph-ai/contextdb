//! Measurement driver for the bounded-tables + transfer-receipts live-smoke
//! (`scripts/bounded-tables-live-smoke.sh`).
//!
//! This binary is a CALLER of the public library surface — the same shape as
//! `examples/media_transfer_fabric_demo.rs`. It runs the real product path: a
//! real file-backed `Database`, the engine's OWN maintenance loop (never a
//! shortened interval — the shipped 60s tick, with seconds-scale `RETAIN`
//! windows for time compression), a real `contextdb-server` hub over the real
//! iroh transport reached by its enrollment ticket, and a real `SyncClient`.
//!
//! Division of labour: this binary MEASURES and prints `key=value` lines. It
//! makes no pass/fail judgement — every assertion lives in the shell script, so
//! a reviewer reads the pass/fail logic there and never has to trust a verdict
//! computed inside the thing under test.
//!
//! The named table is a downstream application's retained-metric table with the
//! retention window compressed to seconds, plus ONE deliberate deviation from a
//! verbatim table declaration — the surrogate primary key. See `retained_ddl`
//! for the full reasoning: the literal keyless shape silently destroyed data on
//! this smoke's first run and the engine now refuses it at declaration.
//!
//! ```sql
//! CREATE TABLE metric_windows (
//!     window_key   TEXT PRIMARY KEY,   -- the deliberate deviation
//!     machine_id   TEXT,
//!     sensor_id    TEXT,
//!     metric       TEXT,
//!     window_start INTEGER,
//!     window_secs  INTEGER,
//!     value        REAL
//! ) RETAIN <n> SECONDS SYNC SAFE;
//! ```

use clap::{Parser, Subcommand};
use contextdb_core::{TenantId, Value, Wallclock};
use contextdb_engine::Database;
use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
use contextdb_engine::work_ledger::{
    BlobHash, ClaimInsert, InputRef, JobSpec, MovementPolicy, insert_claim,
    install_work_ledger_schema, submit_job,
};
use contextdb_server::exit_codes::EXIT_ERROR;
use contextdb_server::transport::iroh::IrohServer;
use contextdb_server::{
    BlobStore, FabricIdentity, SyncClient, SyncServer, TransferDirection, TransferPlane,
    TransferReceipt, peer_bind_spec, peer_dial_spec,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// The retained table's name and shape.
const RETAINED_TABLE: &str = "metric_windows";
/// An ordinary two-way table alongside it: the control that proves a pull path
/// works at all, so "the retained table did not come back" is never a
/// failed-transfer tautology.
const CONTROL_TABLE: &str = "smoke_notes";

/// How often the plateau measurement reads the database file's size. Fine
/// enough to catch the sawtooth's peaks between the engine's 60s maintenance
/// ticks, rather than one reading per cycle at an arbitrary phase.
const FILE_SAMPLE_INTERVAL: Duration = Duration::from_secs(5);

/// The retained-metric table, plus a surrogate primary key.
///
/// DELIBERATE DEVIATION from a verbatim table declaration. The original example
/// declares no key at all, and this smoke's first run proved that shape destroys
/// data: rows of a keyless table never enter the pushed changeset, the push
/// still reports success, the watermark advances over their LSNs, and the
/// engine's retention loop then deletes rows the hub never received — with
/// `skipped_rows: 0`, so nothing anywhere reports the loss. The engine now
/// REFUSES keyless + `SYNC SAFE` at declaration (proved on the product path by
/// the `keyless-probe` subcommand below), so the literal keyless example is no
/// longer a legal declaration.
///
/// The key is a surrogate rather than the natural composite because the engine
/// supports no composite primary key: `grammar.pest:224` offers `PRIMARY KEY`
/// only as a COLUMN constraint, table-level constraints are `UNIQUE(...)` and
/// foreign keys only (`:146-147`), and the syncable natural key resolves as
/// explicit natural key → the single primary-key column → a column named `id`
/// (`sync_types.rs:199-214`) — a composite UNIQUE establishes nothing. The
/// surrogate therefore carries the natural identity, MACHINE ID FIRST, so two
/// machines' windows can never collide in the hub's union.
fn retained_ddl(retain_secs: u64, sync_safe: bool) -> String {
    let sync = if sync_safe { " SYNC SAFE" } else { "" };
    format!(
        "CREATE TABLE {RETAINED_TABLE} (\
            window_key   TEXT PRIMARY KEY, \
            machine_id   TEXT, \
            sensor_id    TEXT, \
            metric       TEXT, \
            window_start INTEGER, \
            window_secs  INTEGER, \
            value        REAL\
        ) RETAIN {retain_secs} SECONDS{sync}"
    )
}

/// The literal keyless declaration, kept ONLY so the smoke can prove the engine
/// now refuses it on the real product path.
fn keyless_retained_ddl(retain_secs: u64) -> String {
    format!(
        "CREATE TABLE {RETAINED_TABLE}_keyless (\
            machine_id   TEXT, \
            sensor_id    TEXT, \
            metric       TEXT, \
            window_start INTEGER, \
            window_secs  INTEGER, \
            value        REAL\
        ) RETAIN {retain_secs} SECONDS SYNC SAFE"
    )
}

fn control_ddl() -> String {
    format!("CREATE TABLE {CONTROL_TABLE} (id INTEGER PRIMARY KEY, body TEXT)")
}

fn p() -> HashMap<String, Value> {
    HashMap::new()
}

/// Wall-clock milliseconds for values that get PERSISTED — work-ledger job
/// submission stamps and claim lease/now stamps. Routed through
/// `Wallclock::now()` rather than reading `SystemTime` directly, per the repo
/// rule that every persisted timestamp flows through the clock seam. Raw
/// monotonic clocks are used in this file ONLY for measuring durations that
/// never land in a row (the writer's stall gaps and the sampler's elapsed
/// offsets), which is what `Instant` is for.
fn now_ms() -> i64 {
    Wallclock::now().0 as i64
}

/// Cheap live-row count for the sampling loop — `COUNT(*)` rather than
/// materialising every row, so measuring cannot perturb the writer-stall
/// numbers being measured alongside it.
fn row_count_fast(db: &Database, table: &str) -> i64 {
    match db.execute(&format!("SELECT COUNT(*) FROM {table}"), &p()) {
        Ok(result) => match result.rows.first().and_then(|row| row.first()) {
            Some(Value::Int64(count)) => *count,
            _ => -1,
        },
        Err(_) => -1,
    }
}

fn row_count(db: &Database, table: &str) -> i64 {
    match db.execute(&format!("SELECT * FROM {table}"), &p()) {
        Ok(result) => result.rows.len() as i64,
        // -1 is an unmistakable "could not read", never confusable with a real
        // count, so a script assertion on it fails rather than reads as empty.
        Err(_) => -1,
    }
}

fn die(context: &str, err: impl std::fmt::Display) -> ! {
    eprintln!("[driver] FATAL {context}: {err}");
    std::process::exit(EXIT_ERROR);
}

fn emit(key: &str, value: impl std::fmt::Display) {
    println!("{key}={value}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
}

/// A padded metric label so a row carries a realistic payload — the file-size
/// movement S3 and S4 measure needs rows with real bytes in them, not 40-byte
/// stubs whose churn disappears inside one 4KiB page.
fn padded_metric(row_bytes: usize) -> String {
    let base = "frames_per_second";
    if row_bytes <= base.len() {
        return base.to_string();
    }
    format!("{base}{}", "x".repeat(row_bytes - base.len()))
}

fn insert_windows(db: &Database, count: u64, start_id: u64, metric: &str) {
    for i in 0..count {
        let n = start_id + i;
        let machine_id = format!("machine-{n}");
        let sensor_id = format!("sensor-{}", n % 4);
        let mut row = p();
        // The surrogate key carries the natural identity, machine first, so
        // windows from different machines never collide in the hub's union.
        row.insert(
            "window_key".to_string(),
            Value::Text(format!("{machine_id}:{sensor_id}:{metric}:{n}:60")),
        );
        row.insert("machine_id".to_string(), Value::Text(machine_id));
        row.insert("sensor_id".to_string(), Value::Text(sensor_id));
        row.insert("metric".to_string(), Value::Text(metric.to_string()));
        row.insert("window_start".to_string(), Value::Int64(n as i64));
        row.insert("window_secs".to_string(), Value::Int64(60));
        row.insert("value".to_string(), Value::Float64(n as f64));
        if let Err(err) = db.execute(
            &format!(
                "INSERT INTO {RETAINED_TABLE} \
                 (window_key, machine_id, sensor_id, metric, window_start, window_secs, value) \
                 VALUES ($window_key, $machine_id, $sensor_id, $metric, $window_start, \
                 $window_secs, $value)"
            ),
            &row,
        ) {
            die("window insert", err);
        }
    }
}

fn insert_notes(db: &Database, count: u64, start_id: u64) {
    for i in 0..count {
        let n = (start_id + i) as i64;
        let mut row = p();
        row.insert("id".to_string(), Value::Int64(n));
        row.insert("body".to_string(), Value::Text(format!("note-{n}")));
        if let Err(err) = db.execute(
            &format!("INSERT INTO {CONTROL_TABLE} (id, body) VALUES ($id, $body)"),
            &row,
        ) {
            die("note insert", err);
        }
    }
}

/// Open a file-backed edge database and declare the two tables if they are not
/// already there. Nothing here registers retention, schedules a prune, or
/// touches a maintenance knob — declaring the table IS the whole consumer
/// contribution, which is the product promise under test.
fn open_edge(path: &Path, retain_secs: u64) -> Arc<Database> {
    let db = match Database::open(path) {
        Ok(db) => Arc::new(db),
        Err(err) => die(&format!("open edge database {}", path.display()), err),
    };
    if db.table_meta(RETAINED_TABLE).is_none()
        && let Err(err) = db.execute(&retained_ddl(retain_secs, true), &p())
    {
        die("declare the retained table", err);
    }
    if db.table_meta(CONTROL_TABLE).is_none()
        && let Err(err) = db.execute(&control_ddl(), &p())
    {
        die("declare the control table", err);
    }
    db
}

fn identity_node_id(identity: &Path) -> String {
    match FabricIdentity::load_or_generate(identity) {
        Ok(identity) => identity.node_id(),
        Err(err) => die("load or generate the fabric identity", err),
    }
}

fn client_for(db: &Arc<Database>, ticket: &str, identity: &Path, tenant: &str) -> SyncClient {
    SyncClient::new(
        db.clone(),
        &peer_dial_spec(ticket, identity),
        TenantId::from(tenant),
    )
}

fn receipt_lines(prefix: &str, receipts: &[TransferReceipt]) {
    emit(&format!("{prefix}.count"), receipts.len());
    for r in receipts {
        let plane = match r.plane {
            TransferPlane::Sync => "sync",
            TransferPlane::Blob => "blob",
        };
        let direction = match r.direction {
            TransferDirection::Sent => "sent",
            TransferDirection::Received => "received",
        };
        emit(
            &format!("{prefix}.{}.{plane}.{direction}.items", r.peer_node_id),
            r.counters.items,
        );
        emit(
            &format!("{prefix}.{}.{plane}.{direction}.bytes", r.peer_node_id),
            r.counters.payload_bytes,
        );
    }
}

/// Poll `probe` until it returns true or `budget` elapses. This is NOT
/// sleep-as-synchronization dressed up: what is being waited for is the
/// engine's own 60-second maintenance tick doing real work on real wall-clock
/// time, which is precisely the product behaviour under test. The budget only
/// bounds the failure case.
async fn wait_until<F: FnMut() -> bool>(budget: Duration, mut probe: F) -> bool {
    let start = Instant::now();
    loop {
        if probe() {
            return true;
        }
        if start.elapsed() >= budget {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[derive(Parser)]
#[command(
    name = "bounded_tables_smoke",
    about = "Measurement driver for the bounded-tables live-smoke"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print the node id derived from a fabric identity key file.
    NodeId {
        #[arg(long)]
        identity: PathBuf,
    },
    /// Try to declare the LITERAL keyless retained table and report whether the
    /// engine refused it, with the message it gave. Run 1 of this
    /// smoke proved that shape silently destroys data; this turns that
    /// disaster into a standing assertion on the product path.
    KeylessProbe {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        retain_secs: u64,
    },
    /// Read back what a database actually declares and holds: the retained
    /// table's persisted policy, window, row counts, and watermark. Pure
    /// observation — it declares nothing and prunes nothing.
    Probe {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long, default_value = "probe")]
        prefix: String,
    },
    /// Run a real hub: a file-backed database served by a real `SyncServer`
    /// over a real iroh endpoint, publishing its enrollment ticket and a
    /// `key=value` status file the script reads while it runs.
    Hub {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        port: u16,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        ticket_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        /// The hub's OWN retention window. Deliberately longer than the edge's
        /// in S1 so the hub provably still holds the rows while the edge prunes
        /// — otherwise "a later pull did not resurrect them" would be a
        /// tautology about an empty hub.
        #[arg(long)]
        retain_secs: u64,
    },
    /// S1 first half: write, push, let the ENGINE'S OWN loop prune, then pull.
    S1Lifecycle {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        retain_secs: u64,
        #[arg(long)]
        rows: u64,
        #[arg(long, default_value_t = 240)]
        prune_budget_secs: u64,
    },
    /// S1 second half: a full worker wipe, then a restore pull.
    S1Restore {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        retain_secs: u64,
    },
    /// S2: an offline worker keeps its whole unconfirmed backlog past the
    /// window, and prunes only after reconnect + confirmed push.
    S2Offline {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        retain_secs: u64,
        #[arg(long)]
        rows: u64,
        /// How long to stay offline past the retention window. Must clear the
        /// window plus at least two engine maintenance ticks, so a prune that
        /// ignored delivery would have had every chance to fire.
        #[arg(long)]
        offline_secs: u64,
        #[arg(long, default_value_t = 240)]
        prune_budget_secs: u64,
    },
    /// S3 first half: steady write-and-expire churn across full retention
    /// cycles under the engine's own loop, with a concurrent writer.
    S3Plateau {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        retain_secs: u64,
        #[arg(long)]
        cycles: u64,
        #[arg(long)]
        cycle_secs: u64,
        #[arg(long)]
        row_bytes: usize,
        #[arg(long)]
        writes_per_second: u64,
        /// DIFFERENTIAL MODE. Declare the retained table WITHOUT `SYNC SAFE`
        /// and never construct a sync client or push anything, so the identical
        /// churn runs with no sync history accumulating anywhere. If the
        /// settled file size still climbs cycle over cycle in this mode, the
        /// growth is not sync-related; if it plateaus, the sync side is the
        /// source. Diagnosis only — the graded S3 assertion always runs the
        /// full synced product path.
        #[arg(long, default_value_t = false)]
        unsynced: bool,
    },
    /// S3 second half: the prune report itself — bytes reclaimed, and a
    /// deliberately fragmented file that shrinks after compaction.
    S3Reclaim {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        retain_secs: u64,
        #[arg(long)]
        rows: u64,
        #[arg(long)]
        row_bytes: usize,
    },
    /// S4: the per-table size answer moves with inserts and prunes, is named an
    /// estimate, and carries whole-file bytes alongside as themselves.
    S4Size {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        retain_secs: u64,
        #[arg(long)]
        rows: u64,
        #[arg(long)]
        row_bytes: usize,
    },
    /// S5 sync plane: a controlled transfer of known row batches, reported from
    /// the client end (the hub end comes from the hub's status file).
    S5Sync {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        retain_secs: u64,
        /// `seed` pushes foreign rows and stops; `measured` pulls those rows
        /// and then pushes its own, so both directions have exact counts.
        #[arg(long)]
        role: String,
        #[arg(long)]
        rows: u64,
    },
    /// S5 blob plane, holder end: ingest one blob, entitle one consumer node,
    /// serve it, and publish receipts through a status file.
    BlobHolder {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        ticket_file: PathBuf,
        #[arg(long)]
        status_file: PathBuf,
        #[arg(long)]
        entitle_node: String,
        #[arg(long)]
        blob_bytes: usize,
        #[arg(long)]
        job_id: String,
    },
    /// S5 blob plane, fetching end.
    BlobFetch {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        identity: PathBuf,
        #[arg(long)]
        holder_ticket: String,
        #[arg(long)]
        holder_node: String,
        #[arg(long)]
        hash: String,
        #[arg(long)]
        job_id: String,
    },
    /// S6: a trigger-heavy workload plus retention cycles leaves the engine's
    /// own durable `__trigger_audit` bounded, with the in-memory ring intact.
    S6Audit {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        firings_per_round: u64,
    },
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    match Args::parse().command {
        Command::NodeId { identity } => emit("node_id", identity_node_id(&identity)),
        Command::Probe { db_path, prefix } => run_probe(&db_path, &prefix),
        Command::KeylessProbe {
            db_path,
            retain_secs,
        } => run_keyless_probe(&db_path, retain_secs),
        Command::Hub {
            db_path,
            identity,
            port,
            tenant,
            ticket_file,
            status_file,
            retain_secs,
        } => {
            run_hub(
                &db_path,
                &identity,
                port,
                &tenant,
                &ticket_file,
                &status_file,
                retain_secs,
            )
            .await
        }
        Command::S1Lifecycle {
            db_path,
            identity,
            ticket,
            tenant,
            retain_secs,
            rows,
            prune_budget_secs,
        } => {
            run_s1_lifecycle(
                &db_path,
                &identity,
                &ticket,
                &tenant,
                retain_secs,
                rows,
                prune_budget_secs,
            )
            .await
        }
        Command::S1Restore {
            db_path,
            identity,
            ticket,
            tenant,
            retain_secs,
        } => run_s1_restore(&db_path, &identity, &ticket, &tenant, retain_secs).await,
        Command::S2Offline {
            db_path,
            identity,
            ticket,
            tenant,
            retain_secs,
            rows,
            offline_secs,
            prune_budget_secs,
        } => {
            run_s2_offline(
                &db_path,
                &identity,
                &ticket,
                &tenant,
                retain_secs,
                rows,
                offline_secs,
                prune_budget_secs,
            )
            .await
        }
        Command::S3Plateau {
            db_path,
            identity,
            ticket,
            tenant,
            retain_secs,
            cycles,
            cycle_secs,
            row_bytes,
            writes_per_second,
            unsynced,
        } => {
            run_s3_plateau(
                &db_path,
                &identity,
                &ticket,
                &tenant,
                retain_secs,
                cycles,
                cycle_secs,
                row_bytes,
                writes_per_second,
                unsynced,
            )
            .await
        }
        Command::S3Reclaim {
            db_path,
            retain_secs,
            rows,
            row_bytes,
        } => run_s3_reclaim(&db_path, retain_secs, rows, row_bytes).await,
        Command::S4Size {
            db_path,
            retain_secs,
            rows,
            row_bytes,
        } => run_s4_size(&db_path, retain_secs, rows, row_bytes).await,
        Command::S5Sync {
            db_path,
            identity,
            ticket,
            tenant,
            retain_secs,
            role,
            rows,
        } => {
            run_s5_sync(
                &db_path,
                &identity,
                &ticket,
                &tenant,
                retain_secs,
                &role,
                rows,
            )
            .await
        }
        Command::BlobHolder {
            db_path,
            identity,
            ticket_file,
            status_file,
            entitle_node,
            blob_bytes,
            job_id,
        } => {
            run_blob_holder(
                &db_path,
                &identity,
                &ticket_file,
                &status_file,
                &entitle_node,
                blob_bytes,
                &job_id,
            )
            .await
        }
        Command::BlobFetch {
            db_path,
            identity,
            holder_ticket,
            holder_node,
            hash,
            job_id,
        } => {
            run_blob_fetch(
                &db_path,
                &identity,
                &holder_ticket,
                &holder_node,
                &hash,
                &job_id,
            )
            .await
        }
        Command::S6Audit {
            db_path,
            firings_per_round,
        } => run_s6_audit(&db_path, firings_per_round),
    }
}

/// Declare the literal keyless retained table and report whether the engine
/// refused it. A refusal at declaration is the only safe outcome: a
/// keyless retained table's rows cannot enter a changeset, so accepting one
/// means acking a delivery that never happened and then deleting the rows.
fn run_keyless_probe(db_path: &Path, retain_secs: u64) {
    let db = match Database::open(db_path) {
        Ok(db) => db,
        Err(err) => die("open the keyless-probe database", err),
    };
    match db.execute(&keyless_retained_ddl(retain_secs), &p()) {
        Ok(_) => {
            emit("keyless.refused", 0);
            emit("keyless.error", "<none: the declaration was ACCEPTED>");
        }
        Err(err) => {
            emit("keyless.refused", 1);
            // One line: the script echoes it so the operator sees whether the
            // refusal actually teaches what to do instead.
            emit(
                "keyless.error",
                err.to_string()
                    .replace(['\n', '\r'], " ")
                    .trim()
                    .to_string(),
            );
        }
    }
    let _ = db.close();
    emit("keyless.done", 1);
}

/// Read back what a database actually declares and holds. Used to tell a
/// declaration problem apart from a transport problem when an assertion fails.
fn run_probe(db_path: &Path, prefix: &str) {
    let db = match Database::open(db_path) {
        Ok(db) => db,
        Err(err) => die(&format!("open {} for probing", db_path.display()), err),
    };
    emit(
        &format!("{prefix}.tables"),
        db.table_names().join(",").to_string(),
    );
    match db.table_meta(RETAINED_TABLE) {
        Some(meta) => {
            emit(&format!("{prefix}.retained.present"), 1);
            emit(&format!("{prefix}.retained.sync_safe"), meta.sync_safe);
            emit(
                &format!("{prefix}.retained.direction"),
                format!("{:?}", meta.sync_direction),
            );
            emit(
                &format!("{prefix}.retained.window_secs"),
                format!("{:?}", meta.default_ttl_seconds),
            );
            emit(
                &format!("{prefix}.retained.declared_unit"),
                format!("{:?}", meta.retain_declared_unit),
            );
        }
        None => emit(&format!("{prefix}.retained.present"), 0),
    }
    emit(
        &format!("{prefix}.retained.rows"),
        row_count(&db, RETAINED_TABLE),
    );
    emit(
        &format!("{prefix}.control.rows"),
        row_count(&db, CONTROL_TABLE),
    );
    emit(&format!("{prefix}.sync_watermark"), db.sync_watermark().0);
    emit(&format!("{prefix}.current_lsn"), db.current_lsn().0);
    emit(
        &format!("{prefix}.retention_sync_peer"),
        db.retention_sync_peer()
            .unwrap_or_else(|| "none".to_string()),
    );
    let _ = db.close();
}

// ---------------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------------

fn write_atomically(path: &Path, contents: &str) {
    let tmp = path.with_extension("tmp");
    if let Err(err) = std::fs::write(&tmp, contents) {
        die(&format!("write {}", tmp.display()), err);
    }
    if let Err(err) = std::fs::rename(&tmp, path) {
        die(&format!("rename into {}", path.display()), err);
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_hub(
    db_path: &Path,
    identity: &Path,
    port: u16,
    tenant: &str,
    ticket_file: &Path,
    status_file: &Path,
    retain_secs: u64,
) {
    let db = open_edge(db_path, retain_secs);
    let node_id = identity_node_id(identity);
    emit("hub.node_id", &node_id);

    // A stable port next to a stable identity keeps one enrollment ticket valid
    // across a hub restart — S2 stops and restarts this hub under the edge.
    let bind = format!("{}&port={port}", peer_bind_spec(identity));
    let endpoint = match IrohServer::bind(&bind).await {
        Ok(endpoint) => endpoint,
        Err(err) => die("bind the hub sync endpoint", err),
    };
    let ticket = endpoint.ticket();
    write_atomically(ticket_file, &ticket.to_string());
    emit("hub.ticket_file", ticket_file.display());

    let server = Arc::new(SyncServer::with_transport(
        db.clone(),
        endpoint.transport(),
        TenantId::from(tenant),
        ConflictPolicies::uniform(ConflictPolicy::LatestWins),
    ));

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(sig) => sig,
                    Err(err) => die("install the SIGTERM handler", err),
                };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    // The status writer is the script's window into a hub that keeps running:
    // row counts (does the hub still hold the rows? has its OWN loop pruned
    // them?) and its per-peer transfer receipts.
    {
        let db = db.clone();
        let server = server.clone();
        let status_file = status_file.to_path_buf();
        let node_id = node_id.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            while !shutdown.load(Ordering::SeqCst) {
                let mut out = String::new();
                out.push_str(&format!("hub.node_id={node_id}\n"));
                out.push_str(&format!(
                    "hub.retained_rows={}\n",
                    row_count(&db, RETAINED_TABLE)
                ));
                out.push_str(&format!(
                    "hub.control_rows={}\n",
                    row_count(&db, CONTROL_TABLE)
                ));
                let status = db.maintenance_status();
                out.push_str(&format!("hub.maintenance_running={}\n", status.running));
                out.push_str(&format!(
                    "hub.retention_enabled={}\n",
                    status.retention_enabled
                ));
                out.push_str(&format!(
                    "hub.active_maintenance_loops={}\n",
                    status.active_maintenance_loops
                ));
                out.push_str(&format!(
                    "hub.file_bytes={}\n",
                    db.disk_file_size().unwrap_or(0)
                ));
                for r in server.transfer_receipts() {
                    let plane = match r.plane {
                        TransferPlane::Sync => "sync",
                        TransferPlane::Blob => "blob",
                    };
                    let direction = match r.direction {
                        TransferDirection::Sent => "sent",
                        TransferDirection::Received => "received",
                    };
                    out.push_str(&format!(
                        "hub.receipt.{}.{plane}.{direction}.items={}\n",
                        r.peer_node_id, r.counters.items
                    ));
                    out.push_str(&format!(
                        "hub.receipt.{}.{plane}.{direction}.bytes={}\n",
                        r.peer_node_id, r.counters.payload_bytes
                    ));
                }
                out.push_str("hub.ready=1\n");
                write_atomically(&status_file, &out);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });
    }

    emit("hub.serving", 1);
    server.run_until(shutdown).await;
    let _ = db.close();
    emit("hub.stopped", 1);
}

// ---------------------------------------------------------------------------
// S1 — lifecycle end to end
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_s1_lifecycle(
    db_path: &Path,
    identity: &Path,
    ticket: &str,
    tenant: &str,
    retain_secs: u64,
    rows: u64,
    prune_budget_secs: u64,
) {
    let db = open_edge(db_path, retain_secs);
    emit("s1.edge_node_id", identity_node_id(identity));
    emit("s1.retain_secs", retain_secs);

    let status = db.maintenance_status();
    emit("s1.maintenance_running_at_open", status.running);
    emit("s1.retention_enabled_at_open", status.retention_enabled);
    emit(
        "s1.active_maintenance_loops_at_open",
        status.active_maintenance_loops,
    );

    insert_windows(&db, rows, 1, "frames_per_second");
    insert_notes(&db, 3, 1);
    emit("s1.rows_written", row_count(&db, RETAINED_TABLE));
    emit("s1.control_rows_written", row_count(&db, CONTROL_TABLE));
    emit("s1.watermark_before_push", db.sync_watermark().0);

    let client = client_for(&db, ticket, identity, tenant);
    let applied = match client.push().await {
        Ok(applied) => applied,
        Err(err) => die("push to the hub", err),
    };
    emit("s1.push_applied_rows", applied.applied_rows);
    emit("s1.push_skipped_rows", applied.skipped_rows);
    emit("s1.push_watermark", client.push_watermark().0);
    emit("s1.watermark_after_push", db.sync_watermark().0);
    emit(
        "s1.retention_sync_peer",
        db.retention_sync_peer()
            .unwrap_or_else(|| "none".to_string()),
    );

    // No prune is scheduled, driven, or asked for here: the engine's own
    // maintenance loop is the only thing that can empty this table. Waiting is
    // the measurement.
    let start = Instant::now();
    let pruned = wait_until(Duration::from_secs(prune_budget_secs), || {
        row_count(&db, RETAINED_TABLE) == 0
    })
    .await;
    emit("s1.self_pruned", if pruned { 1 } else { 0 });
    emit("s1.self_prune_wait_secs", start.elapsed().as_secs());
    emit("s1.rows_after_self_prune", row_count(&db, RETAINED_TABLE));
    emit(
        "s1.control_rows_after_self_prune",
        row_count(&db, CONTROL_TABLE),
    );

    match client.pull_default().await {
        Ok(result) => emit("s1.pull_applied_rows", result.applied_rows),
        Err(err) => die("pull from the hub", err),
    }
    emit("s1.rows_after_pull", row_count(&db, RETAINED_TABLE));
    emit("s1.control_rows_after_pull", row_count(&db, CONTROL_TABLE));
    let _ = db.close();
    emit("s1.done", 1);
}

async fn run_s1_restore(
    db_path: &Path,
    identity: &Path,
    ticket: &str,
    tenant: &str,
    retain_secs: u64,
) {
    // A wiped machine: no database file at all, the same fabric identity, and
    // a restore pull from a hub that still holds every row.
    let db = open_edge(db_path, retain_secs);
    emit(
        "s1.restore_rows_before_pull",
        row_count(&db, RETAINED_TABLE),
    );
    let client = client_for(&db, ticket, identity, tenant);
    match client.pull_default().await {
        Ok(result) => emit("s1.restore_pull_applied_rows", result.applied_rows),
        Err(err) => die("restore pull", err),
    }
    emit("s1.restore_retained_rows", row_count(&db, RETAINED_TABLE));
    emit("s1.restore_control_rows", row_count(&db, CONTROL_TABLE));
    let _ = db.close();
    emit("s1.restore_done", 1);
}

// ---------------------------------------------------------------------------
// S2 — offline backlog
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_s2_offline(
    db_path: &Path,
    identity: &Path,
    ticket: &str,
    tenant: &str,
    retain_secs: u64,
    rows: u64,
    offline_secs: u64,
    prune_budget_secs: u64,
) {
    let db = open_edge(db_path, retain_secs);
    emit("s2.retain_secs", retain_secs);
    insert_windows(&db, rows, 1, "frames_per_second");
    emit("s2.rows_written", row_count(&db, RETAINED_TABLE));

    let client = client_for(&db, ticket, identity, tenant);
    match client.push().await {
        Ok(applied) => emit("s2.offline_push_applied_rows", applied.applied_rows),
        Err(err) => {
            emit("s2.offline_push_failed", 1);
            eprintln!("[driver] s2 offline push failed as intended: {err}");
        }
    }
    emit("s2.watermark_while_offline", db.sync_watermark().0);
    // The script watches for this line: it is the signal to bring the hub back.
    emit("s2.offline_phase_started", 1);

    // Stay offline well past the retention window AND past two engine
    // maintenance ticks, so an implementation that pruned without delivery
    // would have had every opportunity to do it.
    tokio::time::sleep(Duration::from_secs(offline_secs)).await;
    emit("s2.offline_secs", offline_secs);
    emit(
        "s2.rows_after_offline_window",
        row_count(&db, RETAINED_TABLE),
    );

    // Reconnect: the same database, the same ticket, a hub that is back.
    let mut reconnect_applied = None;
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(180) {
        match client.push().await {
            Ok(applied) => {
                reconnect_applied = Some(applied.applied_rows);
                break;
            }
            Err(err) => {
                eprintln!("[driver] s2 waiting for the hub to return: {err}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }
    match reconnect_applied {
        Some(applied) => emit("s2.reconnect_push_applied_rows", applied),
        None => {
            emit("s2.reconnect_push_applied_rows", -1);
            emit("s2.done", 0);
            return;
        }
    }
    emit("s2.watermark_after_reconnect", db.sync_watermark().0);
    emit(
        "s2.rows_after_reconnect_push",
        row_count(&db, RETAINED_TABLE),
    );

    let pruned = wait_until(Duration::from_secs(prune_budget_secs), || {
        row_count(&db, RETAINED_TABLE) == 0
    })
    .await;
    emit("s2.self_pruned_after_confirm", if pruned { 1 } else { 0 });
    emit(
        "s2.rows_after_confirm_prune",
        row_count(&db, RETAINED_TABLE),
    );
    let _ = db.close();
    emit("s2.done", 1);
}

// ---------------------------------------------------------------------------
// S3 — plateau + reclaim
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_s3_plateau(
    db_path: &Path,
    identity: &Path,
    ticket: &str,
    tenant: &str,
    retain_secs: u64,
    cycles: u64,
    cycle_secs: u64,
    row_bytes: usize,
    writes_per_second: u64,
    unsynced: bool,
) {
    // Differential mode declares the retained table WITHOUT `SYNC SAFE` and
    // never builds a client, so the same churn runs with no sync history at
    // all. Everything else — writer, rates, windows, sampling — is identical.
    let db = if unsynced {
        let db = match Database::open(db_path) {
            Ok(db) => Arc::new(db),
            Err(err) => die("open the unsynced plateau database", err),
        };
        if db.table_meta(RETAINED_TABLE).is_none()
            && let Err(err) = db.execute(&retained_ddl(retain_secs, false), &p())
        {
            die("declare the unsynced retained table", err);
        }
        db
    } else {
        open_edge(db_path, retain_secs)
    };
    emit("s3.unsynced", u8::from(unsynced));
    emit("s3.retain_secs", retain_secs);
    emit("s3.cycles", cycles);
    emit("s3.cycle_secs", cycle_secs);

    let stop = Arc::new(AtomicBool::new(false));
    let max_gap_ms = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));

    // The file SAWTOOTHS: it grows with churn and drops when the engine's loop
    // compacts. A single reading per cycle therefore measures which phase of
    // the sawtooth you happened to look at — and a reading that lands in a
    // trough would pass a genuinely growing file. So sample continuously and
    // judge the plateau on each cycle's PEAK, which no phase accident can
    // flatter. The full trace is printed as evidence either way.
    // Each sample carries the LIVE ROW COUNT taken at the same instant as the
    // file size, so the absolute witness (settled size against live payload)
    // compares two numbers from the same moment rather than a trough against a
    // row count read at some other phase of the sawtooth.
    let samples: Arc<std::sync::Mutex<Vec<(u64, u64, i64)>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));
    let sampler = {
        let db = db.clone();
        let stop = stop.clone();
        let samples = samples.clone();
        std::thread::spawn(move || {
            let start = Instant::now();
            while !stop.load(Ordering::SeqCst) {
                let bytes = db.disk_file_size().unwrap_or(0);
                let rows = row_count_fast(&db, RETAINED_TABLE);
                samples.lock().unwrap_or_else(|err| err.into_inner()).push((
                    start.elapsed().as_secs(),
                    bytes,
                    rows,
                ));
                std::thread::sleep(FILE_SAMPLE_INTERVAL);
            }
        })
    };

    // A concurrent writer that records the gap between its own successive
    // committed writes. A prune or compaction pause that stalled writers shows
    // up here as observed state — no elapsed-ratio guesswork, no sleeping as
    // synchronisation.
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        let max_gap_ms = max_gap_ms.clone();
        let writes = writes.clone();
        let metric = padded_metric(row_bytes);
        let interval = Duration::from_micros(1_000_000 / writes_per_second.max(1));
        std::thread::spawn(move || {
            let mut n = 1_000_000u64;
            let mut last = Instant::now();
            while !stop.load(Ordering::SeqCst) {
                insert_windows(&db, 1, n, &metric);
                n += 1;
                writes.fetch_add(1, Ordering::SeqCst);
                let gap = last.elapsed().as_millis() as u64;
                max_gap_ms.fetch_max(gap, Ordering::SeqCst);
                last = Instant::now();
                std::thread::sleep(interval);
            }
        })
    };

    // Push continuously: rows only become prunable once the hub confirms them,
    // which is the delete-only-after-delivery contract doing its job under
    // steady churn.
    let client = if unsynced {
        None
    } else {
        Some(client_for(&db, ticket, identity, tenant))
    };
    for cycle in 1..=cycles {
        let cycle_end = Instant::now() + Duration::from_secs(cycle_secs);
        while Instant::now() < cycle_end {
            // In differential mode there is no client at all: nothing is
            // delivered anywhere, so nothing gates the prune and no sync
            // history can accumulate.
            if let Some(client) = &client {
                match client.push().await {
                    Ok(applied) => {
                        if applied.applied_rows == 0 {
                            eprintln!("[driver] s3 push applied nothing this round");
                        }
                    }
                    Err(err) => die("s3 push", err),
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        // Drain this cycle's samples: print the whole sawtooth as evidence and
        // report the cycle's PEAK, which is the quantity "the file size
        // plateaus" actually refers to.
        let cycle_samples: Vec<(u64, u64, i64)> = {
            let mut held = samples.lock().unwrap_or_else(|err| err.into_inner());
            std::mem::take(&mut *held)
        };
        let trace = cycle_samples
            .iter()
            .map(|(at, bytes, rows)| format!("{at}s:{bytes}/{rows}r"))
            .collect::<Vec<_>>()
            .join(" ");
        emit(&format!("s3.cycle{cycle}.trace"), trace);
        emit(&format!("s3.cycle{cycle}.samples"), cycle_samples.len());
        emit(
            &format!("s3.cycle{cycle}.peak_file_bytes"),
            cycle_samples
                .iter()
                .map(|(_, bytes, _)| *bytes)
                .max()
                .unwrap_or(0),
        );
        // The TROUGH is the settled, post-compaction size — the file with its
        // dead space actually returned. "The database file stops growing" is a
        // claim about this number, so it carries the trend test, while the peak
        // carries the looser provisioning headroom.
        let trough = cycle_samples
            .iter()
            .min_by_key(|(_, bytes, _)| *bytes)
            .copied()
            .unwrap_or((0, 0, 0));
        emit(&format!("s3.cycle{cycle}.trough_file_bytes"), trough.1);
        // The live row count AT the trough instant.
        emit(&format!("s3.cycle{cycle}.trough_live_rows"), trough.2);
        // The MOST rows alive at once during the cycle. This is the honest
        // denominator for "is the file the right size": the database must be
        // able to hold the peak live set — between prunes this workload
        // accumulates a full maintenance interval of writes — so measuring the
        // settled floor against the trough's momentarily-small row count would
        // demand a file smaller than the workload actually needs.
        emit(
            &format!("s3.cycle{cycle}.peak_live_rows"),
            cycle_samples
                .iter()
                .map(|(_, _, rows)| *rows)
                .max()
                .unwrap_or(0),
        );
        emit(
            &format!("s3.cycle{cycle}.file_bytes"),
            db.disk_file_size().unwrap_or(0),
        );
        emit(
            &format!("s3.cycle{cycle}.retained_rows"),
            row_count(&db, RETAINED_TABLE),
        );
        emit(
            &format!("s3.cycle{cycle}.writes"),
            writes.load(Ordering::SeqCst),
        );
        emit(&format!("s3.cycle{cycle}.watermark"), db.sync_watermark().0);
    }

    stop.store(true, Ordering::SeqCst);
    let _ = writer.join();
    let _ = sampler.join();
    emit(
        "s3.file_sample_interval_secs",
        FILE_SAMPLE_INTERVAL.as_secs(),
    );
    emit("s3.writer_writes_total", writes.load(Ordering::SeqCst));
    emit("s3.writer_max_gap_ms", max_gap_ms.load(Ordering::SeqCst));
    emit("s3.maintenance_running", db.maintenance_status().running);
    let _ = db.close();
    emit("s3.plateau_done", 1);
}

async fn run_s3_reclaim(db_path: &Path, retain_secs: u64, rows: u64, row_bytes: usize) {
    // A deliberately fragmented file: bulk-load a fat table, let the window
    // pass, then drive ONE maintenance cycle and read its report. Driving the
    // cycle here is deliberate — this assertion is about what the REPORT says,
    // not about self-running (S1/S2/S3-plateau prove that). The whole phase
    // finishes inside the engine loop's first 60-second tick from open, and
    // `open_elapsed_secs` is printed so that is checkable, not assumed.
    let opened = Instant::now();
    let db = match Database::open(db_path) {
        Ok(db) => Arc::new(db),
        Err(err) => die("open the reclaim database", err),
    };
    // No SYNC SAFE: this fixture never syncs, so nothing is gated on delivery
    // and the report is about retention and compaction alone.
    if db.table_meta(RETAINED_TABLE).is_none()
        && let Err(err) = db.execute(&retained_ddl(retain_secs, false), &p())
    {
        die("declare the reclaim table", err);
    }

    let metric = padded_metric(row_bytes);
    insert_windows(&db, rows, 1, &metric);
    emit("s3r.rows_written", row_count(&db, RETAINED_TABLE));
    emit(
        "s3r.file_bytes_after_load",
        db.disk_file_size().unwrap_or(0),
    );

    tokio::time::sleep(Duration::from_secs(retain_secs + 3)).await;

    let report = match db.run_maintenance_cycle() {
        Ok(report) => report,
        Err(err) => die("drive a maintenance cycle", err),
    };
    let pruning = report.pruning;
    emit("s3r.open_elapsed_secs", opened.elapsed().as_secs());
    emit("s3r.pruned_rows", pruning.pruned_rows);
    emit("s3r.reclaimed_bytes", pruning.reclaimed_bytes);
    emit("s3r.fragmentation_before", pruning.fragmentation_before);
    emit("s3r.compacted", pruning.compacted);
    emit("s3r.file_shrank", pruning.file_shrank);
    emit(
        "s3r.file_bytes_before",
        pruning.file_bytes_before.unwrap_or(0),
    );
    emit(
        "s3r.file_bytes_after",
        pruning.file_bytes_after.unwrap_or(0),
    );
    emit("s3r.rows_after_prune", row_count(&db, RETAINED_TABLE));
    emit("s3r.future_dated_rows", pruning.future_dated_rows);
    let _ = db.close();
    emit("s3r.done", 1);
}

// ---------------------------------------------------------------------------
// S4 — size honesty
// ---------------------------------------------------------------------------

async fn run_s4_size(db_path: &Path, retain_secs: u64, rows: u64, row_bytes: usize) {
    let db = match Database::open(db_path) {
        Ok(db) => Arc::new(db),
        Err(err) => die("open the size database", err),
    };
    if db.table_meta(RETAINED_TABLE).is_none()
        && let Err(err) = db.execute(&retained_ddl(retain_secs, false), &p())
    {
        die("declare the size table", err);
    }
    if db.table_meta(CONTROL_TABLE).is_none()
        && let Err(err) = db.execute(&control_ddl(), &p())
    {
        die("declare the size control table", err);
    }

    // The field names ARE part of the honesty claim: an estimate named an
    // estimate, with the whole file reported alongside as itself. That claim is
    // guarded by COMPILATION, not at runtime — every read below names
    // `estimated_live_bytes` and `whole_file_bytes` on `TableSizeEstimate`, so
    // renaming either field fails the build. These two lines report which
    // identifiers this binary compiled against; they are evidence for a reader,
    // not a runtime check, and are labelled so they cannot be mistaken for one.
    emit("s4.estimate_field_compiled_against", "estimated_live_bytes");
    emit("s4.whole_file_field_compiled_against", "whole_file_bytes");

    let empty = match db.table_size_estimate(RETAINED_TABLE) {
        Some(estimate) => estimate,
        None => die("size estimate for the retained table", "no answer"),
    };
    emit("s4.empty.estimated_live_bytes", empty.estimated_live_bytes);
    emit("s4.empty.row_count", empty.row_count);
    emit(
        "s4.empty.whole_file_bytes",
        empty.whole_file_bytes.unwrap_or(0),
    );
    emit(
        "s4.empty.whole_file_present",
        u8::from(empty.whole_file_bytes.is_some()),
    );

    let metric = padded_metric(row_bytes);
    insert_windows(&db, rows, 1, &metric);
    insert_notes(&db, 5, 1);
    let filled = match db.table_size_estimate(RETAINED_TABLE) {
        Some(estimate) => estimate,
        None => die("size estimate after inserts", "no answer"),
    };
    emit(
        "s4.filled.estimated_live_bytes",
        filled.estimated_live_bytes,
    );
    emit("s4.filled.row_count", filled.row_count);
    emit(
        "s4.filled.whole_file_bytes",
        filled.whole_file_bytes.unwrap_or(0),
    );
    let control = match db.table_size_estimate(CONTROL_TABLE) {
        Some(estimate) => estimate,
        None => die("size estimate for the control table", "no answer"),
    };
    emit(
        "s4.control.estimated_live_bytes",
        control.estimated_live_bytes,
    );
    emit(
        "s4.control.whole_file_bytes",
        control.whole_file_bytes.unwrap_or(0),
    );
    emit("s4.payload_bytes_written", rows * row_bytes as u64);

    tokio::time::sleep(Duration::from_secs(retain_secs + 3)).await;
    let report = match db.run_maintenance_cycle() {
        Ok(report) => report,
        Err(err) => die("drive a maintenance cycle for the size check", err),
    };
    emit("s4.pruned_rows", report.pruning.pruned_rows);
    let pruned = match db.table_size_estimate(RETAINED_TABLE) {
        Some(estimate) => estimate,
        None => die("size estimate after prune", "no answer"),
    };
    emit(
        "s4.pruned.estimated_live_bytes",
        pruned.estimated_live_bytes,
    );
    emit("s4.pruned.row_count", pruned.row_count);
    emit(
        "s4.pruned.whole_file_bytes",
        pruned.whole_file_bytes.unwrap_or(0),
    );
    emit("s4.disk_file_size", db.disk_file_size().unwrap_or(0));
    match db.table_size_estimates() {
        Ok(all) => emit("s4.table_count", all.len()),
        Err(err) => die("read all table size estimates", err),
    }
    let _ = db.close();
    emit("s4.done", 1);
}

// ---------------------------------------------------------------------------
// S5 — transfer receipts, sync plane
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_s5_sync(
    db_path: &Path,
    identity: &Path,
    ticket: &str,
    tenant: &str,
    retain_secs: u64,
    role: &str,
    rows: u64,
) {
    let db = open_edge(db_path, retain_secs);
    let node_id = identity_node_id(identity);
    let client = client_for(&db, ticket, identity, tenant);

    match role {
        // The seeder puts a known batch of FOREIGN rows on the hub, so the
        // measured edge's pull has an exact, non-zero size.
        "seed" => {
            emit("s5.seed_node_id", &node_id);
            insert_notes(&db, rows, 500);
            match client.push().await {
                Ok(applied) => emit("s5.seed_push_applied_rows", applied.applied_rows),
                Err(err) => die("s5 seed push", err),
            }
            receipt_lines("s5.seed_receipt", &client.transfer_receipts());
            emit("s5.seed_rows", rows);
        }
        "measured" => {
            emit("s5.edge_node_id", &node_id);
            insert_notes(&db, rows, 1);
            // Pull BEFORE push, deliberately: the hub then holds only the
            // seeder's foreign rows, so the Received count is exact without
            // depending on the open self-echo question.
            match client.pull_default().await {
                Ok(result) => emit("s5.pull_applied_rows", result.applied_rows),
                Err(err) => die("s5 measured pull", err),
            }
            match client.push().await {
                Ok(applied) => emit("s5.push_applied_rows", applied.applied_rows),
                Err(err) => die("s5 measured push", err),
            }
            emit("s5.pushed_rows", rows);
            receipt_lines("s5.client_receipt", &client.transfer_receipts());
        }
        other => die("s5 role", format!("unknown role {other}")),
    }
    let _ = db.close();
    emit("s5.sync_done", 1);
}

// ---------------------------------------------------------------------------
// S5 — transfer receipts, blob plane
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn run_blob_holder(
    db_path: &Path,
    identity: &Path,
    ticket_file: &Path,
    status_file: &Path,
    entitle_node: &str,
    blob_bytes: usize,
    job_id: &str,
) {
    let db = match Database::open(db_path) {
        Ok(db) => Arc::new(db),
        Err(err) => die("open the blob holder database", err),
    };
    if let Err(err) = install_work_ledger_schema(&db) {
        die("install the work ledger schema", err);
    }
    let node_id = identity_node_id(identity);
    emit("blob.holder_node_id", &node_id);

    let content: Vec<u8> = (0..blob_bytes).map(|i| (i % 251) as u8).collect();
    let hash = BlobHash::of(&content);
    emit("blob.hash", hash.to_hex());
    emit("blob.bytes", blob_bytes);

    // The holder's OWN ledger is the security boundary, so the entitlement is
    // seeded here through the public work-ledger surface.
    let spec = JobSpec::builder(
        job_id,
        "bounded.tables.smoke",
        "live-smoke",
        node_id.clone(),
    )
    .input_refs(vec![InputRef::blob_ref(hash.clone())])
    .submitted_at_ms(now_ms())
    .build();
    if let Err(err) = submit_job(&db, &spec, &[] as &[&[u8]]) {
        die("submit the blob job", err);
    }
    let now = now_ms();
    match insert_claim(&db, job_id, 1, entitle_node, now + 30 * 60_000, now) {
        Ok(ClaimInsert::Inserted) => {}
        Ok(other) => die("seed the blob claim", format!("{other:?}")),
        Err(err) => die("seed the blob claim", err),
    }

    let holder = BlobStore::new(
        db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        identity.to_path_buf(),
    );
    match holder.ingest_bytes(&content) {
        Ok(ingested) if ingested == hash => {}
        Ok(other) => die("ingest the blob", format!("hash mismatch {other:?}")),
        Err(err) => die("ingest the blob", err),
    }

    // This holder runs ONE endpoint (it serves blobs and dials nothing), so the
    // serving endpoint carries the holder's own fabric identity — which is what
    // makes the fetching end's receipt key the holder's real node id.
    let endpoint = match IrohServer::bind(&peer_bind_spec(identity)).await {
        Ok(endpoint) => endpoint,
        Err(err) => die("bind the blob serving endpoint", err),
    };
    holder.serve_on(&endpoint);
    write_atomically(ticket_file, &endpoint.ticket().to_string());
    emit("blob.holder_ticket_file", ticket_file.display());

    let shutdown = Arc::new(AtomicBool::new(false));
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(sig) => sig,
                    Err(err) => die("install the SIGTERM handler", err),
                };
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = sigterm.recv() => {}
            }
            shutdown.store(true, Ordering::SeqCst);
        });
    }

    emit("blob.holder_serving", 1);
    while !shutdown.load(Ordering::SeqCst) {
        let mut out = String::new();
        out.push_str(&format!("blob.holder_node_id={node_id}\n"));
        for r in holder.transfer_receipts() {
            let plane = match r.plane {
                TransferPlane::Sync => "sync",
                TransferPlane::Blob => "blob",
            };
            let direction = match r.direction {
                TransferDirection::Sent => "sent",
                TransferDirection::Received => "received",
            };
            out.push_str(&format!(
                "blob.holder_receipt.{}.{plane}.{direction}.items={}\n",
                r.peer_node_id, r.counters.items
            ));
            out.push_str(&format!(
                "blob.holder_receipt.{}.{plane}.{direction}.bytes={}\n",
                r.peer_node_id, r.counters.payload_bytes
            ));
        }
        out.push_str("blob.holder_ready=1\n");
        write_atomically(status_file, &out);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    endpoint.close().await;
    let _ = db.close();
    emit("blob.holder_stopped", 1);
}

async fn run_blob_fetch(
    db_path: &Path,
    identity: &Path,
    holder_ticket: &str,
    holder_node: &str,
    hash_hex: &str,
    job_id: &str,
) {
    let db = match Database::open(db_path) {
        Ok(db) => Arc::new(db),
        Err(err) => die("open the blob consumer database", err),
    };
    if let Err(err) = install_work_ledger_schema(&db) {
        die("install the work ledger schema", err);
    }
    let node_id = identity_node_id(identity);
    emit("blob.consumer_node_id", &node_id);
    let hash = match BlobHash::from_hex(hash_hex) {
        Ok(hash) => hash,
        Err(err) => die("parse the blob hash", err),
    };

    // The consumer's own ledger mirror, enough to pass its client-side
    // pre-check; the holder authorises against its own copy independently.
    let spec = JobSpec::builder(job_id, "bounded.tables.smoke", "live-smoke", holder_node)
        .input_refs(vec![InputRef::blob_ref(hash.clone())])
        .submitted_at_ms(now_ms())
        .build();
    if let Err(err) = submit_job(&db, &spec, &[] as &[&[u8]]) {
        die("mirror the blob job", err);
    }
    let now = now_ms();
    match insert_claim(&db, job_id, 1, &node_id, now + 30 * 60_000, now) {
        Ok(ClaimInsert::Inserted) => {}
        Ok(other) => die("mirror the blob claim", format!("{other:?}")),
        Err(err) => die("mirror the blob claim", err),
    }

    let consumer = BlobStore::new(
        db.clone(),
        MovementPolicy {
            auto_propagate: true,
        },
        identity.to_path_buf(),
    );
    let mut sink: Vec<u8> = Vec::new();
    match consumer
        .resolve_blob_ref(&hash, holder_ticket, &mut sink)
        .await
    {
        Ok(bytes) => {
            emit("blob.fetched_bytes", bytes);
            emit("blob.sink_len", sink.len());
            emit("blob.hash_matches", u8::from(BlobHash::of(&sink) == hash));
        }
        Err(err) => {
            emit("blob.fetch_failed", 1);
            eprintln!("[driver] blob fetch failed: {err}");
        }
    }
    receipt_lines("blob.consumer_receipt", &consumer.transfer_receipts());
    let _ = db.close();
    emit("blob.fetch_done", 1);
}

// ---------------------------------------------------------------------------
// S6 — the engine's own durable trigger audit stays bounded
// ---------------------------------------------------------------------------

fn run_s6_audit(db_path: &Path, firings_per_round: u64) {
    let db = match Database::open(db_path) {
        Ok(db) => db,
        Err(err) => die("open the audit database", err),
    };
    if let Err(err) = db.execute("CREATE TABLE fire (id INTEGER PRIMARY KEY)", &p()) {
        die("declare the trigger source table", err);
    }
    if let Err(err) = db.execute("CREATE TRIGGER fire_guard ON fire WHEN INSERT", &p()) {
        die("declare the trigger", err);
    }
    if let Err(err) = db.register_trigger_callback("fire_guard", |_, _| Ok(())) {
        die("register the trigger callback", err);
    }
    if let Err(err) = db.complete_initialization() {
        die("complete initialization", err);
    }

    let ring = db.trigger_audit_ring_capacity();
    let retention = match db.trigger_audit_retention() {
        Some(retention) => retention,
        None => die("read the durable audit retention", "no shipped default"),
    };
    emit("s6.ring_capacity", ring);
    emit("s6.retention_secs", retention.as_secs());

    // DELIBERATE DEVIATION from this smoke's blanket "no clock mocking" rule,
    // explained in place so a reviewer sees why it is justified here.
    //
    // Why the clause exists: `Wallclock`'s override is thread-local, so an
    // engine BACKGROUND thread never sees it — mocking time to prove background
    // retention would be a false green. That risk does not exist here: the two
    // maintenance cycles below are driven SYNCHRONOUSLY on this very thread,
    // which is the one place the override is genuinely honoured, and the rows
    // being aged were stamped on this thread too.
    //
    // Why it is unavoidable: the durable trigger-audit window is the hardcoded
    // `TRIGGER_AUDIT_RETENTION` = 7 days, with a read-only getter and NO setter
    // or operator surface anywhere, so no real-time run can ever reach it.
    //
    // Scope: S6 ONLY, and only for the duration of this function's guard. Every
    // retained-table window elsewhere in this smoke (S1/S2/S3/S4) is real
    // wall-clock seconds against the engine's own unshortened 60s loop and
    // touches no mock whatsoever.
    let window_ms = retention.as_millis() as u64;
    let base = now_ms() as u64;
    let clock = Arc::new(AtomicU64::new(base));
    let _guard = {
        let clock = clock.clone();
        Wallclock::test_clock_guard(move || clock.load(Ordering::SeqCst))
    };
    emit("s6.clock_override_scope", "s6_only_driven_cycle");

    let fire = |db: &Database, start: i64, count: u64| {
        for i in 0..count {
            let mut row = p();
            row.insert("id".to_string(), Value::Int64(start + i as i64));
            if let Err(err) = db.execute("INSERT INTO fire (id) VALUES ($id)", &row) {
                die("fire the trigger", err);
            }
        }
    };

    let history_len = |db: &Database| -> usize {
        match db.trigger_audit_history(Default::default()) {
            Ok(history) => history.len(),
            Err(err) => die("read the durable audit history", err),
        }
    };

    // Round 0: enough firings to exceed the in-memory ring, so the existing
    // durable-exceeds-ring contract is visibly intact before anything prunes.
    let flood = ring as u64 + firings_per_round;
    fire(&db, 0, flood);
    emit("s6.history_after_flood", history_len(&db));
    emit("s6.ring_len_after_flood", db.trigger_audit_log().len());

    // Round 1: age everything past the window, fire a fresh batch, drive a
    // cycle. What survives is exactly the current window's firings.
    clock.fetch_add(window_ms * 2, Ordering::SeqCst);
    fire(&db, 100_000, firings_per_round);
    let first = match db.run_maintenance_cycle() {
        Ok(report) => report,
        Err(err) => die("drive the first audit maintenance cycle", err),
    };
    emit(
        "s6.round1_pruned_audit_rows",
        first.pruned_trigger_audit_rows,
    );
    let after_first = history_len(&db);
    emit("s6.history_after_round1", after_first);

    // Round 2: identical churn. A durable audit that accumulates would grow;
    // a bounded one plateaus.
    clock.fetch_add(window_ms * 2, Ordering::SeqCst);
    fire(&db, 200_000, firings_per_round);
    let second = match db.run_maintenance_cycle() {
        Ok(report) => report,
        Err(err) => die("drive the second audit maintenance cycle", err),
    };
    emit(
        "s6.round2_pruned_audit_rows",
        second.pruned_trigger_audit_rows,
    );
    let after_second = history_len(&db);
    emit("s6.history_after_round2", after_second);
    emit("s6.firings_per_round", firings_per_round);
    emit("s6.ring_len_final", db.trigger_audit_log().len());
    let _ = db.close();
    emit("s6.done", 1);
}
