//! A store that is perfectly sound must never be called corrupt because the
//! reader's declared memory ceiling was too small for the proof walk the read
//! needs. An operator who sees `StoreCorrupted` goes looking for damage and
//! plans a restore; what they actually have is a number to raise. The two
//! answers send them to opposite places, so the public read route has to keep
//! them apart: a refused admission is the memory-limit refusal, and only bytes
//! that genuinely fail to authenticate are corruption.
use contextdb_core::Error;
use contextdb_core::Value;
use contextdb_core::read_contract::{DeadlineClock, ReadLimits, ReadRoute};
use contextdb_engine::local_transport::ManualDeadlineClock;
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use redb::{ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;
use std::sync::Arc;

/// Enough committed rows that answering `SELECT id` walks the committed proof
/// far past what a single admission holds.
const ROWS: i64 = 8_000;
/// Leave one admission batch above the measured cold-open charge. The 8,000-row
/// scan needs more than this margin, so the refusal belongs to the query.
const QUERY_MARGIN_BYTES: usize = 64 * 1024;
/// The compact open on this fixture charges 656,577 B (formerly 1,711,157 B).
const MAX_COLD_OPEN_BYTES: u64 = 700_000;
/// A ceiling the same query finishes comfortably inside, so the second half of
/// this contract is about the bytes on disk and nothing else.
const ROOMY_SESSION_BYTES: usize = 24 * 1024 * 1024;
/// One read, asked three times: the only thing that changes between the three
/// is the declared ceiling and whether the bytes on disk were altered.
const SCAN: &str = "SELECT id, body FROM wide";

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn seed(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE wide (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..ROWS).step_by(500) {
        let tx = db.begin().unwrap();
        for row in start..start + 500 {
            db.execute_in_tx(
                tx,
                "INSERT INTO wide VALUES ($id, $body)",
                &params([
                    ("id", Value::Int64(row)),
                    ("body", Value::Text(format!("{row:08}").repeat(8))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    db.close().unwrap();
}

fn session(path: &std::path::Path, memory: usize) -> ReadSession {
    let options = ReadSessionOptions {
        memory_limit: Some(memory),
        limits: ReadLimits {
            result_rows: 100_000,
            result_bytes: 64 * 1024 * 1024,
            work: 10_000_000,
            active_ms: 60_000,
            memory: memory as u64,
            cursor_page_rows: 64,
            cursor_page_bytes: 1024 * 1024,
            cursor_idle_ms: 10_000,
            cursor_lifetime_ms: 20_000,
        },
        ..Default::default()
    };
    let clock: Arc<dyn DeadlineClock> = Arc::new(ManualDeadlineClock::default());
    let session = ReadSession::open_with_observations_for_test(path, options, clock, None, None)
        .expect("the committed file opens under the declared ceiling");
    assert_eq!(session.route(), ReadRoute::File);
    session
}

/// Rewrite one committed row body in place so the stored bytes no longer match
/// the identity the committed proof holds for them: a genuinely damaged page,
/// produced without touching the proof.
fn damage_one_committed_row_body(path: &std::path::Path) {
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rel_wide");
    let raw = redb::Database::open(path).unwrap();
    let (key, mut body) = {
        let read = raw.begin_read().unwrap();
        let table = read.open_table(definition).unwrap();
        let entry = table
            .iter()
            .unwrap()
            .next()
            .expect("the seeded table holds committed rows")
            .unwrap();
        (entry.0.value().to_vec(), entry.1.value().to_vec())
    };
    // Change one stored byte to a different, equally well-formed one: the
    // record still decodes, so what fails is authentication against the
    // committed root and nothing else.
    let middle = body
        .iter()
        .position(|byte| byte.is_ascii_digit())
        .expect("the seeded body is stored as digits");
    body[middle] = if body[middle] == b'9' {
        b'0'
    } else {
        body[middle] + 1
    };
    let write = raw.begin_write().unwrap();
    {
        let mut table = write.open_table(definition).unwrap();
        table.insert(key.as_slice(), body.as_slice()).unwrap();
    }
    write.commit().unwrap();
}

#[test]
fn a_tight_budget_refuses_by_memory_while_damaged_bytes_still_report_corruption() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("budget-versus-damage.db");
    seed(&path);

    // Healthy store, roomy ceiling: the read is simply answered.
    assert_eq!(
        session(&path, ROOMY_SESSION_BYTES)
            .execute(SCAN, &HashMap::new())
            .expect("a sound store answers under a ceiling that fits the proof walk")
            .rows()
            .len(),
        ROWS as usize
    );

    // A constrained cold open succeeds and stays compact. Derive the tighter
    // query ceiling from its charge before asking for any rows.
    let compact = session(&path, MAX_COLD_OPEN_BYTES as usize);
    let open_charge = compact.charged_bytes().expect("cold-open charge");
    assert!(
        open_charge <= MAX_COLD_OPEN_BYTES,
        "compact cold open must stay within 700,000 B; charged {open_charge} B"
    );
    drop(compact);

    // Same healthy store, ceiling too small for the proof walk.
    let tight_budget = usize::try_from(open_charge).unwrap() + QUERY_MARGIN_BYTES;
    let tight = session(&path, tight_budget);
    let refusal = tight
        .execute(SCAN, &HashMap::new())
        .map(|answer| format!("{} rows", answer.rows().len()))
        .expect_err("the declared ceiling cannot hold the proof this read needs");
    assert!(
        matches!(&refusal, Error::MemoryBudgetExceeded { .. }),
        "a sound store refused for want of memory names the memory limit, not damage: {refusal:?}"
    );

    // Damaged bytes, roomy ceiling: corruption keeps its own answer.
    damage_one_committed_row_body(&path);
    let damaged = session(&path, ROOMY_SESSION_BYTES)
        .execute(SCAN, &HashMap::new())
        .map(|answer| format!("{} rows", answer.rows().len()))
        .expect_err("a body that does not authenticate is never answered");
    assert!(
        matches!(damaged, Error::StoreCorrupted { .. }),
        "bytes that fail to authenticate stay corruption: {damaged:?}"
    );
}
