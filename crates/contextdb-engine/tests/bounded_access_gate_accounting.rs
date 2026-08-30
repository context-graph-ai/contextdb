#![cfg(feature = "test-seams")]
//! What an access gate does is charged to the read that asked for it.
//!
//! A gated read pays its declared work ceiling for the source rows it
//! inspects, and the grant rows an access decision reads are source rows. Two
//! failures break that in opposite directions. A gate that rebuilds the
//! principal's grant set for every candidate row charges the operator for the
//! whole grant table once per row, so a read that is served over a small grant
//! table is refused over a large one holding the same decisions. A gate that
//! reads the grant table without charging anything spends unbounded work and
//! retains an unbounded set outside the ceiling the operator declared, so the
//! ceiling stops describing what the read may do.
//!
//! Every read below is issued through the production bounded-kernel entrance.

use contextdb_core::read_contract::{DeadlineClock, DeadlineWait, ReadLimits};
use contextdb_core::{Principal, Value};
use contextdb_engine::Database;
use contextdb_engine::executor::bounded_read_test_support as bounded;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// Gated rows a candidate read inspects.
const GATED_ROWS: u64 = 40;
/// Grants the principal holds. Only the first one is carried by any row.
const GRANTS: u64 = 400;
/// The grant count of the comparison fixture in the traversal proof.
const FEW_GRANTS: u64 = 4;
/// Gated node pairs the traversal fixture links.
const TRAVERSAL_EDGES: u64 = 6;

#[derive(Clone, Copy)]
struct FrozenClock;

impl DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }

    fn wait_until(&self, _deadline_ms: u64) -> DeadlineWait<'_> {
        // These reads are synchronous; the immediately-completing future
        // satisfies the shared transport-facing clock trait.
        Box::pin(async {})
    }
}

fn roomy_limits() -> ReadLimits {
    ReadLimits {
        result_rows: 256,
        result_bytes: 16 * 1024 * 1024,
        work: 10_000_000,
        active_ms: 1_000_000,
        memory: 64 * 1024 * 1024,
        cursor_page_rows: 64,
        cursor_page_bytes: 4 * 1024 * 1024,
        cursor_idle_ms: 10_000,
        cursor_lifetime_ms: 100_000,
    }
}

fn params(pairs: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

const PRINCIPAL: &str = "reader-principal";

fn create_grant_table(db: &Database) {
    db.execute(
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        &HashMap::new(),
    )
    .expect("create the grant table");
}

/// Grant `count` distinct entitlements to the reading principal and answer with
/// the entitlement the gated rows carry. The carried entitlement is the last
/// one stored, so a decision that walks the grant table walks all of it — an
/// operator's grants arrive in no particular order, and a gate whose cost
/// depends on where in the table the answer happens to sit is the same defect
/// either way.
fn grant_entitlements(db: &Database, count: u64, salt: u128) -> Uuid {
    let mut carried = None;
    for ordinal in 0..count {
        let acl = Uuid::from_u128(salt + 0x1_0000u128 + ordinal as u128);
        carried = Some(acl);
        db.execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
             VALUES ($id, 'Agent', $principal, $acl)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(salt + 0x2_0000u128 + ordinal as u128)),
                ),
                ("principal", Value::Text(PRINCIPAL.to_owned())),
                ("acl", Value::Uuid(acl)),
            ]),
        )
        .expect("store a grant for the reading principal");
    }
    carried.expect("at least one grant is stored")
}

fn access_rows_charged(telemetry: &bounded::TestTelemetry) -> u64 {
    telemetry
        .source_work
        .get(&bounded::TestWorkSource::AccessControl)
        .copied()
        .unwrap_or_default()
}

/// A gated relational table whose rows all carry one entitlement, read by a
/// principal holding many.
fn gated_rows_fixture() -> Database {
    let db = Database::open_memory();
    create_grant_table(&db);
    let carried = grant_entitlements(&db, GRANTS, 0xACC0_0000_0000_0000_0000_0000_0000_0000);
    db.execute(
        "CREATE TABLE gated_rows (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), payload TEXT)",
        &HashMap::new(),
    )
    .expect("create the gated table");
    for ordinal in 0..GATED_ROWS {
        db.execute(
            "INSERT INTO gated_rows (id, acl_id, payload) VALUES ($id, $acl, $payload)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(0xACC1_0000u128 + ordinal as u128)),
                ),
                ("acl", Value::Uuid(carried)),
                ("payload", Value::Text(format!("row {ordinal}"))),
            ]),
        )
        .expect("store a gated row");
    }
    db
}

/// The grant set the principal holds is one set. Deciding forty rows against
/// it is forty decisions, not forty rebuilds of the set.
#[test]
fn a_gated_relational_read_charges_the_grant_table_once_not_once_per_row() {
    let db = gated_rows_fixture();
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(PRINCIPAL.to_owned())));
    let outcome = bounded::execute(
        &scoped,
        &bounded::BoundedReadRequest::new(
            "SELECT id FROM gated_rows",
            HashMap::new(),
            roomy_limits(),
            Arc::new(FrozenClock),
        ),
    )
    .expect("a gated relational read must be served");

    assert_eq!(
        outcome.result.rows.len(),
        GATED_ROWS as usize,
        "every stored row carries an entitlement the principal holds"
    );
    let charged = access_rows_charged(&outcome.telemetry);
    let ceiling = GATED_ROWS + GRANTS + GATED_ROWS;
    assert!(
        charged <= ceiling,
        "deciding {GATED_ROWS} rows against {GRANTS} grants must cost the rows plus the \
         grant table once; the read charged {charged} access inspections, which is the \
         grant table re-read for every candidate row"
    );
}

/// The same read under a ceiling sized for its own rows and its own grant set.
#[test]
fn a_gated_relational_read_is_served_under_a_ceiling_sized_for_its_rows_and_grants() {
    let db = gated_rows_fixture();
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(PRINCIPAL.to_owned())));
    let mut limits = roomy_limits();
    limits.work = (GATED_ROWS + GRANTS) * 8;
    let outcome = bounded::execute(
        &scoped,
        &bounded::BoundedReadRequest::new(
            "SELECT id FROM gated_rows",
            HashMap::new(),
            limits,
            Arc::new(FrozenClock),
        ),
    );

    let served = match outcome {
        Ok(served) => served,
        Err(error) => panic!(
            "a gated read of {GATED_ROWS} rows against {GRANTS} grants must be served under a \
             work ceiling of {} units; it was answered with {error:?}",
            limits.work
        ),
    };
    assert_eq!(served.result.rows.len(), GATED_ROWS as usize);
}

/// A gated graph fixture: every node and edge carries the entitlement, and the
/// principal holds `grants` of them.
fn gated_traversal_fixture(grants: u64, salt: u128) -> (Database, Uuid) {
    let db = Database::open_memory();
    create_grant_table(&db);
    let carried = grant_entitlements(&db, grants, salt);
    db.execute(
        "CREATE TABLE nodes (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        &HashMap::new(),
    )
    .expect("create the gated node table");
    db.execute(
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id))",
        &HashMap::new(),
    )
    .expect("create the gated edge table");
    let start = Uuid::from_u128(salt + 0x9_0000u128);
    db.execute(
        "INSERT INTO nodes (id, acl_id) VALUES ($id, $acl)",
        &params([("id", Value::Uuid(start)), ("acl", Value::Uuid(carried))]),
    )
    .expect("store the traversal start node");
    for ordinal in 0..TRAVERSAL_EDGES {
        let target = Uuid::from_u128(salt + 0xA_0000u128 + ordinal as u128);
        db.execute(
            "INSERT INTO nodes (id, acl_id) VALUES ($id, $acl)",
            &params([("id", Value::Uuid(target)), ("acl", Value::Uuid(carried))]),
        )
        .expect("store a gated neighbour node");
        db.execute(
            "INSERT INTO edges (id, source_id, target_id, edge_type, acl_id) \
             VALUES ($id, $source, $target, 'LINKS', $acl)",
            &params([
                (
                    "id",
                    Value::Uuid(Uuid::from_u128(salt + 0xB_0000u128 + ordinal as u128)),
                ),
                ("source", Value::Uuid(start)),
                ("target", Value::Uuid(target)),
                ("acl", Value::Uuid(carried)),
            ]),
        )
        .expect("store a gated edge");
    }
    (db, start)
}

const TRAVERSAL_SQL: &str = "SELECT target FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) \
                             WHERE a.id = $start COLUMNS (b.id AS target))";

fn traversal_access_work(grants: u64, salt: u128) -> u64 {
    let (db, start) = gated_traversal_fixture(grants, salt);
    let scoped =
        db.scoped_with_constraints(None, None, Some(Principal::Agent(PRINCIPAL.to_owned())));
    let outcome = bounded::execute(
        &scoped,
        &bounded::BoundedReadRequest::new(
            TRAVERSAL_SQL,
            params([("start", Value::Uuid(start))]),
            roomy_limits(),
            Arc::new(FrozenClock),
        ),
    )
    .expect("a gated traversal must be served");
    assert_eq!(
        outcome.result.rows.len(),
        TRAVERSAL_EDGES as usize,
        "every neighbour carries an entitlement the principal holds"
    );
    access_rows_charged(&outcome.telemetry)
}

/// Deciding a traversal against a large grant table reads that grant table.
/// The declared ceiling has to see it, or the ceiling does not bound the read.
#[test]
fn a_gated_traversal_charges_the_grant_rows_its_decisions_read() {
    let few = traversal_access_work(FEW_GRANTS, 0xACD0_0000_0000_0000_0000_0000_0000_0000);
    let many = traversal_access_work(GRANTS, 0xACE0_0000_0000_0000_0000_0000_0000_0000);
    let growth = many.saturating_sub(few);

    assert!(
        growth >= GRANTS - FEW_GRANTS,
        "a traversal deciding against {GRANTS} grants reads {GRANTS} grant rows where a \
         traversal deciding against {FEW_GRANTS} reads {FEW_GRANTS}; the charged access work \
         moved from {few} to {many}, so the grant rows the gate reads are spent outside the \
         ceiling the operator declared"
    );
}
