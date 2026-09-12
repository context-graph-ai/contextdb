//! A bounded read resolves an indexed key to the row version it can see.
//!
//! A directory row that is rotated -- a node's ticket replaced, then replaced
//! again -- leaves several committed versions behind it, and an index over
//! the key has to land on the one the reader's snapshot can see. Landing on
//! another version is not a slow answer, it is a wrong one: either the reader
//! is handed a version it may not see, or the lookup fails outright and a
//! node that IS enrolled reads as unreachable.
//!
//! The ORACLE is the executor's own answer to the same statement, never a
//! ticket restated by hand. A cursor is held to a stricter promise still: it
//! outlives the call that opened it, so a rotation by another writer while it
//! is open must not change what it is still handing out.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, QueryResult};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use uuid::Uuid;

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

/// Ceilings far above anything this fixture needs, so a ceiling can never be
/// what makes the two doors disagree.
fn roomy() -> ReadLimits {
    ReadLimits {
        result_rows: 1_000_000,
        result_bytes: 512 * 1024 * 1024,
        work: 1_000_000_000,
        active_ms: 600_000,
        memory: 512 * 1024 * 1024,
        cursor_page_rows: 100_000,
        cursor_page_bytes: 64 * 1024 * 1024,
        cursor_idle_ms: 600_000,
        cursor_lifetime_ms: 1_800_000,
    }
}

/// The same ceilings, but handing out one row per page, so a cursor really
/// spans the moment another writer rotates a row it has not reached yet.
fn one_row_pages() -> ReadLimits {
    ReadLimits {
        cursor_page_rows: 1,
        ..roomy()
    }
}

const ROTATIONS: usize = 5;

fn peer(ordinal: u128) -> Uuid {
    Uuid::from_u128(0x00F0_0000_0000_0000_0000_0000_0000_0000 + ordinal)
}

fn ticket(node: &str, version: usize) -> String {
    format!("ticket-{node}-v{version}")
}

/// A directory keyed by a text node name and one keyed by an identifier, so
/// both shapes of key index are covered, plus an unrelated table whose writes
/// move the store's watermark past every version this fixture wrote.
fn seeded() -> Database {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE directory (node_id TEXT PRIMARY KEY, tenant TEXT, ticket TEXT)",
            &empty(),
        )
        .expect("create the named directory");
    database
        .execute(
            "CREATE INDEX directory_tenant ON directory (tenant)",
            &empty(),
        )
        .expect("index the directory by tenant");
    database
        .execute(
            "CREATE TABLE peers (id UUID PRIMARY KEY, ticket TEXT)",
            &empty(),
        )
        .expect("create the identified directory");
    database
        .execute("CREATE TABLE traffic (id UUID PRIMARY KEY)", &empty())
        .expect("create the unrelated table");

    for ordinal in 0..3_u128 {
        let name = format!("node-{ordinal}");
        database
            .execute(
                "INSERT INTO directory (node_id, tenant, ticket) VALUES ($id, 'shared', $ticket)",
                &params(vec![
                    ("id", Value::Text(name.clone())),
                    ("ticket", Value::Text(ticket(&name, 1))),
                ]),
            )
            .expect("enrol a named node");
        database
            .execute(
                "INSERT INTO peers (id, ticket) VALUES ($id, $ticket)",
                &params(vec![
                    ("id", Value::Uuid(peer(ordinal))),
                    ("ticket", Value::Text(ticket(&name, 1))),
                ]),
            )
            .expect("enrol an identified node");
    }
    database
}

/// Rotate every enrolled ticket several times, with unrelated writes in
/// between so the store's watermark runs well past the version each rotation
/// left behind.
fn rotate_every_ticket(database: &Database) {
    for version in 2..=ROTATIONS + 1 {
        for ordinal in 0..3_u128 {
            let name = format!("node-{ordinal}");
            database
                .execute(
                    "UPDATE directory SET ticket = $ticket WHERE node_id = $id",
                    &params(vec![
                        ("id", Value::Text(name.clone())),
                        ("ticket", Value::Text(ticket(&name, version))),
                    ]),
                )
                .expect("rotate a named node's ticket");
            database
                .execute(
                    "UPDATE peers SET ticket = $ticket WHERE id = $id",
                    &params(vec![
                        ("id", Value::Uuid(peer(ordinal))),
                        ("ticket", Value::Text(ticket(&name, version))),
                    ]),
                )
                .expect("rotate an identified node's ticket");
        }
        advance_the_watermark(database);
    }
}

/// Committed writes that touch nothing this fixture reads, so the only thing
/// they change for a later lookup is how far the store's watermark has moved
/// past the versions it will resolve.
fn advance_the_watermark(database: &Database) {
    for _ in 0..4 {
        database
            .execute(
                "INSERT INTO traffic (id) VALUES ($id)",
                &params(vec![("id", Value::Uuid(Uuid::new_v4()))]),
            )
            .expect("write an unrelated row");
    }
}

fn answer(result: &QueryResult) -> String {
    let mut rows: Vec<String> = result.rows.iter().map(|row| format!("{row:?}")).collect();
    rows.sort();
    format!("{rows:?}")
}

fn both_doors_agree(
    database: &Database,
    what: &str,
    sql: &str,
    sql_params: &HashMap<String, Value>,
    disagreements: &mut Vec<String>,
) {
    let eager = database
        .execute(sql, sql_params)
        .unwrap_or_else(|error| panic!("the executor answers {what}: {error}"));
    let bounded = match database
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, sql_params)
    {
        Ok(bounded) => bounded,
        Err(error) => {
            disagreements.push(format!(
                "{what}: the executor answers {} and a bounded read refuses the same statement: \
                 {error}",
                answer(&eager)
            ));
            return;
        }
    };
    println!(
        "OBSERVED {what}: executor {} | bounded {}",
        answer(&eager),
        answer(&bounded)
    );
    if answer(&bounded) != answer(&eager) {
        disagreements.push(format!(
            "{what}: the executor answers {} and a bounded read answers {}",
            answer(&eager),
            answer(&bounded)
        ));
    }
}

#[test]
fn a_bounded_lookup_of_a_rotated_row_resolves_the_version_the_executor_resolves() {
    let database = seeded();
    rotate_every_ticket(&database);
    let mut disagreements = Vec::new();

    both_doors_agree(
        &database,
        "a named node looked up by its key",
        "SELECT ticket FROM directory WHERE node_id = $id",
        &params(vec![("id", Value::Text("node-1".to_owned()))]),
        &mut disagreements,
    );
    both_doors_agree(
        &database,
        "an identified node looked up by its key",
        "SELECT ticket FROM peers WHERE id = $id",
        &params(vec![("id", Value::Uuid(peer(1)))]),
        &mut disagreements,
    );
    both_doors_agree(
        &database,
        "every node of a tenant looked up by an indexed column",
        "SELECT node_id, ticket FROM directory WHERE tenant = $tenant",
        &params(vec![("tenant", Value::Text("shared".to_owned()))]),
        &mut disagreements,
    );
    both_doors_agree(
        &database,
        "a key nobody enrolled",
        "SELECT ticket FROM directory WHERE node_id = $id",
        &params(vec![("id", Value::Text("node-stranger".to_owned()))]),
        &mut disagreements,
    );

    assert!(
        disagreements.is_empty(),
        "a rotated row has several committed versions and an indexed lookup has to land on the \
         one the reader can see -- landing elsewhere hands out a version the reader may not have \
         or reports an enrolled node as missing:\n{}",
        disagreements.join("\n")
    );
}

#[test]
fn an_open_cursor_keeps_handing_out_the_version_it_opened_on() {
    let database = seeded();
    rotate_every_ticket(&database);

    let sql = "SELECT node_id, ticket FROM directory WHERE tenant = $tenant";
    let tenant = params(vec![("tenant", Value::Text("shared".to_owned()))]);
    let at_open = answer(
        &database
            .execute(sql, &tenant)
            .expect("the executor answers before the cursor is opened"),
    );

    let session = database
        .read_session(one_row_pages())
        .expect("open a bounded read view");
    let mut cursor = session.open_cursor(sql, &tenant).expect("open a cursor");
    // Opening the cursor already produced its first page; the rows it holds
    // are part of what the cursor hands out, not something a later fetch
    // repeats.
    let mut handed_out: Vec<String> = cursor
        .first_page()
        .rows
        .iter()
        .map(|row| format!("{row:?}"))
        .collect();
    let mut has_more = cursor.first_page().has_more;

    // Another writer rotates every ticket again, including rows this cursor
    // has not reached. The cursor was opened before any of it.
    rotate_every_ticket(&database);
    advance_the_watermark(&database);

    while has_more {
        let page = cursor
            .fetch(NonZeroUsize::new(1))
            .expect("the cursor hands out its remaining rows after the rotation");
        handed_out.extend(page.rows.iter().map(|row| format!("{row:?}")));
        has_more = page.has_more;
    }
    cursor.close().expect("close the cursor");
    handed_out.sort();

    println!(
        "OBSERVED cursor: opened on {at_open} | handed out {:?}",
        handed_out
    );
    assert_eq!(
        format!("{handed_out:?}"),
        at_open,
        "a cursor outlives the call that opened it, so a rotation by another writer while it is \
         open cannot change what it is still handing out: it opened on {at_open} and handed out \
         {handed_out:?}"
    );
}
