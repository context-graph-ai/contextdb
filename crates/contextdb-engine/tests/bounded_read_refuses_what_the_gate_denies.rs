//! A read the gate denies is denied through every door.
//!
//! A handle can be narrowed to a set of contexts, or to a principal whose
//! grants decide which rows it may see. When such a handle asks for a row it
//! is not allowed, the store does not quietly answer with nothing -- it says
//! which gate refused and what it was asked for. That distinction is the
//! whole point: "no rows matched" and "you may not look at that row" are
//! different answers, and a caller that cannot tell them apart cannot tell a
//! missing record from a forbidden one. An empty answer to a forbidden read
//! is the worse failure of the two, because it looks like success.
//!
//! So the same denied read is asked through the executor and through a
//! bounded read view on the SAME narrowed handle, and the two refusals are
//! compared. The executor's refusal is the oracle; nothing is restated by
//! hand except the gate each shape is about.

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Error, Principal, Value};
use contextdb_engine::Database;
use std::collections::{BTreeSet, HashMap};
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

/// Ceilings far above anything these lookups need, so a ceiling can never be
/// what refuses instead of the gate.
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

fn id(ordinal: u128) -> Uuid {
    Uuid::from_u128(ordinal)
}

fn contexts(ids: &[Uuid]) -> BTreeSet<contextdb_core::ContextId> {
    ids.iter()
        .copied()
        .map(contextdb_core::ContextId::new)
        .collect()
}

/// Ask one denied read through both doors and record any disagreement.
fn both_doors_refuse(
    narrowed: &Database,
    what: &str,
    sql: &str,
    sql_params: &HashMap<String, Value>,
    gate_named_by: impl Fn(&Error) -> bool,
    disagreements: &mut Vec<String>,
) {
    let eager = narrowed.execute(sql, sql_params);
    let Err(eager_refusal) = eager else {
        panic!("{what}: the fixture is only about a denied read if the executor denies it");
    };
    assert!(
        gate_named_by(&eager_refusal),
        "{what}: the executor names the gate this shape is about: {eager_refusal:?}"
    );

    let bounded = narrowed
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(sql, sql_params);
    println!("OBSERVED {what}: executor {eager_refusal:?} | bounded {bounded:?}");

    match bounded {
        Ok(answered) => disagreements.push(format!(
            "{what}: the executor refuses with {eager_refusal:?} and a bounded read answers with \
             {} rows -- a forbidden read that comes back empty looks exactly like a read that \
             found nothing",
            answered.rows.len()
        )),
        Err(bounded_refusal) => {
            if format!("{bounded_refusal:?}") != format!("{eager_refusal:?}") {
                disagreements.push(format!(
                    "{what}: the executor refuses with {eager_refusal:?} and a bounded read \
                     refuses with {bounded_refusal:?}"
                ));
            }
        }
    }
}

#[test]
fn a_row_outside_the_handles_contexts_is_refused_by_both_doors() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID, data TEXT)",
            &empty(),
        )
        .expect("create the context-scoped table");
    let mine = id(0xA);
    let theirs = id(0xB);
    database
        .execute(
            "INSERT INTO t (id, data) VALUES ($id, 'no-context')",
            &params(vec![("id", Value::Uuid(id(1)))]),
        )
        .expect("insert a row that carries no context");
    database
        .execute(
            "INSERT INTO t (id, context_id, data) VALUES ($id, $ctx, 'another-context')",
            &params(vec![
                ("id", Value::Uuid(id(2))),
                ("ctx", Value::Uuid(theirs)),
            ]),
        )
        .expect("insert a row belonging to another context");

    let narrowed = database.scoped_with_contexts(contexts(&[mine]));
    let mut disagreements = Vec::new();
    for (what, row) in [
        ("a row that carries no context at all", id(1)),
        ("a row belonging to another context", id(2)),
    ] {
        both_doors_refuse(
            &narrowed,
            what,
            "SELECT data FROM t WHERE id = $id",
            &params(vec![("id", Value::Uuid(row))]),
            |error| matches!(error, Error::ContextScopeViolation { .. }),
            &mut disagreements,
        );
    }

    assert!(
        disagreements.is_empty(),
        "a handle narrowed to one context is told which context it asked for and which it holds, \
         whichever door it asks through:\n{}",
        disagreements.join("\n")
    );
}

#[test]
fn a_row_the_principals_grants_hide_is_refused_by_both_doors() {
    let database = Database::open_memory();
    database
        .execute(
            "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
             principal_id TEXT, acl_id UUID)",
            &empty(),
        )
        .expect("create the grant table");
    database
        .execute(
            "CREATE TABLE t (id UUID PRIMARY KEY, acl_id UUID ACL REFERENCES acl_grants(acl_id), \
             uniq_col TEXT UNIQUE, data TEXT)",
            &empty(),
        )
        .expect("create the guarded table");

    let principal = "agent-a";
    let granted = id(0xA1);
    database
        .execute(
            "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
             VALUES ($id, 'Agent', $principal, $acl)",
            &params(vec![
                ("id", Value::Uuid(id(0xAA))),
                ("principal", Value::Text(principal.to_owned())),
                ("acl", Value::Uuid(granted)),
            ]),
        )
        .expect("grant the principal one access list");
    for (row, acl, key) in [
        (id(1), granted, "mine"),
        (id(2), id(0xB1), "hidden-one"),
        (id(3), id(0xC1), "hidden-two"),
    ] {
        database
            .execute(
                "INSERT INTO t (id, acl_id, uniq_col, data) VALUES ($id, $acl, $key, $key)",
                &params(vec![
                    ("id", Value::Uuid(row)),
                    ("acl", Value::Uuid(acl)),
                    ("key", Value::Text(key.to_owned())),
                ]),
            )
            .expect("insert a guarded row");
    }

    let narrowed =
        database.scoped_with_constraints(None, None, Some(Principal::Agent(principal.to_owned())));
    let mut disagreements = Vec::new();
    for key in ["hidden-one", "hidden-two"] {
        both_doors_refuse(
            &narrowed,
            &format!("a unique key naming a row the principal may not see ({key})"),
            "SELECT data FROM t WHERE uniq_col = $key",
            &params(vec![("key", Value::Text(key.to_owned()))]),
            |error| matches!(error, Error::AclDenied { .. }),
            &mut disagreements,
        );
    }

    // The complement: the row the principal IS granted answers through both
    // doors, so the refusal above is about the grant and not about the shape
    // of the statement.
    let allowed_sql = "SELECT data FROM t WHERE uniq_col = $key";
    let allowed = params(vec![("key", Value::Text("mine".to_owned()))]);
    let eager = narrowed
        .execute(allowed_sql, &allowed)
        .expect("the granted row answers through the executor");
    let bounded = narrowed
        .read_session(roomy())
        .expect("open a bounded read view")
        .execute(allowed_sql, &allowed)
        .expect("the granted row answers through a bounded read");
    assert_eq!(
        format!("{:?}", bounded.rows),
        format!("{:?}", eager.rows),
        "the row the principal is granted reads the same through both doors"
    );

    assert!(
        disagreements.is_empty(),
        "a principal that may not see a row is told so, whichever door it asks through -- an \
         empty answer would be indistinguishable from a key nobody wrote:\n{}",
        disagreements.join("\n")
    );
}
