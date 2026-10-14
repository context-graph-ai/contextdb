//! The relations a row-head reconciliation pass holds open are admitted to the
//! writer budget before they are allocated and returned when they close.

use super::*;
use crate::memory_accounting::MemoryAccountant;
use std::sync::Arc;

const TABLES: usize = 24;

fn relation_name(table: &str) -> String {
    RedbPersistence::rel_table_name(table)
}

/// Long table names make each held relation's native retention larger than
/// any growth of the slot vector, so every new relation raises the peak.
fn tables() -> Vec<String> {
    (0..TABLES)
        .map(|n| format!("t{n:02}_{}", "x".repeat(400)))
        .collect()
}

fn store(tables: &[String]) -> (tempfile::TempDir, redb::Database) {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = redb::Database::create(root.path().join("held.redb")).expect("create the store");
    let write = db.begin_write().expect("begin the fixture write");
    for table in tables {
        let name = relation_name(table);
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(&name))
            .expect("create one relation")
            .insert(b"row".as_slice(), b"body".as_slice())
            .expect("store one body");
    }
    write.commit().expect("commit the fixture");
    (root, db)
}

/// The accountant's high water after holding each table in turn, and the
/// charge then held, with no limit.
fn charges(tables: &[String]) -> Vec<(usize, usize)> {
    let (_root, db) = store(tables);
    let owner = Arc::new(MemoryAccountant::no_limit());
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    let write = db.begin_write().expect("begin");
    let mut held = HeldRelations::new();
    let mut seen = Vec::new();
    for table in tables {
        held.get(&write, table, || relation_name(table), &mut admission)
            .expect("an unlimited budget admits every relation");
        seen.push((owner.high_water_for_test(), held.charged()));
    }
    held.close(&mut admission);
    seen
}

#[test]
fn holding_every_relation_a_commit_writes_is_admitted_and_returned_on_close() {
    let tables = tables();
    let (_root, db) = store(&tables);
    let owner = Arc::new(MemoryAccountant::no_limit());
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    assert_eq!(owner.usage().used, 0);
    let write = db.begin_write().expect("begin");
    let mut held = HeldRelations::new();
    // Rows of every table, interleaved twice over: each relation opens once.
    for round in 0..2 {
        for table in tables.iter().rev().chain(&tables) {
            let relation = held
                .get(&write, table, || relation_name(table), &mut admission)
                .expect("an unlimited budget admits every relation");
            assert!(
                relation.get(b"row".as_slice()).expect("read").is_some(),
                "round {round}: the held relation reads its own body"
            );
        }
    }
    let native: usize = tables
        .iter()
        .map(|table| held_relation_native_bytes(&relation_name(table)))
        .sum();
    assert!(
        held.charged() >= native,
        "every relation's native retention is admitted: {} < {native}",
        held.charged()
    );
    assert_eq!(
        owner.usage().used,
        held.charged(),
        "the accountant holds exactly what the held relations were admitted"
    );
    held.close(&mut admission);
    assert_eq!(owner.usage().used, 0, "closing returns every admitted byte");
    // Each relation was closed: it opens again.
    for table in &tables {
        drop(
            write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(&relation_name(table)))
                .expect("a closed relation opens again"),
        );
    }
}

#[test]
fn a_relation_that_does_not_fit_the_writer_budget_is_refused_before_it_is_opened() {
    let tables = tables();
    let seen = charges(&tables);
    // The seventh relation raises the peak past every earlier one; a budget
    // one byte under it admits the first six only.
    let refused = 6;
    assert!(
        seen[refused].0 > seen[refused - 1].0,
        "the fixture's seventh relation raises the high water: {seen:?}"
    );
    let (_root, db) = store(&tables);
    let owner = Arc::new(MemoryAccountant::with_budget(seen[refused].0 - 1));
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    let write = db.begin_write().expect("begin");
    let mut held = HeldRelations::new();
    for table in &tables[..refused] {
        held.get(&write, table, || relation_name(table), &mut admission)
            .expect("the first relations fit");
    }
    let refusal = held
        .get(
            &write,
            &tables[refused],
            || relation_name(&tables[refused]),
            &mut admission,
        )
        .err()
        .expect("the relation past the budget is refused");
    match &refusal {
        Error::MemoryBudgetExceeded { operation, .. } => {
            assert_eq!(operation, "hold_row_head_relations", "{refusal:?}")
        }
        other => panic!("a typed budget refusal, not {other:?}"),
    }
    assert!(
        owner.usage().used < seen[refused].0,
        "nothing past the budget was charged"
    );
    // The refused relation was never opened, so nothing holds it.
    drop(
        write
            .open_table(TableDefinition::<&[u8], &[u8]>::new(&relation_name(
                &tables[refused],
            )))
            .expect("the refused relation was not left open"),
    );
    held.close(&mut admission);
    assert_eq!(owner.usage().used, 0, "a refused pass returns every admitted byte");
}

#[test]
fn a_pass_that_unwinds_without_closing_returns_its_charge_with_its_admission() {
    let tables = tables();
    let (_root, db) = store(&tables);
    let owner = Arc::new(MemoryAccountant::no_limit());
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    let write = db.begin_write().expect("begin");
    let mut held = HeldRelations::new();
    for table in &tables {
        held.get(&write, table, || relation_name(table), &mut admission)
            .expect("an unlimited budget admits every relation");
    }
    assert!(owner.usage().used > 0);
    drop(held);
    drop(admission);
    assert_eq!(
        owner.usage().used,
        0,
        "an aborted commit's admission returns what the dropped relations held"
    );
    drop(write);
}
