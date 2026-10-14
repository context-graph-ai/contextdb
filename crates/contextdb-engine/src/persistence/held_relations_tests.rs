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

/// Usage the holder's installed and provisional charges account for.
fn charged(ledger: &HoldLedger) -> usize {
    ledger.temp_name
        + ledger.names_pending
        + ledger.names
        + ledger.registry_pending
        + ledger.registry
        + ledger.scratch_pending
        + ledger.scratch
        + ledger.slots_pending
        + ledger.slots
        + ledger.nodes_pending
        + ledger.nodes
}

/// Opens `baseline` other tables first, so the registry the pass grows was
/// not empty when it started.
fn hold_all(
    db: &redb::Database,
    tables: &[String],
    baseline: usize,
    owner: &Arc<MemoryAccountant>,
) -> (usize, HoldLedger, usize, usize) {
    let write = db.begin_write().expect("begin");
    let mut others = Vec::new();
    for n in 0..baseline {
        others.push(
            write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(&format!("other_{n}")))
                .expect("open a baseline table"),
        );
    }
    let baseline_bytes = write.open_table_registry_bytes();
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    let (peak_ledger, held_usage) = {
        let mut held = HeldRelations::new(&write, &mut admission).expect("a holder");
        for round in 0..2 {
            for table in tables.iter().rev().chain(tables) {
                let relation = held
                    .get(table)
                    .expect("an unlimited budget admits every relation")
                    .expect("the relation exists");
                assert!(
                    relation.get(b"row".as_slice()).expect("read").is_some(),
                    "round {round}: the held relation reads its own body"
                );
            }
        }
        let ledger = held.ledger();
        (ledger, owner.usage().used)
    };
    let after_bytes = write.open_table_registry_bytes();
    assert_eq!(
        owner.usage().used,
        0,
        "dropping the holder returns every admitted byte while the transaction lives"
    );
    assert!(
        after_bytes <= baseline_bytes,
        "the registry is back to at most a baseline-sized array: {after_bytes} after, \
         {baseline_bytes} before"
    );
    assert_eq!(write.open_table_registry_len(), baseline);
    drop(others);
    // Each held relation was closed: it opens again.
    for table in tables {
        drop(
            write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(&relation_name(table)))
                .expect("a closed relation opens again"),
        );
    }
    drop(write);
    (held_usage, peak_ledger, baseline_bytes, after_bytes)
}

#[test]
fn holding_every_relation_is_admitted_exactly_and_returned_when_the_pass_ends() {
    let tables = tables();
    let (_root, db) = store(&tables);
    for baseline in [0usize, 5, 37] {
        let owner = Arc::new(MemoryAccountant::no_limit());
        let (held_usage, ledger, _, _) = hold_all(&db, &tables, baseline, &owner);
        assert_eq!(
            held_usage,
            charged(&ledger),
            "while holding, the accountant holds exactly the ledger's charges"
        );
        let names: usize = tables.iter().map(|table| 2 * relation_name(table).len()).sum();
        assert_eq!(ledger.names, names, "each handle's and registry key's name is admitted");
        assert!(ledger.registry > 0, "holding grew the registry and funded it");
        assert_eq!(
            ledger.temp_name + ledger.names_pending + ledger.registry_pending
                + ledger.scratch_pending + ledger.slots_pending + ledger.nodes_pending,
            0,
            "nothing is left provisional between lookups"
        );
        if baseline > 0 {
            assert!(ledger.scratch > 0, "a baseline-sized replacement was prepared");
        }
        assert_eq!(take_held_overdraft(), 0, "no registry growth went unfunded");
    }
}

#[test]
fn a_relation_that_does_not_fit_the_writer_budget_is_refused_before_it_is_opened() {
    let tables = tables();
    let (_root, db) = store(&tables);
    let probe = Arc::new(MemoryAccountant::no_limit());
    let _ = hold_all(&db, &tables[..6], 0, &probe);
    let fits = probe.high_water_for_test();
    let probe = Arc::new(MemoryAccountant::no_limit());
    let _ = hold_all(&db, &tables[..7], 0, &probe);
    let needs = probe.high_water_for_test();
    assert!(needs > fits, "the seventh relation raises the peak: {fits} -> {needs}");
    let owner = Arc::new(MemoryAccountant::with_budget(needs - 1));
    let write = db.begin_write().expect("begin");
    let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
    {
        let mut held = HeldRelations::new(&write, &mut admission).expect("a holder");
        let mut refusal = None;
        for round in 0..2 {
            for table in tables[..7].iter().rev().chain(&tables[..7]) {
                if let Err(error) = held.get(table) {
                    refusal = Some((round, error));
                    break;
                }
            }
            if refusal.is_some() {
                break;
            }
        }
        match refusal.expect("a relation past the budget is refused") {
            (_, Error::MemoryBudgetExceeded { operation, .. }) => {
                assert_eq!(operation, "hold_row_head_relations")
            }
            (_, other) => panic!("a typed budget refusal, not {other:?}"),
        }
    }
    assert_eq!(owner.usage().used, 0, "a refused pass returns every admitted byte");
    assert_eq!(write.open_table_registry_len(), 0, "no relation is left open");
    assert_eq!(take_held_overdraft(), 0);
    drop(write);
}

/// Every point between a first use's admissions and allocations, failing as
/// an error and as a panic, at a first and at a later registry growth, over a
/// non-empty registry: whatever was admitted comes back, the grown registry
/// array is freed, and no relation stays open.
#[test]
fn a_pass_that_fails_at_any_step_returns_everything_and_leaves_nothing_open() {
    const STEPS: [HoldStep; 16] = [
        HoldStep::AdmitName,
        HoldStep::BuildName,
        HoldStep::ProbeRegistry,
        HoldStep::AdmitRegistry,
        HoldStep::AdmitScratch,
        HoldStep::PrepareScratch,
        HoldStep::AfterScratch,
        HoldStep::Reserve,
        HoldStep::AfterReserve,
        HoldStep::AdmitSlots,
        HoldStep::GrowSlots,
        HoldStep::AdmitNodes,
        HoldStep::GrowNodes,
        HoldStep::AdmitNames,
        HoldStep::Open,
        HoldStep::Install,
    ];
    let tables = tables();
    let (_root, db) = store(&tables);
    for step in STEPS {
        for fault in [HoldFault::Error, HoldFault::Panic] {
            for skip in [0usize, 2] {
                let owner = Arc::new(MemoryAccountant::no_limit());
                let write = db.begin_write().expect("begin");
                let mut others = Vec::new();
                for n in 0..5 {
                    others.push(
                        write
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(&format!(
                                "other_{n}"
                            )))
                            .expect("open a baseline table"),
                    );
                }
                let baseline_bytes = write.open_table_registry_bytes();
                let mut admission =
                    content_identity::ProofAdmission::new(Some(owner.clone().into()));
                inject_hold_fault(step, fault, skip);
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let mut held = HeldRelations::new(&write, &mut admission)?;
                    for table in &tables {
                        held.get(table)?;
                    }
                    Ok::<_, Error>(())
                }));
                let fired = !clear_hold_fault();
                let label = format!("{step:?} {fault:?} skip {skip}");
                if fired {
                    match (&outcome, fault) {
                        (Ok(Err(_)), HoldFault::Error) | (Err(_), HoldFault::Panic) => {}
                        _ => panic!("{label}: the injected fault surfaced as itself"),
                    }
                }
                drop(admission);
                assert_eq!(owner.usage().used, 0, "{label}: every admitted byte came back");
                assert!(
                    write.open_table_registry_bytes() <= baseline_bytes,
                    "{label}: the grown registry array was freed"
                );
                assert_eq!(
                    write.open_table_registry_len(),
                    5,
                    "{label}: only the baseline tables are open"
                );
                assert_eq!(take_held_overdraft(), 0, "{label}: no unfunded registry growth");
                drop(others);
                drop(write);
            }
        }
    }
}
/// `(2R + 4T)·H + 4T` with `H = ⌈log2(T+1)⌉ + 1`: R lookups and T first uses
/// over a balanced index, plus at most 2T slot and 2T node relocations.
fn held_relation_work_bound(lookups: usize, tables: usize) -> u64 {
    let height = (usize::BITS - tables.leading_zeros()) as usize + 1;
    ((2 * lookups + 4 * tables) * height + 4 * tables) as u64
}

/// Tables first used in descending name order are each opened once, every
/// lookup answers with its own table, and the cache work stays logarithmic:
/// inserting each new relation in front of every held one is quadratic.
#[test]
fn descending_first_use_holds_each_relation_with_logarithmic_work() {
    use crate::persistence::content_identity::version_manifest::commit_row_work;
    for count in [256usize, 512, 1_024, 2_048] {
        let tables: Vec<String> = (0..count).map(|n| format!("d{n:05}")).collect();
        let root = tempfile::tempdir().expect("fixture directory");
        let db = redb::Database::create(root.path().join("descending.redb"))
            .expect("create the store");
        let write = db.begin_write().expect("begin the fixture write");
        for table in &tables {
            write
                .open_table(TableDefinition::<&[u8], &[u8]>::new(&relation_name(table)))
                .expect("create one relation")
                .insert(b"row".as_slice(), table.as_bytes())
                .expect("store the relation's own body");
        }
        write.commit().expect("commit the fixture");
        let mut order: Vec<String> = tables.iter().rev().cloned().collect();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut shuffled = tables.clone();
        for index in (1..shuffled.len()).rev() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            shuffled.swap(index, (state % (index as u64 + 1)) as usize);
        }
        order.extend(shuffled);
        let owner = Arc::new(MemoryAccountant::no_limit());
        let mut admission = content_identity::ProofAdmission::new(Some(owner.clone().into()));
        let write = db.begin_write().expect("begin");
        commit_row_work::take_held_relation_work();
        let opened = hold_in_order_for_test(&write, &order, &mut admission, |table, relation| {
            assert_eq!(
                relation
                    .get(b"row".as_slice())
                    .expect("read")
                    .expect("the relation's body")
                    .value(),
                table.as_bytes(),
                "a lookup answers with its own table's relation"
            );
        })
        .expect("every relation is held");
        let work = commit_row_work::take_held_relation_work();
        let bound = held_relation_work_bound(order.len(), count);
        eprintln!("held_relation_work tables={count} lookups={} work={work} bound={bound}", order.len());
        assert_eq!(opened, count, "each relation opens once per pass");
        assert!(
            work <= bound,
            "held-relation work grew faster than its logarithmic bound at {count} tables: \
             {work} > {bound}"
        );
        drop(write);
    }
}
