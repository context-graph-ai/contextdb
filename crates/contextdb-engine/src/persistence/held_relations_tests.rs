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
        let names: usize = tables
            .iter()
            .map(|table| 2 * relation_name(table).len())
            .sum();
        assert_eq!(
            ledger.names, names,
            "each handle's and registry key's name is admitted"
        );
        assert!(
            ledger.registry > 0,
            "holding grew the registry and funded it"
        );
        assert_eq!(
            ledger.temp_name
                + ledger.names_pending
                + ledger.registry_pending
                + ledger.scratch_pending
                + ledger.slots_pending
                + ledger.nodes_pending,
            0,
            "nothing is left provisional between lookups"
        );
        if baseline > 0 {
            assert!(
                ledger.scratch > 0,
                "a baseline-sized replacement was prepared"
            );
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
    assert!(
        needs > fits,
        "the seventh relation raises the peak: {fits} -> {needs}"
    );
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
    assert_eq!(
        owner.usage().used,
        0,
        "a refused pass returns every admitted byte"
    );
    assert_eq!(
        write.open_table_registry_len(),
        0,
        "no relation is left open"
    );
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
                            .open_table(TableDefinition::<&[u8], &[u8]>::new(&format!("other_{n}")))
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
                assert_eq!(
                    owner.usage().used,
                    0,
                    "{label}: every admitted byte came back"
                );
                assert!(
                    write.open_table_registry_bytes() <= baseline_bytes,
                    "{label}: the grown registry array was freed"
                );
                assert_eq!(
                    write.open_table_registry_len(),
                    5,
                    "{label}: only the baseline tables are open"
                );
                assert_eq!(
                    take_held_overdraft(),
                    0,
                    "{label}: no unfunded registry growth"
                );
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
        let db =
            redb::Database::create(root.path().join("descending.redb")).expect("create the store");
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
        eprintln!(
            "held_relation_work tables={count} lookups={} work={work} bound={bound}",
            order.len()
        );
        assert_eq!(opened, count, "each relation opens once per pass");
        assert!(
            work <= bound,
            "held-relation work grew faster than its logarithmic bound at {count} tables: \
             {work} > {bound}"
        );
        drop(write);
    }
}

/// The holder builds the same storage name redb's callers use.
#[test]
fn the_holder_builds_the_storage_name_every_caller_uses() {
    for table in ["t", "imported", &"x".repeat(400)] {
        assert_eq!(
            format!("{RELATION_PREFIX}{table}"),
            RedbPersistence::rel_table_name(table)
        );
    }
}

/// A real commit writing one row into a long-named table. Each reconciliation
/// pass opens that table's relation while it is not yet staged (the root hook
/// flushes staged roots on every protected close), so closing it stages a new
/// key. Allocations of exactly the storage name's length are recorded in
/// order with the holder's charges. While a pass holds the relation's name
/// charge (2 × the name), every name copy allocated (the handle's name, the
/// registry key, and the key staging allocates on close) is freed before the
/// charge is released, and no more than two copies are ever alive at once.
/// Before that charge, the first use allocates exactly one name: the admitted
/// temporary, and no unadmitted second copy.
#[test]
fn a_relation_first_staged_by_a_commit_is_funded_from_before_its_key_exists_until_after_it_is_freed()
{
    use super::Charge;
    use super::staging_witness::{self, Event};
    let table = format!("s{}", "t".repeat(776));
    let name_len = RELATION_PREFIX.len() + table.len();
    let root = tempfile::tempdir().expect("fixture directory");
    let db = crate::Database::open(root.path().join("staging.db")).expect("open the database");
    db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
    db.execute(
        &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, payload TEXT NOT NULL)"),
        &std::collections::HashMap::new(),
    )
    .expect("declare the table");
    let tx = db.begin().expect("begin");
    db.execute_in_tx(
        tx,
        &format!("INSERT INTO {table} VALUES ('{}', 'one row')", uuid::Uuid::from_u128(7)),
        &std::collections::HashMap::new(),
    )
    .expect("stage one row");
    staging_witness::arm(name_len);
    db.commit(tx).expect("commit the row");
    let (events, overrun) = staging_witness::take();
    assert_eq!(overrun, None, "no charge was released before what it funds was freed");
    let names = 2 * name_len;
    let mut windows = 0;
    let mut index = 0;
    while let Some(start) = events[index..]
        .iter()
        .position(|event| *event == Event::Admit(Charge::Names, names))
        .map(|offset| index + offset)
    {
        let end = start
            + events[start..]
                .iter()
                .position(|event| *event == Event::Release(Charge::Names, names))
                .expect("the names charge is released");
        let temp = events[..start]
            .iter()
            .rposition(|event| *event == Event::Admit(Charge::TempName, name_len))
            .expect("the temporary name is admitted first");
        let before: Vec<_> = events[temp + 1..start]
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    Event::Alloc(None | Some(Charge::TempName | Charge::Names), _)
                )
            })
            .collect();
        assert_eq!(
            before.len(),
            1,
            "the first use builds exactly one name copy before redb's: {:?}",
            &events[temp..=start]
        );
        assert_eq!(
            events[temp + 1..start]
                .iter()
                .rev()
                .find_map(|event| match event {
                    Event::Staged(staged) => Some(*staged),
                    _ => None,
                }),
            Some(false),
            "the relation was not staged when the pass first used it"
        );
        let mut live = std::collections::HashSet::new();
        let mut allocated = 0;
        let mut peak = 0;
        for event in &events[start + 1..end] {
            match *event {
                Event::Alloc(None | Some(Charge::Names), address) => {
                    live.insert(address);
                    allocated += 1;
                    peak = peak.max(live.len());
                }
                Event::Free(address) => {
                    live.remove(&address);
                }
                _ => {}
            }
        }
        assert!(
            allocated >= 3,
            "the handle's name, the registry key and the staged key are all allocated \
             while the charge is held: {:?}",
            &events[start..=end]
        );
        assert!(
            live.is_empty(),
            "every name copy made under the charge is freed before it is released: {:?}",
            &events[start..=end]
        );
        assert!(
            peak <= 2,
            "no more name copies are alive than the charge funds: {:?}",
            &events[start..=end]
        );
        windows += 1;
        index = end + 1;
    }
    assert_eq!(windows, 2, "both reconciliation passes held the relation: {events:?}");
    db.close().expect("close");
}

/// A store with `count` tables and one staged transaction writing one row to
/// each, in name order. Enough tables that each pass grows the registry,
/// the slot vector and the node vector more than once.
fn many_table_commit(
    count: usize,
) -> (
    tempfile::TempDir,
    crate::Database,
    contextdb_core::TxId,
    Vec<String>,
) {
    let root = tempfile::tempdir().expect("fixture directory");
    let db = crate::Database::open(root.path().join("held.db")).expect("open the database");
    db.set_maintenance_policy(crate::MaintenancePolicy::CallerDriven);
    let tables: Vec<String> = (0..count).map(|n| format!("held_{n:03}_{}", "x".repeat(60))).collect();
    for table in &tables {
        db.execute(
            &format!("CREATE TABLE {table} (id UUID PRIMARY KEY, payload TEXT NOT NULL)"),
            &std::collections::HashMap::new(),
        )
        .expect("declare one table");
    }
    let tx = db.begin().expect("begin");
    for (n, table) in tables.iter().enumerate() {
        db.execute_in_tx(
            tx,
            &format!(
                "INSERT INTO {table} VALUES ('{}', 'one row')",
                uuid::Uuid::from_u128(0x6e1d << 112 | n as u128)
            ),
            &std::collections::HashMap::new(),
        )
        .expect("stage one row");
    }
    (root, db, tx, tables)
}

const COMMIT_TABLES: usize = 40;

/// Through a real commit (both reconciliation passes, several registry,
/// slot and node growths each), every holder charge is released only after
/// the allocations it funds are freed, at every release, and nothing stays
/// charged or tagged-live when the commit ends.
#[test]
fn a_commit_releases_each_holder_charge_only_after_what_it_funds_is_freed() {
    use super::staging_witness;
    let (_root, db, tx, _) = many_table_commit(COMMIT_TABLES);
    take_hold_arrivals();
    staging_witness::arm(0);
    db.commit(tx).expect("commit every table");
    let charged = staging_witness::charged();
    let live = staging_witness::live();
    let (events, overrun) = staging_witness::take();
    let arrivals = take_hold_arrivals();
    assert_eq!(overrun, None, "a charge outlived nothing it funded: {overrun:?}");
    assert_eq!(charged, [0; 6], "every charge was released");
    assert!(live.is_empty(), "every funded allocation was freed: {live:?}");
    for step in [HoldStep::Reserve, HoldStep::GrowSlots, HoldStep::GrowNodes] {
        assert!(
            arrivals[step as usize] >= 2,
            "{step:?} happened in both passes: {arrivals:?}"
        );
    }
    eprintln!("held_commit_events={} arrivals={arrivals:?}", events.len());
    assert_eq!(take_held_overdraft(), 0, "no registry growth went unfunded");
    db.close().expect("close");
}

/// Through a real commit, each holder step fails as an injected error, an
/// injected panic and (where it allocates a registry array) a failed global
/// allocation, at its first arrival, at its first arrival in the second pass
/// and at its last arrival (a later growth). Every injection must fire. At
/// the moment it fires, every category must fit what is still charged; after
/// the commit fails, nothing is left charged, nothing went unfunded, nothing
/// was committed, and (after an error) the store keeps committing.
#[test]
fn a_commit_failing_at_any_holder_step_returns_everything_and_keeps_the_store_usable() {
    use super::staging_witness;
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
    // How often each step is reached in a commit, and in its first pass.
    let (_root, db, tx, _) = many_table_commit(COMMIT_TABLES);
    take_hold_arrivals();
    db.commit(tx).expect("the dry run commits");
    let total = take_hold_arrivals();
    db.close().expect("close");
    // A commit's passes start with no table open, so no baseline-sized
    // replacement is prepared there; those steps are covered over a
    // non-empty registry by the holder-level matrix above.
    const BASELINE_ONLY: [HoldStep; 3] = [
        HoldStep::AdmitScratch,
        HoldStep::PrepareScratch,
        HoldStep::AfterScratch,
    ];
    for step in STEPS {
        let arrivals = total[step as usize];
        if BASELINE_ONLY.contains(&step) {
            assert_eq!(arrivals, 0, "{step:?}: a commit's passes start with an empty registry");
            continue;
        }
        assert!(arrivals > 0, "{step:?} is reached by a commit");
        let mut skips = vec![0, arrivals / 2, arrivals - 1];
        skips.dedup();
        let mut faults = vec![HoldFault::Error, HoldFault::Panic];
        if step == HoldStep::Reserve {
            faults.push(HoldFault::GlobalAllocation);
        }
        for fault in faults {
            for &skip in &skips {
                let label = format!("{step:?} {fault:?} arrival {}", skip + 1);
                let (_root, db, tx, tables) = many_table_commit(COMMIT_TABLES);
                let accountant = db.__accountant_handle_for_test();
                let before = accountant.usage().used;
                staging_witness::arm(0);
                inject_hold_fault(step, fault, skip);
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.commit(tx)));
                let fired = !clear_hold_fault();
                let charged = staging_witness::charged();
                let (_, overrun) = staging_witness::take();
                assert!(fired, "{label}: the injection fired");
                assert_eq!(overrun, None, "{label}: every category fit its charge");
                assert_eq!(charged, [0; 6], "{label}: every holder charge was released");
                assert_eq!(take_held_overdraft(), 0, "{label}: no unfunded registry growth");
                assert!(
                    accountant.usage().used <= before,
                    "{label}: the failed commit holds nothing more than before it ran"
                );
                match (fault, outcome) {
                    (HoldFault::Panic, Err(_)) => {
                        // A panic inside commit may leave the database's own
                        // locks poisoned; this test only proves the holder
                        // gave back what it took.
                        drop(accountant);
                        std::mem::forget(db);
                        continue;
                    }
                    // A commit that contains the panic must still refuse.
                    (_, Ok(Err(_))) => {}
                    (_, Ok(Ok(()))) => panic!("{label}: the commit did not fail"),
                    (_, Err(_)) => panic!("{label}: the injected error surfaced as a panic"),
                }
                let committed: usize = tables
                    .iter()
                    .map(|table| {
                        db.execute(
                            &format!("SELECT id FROM {table}"),
                            &std::collections::HashMap::new(),
                        )
                        .expect("read")
                        .rows
                        .len()
                    })
                    .sum();
                assert_eq!(committed, 0, "{label}: the failed commit published nothing");
                db.execute(
                    &format!(
                        "INSERT INTO {} VALUES ('{}', 'after failure')",
                        tables[0],
                        uuid::Uuid::from_u128(1)
                    ),
                    &std::collections::HashMap::new(),
                )
                .unwrap_or_else(|error| panic!("{label}: the store keeps committing: {error}"));
                drop(accountant);
                db.close().expect("close");
            }
        }
    }
}
