//! A memory refusal names the storage the file-backed table actually owns.
//! Schema descriptors and the captured published root are charged separately;
//! a source-backed primary-key index retains no exact-probe bitmap.
use contextdb_engine::Database;
use contextdb_engine::memory_accounting::{
    BudgetLedgerEntry, take_budget_ledger_for_test, watch_budget_ledger_for_test,
};
use std::collections::HashMap;

/// What is outstanding at the point the ledger reached its highest total, by
/// the name that charged it.
///
/// Releases carry no name, so each one is attributed to the most recent charge
/// still outstanding — last in, first out, which is how a transaction's nested
/// admissions actually unwind.
fn outstanding_at_the_peak(ledger: &[BudgetLedgerEntry]) -> (usize, Vec<(String, usize)>) {
    let peak = ledger
        .iter()
        .enumerate()
        .max_by_key(|(_, entry)| entry.used_after)
        .map(|(index, entry)| (index, entry.used_after))
        .unwrap_or((0, 0));
    let mut outstanding: Vec<(String, usize)> = Vec::new();
    if ledger.is_empty() {
        return (0, Vec::new());
    }
    for entry in &ledger[..=peak.0.min(ledger.len() - 1)] {
        if entry.what == "release" {
            let mut left = entry.bytes;
            while left > 0 {
                let Some((_, held)) = outstanding.last_mut() else {
                    break;
                };
                let taken = (*held).min(left);
                *held -= taken;
                left -= taken;
                if *held == 0 {
                    outstanding.pop();
                }
            }
        } else {
            outstanding.push((entry.what.clone(), entry.bytes));
        }
    }
    let mut totals: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    for (what, bytes) in outstanding {
        *totals.entry(what).or_default() += bytes;
    }
    let mut totals = totals.into_iter().collect::<Vec<_>>();
    totals.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    (peak.1, totals)
}

fn report(label: &str, ledger: &[BudgetLedgerEntry]) -> (usize, Vec<(String, usize)>) {
    eprintln!("{label}: {} ledger entries", ledger.len());
    for entry in ledger {
        eprintln!(
            "  {:<44} {:>6} bytes -> used {}",
            entry.what, entry.bytes, entry.used_after
        );
    }
    let (peak, held) = outstanding_at_the_peak(ledger);
    eprintln!("{label}: at the peak of {peak} bytes, outstanding by charge name:");
    for (what, bytes) in &held {
        eprintln!("  {what:<44} {bytes:>6} bytes");
    }
    (peak, held)
}

/// The exact sequence of `oom_does_not_partially_commit_visible_state`, with
/// the ledger watching the statement that refuses.
#[test]
#[ignore = "A one kilobyte store does not name the schema admission that holds its budget."]
fn a_one_kilobyte_store_says_what_is_holding_its_budget() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("one-kilobyte.db");

    let db = Database::open(&path).expect("open the writable fixture");
    db.execute("SET MEMORY_LIMIT '1K'", &HashMap::new())
        .expect("declare the limit");

    // Refusal must price the declaration rather than a filter the durable
    // source-backed index does not retain.
    watch_budget_ledger_for_test();
    let outcome = db.execute(
        "CREATE TABLE big (id UUID PRIMARY KEY, payload TEXT)",
        &HashMap::new(),
    );
    let ledger = take_budget_ledger_for_test();
    let (peak, held) = report("one minimal CREATE TABLE under MEMORY_LIMIT '1K'", &ledger);

    let error = outcome.expect_err("the captured root and schema descriptors exceed 1024 bytes");
    let contextdb_core::Error::MemoryBudgetExceeded {
        subsystem,
        operation,
        requested_bytes,
        budget_limit_bytes,
        available_bytes,
        ..
    } = &error
    else {
        panic!("the refusal must be the typed memory refusal, not {error:?}");
    };

    assert_eq!(
        (subsystem.as_ref(), operation.as_ref()),
        ("ddl", "create_table"),
        "the refusal names schema admission: {error:?}"
    );
    assert_eq!(*budget_limit_bytes, 1024);
    assert_eq!(
        *requested_bytes, 464,
        "only retained schema descriptors are priced"
    );
    assert_eq!(*available_bytes, 416);
    assert_eq!(peak, 608);
    assert_eq!(held, vec![("writer_source/published_root".to_owned(), 264)]);
    assert_eq!(
        db.accountant().usage().used,
        344,
        "the statement returns its root charge"
    );
    assert_eq!(
        ledger,
        vec![
            BudgetLedgerEntry {
                what: "writer_source/published_root".to_owned(),
                bytes: 264,
                used_after: 608,
                owner_identity: 0,
                parent_identity: 0,
                row_identity: None,
                allocation: None,
            },
            BudgetLedgerEntry {
                what: "release".to_owned(),
                bytes: 264,
                used_after: 344,
                owner_identity: 0,
                parent_identity: 0,
                row_identity: None,
                allocation: None,
            },
        ]
    );
    assert!(
        ledger
            .iter()
            .all(|entry| !entry.what.starts_with("content_identity/")),
        "proof maintenance never runs: the statement is refused before it: {ledger:?}"
    );
}
