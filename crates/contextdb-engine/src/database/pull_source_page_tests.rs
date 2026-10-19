//! A hub pull page is selected from each history source at the source: it
//! visits and copies only the groups it keeps (plus the one entry that ends
//! the page), whatever lies behind it, so the next page costs the same when
//! the history behind the cursor doubles, and a tail larger than the writer
//! budget still pulls in pages whose groups each fit.
use super::resident_history_test_seams::{Work, observe};
use super::*;
use std::cell::Cell;
use std::collections::HashMap;

const PAGE_BYTES: usize = 64 * 1024;

struct Page {
    rows: usize,
    ddl: usize,
    through: Option<Lsn>,
    max_lsn: Option<Lsn>,
    work: Work,
}

/// One page from `since`, selected on this thread so its work is observable,
/// funded by the store's writer budget as a hub serves it.
fn page(db: &Database, since: Lsn, page_bytes: usize) -> Result<Page> {
    let memory = db.engine_sync_read_memory()?;
    let mut vector_copy = crate::vector_read_memory::admission(memory.clone());
    let mut credit = contextdb_core::read_memory::ReadCredit::new(memory.clone());
    let through = Cell::new(None);
    let purge_items = std::cell::RefCell::new(Vec::new());
    let (selected, work) = observe(|| {
        db.checked_changes_since_with_arrivals(
            since,
            &mut vector_copy,
            memory.clone(),
            &mut credit,
            Some(PullPageRequest {
                bytes: page_bytes,
                through: &through,
                purge_items: &purge_items,
            }),
        )
    });
    let (changes, _, _) = selected?;
    Ok(Page {
        rows: changes.rows.len(),
        ddl: changes.ddl.len(),
        through: through.get(),
        max_lsn: changes.max_lsn(),
        work,
    })
}

/// Every page from the start, as an edge pulls: (pages, rows, ddl, the most
/// source entries one page visited).
fn pull_all(db: &Database, page_bytes: usize) -> Result<(usize, usize, usize, usize)> {
    let mut since = Lsn(0);
    let (mut pages, mut rows, mut ddl, mut most) = (0, 0, 0, 0);
    loop {
        let page = page(db, since, page_bytes)?;
        pages += 1;
        rows += page.rows;
        ddl += page.ddl;
        most = most.max(page.work.pull_source_entries);
        match page.through {
            Some(through) => since = through,
            None => break,
        }
        assert!(pages < 100_000, "the pull makes progress");
    }
    Ok((pages, rows, ddl, most))
}

fn synced_table(db: &Database) {
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT) SYNC TWO WAY SYNC CONFLICT KEEP FIRST",
        &HashMap::new(),
    )
    .expect("declare notes");
}

/// `count` single-row commits of about 4 KiB each.
fn commit_rows(db: &Database, from: usize, count: usize) {
    for i in from..from + count {
        db.execute(
            "INSERT INTO notes (id, body) VALUES ($id, $body)",
            &HashMap::from([
                (
                    "id".to_string(),
                    Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1)),
                ),
                (
                    "body".to_string(),
                    Value::Text(format!("{i:08} {}", "x".repeat(4096))),
                ),
            ]),
        )
        .expect("commit a row");
    }
}

// Durable change log: the first page visits only the groups it
// keeps (plus the entry that ends it), the same behind a doubled tail.
#[test]
fn a_durable_page_visits_only_its_groups_as_the_tail_doubles() {
    let mut visited = Vec::new();
    for count in [400usize, 800] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = Database::open(dir.path().join("hub.db")).expect("open hub");
        synced_table(&db);
        commit_rows(&db, 0, count);
        let first = page(&db, Lsn(0), PAGE_BYTES).expect("first page");
        println!(
            "S4_DURABLE_PAGE rows={count} page_rows={} through={:?} visited={}",
            first.rows, first.through, first.work.pull_source_entries
        );
        assert!(first.through.is_some(), "the tail does not fit one page");
        visited.push(first.work.pull_source_entries);
    }
    assert_eq!(
        visited[0], visited[1],
        "the first page visits the same entries behind a doubled tail"
    );
}

// Resident change log: the first page visits the groups it keeps, not the
// remaining tail; it costs the same behind a doubled tail.
#[test]
fn a_resident_page_visits_only_its_groups_as_the_tail_doubles() {
    let mut visited = Vec::new();
    for count in [400usize, 800] {
        let db = Database::open_memory();
        synced_table(&db);
        commit_rows(&db, 0, count);
        let first = page(&db, Lsn(0), PAGE_BYTES).expect("first page");
        assert!(first.through.is_some(), "the tail does not fit one page");
        visited.push(first.work.pull_source_entries);
    }
    assert_eq!(
        visited[0], visited[1],
        "the first page visits the same entries behind a doubled tail"
    );
}

// A resident tail larger than the writer budget's remaining headroom pulls in
// pages whose groups each fit, every row exactly once. The ceiling is set
// above what the rows already hold, so the only pressure is the selected
// copy: a page that copied the whole tail would need more than the headroom.
#[test]
fn a_resident_tail_larger_than_the_writer_budget_pulls_in_pages() {
    let db = Database::open_memory();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    synced_table(&db);
    commit_rows(&db, 0, 3_000);
    let held = db.writer_accountant_for_test().usage().used;
    let headroom = 2 * 1024 * 1024;
    db.set_memory_limit(Some(held + headroom))
        .expect("a writer budget with less headroom than the 12 MiB tail");
    let (pages, rows, _, _) = pull_all(&db, PAGE_BYTES).expect("the tail pulls in pages");
    assert_eq!(rows, 3_000, "every row exactly once");
    assert!(pages > 1);
}

// A durable tail larger than the writer budget pulls in pages
// whose groups each fit, every row exactly once.
#[test]
fn a_durable_tail_larger_than_the_writer_budget_pulls_in_pages() {
    let dir = tempfile::tempdir().expect("store directory");
    let db = Database::open(dir.path().join("hub.db")).expect("open hub");
    synced_table(&db);
    commit_rows(&db, 0, 3_000);
    db.set_memory_limit(Some(8 * 1024 * 1024))
        .expect("a writer budget below the 12 MiB tail");
    let (pages, rows, _, most) = pull_all(&db, PAGE_BYTES).expect("the tail pulls in pages");
    println!("S4_DURABLE_OVER_BUDGET pages={pages} rows={rows} most_visited={most}");
    assert_eq!(rows, 3_000, "every row exactly once");
    assert!(pages > 1);
}

// Declaration history: a history of declarations alone pages at group
// boundaries, every declaration once, on the resident and the durable store.
#[test]
#[serial_test::serial]
fn a_declaration_only_history_pulls_in_pages() {
    // Source-copy admission prices a declaration before copying it. Pricing
    // itself must not allocate an unfunded buffer proportional to that body.
    let mut pricing_peaks = Vec::new();
    for name_bytes in [64 * 1024, 128 * 1024] {
        let change = DdlChange::CreateTable {
            name: "d".repeat(name_bytes),
            columns: vec![("id".into(), "UUID PRIMARY KEY".into())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        };
        let expected = 128
            + 3 * bincode::serde::encode_to_vec(&change, bincode::config::standard())
                .expect("canonical declaration encoding")
                .len();
        let (estimated, peak, live) =
            crate::persistence::content_identity::proof_allocation_oracle_tests::measured(|| {
                change.estimated_bytes()
            });
        assert_eq!(
            estimated, expected,
            "pricing keeps the canonical byte estimate"
        );
        assert_eq!(live, 0);
        pricing_peaks.push(peak);
    }
    assert_eq!(
        pricing_peaks[0], pricing_peaks[1],
        "pricing before source admission has no body-proportional native allocation"
    );
    for durable in [false, true] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = if durable {
            Database::open(dir.path().join("hub.db")).expect("open hub")
        } else {
            Database::open_memory()
        };
        for i in 0..300 {
            db.execute(
                &format!(
                    "CREATE TABLE t{i:04} (id UUID PRIMARY KEY, c{i} TEXT, d{i} TEXT, e{i} TEXT) SYNC TWO WAY"
                ),
                &HashMap::new(),
            )
            .expect("declare a table");
        }
        let first = page(&db, Lsn(0), 16 * 1024).expect("first page");
        let (pages, _, ddl, most) = pull_all(&db, 16 * 1024).expect("pull");
        println!(
            "S4_DDL_ONLY durable={durable} first_ddl={} first_through={:?} first_max={:?} pages={pages} ddl={ddl} most_visited={most}",
            first.ddl, first.through, first.max_lsn
        );
        assert!(
            first.through.is_some(),
            "300 declarations do not fit one page"
        );
        assert!(first.ddl < 300, "the first page holds only its groups");
        assert_eq!(ddl, 300, "every declaration exactly once");
    }
}

// Exact payload lookup: the page that carries a
// row's latest change reads that one version of the row, never its history,
// so the same page costs the same behind a doubled count of that row's
// superseded versions.
#[test]
fn a_page_reads_one_version_of_a_row_whatever_its_history() {
    let mut visited = Vec::new();
    for versions in [50usize, 100] {
        let dir = tempfile::tempdir().expect("store directory");
        let db = Database::open(dir.path().join("hub.db")).expect("open hub");
        db.execute(
            "CREATE TABLE hot (id INT PRIMARY KEY, body TEXT) HISTORY ALL SYNC TWO WAY",
            &HashMap::new(),
        )
        .expect("declare hot");
        let write = |sql: &str, version: usize| {
            db.execute(
                sql,
                &HashMap::from([
                    ("id".to_string(), Value::Int64(1)),
                    (
                        "body".to_string(),
                        Value::Text(format!("version {version}")),
                    ),
                ]),
            )
            .expect("write the hot row");
        };
        write("INSERT INTO hot (id, body) VALUES ($id, $body)", 0);
        for version in 1..versions {
            write("UPDATE hot SET body = $body WHERE id = $id", version);
        }
        let since = db.current_lsn();
        write("UPDATE hot SET body = $body WHERE id = $id", versions);
        let (selected, work) =
            crate::writer_query_test_seams::observe(|| page(&db, since, PAGE_BYTES));
        let selected = selected.expect("the page after the last update");
        println!(
            "S4_ONE_VERSION versions={versions} rows={} row_versions_visited={}",
            selected.rows, work.history.row_versions
        );
        assert_eq!(selected.rows, 1, "the page carries the row's latest change");
        visited.push(work.history.row_versions);
    }
    assert_eq!(
        visited[0], visited[1],
        "the page visits the same row versions behind a doubled history"
    );
}
