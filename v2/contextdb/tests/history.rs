//! A table keeps every replaced version of a row unless it declares `HISTORY CURRENT ONLY`; a
//! cleanup, a held snapshot and a PURGE leave the versions as the promise says.

mod support;

use contextdb::{Database, Value, params};

/// The numbers `n` of a key's versions, newest first, asking for `wanted`, and whether more.
fn versions(db: &Database, table: &str, wanted: usize) -> (Vec<Value>, bool) {
    let kept = db.row_versions(table, &[Value::Int64(1)], wanted).unwrap();
    (
        kept.rows.rows().iter().map(|row| row[2].clone()).collect(),
        kept.more,
    )
}

/// A table with the clause given and row 1 written, then updated `updates` times.
fn updated(db: &Database, table: &str, clause: &str, updates: i64) {
    let create =
        format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, n INTEGER, body TEXT) {clause}");
    support::run(db, &create);
    let body = support::large_text(support::Kind::Scattered, 1, 20_000);
    let insert = format!("INSERT INTO {table} VALUES (1, 0, $body)");
    db.execute(&insert, &params! { "body" => body }).unwrap();
    for n in 1..=updates {
        support::run(db, &format!("UPDATE {table} SET n = {n} WHERE id = 1"));
    }
}

#[test]
fn a_table_keeps_replaced_versions_until_it_declares_current_only_and_a_cleanup_goes_in_cycles() {
    let folder = support::folder("history");
    let db = support::open(&folder);
    // SYNC OFF so the later ALTER to CURRENT ONLY is allowed; the other table says KEEP LATEST.
    updated(&db, "plain", "SYNC OFF", 3);
    updated(
        &db,
        "current",
        "HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        3,
    );
    updated(&db, "kept", "SYNC CONFLICT KEEP LATEST", 3);
    let newest_first = [2, 1, 0].map(Value::Int64).to_vec();
    assert_eq!(versions(&db, "plain", 128), (newest_first.clone(), false));
    assert_eq!(
        versions(&db, "kept", 128).0,
        newest_first,
        "KEEP LATEST alone keeps versions"
    );
    assert_eq!(
        versions(&db, "plain", 2),
        (newest_first[..2].to_vec(), true),
        "a limit says more"
    );
    assert!(
        versions(&db, "current", 128).0.is_empty(),
        "CURRENT ONLY keeps none"
    );
    // A cleanup goes in the cycles after the ALTER.
    let held = db.snapshot().unwrap();
    support::run(&db, "ALTER TABLE plain SET HISTORY CURRENT ONLY");
    assert_eq!(
        versions(&db, "plain", 128).0.len(),
        3,
        "ALTER returns before the cleanup"
    );
    let removed: u64 = (0..8)
        .map(|_| {
            db.run_maintenance_cycle()
                .unwrap()
                .versions_removed
                .unwrap_or(0)
        })
        .sum();
    assert_eq!(
        removed, 3,
        "the cycles report the three versions they removed"
    );
    assert!(
        versions(&db, "plain", 128).0.is_empty(),
        "the cycles removed them"
    );
    // A held snapshot still reads the row as it stood, the large value with it, after an UPDATE and the cycles.
    support::run(&db, "UPDATE plain SET n = 7 WHERE id = 1");
    support::maintenance(&db);
    let read = db
        .execute_at_snapshot("SELECT n, body FROM plain WHERE id = 1", &params! {}, &held)
        .unwrap();
    let body = support::large_text(support::Kind::Scattered, 1, 20_000);
    assert_eq!(
        read.rows(),
        [[Value::Int64(3), Value::Text(body.clone())]],
        "the snapshot reads the row as it stood"
    );
    drop(held);
    // History declared back, later updates keep versions again and the removed ones stay gone.
    support::run(&db, "ALTER TABLE plain SET HISTORY ALL");
    support::run(&db, "UPDATE plain SET n = 9 WHERE id = 1");
    assert_eq!(
        versions(&db, "plain", 128).0,
        [Value::Int64(7)],
        "only the new version"
    );
    // Assigning a key column writes the row whole: its large value reads back, held at new
    // places, and the old places are freed (entry 97; removal by range rests on it).
    let large = |db: &Database| verified(db, "current").large_values;
    let before = large(&db);
    support::run(&db, "UPDATE current SET id = 2 WHERE id = 1");
    support::maintenance(&db);
    assert_eq!(
        large(&db),
        before,
        "one copy of the value is held after the key change"
    );
    let moved = support::rows(&db, "SELECT body FROM current WHERE id = 2");
    assert_eq!(
        moved[0][0],
        Value::Text(body.clone()),
        "a key-assigning UPDATE keeps the large value (LEAN-H04)"
    );
    // DROP COLUMN rewrites each version without the cell and removes none (LEAN-H01); a DELETE on
    // a RETAIN table keeps no version while its UPDATE keeps one (LEAN-H02).
    support::run(&db, "ALTER TABLE kept DROP COLUMN body");
    let dropped = db.row_versions("kept", &[Value::Int64(1)], 128).unwrap();
    assert_eq!(dropped.rows.rows().len(), 3, "no version removed");
    assert_eq!(
        dropped.rows.rows()[0].len(),
        3,
        "the commit, id and n: each without the cell"
    );
    updated(&db, "retained", "RETAIN 2 DAYS", 1);
    assert_eq!(
        versions(&db, "retained", 128).0.len(),
        1,
        "an UPDATE keeps one"
    );
    support::run(&db, "DELETE FROM retained WHERE id = 1");
    assert!(
        versions(&db, "retained", 128).0.is_empty(),
        "a DELETE keeps none"
    );
    // A PURGE takes the versions and the value they share.
    support::run(&db, "PURGE FROM kept WHERE id = 1");
    assert!(
        versions(&db, "kept", 128).0.is_empty(),
        "a purged row has no versions"
    );
    // A row deleted earlier is decided by its newest kept version, never an older one.
    updated(&db, "deleted", "", 2);
    support::run(&db, "DELETE FROM deleted WHERE id = 1");
    support::run(&db, "PURGE FROM deleted WHERE id = 1 AND n = 0");
    let kept = versions(&db, "deleted", 128).0;
    assert_eq!(kept.len(), 3, "an older version does not decide");
    support::run(&db, "PURGE FROM deleted WHERE n = 2");
    assert!(
        versions(&db, "deleted", 128).0.is_empty(),
        "a purged row deleted earlier has no versions"
    );
    db.verify().expect("every address freed once");
}

/// The figures `verify` gives for one table.
fn verified(db: &Database, table: &str) -> contextdb::ContentSize {
    let report = db.verify().expect("verify is clean");
    report
        .tables
        .iter()
        .find(|t| t.table == table)
        .expect("the table")
        .clone()
}

#[test]
fn row_versions_refuses_a_wrong_key_values_past_the_memory_limit_and_a_hidden_version_beyond_the_limit()
 {
    let folder = support::folder("history-refusals");
    let db = support::open(&folder);
    updated(&db, "plain", "SYNC OFF", 0);
    for (key, n) in [(vec![], 1), (vec![Value::Int64(1), Value::Int64(2)], 1)] {
        let text = format!("{:?}", db.row_versions("plain", &key, 8).err());
        assert!(
            text.contains("plain's key takes 1 value(s), in key order"),
            "{n}: {text}"
        );
    }
    for n in 1..=1000 {
        let body = support::large_text(support::Kind::Scattered, n, 20_000);
        db.execute(
            "UPDATE plain SET body = $body WHERE id = 1",
            &params! { "body" => body },
        )
        .unwrap();
    }
    let reader = support::open_reader(&folder, Some(12 << 20));
    assert!(
        matches!(
            reader.row_versions("plain", &[Value::Int64(1)], 1001),
            Err(contextdb::Error::MemoryBudgetExceeded { .. })
        ),
        "versions past the reader's memory are refused"
    );
    // The oldest version is under Context 2 and the rest under Context 1.
    support::run(
        &db,
        "CREATE TABLE seen (id INTEGER PRIMARY KEY, n INTEGER, context_id UUID CONTEXT_ID)",
    );
    for (n, of) in [(0_i64, 2), (1, 1), (2, 1), (3, 1)] {
        let sql = if n == 0 {
            "INSERT INTO seen VALUES (1, 0, $c)"
        } else {
            "UPDATE seen SET n = $n, context_id = $c WHERE id = 1"
        };
        db.execute(sql, &params! { "n" => n, "c" => support::context(of) })
            .unwrap();
    }
    let one = support::in_contexts(&db, &[1]);
    let key = [Value::Int64(1)];
    assert!(
        db.row_versions("seen", &key, 2).unwrap().more,
        "a handle that sees every version is told there are more"
    );
    assert!(
        one.row_versions("seen", &key, 2).is_err(),
        "the hidden version beyond the limit refuses the call, and is never shown as more"
    );
}

/// A PURGE of kept versions of rows deleted earlier loads only the columns it tests and writes
/// its removals in bounded parts: ten thousand keys, and one key whose kept version holds an
/// 8 MiB value the condition does not read, are each erased within 16 MiB.
#[test]
fn a_purge_of_kept_versions_reads_only_what_it_tests_within_the_memory_limit() {
    let db = support::open(&support::folder("history-purge-memory"));
    support::run(&db, "CREATE TABLE many (id INTEGER PRIMARY KEY, n INTEGER)");
    for from in (0..10_000).step_by(1_000) {
        let rows: Vec<_> = (from..from + 1_000)
            .map(|id| format!("({id}, {id})"))
            .collect();
        support::run(&db, &format!("INSERT INTO many VALUES {}", rows.join(", ")));
    }
    support::run(&db, "DELETE FROM many");
    updated(&db, "wide", "", 0);
    let body = support::large_text(support::Kind::Scattered, 2, 8 << 20);
    db.execute("UPDATE wide SET body = $body", &params! { "body" => body })
        .unwrap();
    support::run(&db, "DELETE FROM wide");
    db.set_memory_limit(Some(16 << 20)).unwrap();
    for (purge, table, taken) in [
        ("PURGE FROM many WHERE n >= 0", "many", 10_000),
        ("PURGE FROM wide WHERE id = 1", "wide", 1),
    ] {
        let erased = db.execute(purge, &params! {}).unwrap().rows_affected();
        assert_eq!(erased, taken, "{purge}");
        let kept = db.row_versions(table, &[Value::Int64(1)], 4).unwrap();
        assert!(kept.rows.rows().is_empty(), "{purge}: no version left");
    }
    db.verify().expect("every address freed once");
}
