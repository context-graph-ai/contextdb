//! Declaring or dropping a maintained count on a table that already holds
//! rows: online, atomic, charged, and released.
//!
//! The declaration and the count records it implies are installed by the SAME
//! redb write transaction that installs the new `TableMeta`
//! (`persistence.rs`'s `rewrite_table_meta_rows_and_append_ddl_log`, which
//! calls `rebuild_relational_indexes_in_write` inside its own `begin_write`),
//! so a store is only ever in the old complete state or the new complete
//! state. The allocations that recompute makes are admitted through the
//! memory accountant before they are made, and a recompute that cannot fit is
//! the ordinary typed budget refusal with the declaration not installed.
//!
//! The crash arm of that claim is OBSERVED, at the end of this file: a child
//! process is aborted from inside that write transaction at each of its three
//! boundaries, and the store it leaves behind reopens on the whole old state
//! or the whole new one. The budget refusal below (a declaration that cannot
//! be admitted installs no part of itself) and the restart and
//! checkpoint-restore proof cover the two non-crash arms.
//!
//! This file also carries the two rules that are NOT policy: a count a
//! primary key or unique constraint already bounds is never maintained and
//! cannot be declared, and `.schema` renders only what was written — plus the
//! inspect leg, where an operator asks what is in force and what it costs.

use contextdb_core::Value;
use contextdb_engine::cli_render::{render_table_meta, render_table_meta_verbose};
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;
use std::path::Path;

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

fn count(db: &Database, sql: &str, params: HashMap<String, Value>) -> i64 {
    let result = db
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("count `{sql}`: {error}"));
    match &result.rows()[0][0] {
        Value::Int64(count) => *count,
        other => panic!("a count is an integer, got {other:?}"),
    }
}

/// `owner` and `(owner, slot)` are both index prefixes and neither is bounded
/// by a constraint, so both are declarable. `bounded_rows` exists so the
/// constraint-bounded refusal has something to refuse.
fn populated(path: &Path, rows: u128) -> Database {
    let db = Database::open(path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE placements (id UUID PRIMARY KEY, owner TEXT NOT NULL, slot TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_owner_slot ON placements (owner, slot)",
        HashMap::new(),
    );
    for n in 0..rows {
        execute(
            &db,
            "INSERT INTO placements (id, owner, slot) VALUES ($id, $owner, $slot)",
            HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "owner".into(),
                    Value::Text(if n % 4 == 0 {
                        "ada".into()
                    } else {
                        "bob".into()
                    }),
                ),
                (
                    "slot".into(),
                    Value::Text(if n % 2 == 0 {
                        "left".into()
                    } else {
                        "right".into()
                    }),
                ),
            ]),
        );
    }
    db
}

fn by_owner(owner: &str) -> HashMap<String, Value> {
    HashMap::from([("owner".into(), Value::Text(owner.into()))])
}

const BY_OWNER: &str = "SELECT COUNT(*) FROM placements WHERE owner = $owner";
const BY_OWNER_SLOT: &str = "SELECT COUNT(*) FROM placements WHERE owner = $owner AND slot = $slot";

fn by_owner_slot(owner: &str, slot: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("owner".into(), Value::Text(owner.into())),
        ("slot".into(), Value::Text(slot.into())),
    ])
}

#[test]
fn declaring_a_count_on_a_table_with_rows_is_correct_immediately() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = populated(&root.path().join("declare-over-rows.db"), 40);

    assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 10);
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)",
        HashMap::new(),
    );
    // No reopen, no rebuild step a caller has to ask for: the same session
    // answers from the counts the declaring transaction recomputed.
    assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 10);
    assert_eq!(count(&db, BY_OWNER, by_owner("bob")), 30);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")), 10);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("bob", "left")), 10);

    // Writes after the declaration keep it current.
    execute(
        &db,
        "INSERT INTO placements (id, owner, slot) VALUES ($id, 'ada', 'left')",
        HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(9_000)))]),
    );
    assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 11);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")), 11);

    // Dropping one list leaves the other exactly as it was, and both answers
    // stay exact.
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (owner)",
        HashMap::new(),
    );
    assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 11);
    assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")), 11);

    // And clearing the declaration entirely returns the table to silence.
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT DEFAULT",
        HashMap::new(),
    );
    let meta = db.table_meta("placements").expect("the table still exists");
    assert_eq!(meta.maintained_counts, None);
    assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 11);

    db.close().expect("close the fixture");
}

#[test]
fn a_declaration_a_memory_limit_cannot_admit_installs_nothing() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("declare-under-budget.db");
    let db = populated(&path, 200);

    // A limit this tight cannot hold the table's own retained image, so the
    // refusal may land on the limit statement or on the declaration that has
    // to project under it. Either way the claim is the same: the refusal is
    // the ordinary typed budget refusal, and nothing is installed.
    let limit = db.execute("SET MEMORY_LIMIT '1K'", &HashMap::new());
    let declaring = db.execute(
        "ALTER TABLE placements SET MAINTAIN COUNT (owner)",
        &HashMap::new(),
    );
    let refusal = limit
        .err()
        .or_else(|| declaring.err())
        .expect("a one-kilobyte budget refuses the limit or the declaration under it");
    let text = refusal.to_string();
    assert!(
        text.to_ascii_lowercase().contains("memory")
            || text.to_ascii_lowercase().contains("budget"),
        "the refusal is the ordinary typed budget refusal, not a corrupt-store report: {text}"
    );
    let _ = db.execute("SET MEMORY_LIMIT 'none'", &HashMap::new());
    let meta = db.table_meta("placements").expect("the table still exists");
    assert_eq!(
        meta.maintained_counts, None,
        "a refused declaration applies no part of itself"
    );
    assert_eq!(
        count(&db, BY_OWNER, by_owner("ada")),
        50,
        "and the table still answers exactly, by the ordinary route"
    );
    db.close().expect("close the fixture");
}

#[test]
fn a_count_a_key_already_bounds_is_never_maintained() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join("constraint-bounded.db"))
        .expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE bounded_rows (id UUID PRIMARY KEY, owner TEXT NOT NULL, \
         slot TEXT NOT NULL, UNIQUE (owner, slot))",
        HashMap::new(),
    );

    for list in ["(id)", "(owner, slot)"] {
        let refused = db.execute(
            &format!("ALTER TABLE bounded_rows SET MAINTAIN COUNT {list}"),
            &HashMap::new(),
        );
        let text = refused
            .expect_err(&format!("MAINTAIN COUNT {list} is refused"))
            .to_string();
        assert!(
            text.contains("bounded at 0 or 1"),
            "the refusal says why, and points at the range that answers it: {text}"
        );
    }

    // `ALL PREFIXES` does not resurrect them either: the constraint rule is a
    // correctness rule, not a policy the declaration can override.
    execute(
        &db,
        "ALTER TABLE bounded_rows SET MAINTAIN COUNT ALL PREFIXES",
        HashMap::new(),
    );
    contextdb_engine::append_work::reset();
    execute(
        &db,
        "INSERT INTO bounded_rows (id, owner, slot) VALUES ($id, 'ada', 'left')",
        HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(1)))]),
    );
    let counts = contextdb_engine::append_work::take_index_prefix_counts();
    assert!(
        counts.iter().all(|touch| !touch.uniquely_constrained),
        "no count a declared constraint bounds is maintained, even under ALL PREFIXES: {counts:?}"
    );

    // A list no index leads with is refused too: a count nothing can read is
    // not a thing to maintain.
    let text = db
        .execute(
            "ALTER TABLE bounded_rows SET MAINTAIN COUNT (slot)",
            &HashMap::new(),
        )
        .expect_err("a list no index leads with is refused")
        .to_string();
    assert!(
        text.contains("no index leads with") && text.contains("indexes considered"),
        "the refusal names the list and the indexes considered: {text}"
    );

    db.close().expect("close the fixture");
}

#[test]
fn the_schema_renders_only_what_was_declared() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = populated(&root.path().join("schema-render.db"), 4);

    let undeclared = render_table_meta(
        "placements",
        &db.table_meta("placements").expect("the table exists"),
    );
    assert!(
        !undeclared.to_ascii_uppercase().contains("MAINTAIN COUNT"),
        "a table that declares nothing renders exactly as it did before this vocabulary \
         existed: {undeclared}"
    );

    for (statement, rendered) in [
        (
            "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)",
            "MAINTAIN COUNT (owner), (owner, slot)",
        ),
        (
            "ALTER TABLE placements SET MAINTAIN COUNT ALL PREFIXES",
            "MAINTAIN COUNT ALL PREFIXES",
        ),
        (
            "ALTER TABLE placements SET MAINTAIN COUNT NONE",
            "MAINTAIN COUNT NONE",
        ),
    ] {
        execute(&db, statement, HashMap::new());
        let meta = db.table_meta("placements").expect("the table exists");
        let schema = render_table_meta("placements", &meta);
        assert!(
            schema.contains(rendered),
            "`{statement}` renders back as `{rendered}`: {schema}"
        );
        // The rendered text is the declaration, so it re-parses to the same
        // thing the operator wrote.
        let create_table = schema
            .split_once(";\n")
            .map(|(head, _)| head.to_owned())
            .unwrap_or_else(|| schema.clone());
        contextdb_parser::parse(&create_table)
            .unwrap_or_else(|error| panic!("`.schema` re-parses: {error}\n{create_table}"));
        assert!(
            render_table_meta_verbose("placements", &meta).contains(rendered),
            "verbose rendering carries the declaration too"
        );
    }

    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT DEFAULT",
        HashMap::new(),
    );
    let cleared = render_table_meta(
        "placements",
        &db.table_meta("placements").expect("the table exists"),
    );
    assert_eq!(
        cleared, undeclared,
        "clearing the declaration returns the table to the byte-identical silent rendering"
    );

    db.close().expect("close the fixture");
}

/// Silence in `.schema` means the table uses defaults; an operator
/// who asks what is actually in force is told, including when nothing was
/// declared, and is told what each maintained list currently costs to keep.
#[test]
fn verbose_schema_states_the_value_in_force_and_what_it_holds() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = populated(&root.path().join("verbose-inventory.db"), 40);

    let silent = render_table_meta_verbose(
        "placements",
        &db.table_meta("placements").expect("the table exists"),
    );
    assert!(
        silent.contains(
            "-- effective MAINTAIN COUNT NONE (engine default; this table declared none)"
        ),
        "an operator can see the effective value of a table that declared nothing: {silent}"
    );
    assert!(
        contextdb_engine::cli_render::render_table_meta_verbose_with_counts(&db, "placements")
            .expect("verbose schema with stored counts")
            == silent,
        "and a table that maintains nothing holds no count records to report"
    );

    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)",
        HashMap::new(),
    );
    let declared = render_table_meta_verbose(
        "placements",
        &db.table_meta("placements").expect("the table exists"),
    );
    assert!(
        declared.contains("-- effective MAINTAIN COUNT (owner), (owner, slot)")
            && !declared.contains("engine default"),
        "a declared table states its own value, not the default: {declared}"
    );
    assert!(
        declared.contains("-- MAINTAIN COUNT (owner) is maintained by: placements_by_owner_slot"),
        "and names the indexes whose writes keep each list current: {declared}"
    );

    // 40 rows: two owners, and three owner/slot pairs that actually occur —
    // `ada` rows are all even, so `ada` never pairs with `right`.
    let inventory =
        contextdb_engine::cli_render::render_table_meta_verbose_with_counts(&db, "placements")
            .expect("verbose schema with stored counts");
    assert!(
        inventory.contains("-- MAINTAIN COUNT (owner) holds 2 stored prefix value(s)"),
        "the inventory reports what each list actually holds: {inventory}"
    );
    assert!(
        inventory.contains("-- MAINTAIN COUNT (owner, slot) holds 3 stored prefix value(s)"),
        "{inventory}"
    );

    // `ALL PREFIXES` is a value in force too, and its inventory enumerates
    // every prefix it actually maintains rather than the words it was written
    // with.
    execute(
        &db,
        "ALTER TABLE placements SET MAINTAIN COUNT ALL PREFIXES",
        HashMap::new(),
    );
    let all_prefixes =
        contextdb_engine::cli_render::render_table_meta_verbose_with_counts(&db, "placements")
            .expect("verbose schema with stored counts");
    assert!(
        all_prefixes.contains("-- MAINTAIN COUNT (owner) holds 2 stored prefix value(s)")
            && all_prefixes
                .contains("-- MAINTAIN COUNT (owner, slot) holds 3 stored prefix value(s)"),
        "ALL PREFIXES reports the prefixes it maintains: {all_prefixes}"
    );

    db.close().expect("close the fixture");
}

#[test]
fn a_maintained_count_survives_restart_and_checkpoint_restore() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = root.path().join("restart.db");
    {
        let db = populated(&path, 40);
        execute(
            &db,
            "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)",
            HashMap::new(),
        );
        db.close().expect("close the writer");
    }

    let reopened = Database::open(&path).expect("reopen the store");
    reopened.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let meta = reopened.table_meta("placements").expect("the table exists");
    assert!(
        render_table_meta("placements", &meta).contains("MAINTAIN COUNT (owner), (owner, slot)"),
        "the declaration survives a restart"
    );
    assert_eq!(count(&reopened, BY_OWNER, by_owner("ada")), 10);
    assert_eq!(
        count(&reopened, BY_OWNER_SLOT, by_owner_slot("ada", "left")),
        10
    );

    let checkpoint = root.path().join("restart-checkpoint.db");
    reopened
        .export_snapshot(&checkpoint)
        .expect("export a checkpoint");
    reopened.close().expect("close the reopened store");

    let restored = Database::open(&checkpoint).expect("open the restored checkpoint");
    restored.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let restored_meta = restored.table_meta("placements").expect("the table exists");
    assert_eq!(
        restored_meta.maintained_counts, meta.maintained_counts,
        "the declaration survives checkpoint/restore"
    );
    assert_eq!(count(&restored, BY_OWNER, by_owner("ada")), 10);
    assert_eq!(
        count(&restored, BY_OWNER_SLOT, by_owner_slot("ada", "left")),
        10,
        "and so do the records it implies"
    );
    restored.close().expect("close the restored store");
}

/// A process killed inside the declaration leaves the whole old state or the
/// whole new one — observed, not argued.
///
/// The child arms one of the three boundaries the metadata rewrite carries
/// (`persistence::arm_table_meta_rewrite_crash_for_test`) and runs the
/// `ALTER`; the boundary aborts the process from inside the write. `1` and `2`
/// are inside the transaction, so the store must reopen on the OLD complete
/// state; `3` is immediately after `commit()` returned, so it must reopen on
/// the NEW complete state. "Complete" is checked both ways round: the
/// declaration and the count records it implies agree, and every `COUNT(*)`
/// answers exactly whichever state survived.
#[cfg(unix)]
mod declaration_crash {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{Command, Stdio};

    /// `std::process::abort()` raises SIGABRT. Any other signal, or a plain
    /// exit, means the child died of something this test did not arm.
    const SIGABRT: i32 = 6;

    const CHILD_PATH: &str = "CONTEXTDB_MAINTAINED_COUNT_CRASH_PATH";
    const CHILD_BOUNDARY: &str = "CONTEXTDB_MAINTAINED_COUNT_CRASH_BOUNDARY";
    const CHILD_ACTION: &str = "CONTEXTDB_MAINTAINED_COUNT_CRASH_ACTION";

    const DECLARE: &str = "ALTER TABLE placements SET MAINTAIN COUNT (owner), (owner, slot)";
    const DROP: &str = "ALTER TABLE placements SET MAINTAIN COUNT DEFAULT";

    fn owner_list() -> Vec<(String, contextdb_core::SortDirection)> {
        vec![("owner".to_string(), contextdb_core::SortDirection::Asc)]
    }

    fn owner_slot_list() -> Vec<(String, contextdb_core::SortDirection)> {
        vec![
            ("owner".to_string(), contextdb_core::SortDirection::Asc),
            ("slot".to_string(), contextdb_core::SortDirection::Asc),
        ]
    }

    /// The store after a crash, read back: what it declares, and how many
    /// prefix values it actually holds for each of the two lists.
    fn surviving_state(path: &Path) -> (bool, u64, u64) {
        let db = Database::open(path).expect("the crashed store reopens");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        let declared = db
            .table_meta("placements")
            .expect("the table survives either way")
            .maintained_counts
            .is_some();
        let owner_values = db
            .stored_maintained_count_values("placements", &owner_list())
            .expect("the reopened store reports what it holds");
        let owner_slot_values = db
            .stored_maintained_count_values("placements", &owner_slot_list())
            .expect("the reopened store reports what it holds");
        // Whichever state survived, every answer is still exact.
        assert_eq!(count(&db, BY_OWNER, by_owner("ada")), 100);
        assert_eq!(count(&db, BY_OWNER, by_owner("bob")), 300);
        assert_eq!(count(&db, BY_OWNER_SLOT, by_owner_slot("ada", "left")), 100);
        assert_eq!(
            count(&db, BY_OWNER_SLOT, by_owner_slot("bob", "right")),
            200
        );
        db.close().expect("close the reopened store");
        (declared, owner_values, owner_slot_values)
    }

    /// `400` rows over two owners and three occurring owner/slot pairs: the
    /// complete declared state holds 2 prefix values for `(owner)` and 3 for
    /// `(owner, slot)`, and the complete undeclared state holds none.
    const DECLARED_STATE: (bool, u64, u64) = (true, 2, 3);
    const UNDECLARED_STATE: (bool, u64, u64) = (false, 0, 0);

    fn crash_at(boundary: u8, action: &str) -> (bool, u64, u64) {
        let root = tempfile::tempdir().expect("task-scoped fixture root");
        let path = root.path().join(format!("crash-{action}-{boundary}.db"));
        {
            let db = populated(&path, 400);
            if action == "drop" {
                execute(&db, DECLARE, HashMap::new());
            }
            db.close().expect("close the seeding writer");
        }
        let module = module_path!()
            .split_once("::")
            .expect("an integration test module path")
            .1;
        let done = Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--exact",
                &format!("{module}::the_crashing_child"),
                "--ignored",
                "--nocapture",
            ])
            .env(CHILD_PATH, &path)
            .env(CHILD_BOUNDARY, boundary.to_string())
            .env(CHILD_ACTION, action)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::piped())
            .output()
            .expect("run the declaring child to its crash");
        // A non-zero exit is NOT evidence of this kill. For every boundary
        // whose expected outcome is the PRE-crash state, a child that died
        // before it reached the write -- an unopenable store, a filter that
        // matched no test, a bad environment value -- leaves exactly that
        // state and would read as "the crash was survived". So the kill has
        // to identify itself: the armed boundary aborts, and says which one.
        let stderr = String::from_utf8_lossy(&done.stderr);
        let marker = format!("TABLE_META_REWRITE_CRASH={boundary}");
        assert!(
            stderr.contains(&marker),
            "the child must die AT the armed boundary and say so; \
             stderr had no `{marker}`:\n{stderr}"
        );
        assert_eq!(
            done.status.signal(),
            Some(SIGABRT),
            "the armed boundary aborts; any other death is a different \
             failure wearing this test's result: {:?}",
            done.status
        );
        surviving_state(&path)
    }

    #[test]
    fn a_crash_before_the_counts_are_rebuilt_keeps_the_whole_old_state() {
        assert_eq!(crash_at(1, "declare"), UNDECLARED_STATE);
        assert_eq!(crash_at(1, "drop"), DECLARED_STATE);
    }

    #[test]
    fn a_crash_after_the_counts_are_rebuilt_but_before_commit_keeps_the_whole_old_state() {
        assert_eq!(crash_at(2, "declare"), UNDECLARED_STATE);
        assert_eq!(crash_at(2, "drop"), DECLARED_STATE);
    }

    #[test]
    fn a_crash_the_instant_the_commit_returned_has_the_whole_new_state() {
        assert_eq!(crash_at(3, "declare"), DECLARED_STATE);
        assert_eq!(crash_at(3, "drop"), UNDECLARED_STATE);
    }

    /// The child half of the three tests above. Ignored in an ordinary run;
    /// each parent names it exactly and passes `--ignored`.
    #[test]
    #[ignore = "child process of the declaration-crash tests"]
    fn the_crashing_child() {
        let Ok(path) = std::env::var(CHILD_PATH) else {
            return;
        };
        let boundary: u8 = std::env::var(CHILD_BOUNDARY)
            .expect("the parent names a boundary")
            .parse()
            .expect("a boundary is 1, 2 or 3");
        let action = std::env::var(CHILD_ACTION).expect("the parent names an action");
        let db = Database::open(&path).expect("the child opens the store the parent seeded");
        db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        db.arm_table_meta_rewrite_crash_for_test(boundary);
        let sql = if action == "drop" { DROP } else { DECLARE };
        let outcome = db.execute(sql, &HashMap::new());
        panic!("the armed boundary did not abort this process: {outcome:?}");
    }
}
