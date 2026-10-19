//! Validating a write's constraint keys is write working memory: the writer
//! budget funds it, never the statement's read budget, and the charge is what
//! the check actually allocates (the key and its encoded form).
use super::*;

const BODY_BYTES: usize = 9 * 1024 * 1024;

fn open(path: &std::path::Path, memory_limit: usize) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: crate::MaintenancePolicy::CallerDriven,
            memory_limit: Some(memory_limit),
            ..DatabaseOpenOptions::default()
        },
    )
    .unwrap()
}

fn body(params: &str) -> HashMap<String, Value> {
    HashMap::from([("body".into(), Value::Text(params.repeat(BODY_BYTES)))])
}

#[test]
fn a_unique_key_larger_than_the_read_limit_validates_on_the_writer_budget() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(
        &directory.path().join("writer-funded.db"),
        512 * 1024 * 1024,
    );
    db.execute(
        "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT UNIQUE)",
        &HashMap::new(),
    )
    .unwrap();
    // The key alone exceeds the statement's default read limit (16 MiB) once
    // built and encoded; the writer's 512 MiB funds its validation.
    db.execute("INSERT INTO notes VALUES (1, $body)", &body("k"))
        .unwrap();
    // A plain INSERT matching exactly one committed row on a single-column
    // UNIQUE is the engine's documented no-op: the duplicate is found only by
    // validating against the committed 9 MiB key.
    let duplicate = db
        .execute("INSERT INTO notes VALUES (2, $body)", &body("k"))
        .unwrap();
    assert_eq!(
        duplicate.rows_affected, 0,
        "validation found the committed key and stored nothing"
    );
    db.close().unwrap();
}

#[test]
fn a_constraint_key_the_writer_budget_cannot_fund_refuses_typed_with_nothing_allocated() {
    // Staging the 9 MiB row holds about 19 MiB of the writer; this fits that
    // but not the further 9 MiB its native key needs at validation.
    const WRITER_LIMIT: usize = 22 * 1024 * 1024;
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("writer-refused.db"), WRITER_LIMIT);
    db.execute(
        "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT UNIQUE)",
        &HashMap::new(),
    )
    .unwrap();
    let before = db.accountant().usage().used;
    let error = db
        .execute("INSERT INTO notes VALUES (1, $body)", &body("k"))
        .unwrap_err();
    assert!(
        matches!(&error, Error::MemoryBudgetExceeded {
            operation, requested_bytes, budget_limit_bytes, ..
        } if (operation == "staged_constraint_keys" || operation == "constraint_keys")
            && *requested_bytes >= BODY_BYTES
            && *budget_limit_bytes == WRITER_LIMIT),
        "the writer budget refuses the key's validation: {error:?}"
    );
    assert_eq!(
        db.accountant().usage().used,
        before,
        "the refused write leaves nothing charged"
    );
    assert_eq!(
        db.execute("SELECT COUNT(*) FROM notes", &HashMap::new())
            .unwrap()
            .rows[0][0],
        Value::Int64(0),
        "the refused write stored nothing"
    );
    db.close().unwrap();
}

/// A read budget whose charges a test can read back.
#[derive(Debug)]
struct CountingBudget {
    limit: usize,
    used: std::sync::atomic::AtomicUsize,
}

impl CountingBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn used(&self) -> usize {
        self.used.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl contextdb_core::read_memory::ReadMemoryBudget for CountingBudget {
    fn try_reserve(
        &self,
        bytes: usize,
        subsystem: &'static str,
        operation: &'static str,
        hint: &'static str,
    ) -> Result<()> {
        let used = self.used();
        if used.saturating_add(bytes) > self.limit {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: subsystem.to_string().into(),
                operation: operation.to_string().into(),
                requested_bytes: bytes,
                available_bytes: self.limit - used,
                budget_limit_bytes: self.limit,
                hint: hint.to_string().into(),
            });
        }
        self.used
            .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    fn release(&self, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

const WIDE: usize = 64;

fn wide_columns(width: usize) -> Vec<String> {
    (0..width).map(|column| format!("c{column}")).collect()
}

fn wide_table(db: &Database, width: usize) {
    let columns = wide_columns(width);
    let declared = columns
        .iter()
        .map(|column| format!("{column} BOOL"))
        .collect::<Vec<_>>()
        .join(", ");
    db.execute(
        &format!(
            "CREATE TABLE wide (id INT PRIMARY KEY, {declared}, UNIQUE ({}))",
            columns.join(", ")
        ),
        &HashMap::new(),
    )
    .unwrap();
}

/// The native key a constraint check builds holds one `DirectedValue` slot per
/// component, whatever each value's own estimate. A key of scalar components
/// is funded for those slots, not for a multiple of the values' estimates.
#[test]
fn a_constraint_key_charge_covers_the_native_key_it_builds() {
    for width in [1usize, WIDE, 1024] {
        let values = vec![Value::Bool(true); width];
        let columns = wide_columns(width)
            .into_iter()
            .map(|column| (column, contextdb_core::SortDirection::Asc))
            .collect::<Vec<_>>();
        let key = index_key_from_values(&columns, &values);
        let native = key
            .capacity()
            .saturating_mul(std::mem::size_of::<contextdb_core::DirectedValue>());
        assert!(
            writer_read::constraint_key_bytes(&values) >= native,
            "a {width}-component scalar key needs {native} bytes of slots; the check charges {}",
            writer_read::constraint_key_bytes(&values)
        );
    }
}

/// A staged constraint probe whose wide scalar key does not fit its funding
/// refuses, typed, before building the key, and leaves nothing charged.
#[test]
fn a_wide_scalar_staged_probe_refuses_before_it_builds_its_key() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("wide-staged.db"), 512 * 1024 * 1024);
    wide_table(&db, WIDE);
    let tx = db.begin().unwrap();
    let values = vec![Value::Bool(true); WIDE];
    let budget = CountingBudget::new(2048);
    let refused = db.writer_constraint_staged_match(
        tx,
        "wide",
        &wide_columns(WIDE),
        &values,
        None,
        budget.clone(),
    );
    assert!(
        matches!(&refused, Err(Error::MemoryBudgetExceeded { operation, requested_bytes, .. })
            if operation == "staged_constraint_keys"
                && *requested_bytes
                    >= WIDE * std::mem::size_of::<contextdb_core::DirectedValue>()),
        "the staged probe refuses its key's native size: {refused:?}"
    );
    assert_eq!(budget.used(), 0, "the refused probe retains nothing");
    db.rollback(tx).unwrap();
    db.close().unwrap();
}

/// The committed probe refuses the same way before building its key.
#[test]
fn a_wide_scalar_committed_probe_refuses_before_it_builds_its_key() {
    let directory = tempfile::tempdir().unwrap();
    let db = open(
        &directory.path().join("wide-committed.db"),
        512 * 1024 * 1024,
    );
    wide_table(&db, WIDE);
    let tx = db.begin().unwrap();
    let values = vec![Value::Bool(true); WIDE];
    let budget = CountingBudget::new(2048);
    let published = db.tx_mgr.capture_published_read();
    let refused = {
        let _scope = db.enter_writer_read(
            writer_read::durable_root(published.handle).unwrap(),
            budget.clone(),
        );
        db.writer_indexed_row_with_overlay(
            "wide",
            &wide_columns(WIDE),
            &values,
            published.snapshot,
            &HashSet::new(),
            None,
            Some((tx, None)),
            budget.clone(),
        )
    };
    assert!(
        matches!(&refused, Err(Error::MemoryBudgetExceeded { operation, requested_bytes, .. })
            if operation == "constraint_keys"
                && *requested_bytes
                    >= WIDE * std::mem::size_of::<contextdb_core::DirectedValue>()),
        "the committed probe refuses its key's native size: {refused:?}"
    );
    assert_eq!(budget.used(), 0, "the refused probe retains nothing");
    db.rollback(tx).unwrap();
    db.close().unwrap();
}

/// A multi-row commit over a wide scalar composite UNIQUE validates every row
/// on the writer budget, refuses a duplicate, and returns the writer to its
/// starting charge.
#[test]
fn a_multi_row_wide_scalar_commit_validates_each_row_and_releases_its_charge() {
    const ROWS: i64 = 200;
    const WIDTH: usize = 8;
    let directory = tempfile::tempdir().unwrap();
    let db = open(&directory.path().join("wide-commit.db"), 512 * 1024 * 1024);
    wide_table(&db, WIDTH);
    let row = |id: i64, pattern: i64| {
        let flags = (0..WIDTH)
            .map(|bit| {
                if (pattern >> bit) & 1 == 1 {
                    "TRUE"
                } else {
                    "FALSE"
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("INSERT INTO wide VALUES ({id}, {flags})")
    };
    let before = db.accountant().usage().used;
    db.execute("BEGIN", &HashMap::new()).unwrap();
    for id in 0..ROWS {
        db.execute(&row(id, id), &HashMap::new()).unwrap();
    }
    db.execute("COMMIT", &HashMap::new()).unwrap();
    assert_eq!(
        db.accountant().usage().used,
        before,
        "the committed rows' validation charges are all released"
    );
    // A new id carrying row 5's key collides on the composite UNIQUE.
    let duplicate = db.execute(&row(ROWS, 5), &HashMap::new()).unwrap_err();
    assert!(
        matches!(duplicate, Error::UniqueViolation { .. }),
        "a duplicate wide composite key is refused: {duplicate:?}"
    );
    db.close().unwrap();
}
