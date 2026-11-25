//! The only shared test code. Each test file says `mod support;`.
//!
//! It holds what every test needs to stay exact: one store folder per test, the one way a store
//! is opened, a table whose rows are a function of their number, vectors that are a function of
//! their row's number with the answer every similarity search is held to, a storage door that
//! lets a test watch the data file, and a second process spoken to over its pipes.

// Each test program uses a part of this module; the rest would be reported as never used.
#![allow(dead_code)]

mod process;
pub mod records;
pub mod report;
pub use process::*;
pub use report::*;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::fs;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use contextdb::{
    Clock, ContextId, Database, DatabaseOpenOptions, Error, Maintenance, Principal, QueryResult,
    ReadLimits, ReadProgress, ReadProgressObserver, ReadSession, ReadSessionOptions, ScopeLabel,
    StorageBackend, Transaction, Uuid, Value, WrapStorage, params,
};

// ---- folders and opening ----

/// An empty folder for one test, under the build's temporary directory. It is left behind when
/// the test ends, so that a failure can be looked at.
pub fn folder(name: &str) -> PathBuf {
    let folder = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    if folder.exists() {
        fs::remove_dir_all(&folder).expect("clear the folder an earlier run left");
    }
    fs::create_dir_all(&folder).expect("create the test's folder");
    folder
}

/// The names in a folder, sorted.
pub fn listing(folder: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(folder)
        .expect("read the folder")
        .map(|entry| {
            entry
                .expect("read the folder")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

/// Opens a store for writing with no configuration. It must open.
pub fn open(folder: &Path) -> Database {
    open_with(folder, DatabaseOpenOptions::default()).expect("open the store for writing")
}

/// Opens a store for writing. Every test opens its stores here, so that an option every test
/// needs is set in this one place: maintenance runs only in the cycles a test calls.
pub fn open_with(folder: &Path, mut options: DatabaseOpenOptions) -> contextdb::Result<Database> {
    options.maintenance = contextdb::Maintenance::Caller;
    Database::open_with_options(folder, options)
}

/// Closes a store and gives up the handle, so that the folder is free for the next open.
pub fn close(db: Database) {
    db.close().expect("close the store");
}

/// Closes a store and opens it again with no configuration.
pub fn reopen(db: Database, folder: &Path) -> Database {
    close(db);
    open(folder)
}

pub mod opening;

/// The storage library's bounded freed-page workspace.
pub fn limit_commit_bytes() -> usize {
    redb::WriteTransaction::DRAIN_BYTES
}

/// Spare for changing a limit and observing it, beside the data room a test specifies.
pub fn limit_work_bytes() -> usize {
    limit_commit_bytes().max(statement_text_bytes("SHOW MEMORY_LIMIT"))
}

/// Opens a store for writing under a memory limit of this many bytes. It must open.
pub fn open_limited(folder: &Path, limit: usize) -> Database {
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(limit);
    open_with(folder, options).unwrap_or_else(|error| {
        panic!("an open under a limit of {limit} bytes was refused: {error:?}")
    })
}

/// Opens a roomy reader under the given memory cap, or 512 MiB for `None`. It must open.
pub fn open_reader(folder: &Path, limit: Option<usize>) -> ReadSession {
    let options = reader_options(limit);
    ReadSession::open_with_options(folder, options).unwrap_or_else(|error| {
        panic!("a reader under a limit of {limit:?} was refused: {error:?}")
    })
}

/// Roomy limits; `None` selects 512 MiB, not the shipped default. A given cap stays exact.
pub fn reader_options(limit: Option<usize>) -> ReadSessionOptions {
    let mut options = ReadSessionOptions::default();
    options.limits.result_rows = u64::MAX;
    options.limits.result_bytes = u64::MAX;
    options.limits.work = u64::MAX;
    options.limits.active_ms = u64::MAX;
    options.limits.memory = limit.unwrap_or(512 << 20) as u64;
    options
}
pub fn read_session(
    db: &Database,
    path: &Path,
    file: bool,
    limits: ReadLimits,
) -> contextdb::Result<ReadSession> {
    if file {
        ReadSession::open(path, limits)
    } else {
        db.read_session(limits)
    }
}
/// What the statement record holds for one form of a statement once it has run: a line of
/// 144 bytes (the form's text at 16, seven counts at 8 each, the unused-index note at 40 and
/// the line's own charge at 32) and the text of the form, constants as `?`.
pub fn form_charge(form: &str) -> usize {
    144 + form.len()
}
/// A statement's form: its text with each number and each quoted text written in it as `?`.
pub fn form_of(sql: &str) -> String {
    let mut form = String::new();
    let (mut digits, mut quoted) = (false, false);
    for c in sql.chars() {
        if c == '\'' {
            if !quoted {
                form.push('?');
            }
            quoted = !quoted;
        } else if quoted {
        } else if c.is_ascii_digit() {
            if !digits {
                form.push('?');
            }
            digits = true;
        } else {
            digits = false;
            form.push(c);
        }
    }
    form
}
pub fn used(db: &Database) -> usize {
    db.memory_used().unwrap()
}
pub fn limit(db: &Database) -> Option<usize> {
    db.memory_limit().unwrap()
}
pub fn names(db: &Database) -> Vec<String> {
    db.table_names().unwrap().to_vec()
}
pub fn watermark(db: &Database) -> contextdb::TxId {
    db.committed_watermark().unwrap()
}

// ---- statements ----

/// Whatever runs a statement: the writer, one of its transactions, or a reader.
pub trait Run {
    fn run(&self, sql: &str, params: &HashMap<String, Value>) -> contextdb::Result<QueryResult>;
}

impl Run for Database {
    fn run(&self, sql: &str, params: &HashMap<String, Value>) -> contextdb::Result<QueryResult> {
        self.execute(sql, params)
    }
}

impl Run for Transaction<'_> {
    fn run(&self, sql: &str, params: &HashMap<String, Value>) -> contextdb::Result<QueryResult> {
        self.execute(sql, params)
    }
}

impl Run for ReadSession {
    fn run(&self, sql: &str, params: &HashMap<String, Value>) -> contextdb::Result<QueryResult> {
        self.execute(sql, params)
    }
}

/// A reference to something that runs a statement runs it too, so a test can hand the writer,
/// a transaction or a reader to one function as `&dyn Run`.
impl<R: Run + ?Sized> Run for &R {
    fn run(&self, sql: &str, params: &HashMap<String, Value>) -> contextdb::Result<QueryResult> {
        (**self).run(sql, params)
    }
}

/// Runs a statement that must succeed.
pub fn run(on: &impl Run, sql: &str) -> QueryResult {
    match on.run(sql, &params! {}) {
        Ok(result) => result,
        Err(error) => panic!("{sql}\nwas refused: {error:?}"),
    }
}

/// Parsing plus planning's one copy (M5), measured through the parse refusal on an empty
/// reader with no spare bytes. The reader is shared only by fixtures in this test process.
pub fn statement_text_bytes(sql: &str) -> usize {
    static READER: std::sync::OnceLock<ReadSession> = std::sync::OnceLock::new();
    let reader = READER.get_or_init(|| {
        let folder = folder(&format!("statement-charge-{}", std::process::id()));
        close(open(&folder));
        let used = opening::reader_charge(&folder, Some(32 << 20));
        open_reader(&folder, Some(used))
    });
    match reader.execute(sql, &params! {}) {
        Err(Error::MemoryBudgetExceeded {
            operation: "SQL parsing",
            requested_bytes,
            ..
        }) => 2 * usize::try_from(requested_bytes).expect("a statement's charge fits usize"),
        other => panic!("text admission with no spare bytes gave {other:?}"),
    }
}

/// The rows a statement returns.
pub fn rows(on: &impl Run, sql: &str) -> Vec<Vec<Value>> {
    run(on, sql).rows().to_vec()
}

/// The one integer a statement returns.
pub fn int(on: &impl Run, sql: &str) -> i64 {
    match rows(on, sql).as_slice() {
        [row] => match row.as_slice() {
            [Value::Int64(number)] => *number,
            other => panic!("{sql}\nreturned {other:?}, not one integer"),
        },
        other => panic!("{sql}\nreturned {} rows, not one", other.len()),
    }
}

/// The refusal of a statement that must be refused.
pub fn refusal(on: &impl Run, sql: &str) -> Error {
    match on.run(sql, &params! {}) {
        Err(error) => error,
        Ok(result) => panic!("{sql}\nwas not refused: {result:?}"),
    }
}

/// One column of what `SHOW MEMORY_LIMIT` reports.
pub fn memory_shown(on: &impl Run, column: &str) -> Value {
    let report = run(on, "SHOW MEMORY_LIMIT");
    let at = report
        .columns()
        .iter()
        .position(|name| name == column)
        .unwrap_or_else(|| panic!("SHOW MEMORY_LIMIT has no column {column}"));
    match report.rows() {
        [row] => row[at].clone(),
        rows => panic!("SHOW MEMORY_LIMIT returned {} rows", rows.len()),
    }
}

/// Asserts that `limit` is the default a handle with no limit declared takes (M4): a quarter of
/// the memory the process may use, the smallest of `MemTotal` and every cgroup `memory.max`
/// from its own group up to the root, 2 GiB where that is unknown, never below a writer's floor
/// on a store under 4 GiB, 10 MiB and 768 KiB.
#[track_caller]
pub fn assert_default_limit(limit: Option<usize>, when: &str) {
    let read = |path: &str| fs::read_to_string(path).unwrap_or_default();
    let number = |text: &str| {
        text.trim()
            .trim_end_matches("kB")
            .trim()
            .parse::<usize>()
            .ok()
    };
    let meminfo = read("/proc/meminfo");
    let total = meminfo
        .lines()
        .find_map(|line| number(line.strip_prefix("MemTotal:")?));
    let cgroup = read("/proc/self/cgroup");
    let group = cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .unwrap_or("");
    let groups = Path::new(group)
        .ancestors()
        .map(|at| at.to_string_lossy().into_owned());
    let maxes = groups.filter_map(|at| number(&read(&format!("/sys/fs/cgroup{at}/memory.max"))));
    let allowed = total.map(|kib| kib * 1024).into_iter().chain(maxes).min();
    if cfg!(target_os = "linux") {
        let quarter = allowed.map_or(2 << 30, |bytes| bytes / 4);
        assert_eq!(
            limit,
            Some(quarter.max((10 << 20) + (768 << 10))),
            "{when}: the default"
        );
    } else {
        assert!(limit.is_some(), "{when}: the default holds, never none");
    }
}

/// The bytes `SHOW MEMORY_LIMIT` reports as used.
pub fn memory_used(on: &impl Run) -> usize {
    match memory_shown(on, "used") {
        Value::Int64(bytes) => bytes as usize,
        other => panic!("SHOW MEMORY_LIMIT reports {other:?} as used"),
    }
}

/// The first column of every row a statement returns, which must be an INTEGER.
pub fn ids(on: &impl Run, sql: &str) -> Vec<i64> {
    rows(on, sql)
        .iter()
        .map(|row| match row.first() {
            Some(Value::Int64(id)) => *id,
            other => panic!("{sql}\nreturned {other:?} where an INTEGER was wanted"),
        })
        .collect()
}

/// What a table is printed back as.
pub fn printed(db: &Database, table: &str) -> String {
    db.declaration(table)
        .unwrap_or_else(|error| panic!("the declaration of {table} was refused: {error:?}"))
        .to_string()
}

/// Asserts that what these tables are printed back as declares the same tables in an empty
/// store (made under `prefix`), printed the same. The tables are named in the order they may be
/// declared in. Gives back the printed texts, in that order.
pub fn assert_roundtrips(prefix: &str, db: &Database, tables: &[&str]) -> Vec<String> {
    let texts: Vec<String> = tables.iter().map(|table| printed(db, table)).collect();
    let again = open(&folder(&format!("{prefix}-again")));
    for text in &texts {
        for statement in text.split_inclusive(";\n") {
            run(&again, statement);
        }
    }
    for (table, text) in tables.iter().zip(&texts) {
        let declared = again.declaration(table).expect("the declaration");
        assert_eq!(
            declared.as_str(),
            text.as_str(),
            "{prefix}: {table} declared from its printed text"
        );
    }
    texts
}

// ---- who is asking ----

/// Context number `n`.
pub fn context(n: u128) -> Uuid {
    Uuid::from_u128(0x00C0_0000 + n)
}

/// Access-control id number `n`.
pub fn acl(n: u128) -> Uuid {
    Uuid::from_u128(0x00AC_0000 + n)
}

/// A handle narrowed to the Contexts with these numbers.
pub fn in_contexts(db: &Database, numbers: &[u128]) -> Database {
    db.scoped_with_contexts(context_set(numbers))
}

/// The length of the store's data file, in bytes.
pub fn data_file(folder: &Path) -> u64 {
    fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len()
}

/// The keys of a search's answer, in its order; each must be an INTEGER.
pub fn near_ids(answer: &[Near]) -> Vec<i64> {
    answer
        .iter()
        .map(|near| match near.key {
            Value::Int64(id) => id,
            ref other => panic!("the key {other:?} is not an INTEGER"),
        })
        .collect()
}

/// The Contexts with these numbers.
pub fn context_set(numbers: &[u128]) -> BTreeSet<ContextId> {
    numbers
        .iter()
        .map(|n| ContextId::new(context(*n)))
        .collect()
}

/// The scope labels with these names.
pub fn label_set(names: &[&str]) -> BTreeSet<ScopeLabel> {
    names.iter().map(|name| ScopeLabel::new(*name)).collect()
}

/// A handle narrowed to these scope labels.
pub fn with_labels(db: &Database, names: &[&str]) -> Database {
    db.scoped_with_constraints(None, Some(label_set(names)), None)
}

/// A handle that names this principal and nothing else.
pub fn named(db: &Database, principal: Principal) -> Database {
    db.scoped_with_constraints(None, None, Some(principal))
}

pub fn agent(name: &str) -> Principal {
    Principal::Agent(name.to_owned())
}

pub fn human(name: &str) -> Principal {
    Principal::Human(name.to_owned())
}

/// The documented grant table.
pub const GRANTS: &str = "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, \
                          principal_id TEXT, acl_id UUID)";

/// Grants a principal one access-control id. `number` makes the grant row's own key.
pub fn grant(db: &Database, number: u128, kind: &str, name: &str, acl_id: Uuid) {
    let values = params! {
        "id" => Uuid::from_u128(0x0060_0000 + number), "kind" => kind, "name" => name,
        "acl" => acl_id,
    };
    let sql = "INSERT INTO acl_grants (id, principal_kind, principal_id, acl_id) \
               VALUES ($id, $kind, $name, $acl)";
    if let Err(error) = db.execute(sql, &values) {
        panic!("granting {acl_id} to {kind} {name} was refused: {error:?}");
    }
}

// ---- the table most tests use ----

/// Row `n` of this table is `(n, "row-000000n", n % 10)`: see [`row`].
pub const TABLE: &str =
    "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL, tag INTEGER NOT NULL)";

/// The index that every check of the table also reads through.
pub const INDEX: &str = "CREATE INDEX t_tag ON t (tag)";

/// Every column of the table, in key order.
pub const SCAN: &str = "SELECT id, name, tag FROM t ORDER BY id";

/// Declares the table and its index.
pub fn declare(db: &Database) {
    run(db, TABLE);
    run(db, INDEX);
}

/// [`declare`] keeping no old versions (a table that declares no history keeps them); a synced
/// `CURRENT ONLY` table keeps the latest write.
pub fn declare_current_only(db: &Database) {
    let current_only = format!("{TABLE} HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST");
    run(db, &current_only);
    run(db, INDEX);
}

/// The threads of this process, as Linux lists them; 0 on a system that has no such list.
pub fn threads() -> usize {
    std::fs::read_dir("/proc/self/task").map_or(0, |tasks| tasks.count())
}

/// Row number `n`, as it is stored and as it must come back.
pub fn row(n: i64) -> Vec<Value> {
    vec![
        Value::Int64(n),
        Value::Text(format!("row-{n:07}")),
        Value::Int64(n % 10),
    ]
}

/// Inserts the rows with these numbers, one statement each: on a `Database` that is one commit
/// per row, on a `Transaction` they commit together.
pub fn insert(on: &impl Run, numbers: impl IntoIterator<Item = i64>) {
    for n in numbers {
        let [id, name, tag] = <[Value; 3]>::try_from(row(n)).expect("a row has three columns");
        let values = params! { "id" => id, "name" => name, "tag" => tag };
        if let Err(error) = on.run(
            "INSERT INTO t (id, name, tag) VALUES ($id, $name, $tag)",
            &values,
        ) {
            panic!("inserting row {n} was refused: {error:?}");
        }
    }
}

/// Inserts the rows with these numbers in one transaction and commits it.
pub fn insert_together(db: &Database, numbers: impl IntoIterator<Item = i64>) {
    let tx = db.transaction().expect("begin");
    insert(&tx, numbers);
    tx.commit().expect("commit");
}

/// Asserts that the table holds exactly the rows with these numbers, each one whole, and that
/// the kept count and the index say the same as the rows.
pub fn assert_rows_are(on: &impl Run, numbers: RangeInclusive<i64>) {
    let wanted: Vec<Vec<Value>> = numbers.clone().map(row).collect();
    assert_same_rows(&rows(on, SCAN), &wanted, "the rows of t");
    assert_eq!(
        int(on, "SELECT COUNT(*) FROM t"),
        wanted.len() as i64,
        "the kept count of t"
    );
    for tag in 0..10 {
        let ids: Vec<Vec<Value>> = numbers
            .clone()
            .filter(|n| n % 10 == tag)
            .map(|n| vec![Value::Int64(n)])
            .collect();
        let found = rows(
            on,
            &format!("SELECT id FROM t WHERE tag = {tag} ORDER BY id"),
        );
        assert_same_rows(
            &found,
            &ids,
            &format!("the rows the index gives for tag {tag}"),
        );
    }
}

/// Asserts that two lists of rows are the same. A failure shows how many rows each has and the
/// first place they differ, not two whole tables.
pub fn assert_same_rows<F, W>(found: &[F], wanted: &[W], what: &str)
where
    F: PartialEq<W> + fmt::Debug,
    W: fmt::Debug,
{
    let shared = found.len().min(wanted.len());
    let differs = found
        .iter()
        .zip(wanted)
        .position(|(found, wanted)| found != wanted)
        .unwrap_or(shared);
    assert!(
        found.len() == wanted.len() && differs == shared,
        "{what}: {} rows found, {} wanted; at position {differs} found {:?}, wanted {:?}",
        found.len(),
        wanted.len(),
        found.get(differs),
        wanted.get(differs)
    );
}

// ---- fixed numbers, vectors and text ----

/// A generator of fixed numbers: a xorshift seeded with one number, usually a row's. The same
/// seed gives the same numbers on every machine and every run, and neighbouring seeds give
/// numbers that have nothing to do with each other.
pub struct Fixed(u64);

impl Fixed {
    pub fn seeded(seed: i64) -> Fixed {
        // Spread the seed over all 64 bits, and never start at zero, where a xorshift stays.
        let spread = (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD6E8_FEB8_6659_FD93;
        let mut fixed = Fixed(if spread == 0 {
            0x2545_F491_4F6C_DD1D
        } else {
            spread
        });
        for _ in 0..8 {
            fixed.draw();
        }
        fixed
    }

    /// The next 64 bits.
    pub fn draw(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// The next number below `bound`.
    pub fn below(&mut self, bound: u64) -> u64 {
        (self.draw() >> 11) % bound
    }

    /// The next number from -1 up to, not including, 1: one of 2^24 evenly spaced values.
    pub fn unit(&mut self) -> f32 {
        (self.draw() >> 40) as f32 / (1_u64 << 23) as f32 - 1.0
    }
}

/// The vector of row `n` in a column of this dimension. It is scattered, not a smooth function
/// of `n`: a few rows lie near any vector a test searches with and most lie far from it.
pub fn vector(n: i64, dimension: usize) -> Vec<f32> {
    let mut fixed = Fixed::seeded(n);
    (0..dimension).map(|_| fixed.unit()).collect()
}

/// `len` lower-case letters, fixed by `seed`.
pub fn letters(seed: i64, len: usize) -> String {
    let mut fixed = Fixed::seeded(seed);
    (0..len)
        .map(|_| char::from(b'a' + fixed.below(26) as u8))
        .collect()
}

// ---- large text and JSON ----

/// What a large text value is made of. The store keeps a text of more than 1,015 bytes beside
/// its row, compressed when that saves an eighth of it; each kind says which way that goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// One sentence of 60 bytes, repeated: shrinks to a small part of its size.
    Repeating,
    /// Words drawn from a list of 64, a space between: shrinks to about half.
    Words,
    /// Bytes drawn one by one from the 94 printable ASCII characters: does not shrink.
    Scattered,
    /// Scattered for the first fifteen sixteenths, then one letter: shrinks by a sixteenth,
    /// which is less than an eighth.
    MostlyScattered,
    /// Scattered for the first half, then one letter: shrinks by about half.
    HalfScattered,
    /// One sentence of two-, three- and four-byte characters, repeated: shrinks.
    Wide,
}

impl Kind {
    pub const ALL: [Kind; 6] = [
        Kind::Repeating,
        Kind::Words,
        Kind::Scattered,
        Kind::MostlyScattered,
        Kind::HalfScattered,
        Kind::Wide,
    ];

    /// The kind `steps` places further on in [`Kind::ALL`], going round at its end. One step
    /// turns `Words`, which shrinks by an eighth, into `Scattered`, which does not, and
    /// `MostlyScattered`, which does not, into `HalfScattered`, which does.
    pub fn after(self, steps: usize) -> Kind {
        let at = Kind::ALL
            .iter()
            .position(|kind| *kind == self)
            .expect("every kind is in the list");
        Kind::ALL[(at + steps) % Kind::ALL.len()]
    }
}

/// The sentence of [`Kind::Repeating`]: 60 bytes.
const SENTENCE: &str = "A store keeps what it is given and hands it back as it was. ";

/// The sentence of [`Kind::Wide`]: characters of one, two, three and four bytes.
const WIDE_SENTENCE: &str = "Grüße aus Zürich für José: 東京の夜は静かで、川は海へ流れる。 🚀🌊✨ ";

/// The words of [`Kind::Words`].
const WORDS: [&str; 64] = [
    "about", "after", "again", "always", "answer", "before", "between", "block", "bytes", "cache",
    "change", "close", "column", "commit", "count", "data", "every", "file", "first", "folder",
    "found", "given", "holds", "index", "into", "keeps", "large", "later", "length", "limit",
    "memory", "moment", "never", "number", "once", "open", "order", "other", "page", "place",
    "read", "reader", "record", "row", "same", "search", "second", "size", "slice", "small",
    "stays", "store", "table", "takes", "text", "then", "there", "three", "under", "value",
    "vector", "where", "while", "writer",
];

/// One of the 94 printable ASCII characters from `!` to `~`.
fn scattered(fixed: &mut Fixed) -> u8 {
    b'!' + fixed.below(94) as u8
}

/// Exactly `len` bytes of text of this kind, fixed by `id`: the same on every machine and
/// every run, and different for different ids.
pub fn large_text(kind: Kind, id: i64, len: usize) -> String {
    let mut fixed = Fixed::seeded(id);
    let mut text = String::with_capacity(len);
    match kind {
        Kind::Repeating => {
            // The sentence over and over, begun at a place that depends on the id.
            let from = id.rem_euclid(SENTENCE.len() as i64) as usize;
            text.extend(SENTENCE.chars().cycle().skip(from).take(len));
        }
        Kind::Words => {
            while text.len() < len {
                text.push_str(WORDS[fixed.below(64) as usize]);
                text.push(' ');
            }
            // Every word is ASCII, so any byte is a place to cut.
            text.truncate(len);
        }
        Kind::Scattered | Kind::MostlyScattered | Kind::HalfScattered => {
            let drawn = match kind {
                Kind::Scattered => len,
                Kind::MostlyScattered => len - len / 16,
                _ => len / 2,
            };
            text.extend((0..drawn).map(|_| char::from(scattered(&mut fixed))));
            text.extend(std::iter::repeat_n('e', len - drawn));
        }
        Kind::Wide => {
            let from = id.rem_euclid(WIDE_SENTENCE.chars().count() as i64) as usize;
            for character in WIDE_SENTENCE.chars().cycle().skip(from) {
                // Cut at a character: one that would pass the size is left out.
                if text.len() + character.len_utf8() > len {
                    break;
                }
                text.push(character);
            }
            text.extend(std::iter::repeat_n('x', len - text.len()));
        }
    }
    assert_eq!(text.len(), len, "a {kind:?} text for row {id}");
    text
}

/// A JSON value that `serde_json::to_vec` writes in exactly `len` bytes, fixed by `id`; `len`
/// is at least 64. One that `shrinks` is an object holding an array of small objects with the
/// same three keys, and compresses to a small part of its size; one that does not is an
/// object holding one string of scattered text.
pub fn large_json(shrinks: bool, id: i64, len: usize) -> contextdb::serde_json::Value {
    use contextdb::serde_json::{Value as Json, json, to_vec};
    let json = if shrinks {
        // `{"items":[],"pad":""}` is 21 bytes; every item adds its own bytes and, after the
        // first, a comma. Items are added while they fit and `pad` takes up the rest.
        let (mut items, mut size) = (Vec::new(), 21);
        for number in id.. {
            let item = json!({ "id": number, "kind": "note", "ok": true });
            let bytes = to_vec(&item).expect("JSON writes as text").len() + 1;
            let bytes = bytes - usize::from(items.is_empty());
            if size + bytes > len {
                break;
            }
            size += bytes;
            items.push(item);
        }
        json!({ "items": items, "pad": "x".repeat(len - size) })
    } else {
        // `{"text":""}` is 11 bytes. A quote or a backslash is written as two bytes; where
        // only one byte is left, an `x` takes it.
        let mut fixed = Fixed::seeded(id);
        let (mut text, mut size) = (String::new(), 11);
        while size < len {
            let character = char::from(scattered(&mut fixed));
            let bytes = if matches!(character, '"' | '\\') {
                2
            } else {
                1
            };
            if size + bytes > len {
                text.push('x');
                size += 1;
            } else {
                text.push(character);
                size += bytes;
            }
        }
        json!({ "text": Json::String(text) })
    };
    let written = to_vec(&json).expect("JSON writes as text").len();
    assert_eq!(written, len, "the JSON value for row {id}");
    json
}

/// Asserts that the bytes read back are the bytes put in. A failure says what was compared,
/// the two lengths and the first place they differ, and never prints the bytes.
pub fn assert_same_bytes(found: &[u8], wanted: &[u8], what: &str) {
    if found == wanted {
        return;
    }
    let shared = found.len().min(wanted.len());
    let differs = found
        .iter()
        .zip(wanted)
        .position(|(found, wanted)| found != wanted)
        .unwrap_or(shared);
    panic!(
        "{what}: {} bytes found, {} wanted; they first differ at byte {differs}",
        found.len(),
        wanted.len()
    );
}

/// The name of what a value is, for a failure that must not print the value.
fn type_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "a BOOLEAN",
        Value::Int64(_) => "an INTEGER",
        Value::Float64(_) => "a REAL",
        Value::Text(_) => "a TEXT",
        Value::Uuid(_) => "a UUID",
        Value::Timestamp(_) => "a TIMESTAMP",
        Value::Json(_) => "a JSON value",
        Value::Vector(_) => "a VECTOR",
        Value::TxId(_) => "a transaction id",
    }
}

/// Asserts that a value read back is a `Value::Text` of exactly these bytes.
pub fn assert_text_is(found: &Value, wanted: &str, what: &str) {
    match found {
        Value::Text(found) => assert_same_bytes(found.as_bytes(), wanted.as_bytes(), what),
        other => panic!("{what}: read back as {}, not as a TEXT", type_of(other)),
    }
}

/// Asserts that a value read back is a `Value::Json` equal to this one, and that it is written
/// in exactly the bytes this one is written in.
pub fn assert_json_is(found: &Value, wanted: &contextdb::serde_json::Value, what: &str) {
    use contextdb::serde_json::to_vec;
    match found {
        Value::Json(found) => {
            let (found_bytes, wanted_bytes) = (to_vec(found), to_vec(wanted));
            assert_same_bytes(
                &found_bytes.expect("JSON writes as text"),
                &wanted_bytes.expect("JSON writes as text"),
                what,
            );
            assert!(found == wanted, "{what}: a JSON value that is not equal");
        }
        other => panic!("{what}: read back as {}, not as JSON", type_of(other)),
    }
}

// ---- the answer a similarity search is held to ----

/// The cosine distance between a stored vector and the vector of a search, worked out the one
/// way the engine is to work it out: each number widened to 64 bits, the dot product and the
/// two squared lengths summed in the order of the numbers, `1 - dot / (sqrt(aa) * sqrt(bb))`,
/// and 1 when either vector is all zeros.
pub fn distance(stored: &[f32], query: &[f32]) -> f64 {
    let (mut dot, mut aa, mut bb) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (x, y) in stored.iter().zip(query) {
        let (x, y) = (f64::from(*x), f64::from(*y));
        dot += x * y;
        aa += x * x;
        bb += y * y;
    }
    if aa == 0.0 || bb == 0.0 {
        1.0
    } else {
        1.0 - dot / (aa.sqrt() * bb.sqrt())
    }
}

/// One row of a search's answer: its primary key and its distance. Two are equal when the keys
/// are equal and the distances are the same to the bit.
#[derive(Clone)]
pub struct Near {
    pub key: Value,
    pub distance: f64,
}

impl PartialEq for Near {
    fn eq(&self, other: &Near) -> bool {
        self.key == other.key && self.distance.to_bits() == other.distance.to_bits()
    }
}

impl fmt::Debug for Near {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} at {:e} ({:#018x})",
            self.key,
            self.distance,
            self.distance.to_bits()
        )
    }
}

/// The rows of a table as a plain SELECT returned them: the primary key first, then the
/// columns a test filters by, then the VECTOR column last.
pub struct Stored {
    rows: Vec<Vec<Value>>,
}

/// Reads every row a plain SELECT returns, for the answers of [`Stored::nearest`].
pub fn stored(on: &impl Run, plain: &str) -> Stored {
    Stored {
        rows: rows(on, plain),
    }
}

impl Stored {
    pub fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }

    /// The reference answer: of the rows `passes` lets through that have a vector, the `k`
    /// least by `(distance, primary key)`, in that order. A search must return exactly these.
    pub fn nearest(&self, passes: impl Fn(&[Value]) -> bool, query: &[f32], k: usize) -> Vec<Near> {
        let mut all = Vec::new();
        for row in self.rows.iter().filter(|row| passes(row)) {
            match row.last() {
                Some(Value::Vector(vector)) => all.push(Near {
                    key: row[0].clone(),
                    distance: distance(vector, query),
                }),
                Some(Value::Null) => {}
                other => panic!("the last column of a stored row is {other:?}, not a vector"),
            }
        }
        all.sort_by(|a, b| {
            a.distance
                .total_cmp(&b.distance)
                .then_with(|| key_order(&a.key, &b.key))
        });
        all.truncate(k);
        all
    }
}

/// The order of two primary keys of one type.
fn key_order(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a, b) {
        (Value::Int64(a), Value::Int64(b)) => a.cmp(b),
        (Value::Text(a), Value::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
        other => panic!("no order is written here for the keys {other:?}"),
    }
}

/// The search every test writes unless it says otherwise: the key and the distance of the `k`
/// rows of `table` that pass `filter` and lie nearest `$q` in `column`.
pub fn nearest_sql(table: &str, column: &str, filter: Option<&str>, k: usize) -> String {
    let filter = filter.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
    format!(
        "SELECT id, {column} <=> $q AS distance FROM {table}{filter} ORDER BY distance LIMIT {k}"
    )
}

/// Runs a statement with `$q` bound to this vector.
pub fn search(on: &impl Run, sql: &str, query: &[f32]) -> contextdb::Result<QueryResult> {
    on.run(sql, &params! { "q" => query.to_vec() })
}

/// What a search returned, read as an answer: the key is the first column and the distance the
/// last, which must be a `Value::Float64`.
pub fn found(result: &QueryResult, sql: &str) -> Vec<Near> {
    let rows = result.rows().iter();
    rows.map(|row| match (row.first(), row.last()) {
        (Some(key), Some(Value::Float64(distance))) if row.len() >= 2 => Near {
            key: key.clone(),
            distance: *distance,
        },
        _ => panic!("{sql}\nreturned the row {row:?}, not a key and a distance that is a REAL"),
    })
    .collect()
}

/// Asserts that a search with `$q` bound to `query` returns exactly `wanted`: the same keys in
/// the same order, each distance the same to the bit. Returns the result, for its counted work.
pub fn assert_nearest(
    on: &impl Run,
    sql: &str,
    query: &[f32],
    wanted: &[Near],
    what: &str,
) -> QueryResult {
    let result = match search(on, sql, query) {
        Ok(result) => result,
        Err(error) => panic!("{what}: {sql}\nwas refused: {error:?}"),
    };
    assert_same_rows(&found(&result, sql), wanted, &format!("{what}: {sql}"));
    result
}

// ---- the table most vector tests search ----

/// Every column of `docs`, the key first and the vector last: what [`stored`] reads.
pub const DOCS: &str = "SELECT id, ctx, tag, v FROM docs";

/// Declares `docs`, whose row `n` is `(n, <a context>, n % 50, <a vector>)`. `clauses` is what
/// follows `VECTOR(dimension)`: `PARTITION_KEY (ctx)`, `MAX_PARTITIONS 2`, `SEARCH_MODE EXACT`
/// or nothing.
pub fn declare_docs(db: &Database, dimension: usize, clauses: &str) {
    run(
        db,
        &format!(
            "CREATE TABLE docs (id INTEGER PRIMARY KEY, ctx TEXT NOT NULL, tag INTEGER NOT NULL, \
             v VECTOR({dimension}) {clauses})"
        ),
    );
}

/// Inserts row `n` of `docs` under this context with this vector, or with no vector for `None`.
pub fn insert_doc(
    on: &impl Run,
    n: i64,
    ctx: &str,
    vector: Option<Vec<f32>>,
) -> contextdb::Result<QueryResult> {
    on.run(
        "INSERT INTO docs (id, ctx, tag, v) VALUES ($id, $ctx, $tag, $v)",
        &params! {
            "id" => n, "ctx" => ctx, "tag" => n % 50,
            "v" => vector.map_or(Value::Null, Value::Vector),
        },
    )
}

/// Inserts the rows of `docs` with these numbers, one statement each, row `n` under the
/// context `ctx(n)` with the vector [`vector`] gives for `n`. Every one must be taken.
pub fn insert_docs(
    on: &impl Run,
    numbers: impl IntoIterator<Item = i64>,
    dimension: usize,
    ctx: impl Fn(i64) -> String,
) {
    for n in numbers {
        if let Err(error) = insert_doc(on, n, &ctx(n), Some(vector(n, dimension))) {
            panic!("inserting row {n} of docs was refused: {error:?}");
        }
    }
}

/// The words of a declaration in lower case and its quoted texts as they are written: what the
/// declaration says, whatever the order, the spacing and the punctuation it says it in. A type
/// is given the name it is printed under, and a quoted name that needs no quotes loses them.
pub fn words(sql: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut rest = sql.chars().peekable();
    while let Some(first) = rest.next() {
        if first == '\'' || first == '"' {
            let mut text = String::from(first);
            loop {
                let next = rest.next().expect("a quoted text ends with its quote");
                text.push(next);
                // The quote written twice is one quote of the text.
                if next == first && rest.next_if_eq(&first).is_none() {
                    break;
                }
            }
            let inner = &text[1..text.len() - 1];
            let plain = first == '"'
                && inner.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
                && inner
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            found.push(if plain { inner.to_owned() } else { text });
        } else if first.is_ascii_alphanumeric() || first == '_' {
            let mut word = String::from(first.to_ascii_lowercase());
            while let Some(next) =
                rest.next_if(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            {
                word.push(next.to_ascii_lowercase());
            }
            match word.as_str() {
                "int" => found.push("integer".to_owned()),
                "bool" => found.push("boolean".to_owned()),
                "float" => found.push("real".to_owned()),
                "state_machine" => found.extend(["state".to_owned(), "machine".to_owned()]),
                _ => found.push(word),
            }
        }
    }
    found
}

/// The index of `events` by kind and then newest first.
pub const EVENTS_INDEX: &str = "CREATE INDEX events_kind_seq ON events (kind ASC, seq DESC)";

/// A table of notes, each a key and a body.
pub const NOTES: &str = "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)";

/// A text value, for comparing with a column of a stored row.
pub fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

/// Runs maintenance cycles until nothing is left for one to do, and returns how many vectors
/// the cycles sealed and packed between them. Nothing else in the tests seals or groups.
pub fn seal_all(db: &Database) -> u64 {
    maintenance(db).1
}

/// One maintenance cycle, which must not be refused.
pub fn cycle(db: &Database) -> contextdb::VectorMaintenanceReport {
    match db.run_maintenance_cycle() {
        Ok(report) => report.vector,
        Err(error) => panic!("a maintenance cycle was refused: {error:?}"),
    }
}

/// Runs maintenance cycles as a host does: while the report says work remains.
/// Returns the cycles through vector completion (the last included) and the vectors
/// reported done; later cycles finish the other work. A store that never rests fails.
pub fn maintenance(db: &Database) -> (u64, u64) {
    let (mut done, mut vector_cycles) = (0, None);
    for ran in 1..=100_000_u64 {
        let report = db.run_maintenance_cycle().expect("a maintenance cycle");
        let vector = report.vector;
        done += count(vector.maintenance_vectors_done);
        if count(vector.maintenance_vectors_remaining) == 0
            && count(vector.remaining_partitions) <= count(vector.stalled_partitions)
        {
            vector_cycles.get_or_insert(ran);
        }
        if report.work_remaining == Some(false) || report.compaction_waiting_on.is_some() {
            return (vector_cycles.unwrap_or(ran), done);
        }
    }
    panic!("100,000 maintenance cycles left work to do");
}

// ---- the index ----

/// The vector of row `n` in a scatter around eight directions at right angles: direction
/// `n % 8` has its numbers `2d` and `2d + 1` at one, and every number is moved by a twentieth
/// of what [`vector`] gives. Rows of one direction lie close together and far from the rest.
/// The dimension is 16 or more.
pub fn gathered(n: i64, dimension: usize) -> Vec<f32> {
    let direction = n.rem_euclid(8) as usize;
    let moved = vector(n, dimension);
    (0..dimension)
        .map(|at| f32::from(u8::from(at / 2 == direction)) + 0.05 * moved[at])
        .collect()
}

/// The vector of row `n` of `of` spread evenly round one circle that lies across every number
/// of the dimension, each number moved by a hundredth of what [`vector`] gives: no direction
/// gathers rows, and the rows nearest any of them are its neighbours on the circle.
pub fn ringed(n: i64, of: i64, dimension: usize) -> Vec<f32> {
    let turn = std::f64::consts::TAU * n as f64 / of as f64;
    let moved = vector(n, dimension);
    (0..dimension)
        .map(|at| {
            let phase = std::f64::consts::TAU * at as f64 / dimension as f64;
            (turn - phase).cos() as f32 + 0.01 * moved[at]
        })
        .collect()
}

/// Inserts these rows of `docs` under one context in one commit, row `n` with `vector(n)`.
pub fn load_docs(
    db: &Database,
    numbers: RangeInclusive<i64>,
    ctx: &str,
    vector: impl Fn(i64) -> Vec<f32>,
) {
    let tx = db.transaction().expect("begin");
    for n in numbers {
        if let Err(error) = insert_doc(&tx, n, ctx, Some(vector(n))) {
            panic!("inserting row {n} of docs was refused: {error:?}");
        }
    }
    tx.commit().expect("commit the rows of docs");
}

/// A store whose `docs` has these clauses after `VECTOR(dimension)` and rows 1 to `rows` of
/// [`gathered`] under the context `a`, with every maintenance cycle run.
pub fn built(name: &str, dimension: usize, clauses: &str, rows: i64) -> (PathBuf, Database) {
    let folder = folder(name);
    let db = open(&folder);
    declare_docs(&db, dimension, clauses);
    load_docs(&db, 1..=rows, "a", |n| gathered(n, dimension));
    maintenance(&db);
    (folder, db)
}

/// A search of `docs` for the `k` rows nearest `$q` under this mode: `EXACT`, `AUTO`,
/// `INDEXED`, or nothing for the column's own.
pub fn by_mode(filter: Option<&str>, mode: &str, k: usize) -> String {
    let filter = filter.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
    let clause = if mode.is_empty() {
        String::new()
    } else {
        format!("USE VECTOR {mode} ")
    };
    format!("SELECT id, v <=> $q AS distance FROM docs{filter} ORDER BY distance {clause}LIMIT {k}")
}

/// A search that must answer.
pub fn answer(on: &impl Run, sql: &str, query: &[f32]) -> QueryResult {
    match search(on, sql, query) {
        Ok(result) => result,
        Err(error) => panic!("{sql}\nwas refused: {error:?}"),
    }
}

/// Asserts that an answer is one a search that may leave rows out can give: `count` rows,
/// nearest first and none twice, each a row `passes` lets through, with the distance of the
/// vector stored for it, to the bit. Returns the answer.
pub fn assert_sound(
    result: &QueryResult,
    stored: &Stored,
    passes: impl Fn(&[Value]) -> bool,
    query: &[f32],
    count: usize,
    what: &str,
) -> Vec<Near> {
    let answer = found(result, what);
    assert_eq!(answer.len(), count, "{what}: rows in the answer");
    let rows: BTreeMap<i64, &Vec<Value>> = stored
        .rows()
        .iter()
        .map(|row| match row[0] {
            Value::Int64(id) => (id, row),
            ref other => panic!("the key {other:?} is no INTEGER"),
        })
        .collect();
    for near in &answer {
        let Value::Int64(id) = near.key else {
            panic!("{what}: the key {:?}", near.key)
        };
        let row = rows
            .get(&id)
            .unwrap_or_else(|| panic!("{what}: row {id} is not stored"));
        assert!(passes(row), "{what}: row {id} does not pass the filter");
        let Some(Value::Vector(vector)) = row.last() else {
            panic!("{what}: row {id} has no vector")
        };
        assert_eq!(
            near.distance.to_bits(),
            distance(vector, query).to_bits(),
            "{what}: the distance of row {id}"
        );
    }
    for pair in answer.windows(2) {
        let order = pair[0].distance.total_cmp(&pair[1].distance);
        assert!(
            order
                .then_with(|| key_order(&pair[0].key, &pair[1].key))
                .is_lt(),
            "{what}: {:?} came before {:?}",
            pair[0],
            pair[1]
        );
    }
    answer
}

/// How the refusal of an INDEXED search over an index behind its writes goes on after the
/// column's name (design section 6, F3): a maintenance cycle cures it.
pub const INDEXED_REFUSED: &str = " cannot be searched INDEXED for this statement now: its index \
    is behind its writes; run a maintenance cycle, or use EXACT or AUTO";

/// Asserts that a search of `docs.v` was refused as one whose index is behind its writes.
pub fn assert_indexed_refused(outcome: contextdb::Result<QueryResult>, what: &str) {
    match outcome {
        Err(error @ Error::VectorIndexedRouteUnavailable { .. }) => {
            assert_eq!(
                error.to_string(),
                format!("docs.v{INDEXED_REFUSED}"),
                "{what}"
            );
        }
        other => panic!("{what}: asked for the index and got {other:?}"),
    }
}

/// One row of an inspection statement: each column's name with its value.
pub type Shown = BTreeMap<String, Value>;

/// The rows of `SHOW VECTOR_INDEXES` or `SHOW VECTOR_PARTITIONS`.
pub fn shown(on: &impl Run, sql: &str) -> Vec<Shown> {
    let result = run(on, sql);
    let names = result.columns();
    let named = |row: &Vec<Value>| names.iter().cloned().zip(row.iter().cloned()).collect();
    result.rows().iter().map(named).collect()
}

/// One field of an inspection row.
pub fn field<'a>(row: &'a Shown, name: &str) -> &'a Value {
    row.get(name)
        .unwrap_or_else(|| panic!("inspection has no field {name}: {row:?}"))
}

/// A field that is a whole number.
pub fn whole(row: &Shown, name: &str) -> i64 {
    match field(row, name) {
        Value::Int64(number) => *number,
        other => panic!("the field {name} is {other:?}, not a whole number"),
    }
}

/// A field that is text.
pub fn word(row: &Shown, name: &str) -> String {
    match field(row, name) {
        Value::Text(text) => text.clone(),
        other => panic!("the field {name} is {other:?}, not text"),
    }
}

/// A field that holds JSON, whether the engine hands it over as JSON or as its text.
pub fn json(row: &Shown, name: &str) -> contextdb::serde_json::Value {
    match field(row, name) {
        Value::Json(value) => value.clone(),
        Value::Text(text) => contextdb::serde_json::from_str(text)
            .unwrap_or_else(|error| panic!("the field {name} is no JSON: {error}")),
        other => panic!("the field {name} is {other:?}, not JSON"),
    }
}

/// The one partition of a store whose `docs.v` has one.
pub fn the_partition(on: &impl Run) -> Shown {
    let mut rows = shown(on, "SHOW VECTOR_PARTITIONS FOR docs.v");
    assert_eq!(rows.len(), 1, "partitions of docs.v");
    rows.remove(0)
}

/// A storage door that adds up the bytes read from the data file.
pub fn counting_bytes_read() -> (WrapStorage, Arc<AtomicU64>) {
    let bytes = Arc::new(AtomicU64::new(0));
    let door = storage({
        let bytes = bytes.clone();
        move |call| {
            if let Call::Read(_, read) = call {
                bytes.fetch_add(read.len() as u64, Ordering::SeqCst);
            }
        }
    });
    (door, bytes)
}

/// The child's store opened for reading through [`pausing_at_read`]'s door, with its flag and
/// count.
pub fn reader_pausing(role: &Role) -> (ReadSession, Arc<AtomicBool>, Arc<AtomicU64>) {
    let (door, searching, reads) = pausing_at_read(role.number("pause_at"));
    let mut options = reader_options(None);
    options.storage = Some(door);
    let reader =
        ReadSession::open_with_options(&role.folder, options).expect("open the store for reading");
    (reader, searching, reads)
}

/// A storage door for a child that is to be stopped inside one statement. While the flag it
/// returns is set it counts the reads of the data file, and at read number `pause_at` it
/// announces `reading` and waits for a line from the parent; `pause_at` 0 never stops. Returns
/// the door, the flag and the count.
pub fn pausing_at_read(pause_at: u64) -> (WrapStorage, Arc<AtomicBool>, Arc<AtomicU64>) {
    let counting = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));
    let door = storage({
        let (counting, reads) = (counting.clone(), reads.clone());
        move |call| {
            if matches!(call, Call::Read(..))
                && counting.load(Ordering::SeqCst)
                && reads.fetch_add(1, Ordering::SeqCst) + 1 == pause_at
            {
                say("reading");
                hear();
            }
        }
    });
    (door, counting, reads)
}

/// Starts the child `role` stopped halfway through the storage reads of its statement, once it
/// says so. Where halfway is comes from a run of the same role to its end, `pause_at=0`: the
/// store is the same for both, so both make the same reads. Gives that run's lines and the child.
pub fn stopped_halfway(test: &str, role: &str, folder: &Path) -> (Vec<String>, Child) {
    let lines = Child::start(test, &format!("{role} pause_at=0"), folder).end();
    let reads = last_number(&lines, "reads").expect("the child announces its reads");
    assert!(
        reads > 1,
        "{role} made {reads} storage reads: no halfway inside it"
    );
    let halfway = format!("{role} pause_at={}", reads.div_ceil(2));
    let mut child = Child::start(test, &halfway, folder);
    child.expect("reading");
    (lines, child)
}
/// A controllable clock, shared by retention and maintenance tests.
pub struct At(pub Arc<AtomicU64>);
impl contextdb::Clock for At {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(self.0.load(Ordering::SeqCst))
    }
}
pub struct Millis(pub Arc<AtomicU64>);
impl Clock for Millis {
    fn now(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.0.load(SeqCst))
    }
}
#[derive(Default)]
pub struct Observer {
    pub samples: Mutex<Vec<[u64; 4]>>,
    pub now: Arc<AtomicU64>,
    pub advance: AtomicU64,
    pub cancel: AtomicBool,
    pub session: Mutex<Weak<ReadSession>>,
}
impl Observer {
    pub fn new(now: Arc<AtomicU64>, advance: u64, cancel: bool) -> Arc<Self> {
        Arc::new(Self {
            now,
            advance: AtomicU64::new(advance),
            cancel: AtomicBool::new(cancel),
            ..Self::default()
        })
    }
}
impl ReadProgressObserver for Observer {
    fn progress(&self, p: ReadProgress) {
        let sample = [p.rows, p.bytes, p.work, p.active_ms];
        self.samples.lock().unwrap().push(sample);
        self.now.fetch_max(self.advance.load(SeqCst), SeqCst);
        if self.cancel.swap(false, SeqCst) {
            self.session.lock().unwrap().upgrade().unwrap().cancel();
        }
    }
}
#[derive(Default)]
pub struct Bytes(pub Mutex<u64>);
impl ReadProgressObserver for Bytes {
    fn progress(&self, p: ReadProgress) {
        *self.0.lock().unwrap() = p.bytes;
    }
    fn completed(&self, p: ReadProgress) {
        self.progress(p);
    }
}
impl Clock for Observer {
    fn now(&self) -> SystemTime {
        if self.cancel.load(SeqCst) {
            self.session.lock().unwrap().upgrade().unwrap().cancel();
        }
        UNIX_EPOCH + Duration::from_millis(self.now.fetch_add(self.advance.load(SeqCst), SeqCst))
    }
}
pub fn delivery_queue(path: &Path, bytes: usize, count: i64) -> Database {
    let db = open(path);
    for sql in [
        "CREATE TABLE queued(id INTEGER PRIMARY KEY, body TEXT) \
         HISTORY CURRENT ONLY SYNC CONFLICT KEEP LATEST",
        "CREATE EVENT TYPE added WHEN INSERT ON queued",
        "CREATE SINK outgoing TYPE callback",
        "CREATE ROUTE notify EVENT added TO outgoing",
    ] {
        run(&db, sql);
    }
    for id in 1..=count {
        let body = "x".repeat(if id == 1 { bytes } else { 8 });
        db.execute(
            "INSERT INTO queued VALUES ($id,$body)",
            &params! {"id"=>id,"body"=>body},
        )
        .unwrap();
    }
    db
}

/// The documented vector records from a public exported store.
pub fn vector_records(folder: &Path) -> BTreeMap<Vec<u8>, Vec<u8>> {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    let db = redb::Builder::new()
        .open_read_only(folder.join("data"))
        .unwrap();
    let read = db.begin_read().unwrap();
    let mut records = BTreeMap::new();
    for name in ["vectors", "vector_blocks"] {
        let table = read
            .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
            .unwrap();
        for entry in table.iter().unwrap() {
            let (key, value) = entry.unwrap();
            records.insert(key.value().to_vec(), value.value().to_vec());
        }
    }
    records
}
/// A store whose data file cancels the handle at the first sync after the flag is set: a
/// maintenance batch is one commit, so the cycle ends after it (design 4, the guard).
pub fn cancelling(
    folder: &Path,
    mut options: DatabaseOpenOptions,
) -> (Arc<Database>, Arc<AtomicBool>) {
    let armed = Arc::new(AtomicBool::new(false));
    let handle = Arc::new(std::sync::OnceLock::new());
    options.storage = Some(storage({
        let (armed, handle) = (armed.clone(), handle.clone());
        move |call| {
            if matches!(call, Call::Sync) && armed.swap(false, SeqCst) {
                let db: Option<Arc<Database>> = handle.get().and_then(Weak::upgrade);
                db.expect("the cancelling handle").cancel();
            }
        }
    }));
    let db = Arc::new(open_with(folder, options).expect("open through the cancelling door"));
    handle.set(Arc::downgrade(&db)).ok();
    (db, armed)
}

/// Runs at most `most` cycles through [`cancelling`]'s door, ending early at one that commits
/// nothing. Each that commits must return the interruption with its report kept (S2, rule 2) and
/// leave `verify` clean. Gives each batch's report and the `verify` after it.
pub fn batches(
    db: &Database,
    armed: &AtomicBool,
    most: usize,
) -> Vec<(contextdb::MaintenanceReport, contextdb::VerifyReport)> {
    let mut batches = Vec::new();
    while batches.len() < most {
        armed.store(true, SeqCst);
        let outcome = db.run_maintenance_cycle();
        if armed.swap(false, SeqCst) {
            outcome.expect("a cycle that committed nothing");
            return batches;
        }
        let Err(Error::QueryCanceled {
            statement_timeout: false,
        }) = outcome
        else {
            panic!("a cycle cancelled at its commit returned {outcome:?}, not the interruption");
        };
        let report = db
            .last_maintenance_cycle()
            .unwrap()
            .expect("the cycle's report");
        let verified = db.verify().expect("verify is clean after every batch");
        batches.push((report, (*verified).clone()));
    }
    batches
}
