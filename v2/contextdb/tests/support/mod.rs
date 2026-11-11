//! The only shared test code. Each test file says `mod support;`.
//!
//! It holds what every test needs to stay exact: one store folder per test, the one way a store
//! is opened, a table whose rows are a function of their number, vectors that are a function of
//! their row's number with the answer every similarity search is held to, a storage door that
//! lets a test watch the data file, and a second process spoken to over its pipes.

// Each test program uses a part of this module; the rest would be reported as never used.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::ops::RangeInclusive;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

use contextdb::{
    Database, DatabaseOpenOptions, Error, QueryResult, ReadSession, StorageBackend, Transaction,
    Value, WrapStorage, params,
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
/// needs is set in this one place.
pub fn open_with(folder: &Path, options: DatabaseOpenOptions) -> contextdb::Result<Database> {
    Database::open_with_options(folder, options)
}

/// Closes a store and gives up the handle, so that the folder is free for the next open.
pub fn close(db: Database) {
    db.close().expect("close the store");
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

/// A text value, for comparing with a column of a stored row.
pub fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

/// Runs maintenance cycles until no vector waits to be sealed, and returns how many vectors
/// the cycles sealed between them. Nothing else in the tests seals.
pub fn seal_all(db: &Database) -> u64 {
    let mut sealed = 0;
    loop {
        let report = match db.run_maintenance_cycle() {
            Ok(report) => report.vector,
            Err(error) => panic!("a maintenance cycle was refused: {error:?}"),
        };
        sealed += report.maintenance_vectors_done;
        if report.maintenance_vectors_remaining == 0 {
            return sealed;
        }
        assert!(
            report.maintenance_vectors_done > 0,
            "a cycle sealed nothing and left {} vectors waiting",
            report.maintenance_vectors_remaining
        );
    }
}

// ---- the storage door ----

/// One call the engine makes on its data file, as a test's storage door sees it.
pub enum Call<'a> {
    /// These bytes were just read at this offset. Changing them changes what the engine is
    /// given, not the file.
    Read(u64, &'a mut [u8]),
    /// These bytes are about to be written at this offset.
    Write(u64, &'a [u8]),
    /// The file is about to be forced to disk.
    Sync,
}

/// A storage door that keeps the data file as it is and shows `watch` every read, write and
/// sync. A test counts the calls, stops at one, or changes what a read returned.
pub fn storage(watch: impl Fn(Call<'_>) + Send + Sync + 'static) -> WrapStorage {
    failing_storage(move |call| {
        watch(call);
        Ok(())
    })
}

/// A storage door like [`storage`] whose `watch` may also make a call fail. The error it
/// returns is what the engine gets from that read, write or sync; a write or a sync it fails
/// never reaches the file.
pub fn failing_storage(
    watch: impl Fn(Call<'_>) -> io::Result<()> + Send + Sync + 'static,
) -> WrapStorage {
    let watch: Arc<dyn Fn(Call<'_>) -> io::Result<()> + Send + Sync> = Arc::new(watch);
    Arc::new(move |file| {
        Ok(Box::new(Watched {
            file,
            watch: watch.clone(),
        }) as Box<dyn StorageBackend>)
    })
}

struct Watched {
    file: File,
    watch: Arc<dyn Fn(Call<'_>) -> io::Result<()> + Send + Sync>,
}

impl fmt::Debug for Watched {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "a watched {:?}", self.file)
    }
}

impl StorageBackend for Watched {
    fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read_exact_at(out, offset)?;
        (self.watch)(Call::Read(offset, out))
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    fn sync_data(&self) -> io::Result<()> {
        (self.watch)(Call::Sync)?;
        self.file.sync_data()
    }

    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        (self.watch)(Call::Write(offset, data))?;
        self.file.write_all_at(data, offset)
    }
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

// ---- a second process: the parent's side ----

/// A second process: this test program started again to run one test in a role. It announces
/// what it has done in lines that start with `@ `, and is told what to do next in lines on its
/// standard input. When the value is dropped the process is killed.
pub struct Child {
    process: std::process::Child,
    lines: Option<Receiver<String>>,
    announced: Vec<String>,
    status: Option<ExitStatus>,
}

impl Child {
    /// Starts the child. `test` is the name of the calling test function, which must begin with
    /// `if let Some(role) = support::role() { return child(role); }`. `role` is the role's name,
    /// then any settings as `key=number` words.
    pub fn start(test: &str, role: &str, folder: &Path) -> Child {
        Child::started(test, role, folder, Stdio::inherit())
    }

    /// [`Child::start`] for a child that is handed an open file, which it keeps open for as
    /// long as it runs: the file is its standard error, the one channel the roles do not use.
    pub fn start_holding(test: &str, role: &str, folder: &Path, held: OwnedFd) -> Child {
        Child::started(test, role, folder, Stdio::from(held))
    }

    fn started(test: &str, role: &str, folder: &Path, stderr: Stdio) -> Child {
        let process = Command::new(std::env::current_exe().expect("the path of this test program"))
            .args(["--exact", test, "--nocapture"])
            .env("CONTEXTDB_TEST_ROLE", role)
            .env("CONTEXTDB_TEST_FOLDER", folder)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(stderr)
            .spawn()
            .expect("start this test program a second time");
        Child {
            process,
            lines: None,
            announced: Vec::new(),
            status: None,
        }
    }

    /// The child's process id.
    pub fn id(&self) -> u32 {
        self.process.id()
    }

    /// The next line the child announced, or `None` once it has ended.
    ///
    /// This is the one function in the tests that waits. It gives up after 20 seconds and fails
    /// with every line the child announced and how it ended.
    pub fn line(&mut self) -> Option<String> {
        if self.lines.is_none() {
            let pipe = self
                .process
                .stdout
                .take()
                .expect("the child's standard output");
            let (sender, receiver) = mpsc::channel();
            #[allow(
                clippy::disallowed_methods,
                reason = "the one thread in the tests: it moves lines from the child's pipe to a \
                          channel so that the wait below can give up; nothing a test asserts \
                          depends on when it runs"
            )]
            std::thread::spawn(move || {
                for line in BufReader::new(pipe).lines().map_while(Result::ok) {
                    if sender.send(line).is_err() {
                        break;
                    }
                }
            });
            self.lines = Some(receiver);
        }
        loop {
            let received = self
                .lines
                .as_ref()
                .expect("set above")
                .recv_timeout(Duration::from_secs(20));
            match received {
                Ok(line) => {
                    // Everything else on the pipe is the test harness talking.
                    if let Some(said) = line.strip_prefix("@ ") {
                        self.announced.push(said.to_owned());
                        return Some(said.to_owned());
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.status = Some(self.process.wait().expect("wait for the child"));
                    return None;
                }
                Err(RecvTimeoutError::Timeout) => self.fail("announced nothing for 20 seconds"),
            }
        }
    }

    /// Waits for the child to announce exactly this line next.
    pub fn expect(&mut self, wanted: &str) {
        let line = self.line();
        if line.as_deref() != Some(wanted) {
            self.fail(&format!("was to announce {wanted:?} and gave {line:?}"));
        }
    }

    /// Waits for the child to announce `point`, and returns the lines it announced before it.
    pub fn until(&mut self, point: &str) -> Vec<String> {
        let mut before = Vec::new();
        loop {
            match self.line() {
                Some(line) if line == point => return before,
                Some(line) => before.push(line),
                None => self.fail(&format!("ended before it announced {point:?}")),
            }
        }
    }

    /// Writes one line to the child's standard input.
    pub fn tell(&mut self, line: &str) {
        let pipe = self
            .process
            .stdin
            .as_mut()
            .expect("the child's standard input");
        if writeln!(pipe, "{line}")
            .and_then(|()| pipe.flush())
            .is_err()
        {
            self.fail(&format!("was gone when it was told {line:?}"));
        }
    }

    /// Kills the child where it stands. It must still be running: a child that is to be killed
    /// has announced a point and is waiting there.
    pub fn kill(&mut self) {
        self.process.kill().expect("kill the child");
        let status = self.process.wait().expect("wait for the child");
        if status.signal().is_none() {
            self.fail("had already ended by itself when it was to be killed");
        }
    }

    /// Closes the child's standard input and waits for it to end by itself, which it must do
    /// without a failure. Returns every line it announced.
    pub fn end(&mut self) -> Vec<String> {
        drop(self.process.stdin.take());
        while self.line().is_some() {}
        if !self.status.is_some_and(|status| status.success()) {
            self.fail("did not end well");
        }
        self.announced.clone()
    }

    fn fail(&mut self, what: &str) -> ! {
        let _ = self.process.kill();
        let status = self.process.wait();
        panic!(
            "the child {what}\nit had announced: {:#?}\nit ended with: {status:?}",
            self.announced
        )
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        // A failing test must not leave a process behind that holds its store.
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// The last number announced under `key`, in lines of the form `<key> <number>`.
pub fn last_number(lines: &[String], key: &str) -> Option<u64> {
    lines.iter().rev().find_map(|line| {
        let (name, number) = line.split_once(' ')?;
        if name == key {
            number.parse().ok()
        } else {
            None
        }
    })
}

// ---- a second process: the child's side ----

/// What a child was started as.
pub struct Role {
    pub name: String,
    pub folder: PathBuf,
    settings: Vec<String>,
}

impl Role {
    /// The number of a setting given as `key=number`.
    pub fn number(&self, key: &str) -> u64 {
        self.settings
            .iter()
            .find_map(|setting| setting.strip_prefix(key)?.strip_prefix('=')?.parse().ok())
            .unwrap_or_else(|| panic!("the role {} was not given {key}=<number>", self.name))
    }
}

/// The role this process was started in, or `None` in the process that runs the test itself.
pub fn role() -> Option<Role> {
    let words = std::env::var("CONTEXTDB_TEST_ROLE").ok()?;
    let mut words = words.split_whitespace().map(str::to_owned);
    Some(Role {
        name: words.next().expect("a role has a name"),
        settings: words.collect(),
        folder: std::env::var_os("CONTEXTDB_TEST_FOLDER")
            .expect("a child is given a folder")
            .into(),
    })
}

/// Announces one line to the parent.
pub fn say(line: impl fmt::Display) {
    println!("@ {line}");
}

/// Waits for the parent's next line. `None` when the parent has closed the pipe or is gone.
pub fn hear() -> Option<String> {
    let mut line = String::new();
    match io::stdin().lock().read_line(&mut line) {
        Ok(read) if read > 0 => Some(line.trim_end().to_owned()),
        _ => None,
    }
}

/// Announces a point and stays there until the parent kills this process. A kill therefore
/// lands at a point the child named, never after an amount of time.
pub fn wait_to_be_killed(point: &str) -> ! {
    say(point);
    hear();
    // Reached only when the parent went away without killing.
    std::process::exit(1)
}
