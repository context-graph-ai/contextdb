//! The only shared test code. Each test file says `mod support;`.
//!
//! It holds what every test needs to stay exact: one store folder per test, the one way a store
//! is opened, a table whose rows are a function of their number, a storage door that lets a test
//! watch the data file, and a second process spoken to over its pipes.

// Each test program uses a part of this module; the rest would be reported as never used.
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::ops::RangeInclusive;
use std::os::unix::fs::FileExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::Arc;
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
        let process = Command::new(std::env::current_exe().expect("the path of this test program"))
            .args(["--exact", test, "--nocapture"])
            .env("CONTEXTDB_TEST_ROLE", role)
            .env("CONTEXTDB_TEST_FOLDER", folder)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
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
