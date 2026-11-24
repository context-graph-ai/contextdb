//! What the measuring programs of this folder share: an allocator that counts, the one timer,
//! the size of a folder, the `name value unit` lines they print, and the measuring of a
//! similarity search under each mode beside what the search worked through.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use contextdb::{Database, QueryResult, ReadLimits, ReadSession, Value};

pub type Fallible<T> = Result<T, Box<dyn std::error::Error>>;

/// The values a statement is run with, by name.
pub type Values = HashMap<String, Value>;

/// What makes the search statement of a case: given what follows `ORDER BY distance` and the
/// vector searched for, the statement and its values.
pub type Statement<'a> = &'a dyn Fn(&str, &[f32]) -> (String, Values);

/// The memory limit a read session is opened under: what Context Graph gives a reader.
pub const READER_LIMIT: usize = 1 << 30;

/// Searches of each mode that are run and not timed before the timed ones.
pub const UNTIMED: usize = 5;

/// What follows `ORDER BY distance` under each mode, for the ten nearest.
pub const EXACT: &str = " USE VECTOR EXACT LIMIT 10";
pub const INDEXED: &str = " USE VECTOR INDEXED LIMIT 10";
pub const AUTO: &str = " LIMIT 10";

/// The system allocator, counting the bytes handed out and not yet given back. A program makes
/// it its global allocator.
pub struct Counting;

static IN_USE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every request goes to the system allocator unchanged; this only counts its bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let in_use = IN_USE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
        PEAK.fetch_max(in_use, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        IN_USE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }
}

/// The most bytes the process had allocated at one moment since the last call, or since it
/// started. The next count starts from what is allocated now.
pub fn take_peak_allocated() -> u64 {
    PEAK.swap(IN_USE.load(Ordering::Relaxed), Ordering::Relaxed) as u64
}

/// Runs `work` and says how long it took. Apart from the library's `system` part, this is the
/// only place in the workspace that reads a clock.
#[allow(
    clippy::disallowed_methods,
    reason = "measuring elapsed time is what these programs are for"
)]
pub fn timed<T>(work: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let result = work();
    (result, start.elapsed())
}

pub fn line(name: &str, value: impl Display, unit: &str) {
    println!("{name} {value} {unit}");
}

/// The most memory the operating system has seen this process hold, where it says so.
pub fn peak_resident_bytes() -> Option<u64> {
    status_bytes("VmHWM:")
}

/// One field of the process's status in bytes, such as `VmRSS:`, where the system says so.
pub fn status_bytes(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with(field))?;
    let kibibytes: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kibibytes * 1024)
}

pub fn milliseconds(duration: Duration) -> String {
    format!("{:.3}", duration.as_secs_f64() * 1000.0)
}

pub fn seconds(duration: Duration) -> String {
    format!("{:.1}", duration.as_secs_f64())
}

pub fn mebibytes(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / 1_048_576.0)
}

/// One number over another, to two places.
pub fn ratio(over: u64, under: u64) -> String {
    format!("{:.2}", over as f64 / under as f64)
}

/// The value a `share` of the values lie at or under: the one at position
/// `round((count - 1) * share)` when they are put in order. There must be at least one.
pub fn percentile<T: Copy + Ord>(values: &[T], share: f64) -> T {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[((sorted.len() - 1) as f64 * share).round() as usize]
}

/// The bytes of every file under a folder, by length.
pub fn bytes_on_disk(folder: &Path) -> std::io::Result<u64> {
    let mut bytes = 0;
    for entry in std::fs::read_dir(folder)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        bytes += if meta.is_dir() {
            bytes_on_disk(&entry.path())?
        } else {
            meta.len()
        };
    }
    Ok(bytes)
}

/// Stops the program with a sentence unless the folder is missing or empty.
pub fn must_be_empty(folder: &Path) {
    if folder
        .read_dir()
        .is_ok_and(|mut entries| entries.next().is_some())
    {
        eprintln!(
            "{} is not empty; give a folder that is missing or empty",
            folder.display()
        );
        std::process::exit(2);
    }
}

/// The sum of the numbers over how many there are, to one place.
fn mean(numbers: impl Iterator<Item = i64>) -> String {
    let (mut sum, mut count) = (0_i64, 0_i64);
    for number in numbers {
        sum += number;
        count += 1;
    }
    format!("{:.1}", sum as f64 / count.max(1) as f64)
}

// ---------------------------------------------------------------------------------------------
// A store built for a case

/// Runs maintenance cycles until one reports nothing remaining, and answers how many it ran:
/// no vector unindexed, and no partition with an IVF due or being built that is not stalled.
pub fn maintenance(db: &Database) -> Fallible<u64> {
    for cycle in 1..=10_000_000 {
        let report = db.run_maintenance_cycle()?.vector;
        if report.maintenance_vectors_remaining == Some(0)
            && report.remaining_partitions <= report.stalled_partitions
        {
            if report.stalled_partitions > Some(0) {
                return Err("a partition's build is stalled for memory".into());
            }
            return Ok(cycle);
        }
    }
    Err("ten million maintenance cycles left work remaining".into())
}

/// The first store of a case, in which no partition is ever grouped: runs its cycles, closes
/// it and prints `seal`, `seal_cycles` and `store_size_sealed`. Answers the store's bytes.
pub fn sealed(prefix: &str, db: &Database, store: &Path) -> Fallible<u64> {
    let (cycles, time) = timed(|| maintenance(db));
    let cycles = cycles?;
    db.close()?;
    let bytes = bytes_on_disk(store)?;
    line(&format!("{prefix}seal"), seconds(time), "s");
    line(&format!("{prefix}seal_cycles"), cycles, "cycles");
    line(
        &format!("{prefix}store_size_sealed"),
        mebibytes(bytes),
        "MiB",
    );
    Ok(bytes)
}

/// Runs cycles until nothing remains, then prints maintenance time and cycles, the partition
/// count from inspection, the store's size once the index is built (`store_size_after_build`)
/// and, after the cycles that follow until no work remains, the closed store's size
/// (`store_size`) beside the bytes of its input values.
pub fn built(prefix: &str, db: &Database, store: &Path, held: u64) -> Fallible<()> {
    let (cycles, time) = timed(|| maintenance(db));
    let cycles = cycles?;
    line(
        &format!("{prefix}store_size_after_build"),
        mebibytes(bytes_on_disk(store)?),
        "MiB",
    );
    while db.run_maintenance_cycle()?.work_remaining == Some(true) {}
    let name = |what: &str| format!("{prefix}{what}");
    line(&name("maintenance"), seconds(time), "s");
    line(&name("maintenance_cycles"), cycles, "cycles");
    let partitions = db.execute("SHOW VECTOR_PARTITIONS", &Values::new())?;
    line(&name("partitions"), partitions.rows().len(), "partitions");
    db.close()?;
    let bytes = bytes_on_disk(store)?;
    line(&name("store_size"), mebibytes(bytes), "MiB");
    line(&name("size_over_held"), ratio(bytes, held), "x");
    Ok(())
}

/// Starts this program again with these arguments and waits for it: what a new process
/// measures, it prints itself.
pub fn in_new_process(arguments: &[&OsStr]) -> Fallible<()> {
    let program = std::env::current_exe()?;
    let status = std::process::Command::new(program)
        .args(arguments)
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err("the new process did not finish".into())
    }
}

/// What a new process does first: opens a read session on a built store under the reader's
/// limit and runs one search, and prints the time of each under these two names.
pub fn first(store: &Path, names: (&str, &str), (sql, values): &(String, Values)) -> Fallible<()> {
    let (opened, open) = timed(|| session(store, READER_LIMIT));
    let opened = opened?;
    let (found, find) = timed(|| opened.execute(sql, values));
    found?;
    line(names.0, milliseconds(open), "ms");
    line(
        &format!("{}_rss_anon", names.0),
        mebibytes(status_bytes("RssAnon:").unwrap_or(0)),
        "MiB",
    );
    line(names.1, milliseconds(find), "ms");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Searches under each mode

/// A read session on a store under a memory limit.
pub fn session(store: &Path, limit: usize) -> Fallible<ReadSession> {
    let limits = ReadLimits {
        memory: limit as u64,
        result_rows: u64::MAX,
        result_bytes: u64::MAX,
        active_ms: u64::MAX,
        ..Default::default()
    };
    Ok(ReadSession::open(store, limits)?)
}

/// What the questions gave under one mode: for each round the middle and the 95th percentile
/// of the questions' times, and from the first round, for each question, the search's counted
/// work and its answer (each row's id as a number, and its distance).
#[derive(Default)]
pub struct Asked {
    pub p50: Vec<Duration>,
    pub p95: Vec<Duration>,
    pub evaluations: Vec<u64>,
    pub examined: Vec<u64>,
    pub answers: Vec<Vec<(u128, f64)>>,
}

/// The id and the distance of every row of a search's answer: the id is its first column, a
/// UUID or an INTEGER, and the distance its last.
fn answer(found: &QueryResult) -> Fallible<Vec<(u128, f64)>> {
    let rows = found
        .rows()
        .iter()
        .map(|row| match (row.first(), row.last()) {
            (Some(Value::Uuid(id)), Some(Value::Float64(distance))) => {
                Ok((id.as_u128(), *distance))
            }
            (Some(Value::Int64(id)), Some(Value::Float64(distance))) => {
                Ok((*id as u128, *distance))
            }
            _ => Err("a search row is not an id and a distance".into()),
        });
    rows.collect()
}

/// Asks every question under each of `tails` (what follows `ORDER BY distance`: a mode and a
/// limit), in one session: first [`UNTIMED`] searches under each that are not timed, then
/// `rounds` rounds, and within each round every question under the first tail, then under
/// the next. Each `execute` call is timed whole.
pub fn ask(
    session: &ReadSession,
    statement: Statement<'_>,
    tails: &[&str],
    questions: &[Vec<f32>],
    rounds: usize,
) -> Fallible<Vec<Asked>> {
    let search = |tail: &str, question: &[f32]| {
        let (sql, values) = statement(tail, question);
        timed(|| session.execute(&sql, &values))
    };
    for tail in tails {
        for question in questions.iter().take(UNTIMED) {
            search(tail, question).0?;
        }
    }
    let mut asked: Vec<Asked> = tails.iter().map(|_| Asked::default()).collect();
    for round in 0..rounds {
        for (tail, asked) in tails.iter().zip(&mut asked) {
            let mut times = Vec::with_capacity(questions.len());
            for question in questions {
                let (found, time) = search(tail, question);
                let found = found?;
                times.push(time);
                if round == 0 {
                    asked.evaluations.push(found.distance_evaluations());
                    asked.examined.push(found.rows_examined());
                    asked.answers.push(answer(&found)?);
                }
            }
            asked.p50.push(percentile(&times, 0.5));
            asked.p95.push(percentile(&times, 0.95));
        }
    }
    Ok(asked)
}

/// Prints the memory limit and the count of threads the lines after it were taken under.
pub fn setting(name: &str, limit: usize) {
    let value = format!("memory_limit_{limit}_bytes_threads_1");
    line(&format!("{name}_session"), value, "-");
}

/// Prints `_p50` and `_p95`, each the middle of the rounds' values, and with more rounds than
/// one `_p95_spread`, the largest of the rounds' 95th percentiles less the smallest.
pub fn times(name: &str, asked: &Asked) {
    let p50 = milliseconds(percentile(&asked.p50, 0.5));
    line(&format!("{name}_p50"), p50, "ms");
    let p95 = milliseconds(percentile(&asked.p95, 0.5));
    line(&format!("{name}_p95"), p95, "ms");
    if asked.p95.len() > 1 {
        let spread = percentile(&asked.p95, 1.0) - percentile(&asked.p95, 0.0);
        line(&format!("{name}_p95_spread"), milliseconds(spread), "ms");
    }
}

/// Prints `_distance_evaluations_mean` and `_rows_examined_p50`.
pub fn counted(name: &str, asked: &Asked) {
    let evaluations = mean(asked.evaluations.iter().map(|count| *count as i64));
    let name = |what: &str| format!("{name}_{what}");
    line(
        &name("distance_evaluations_mean"),
        evaluations,
        "evaluations",
    );
    let rows = percentile(&asked.examined, 0.5);
    line(&name("rows_examined_p50"), rows, "rows");
}

/// Prints `_recall` (the share of `exact`'s answers, over all questions, that this mode
/// returned), `_all_ten` (questions whose answer holds every row of `exact`'s) and
/// `_not_fewer_evaluations` (questions on which it did as many evaluations as `exact` or
/// more).
pub fn against(name: &str, asked: &Asked, exact: &Asked) {
    let (mut shared, mut wanted, mut all) = (0, 0, 0);
    for (ours, theirs) in asked.answers.iter().zip(&exact.answers) {
        let found = theirs
            .iter()
            .filter(|(id, _)| ours.iter().any(|row| row.0 == *id));
        let found = found.count();
        shared += found;
        wanted += theirs.len();
        all += usize::from(found == theirs.len());
    }
    let recall = format!("{:.4}", shared as f64 / wanted.max(1) as f64);
    let of = format!("of_{}", exact.answers.len());
    let pairs = asked.evaluations.iter().zip(&exact.evaluations);
    let not_fewer = pairs.filter(|(ours, theirs)| ours >= theirs).count();
    line(&format!("{name}_recall"), recall, "-");
    line(&format!("{name}_all_ten"), all, &of);
    line(&format!("{name}_not_fewer_evaluations"), not_fewer, &of);
}

/// Asks the questions under EXACT, INDEXED and AUTO in turn within each round, in one session
/// under the reader's limit, and prints every line of the three for `name`: the times, the
/// counted work, and the two indexed modes against EXACT of this run. Answers what the three
/// gave, in that order.
pub fn modes(
    name: &str,
    session: &ReadSession,
    statement: Statement<'_>,
    questions: &[Vec<f32>],
    rounds: usize,
) -> Fallible<Vec<Asked>> {
    let asked = ask(
        session,
        statement,
        &[EXACT, INDEXED, AUTO],
        questions,
        rounds,
    )?;
    setting(name, READER_LIMIT);
    for (mode, of) in ["exact", "indexed", "auto"].iter().zip(&asked) {
        let name = format!("{name}_{mode}");
        times(&name, of);
        counted(&name, of);
        if *mode != "exact" {
            against(&name, of, &asked[0]);
        }
    }
    Ok(asked)
}

/// How many of these stored vectors come back first when each is searched for by itself,
/// under INDEXED and beside it under EXACT, with `LIMIT 1` and with `LIMIT 10`. Under EXACT a
/// vector that another row holds too may come back second.
pub fn own(
    name: &str,
    session: &ReadSession,
    statement: Statement<'_>,
    stored: &[(u128, Vec<f32>)],
) -> Fallible<()> {
    setting(name, READER_LIMIT);
    for (mode, clause) in [("", "INDEXED"), ("_exact", "EXACT")] {
        for limit in [1, 10] {
            let tail = format!(" USE VECTOR {clause} LIMIT {limit}");
            let mut found = 0;
            for (id, vector) in stored {
                let (sql, values) = statement(&tail, vector);
                let rows = answer(&session.execute(&sql, &values)?)?;
                found += usize::from(rows.first().is_some_and(|row| row.0 == *id));
            }
            let name = format!("{name}{mode}_limit_{limit}");
            line(&name, found, &format!("of_{}", stored.len()));
        }
    }
    Ok(())
}

/// Runs `work` while another thread runs these searches in turn, over and over, in one read
/// session of the store, until the work is done. Answers what the work answered and the time
/// of each search; with no searches, runs the work alone.
pub fn during<T>(
    store: &Path,
    searches: &[(String, Values)],
    work: impl FnOnce() -> T,
) -> Fallible<(T, Vec<Duration>)> {
    use std::sync::mpsc::{TryRecvError, channel};
    if searches.is_empty() {
        return Ok((work(), Vec::new()));
    }
    let (stop, stopped) = channel::<()>();
    std::thread::scope(|scope| {
        let searching = scope.spawn(move || -> Result<Vec<Duration>, String> {
            let reader = session(store, READER_LIMIT).map_err(|e| e.to_string())?;
            let mut times = Vec::new();
            for (sql, values) in searches.iter().cycle() {
                if stopped.try_recv() != Err(TryRecvError::Empty) {
                    break;
                }
                let (found, time) = timed(|| reader.execute(sql, values));
                found.map_err(|e| e.to_string())?;
                times.push(time);
            }
            Ok(times)
        });
        let done = work();
        drop(stop);
        let times = searching
            .join()
            .map_err(|_| "the searching thread stopped")??;
        Ok((done, times))
    })
}

/// Prints how many searches there were and their 50th and 95th percentile times.
pub fn spread(name: &str, times: &[Duration]) {
    line(&format!("{name}_searches"), times.len(), "searches");
    if !times.is_empty() {
        line(
            &format!("{name}_p50"),
            milliseconds(percentile(times, 0.5)),
            "ms",
        );
        line(
            &format!("{name}_p95"),
            milliseconds(percentile(times, 0.95)),
            "ms",
        );
    }
}
