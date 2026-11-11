//! What the measuring programs of this folder share: an allocator that counts, the one timer,
//! the size of a folder, the `name value unit` lines they print, and the measuring of a
//! similarity search under each mode beside what the search worked through.
//!
//! One thing here reads a store's data file itself and not through the library: [`parts`],
//! which reads the vector part's partition, grouping, centre and sketch records with the
//! storage library, because the library's inspection gives a grouping's group count and
//! nothing of its gap or its width, and counts a search's work as one number. The layout it
//! reads is the one `contextdb/src/store/keys.rs` and `contextdb/src/vector/` write; a change
//! there changes it here, and a record it cannot read stops the program.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use contextdb::{Database, QueryResult, ReadSession, ReadSessionOptions, Value};
use redb::{ReadOnlyDatabase, ReadableDatabase, TableDefinition};

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
/// no vector waiting, and no partition with a grouping due or being built that is not stalled.
pub fn upkeep(db: &Database) -> Fallible<u64> {
    for cycle in 1..=10_000_000 {
        let report = db.run_maintenance_cycle()?.vector;
        if report.maintenance_vectors_remaining == 0
            && report.remaining_partitions <= report.stalled_partitions
        {
            if report.stalled_partitions > 0 {
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
    let (cycles, time) = timed(|| upkeep(db));
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

/// One whole number of the first row of an inspection statement, by the name of its column,
/// or the sum of it over every row. NULL counts as nothing.
fn inspected(found: &QueryResult, field: &str) -> Fallible<u64> {
    let at = found.columns().iter().position(|name| name == field);
    let at = at.ok_or("an inspection statement has no such field")?;
    let mut sum = 0;
    for row in found.rows() {
        if let Value::Int64(number) = row[at] {
            sum += number as u64;
        }
    }
    Ok(sum)
}

/// The second store of a case, declared as the case says: runs cycles until nothing remains,
/// closes it and prints `upkeep`, `upkeep_cycles`, `lists`, `probes`, the index's bytes, the
/// store's size beside what was `held` and beside the first store's bytes, and the least and
/// the most gap and width of its groupings. Answers its partitions as their records describe
/// them.
pub fn built(
    prefix: &str,
    db: &Database,
    store: &Path,
    (held, sealed, dimension): (u64, u64, usize),
) -> Fallible<Vec<Part>> {
    let (cycles, time) = timed(|| upkeep(db));
    let cycles = cycles?;
    let name = |what: &str| format!("{prefix}{what}");
    line(&name("upkeep"), seconds(time), "s");
    line(&name("upkeep_cycles"), cycles, "cycles");
    let partitions = db.execute("SHOW VECTOR_PARTITIONS", &Values::new())?;
    line(&name("lists"), inspected(&partitions, "lists")?, "groups");
    line(&name("probes"), inspected(&partitions, "probes")?, "groups");
    let indexes = db.execute("SHOW VECTOR_INDEXES", &Values::new())?;
    let index_bytes = inspected(&indexes, "durable_index_bytes")?;
    let vectors = inspected(&indexes, "vector_count")?;
    line(&name("index_bytes"), index_bytes, "bytes");
    let each = index_bytes / vectors.max(1);
    line(&name("index_bytes_per_vector"), each, "bytes");
    db.close()?;
    let bytes = bytes_on_disk(store)?;
    line(&name("store_size"), mebibytes(bytes), "MiB");
    line(&name("size_over_held"), ratio(bytes, held), "x");
    line(&name("size_over_sealed"), ratio(bytes, sealed), "x");
    line(&name("bytes_per_passage"), bytes / vectors.max(1), "bytes");
    let parts = parts(store, dimension)?;
    let grouped = parts.iter().filter(|part| part.groups.is_some()).count();
    line(&name("partitions"), parts.len(), "partitions");
    line(&name("partitions_grouped"), grouped, "partitions");
    measured(prefix, &parts.iter().collect::<Vec<_>>());
    Ok(parts)
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
    line(names.1, milliseconds(find), "ms");
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Searches under each mode

/// A read session on a store under a memory limit.
pub fn session(store: &Path, limit: usize) -> Fallible<ReadSession> {
    let mut options = ReadSessionOptions::default();
    options.memory_limit = Some(limit);
    Ok(ReadSession::open_with_options(store, options)?)
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
/// counted work, the two indexed modes against EXACT of this run, and under INDEXED what the
/// price is read from ([`work`]). `parts` are the partitions the statement selects. Answers
/// what the three gave, in that order.
pub fn modes(
    name: &str,
    session: &ReadSession,
    statement: Statement<'_>,
    questions: &[Vec<f32>],
    (rounds, parts): (usize, &[&Part]),
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
        if *mode == "indexed" {
            work(&name, of, parts, questions);
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

// ---------------------------------------------------------------------------------------------
// What a search's price is read from: the groupings as their records hold them

const VECTORS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vectors");
const BLOCKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("vector_blocks");

/// One partition of a VECTOR column as its records describe it.
pub struct Part {
    /// Its key tuple as the store writes it: for each value a tag and the value's bytes.
    tuple: Vec<u8>,
    /// The slots of its grouping and of its tail together.
    slots: u64,
    /// The slots of its tail.
    tail: u64,
    /// Its grouping, when it has one of more groups than one.
    groups: Option<Groups>,
}

struct Groups {
    /// How far a search scans, in ten-thousandths of a product.
    gap: u32,
    /// How wide a search takes a sketch, in quarters of its factor; 0 when it reads none.
    width: u32,
    /// Every centre, one after another.
    centres: Vec<f32>,
    /// The slots of each group.
    sizes: Vec<u64>,
}

fn short(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(std::array::from_fn(|byte| bytes[at + byte]))
}

fn long(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(std::array::from_fn(|byte| bytes[at + byte]))
}

/// The partitions of the one VECTOR column of a closed store, read from its data file.
pub fn parts(store: &Path, dimension: usize) -> Fallible<Vec<Part>> {
    let data = ReadOnlyDatabase::open(store.join("data"))?;
    let read = data.begin_read()?;
    let (vectors, blocks) = (read.open_table(VECTORS)?, read.open_table(BLOCKS)?);
    let mut parts = Vec::new();
    // A partition's record is kept under the byte 1, its column and its key tuple.
    for record in vectors.range::<&[u8]>(&[1_u8][..]..&[2_u8][..])? {
        let (key, entry) = record?;
        let (key, entry) = (key.value(), entry.value());
        if entry.len() != 73 || key.len() < 5 {
            return Err("a partition record is not as this program reads it".into());
        }
        let (tail, flags) = (long(entry, 24), entry[40]);
        let (number, blocks_held, groups) = (short(entry, 41), short(entry, 45), short(entry, 49));
        let grouped = if flags & 1 == 0 { 0 } else { long(entry, 53) };
        let mut part = Part {
            tuple: key[5..].to_vec(),
            slots: grouped + tail,
            tail,
            groups: None,
        };
        if flags & 1 != 0 && groups > 1 {
            // The grouping's record: the gap, the width, the probes, then where each group
            // begins and where the last ends, a block and a slot each.
            let under = [&key[1..5], &number.to_be_bytes()[..]].concat();
            let record = vectors.get([&[3_u8][..], &under].concat().as_slice())?;
            let record = record.ok_or("a grouping has no record")?;
            let record = record.value();
            if record.len() != 20 + 8 * groups as usize {
                return Err("a grouping record is not as this program reads it".into());
            }
            // How many slots lie before each block: a block's count opens its sketch record.
            let mut before = vec![0_u64];
            for block in 0..blocks_held {
                let key = [&[7_u8][..], &under, &block.to_be_bytes()].concat();
                let sketches = blocks.get(key.as_slice())?;
                let sketches = sketches.ok_or("a block of a grouping has no sketches")?;
                before.push(before[block as usize] + u64::from(short(sketches.value(), 0)));
            }
            let place = |group: usize| {
                let (block, slot) = (short(record, 12 + 8 * group), short(record, 16 + 8 * group));
                before
                    .get(block as usize)
                    .map(|slots| slots + u64::from(slot))
            };
            let mut sizes = Vec::with_capacity(groups as usize);
            for group in 0..groups as usize {
                let (Some(from), Some(to)) = (place(group), place(group + 1)) else {
                    return Err("a grouping's places are past its blocks".into());
                };
                sizes.push(to - from);
            }
            let mut centres = Vec::with_capacity(groups as usize * dimension);
            let from = [&[4_u8][..], &under].concat();
            let to = [&[4_u8][..], &key[1..5], &(number + 1).to_be_bytes()].concat();
            for record in blocks.range::<&[u8]>(from.as_slice()..to.as_slice())? {
                let (_, value) = record?;
                let (numbers, _) = value.value().as_chunks::<4>();
                centres.extend(numbers.iter().map(|number| f32::from_le_bytes(*number)));
            }
            if centres.len() != groups as usize * dimension {
                return Err("a grouping's centres are not as many as its groups".into());
            }
            part.groups = Some(Groups {
                gap: short(record, 0),
                width: short(record, 4),
                centres,
                sizes,
            });
        }
        parts.push(part);
    }
    Ok(parts)
}

/// Those of `parts` whose key tuple is one of `tuples`, or every one when none is given.
pub fn selected<'a>(parts: &'a [Part], tuples: &[Vec<u8>]) -> Vec<&'a Part> {
    let wanted = |part: &&Part| tuples.is_empty() || tuples.contains(&part.tuple);
    parts.iter().filter(wanted).collect()
}

/// Prints the least and the most gap and width among the groupings of these partitions: the
/// gap as a difference of products, the width as a multiple of a sketch's factor. `none` when
/// no partition of them has a grouping of more groups than one.
fn measured(name: &str, parts: &[&Part]) {
    let groupings = || parts.iter().filter_map(|part| part.groups.as_ref());
    let gaps = groupings().map(|groups| groups.gap);
    let widths = groupings().map(|groups| groups.width);
    let name = |what: &str| format!("{name}{what}");
    let (Some(gap_least), Some(gap_most)) = (gaps.clone().min(), gaps.max()) else {
        for what in ["gap_least", "gap_most", "width_least", "width_most"] {
            line(&name(what), "none", "-");
        }
        return;
    };
    let gap = |gap: u32| format!("{:.4}", f64::from(gap) / 10_000.0);
    line(&name("gap_least"), gap(gap_least), "product");
    line(&name("gap_most"), gap(gap_most), "product");
    let width = |width: Option<u32>| format!("{:.2}", f64::from(width.unwrap_or(0)) / 4.0);
    line(&name("width_least"), width(widths.clone().min()), "factors");
    line(&name("width_most"), width(widths.max()), "factors");
}

/// The numbers of one vector multiplied by another's and added up as the library adds them:
/// in 32 bits, sixteen sums side by side.
fn product(one: &[f32], other: &[f32]) -> f32 {
    let mut lanes = [0.0_f32; 16];
    let (one_runs, one_rest) = one.as_chunks::<16>();
    let (other_runs, other_rest) = other.as_chunks::<16>();
    for (one, other) in one_runs.iter().zip(other_runs) {
        for ((lane, one), other) in lanes.iter_mut().zip(one).zip(other) {
            *lane += one * other;
        }
    }
    for ((lane, one), other) in lanes.iter_mut().zip(one_rest).zip(other_rest) {
        *lane += one * other;
    }
    lanes.iter().sum()
}

/// What an unfiltered search under INDEXED does with these partitions for one question, worked
/// out from their records as `vector/search.rs` works it out: the centres and means it
/// compares, the groups inside the gap, the slots of those groups and of the tails beside
/// them, how many of those slots it estimates a sketch of, and the slots it screens with no
/// sketch read (a partition with no groups, and a grouping of width 0).
#[derive(Default)]
struct Reached {
    centres: i64,
    groups: i64,
    slots: i64,
    sketches: i64,
    unsketched: i64,
}

fn reached(parts: &[&Part], question: &[f32]) -> Reached {
    let length = question.iter().map(|x| f64::from(*x) * f64::from(*x));
    let length = length.sum::<f64>().sqrt();
    let unit = question.iter().map(|x| (f64::from(*x) / length) as f32);
    let unit: Vec<f32> = unit.collect();
    let mut reached = Reached::default();
    for part in parts {
        let Some(groups) = &part.groups else {
            reached.unsketched += part.slots as i64;
            continue;
        };
        let centres = groups.centres.chunks(unit.len());
        let products: Vec<f32> = centres.map(|centre| product(centre, &unit)).collect();
        let best = products.iter().copied().fold(f32::MIN, f32::max);
        let apart =
            |product: f32| ((f64::from(best) - f64::from(product)) * 10_000.0).min(20_000.0);
        let mut slots = part.tail as i64;
        for (size, product) in groups.sizes.iter().zip(&products) {
            if apart(*product) - f64::from(groups.gap) <= 0.0 {
                reached.groups += 1;
                slots += *size as i64;
            }
        }
        reached.centres += products.len() as i64;
        reached.slots += slots;
        if groups.width == 0 {
            reached.unsketched += slots;
        } else {
            // The partition's mean is compared too.
            reached.centres += 1;
            reached.sketches += slots;
        }
    }
    reached
}

/// Prints, for the unfiltered searches of `asked` under INDEXED over `parts`, what their price
/// is read from: the least and the most gap and width of the groupings, and at the mean
/// question the passes a search made, the centres compared, the groups inside the gap, the
/// slots of those groups and of the tails, the sketches estimated, the slots screened and the
/// rows asked for.
///
/// The library counts one number a search, `distance_evaluations`: centres and means compared,
/// sketches estimated, slots screened and rows asked for. The rows asked for are the rows the
/// statement examined less the rows it returned. The rest is worked out here. A search makes
/// one pass, and another whenever the last row of its answer lies below what its sketches
/// ended with; every pass compares the centres and estimates the sketches of the groups
/// inside the gap again, and a slot is screened once. So with `c` centres and `s` sketches a
/// pass ([`reached`]), a search of `p` passes counts from `p (c + s)` up to `p (c + s) + s`,
/// ranges that do not meet: the passes are the count over `c + s`, rounded down, and the slots
/// screened are what is left. `_work_out_of_range` counts the questions on which what is left
/// is fewer than the slots screened with no sketch or more than those and every sketched
/// slot: there a later pass took in more groups than the first, or this program and the
/// library disagree about a group at the gap's edge, and the three numbers are not exact.
pub fn work(name: &str, asked: &Asked, parts: &[&Part], questions: &[Vec<f32>]) {
    measured(&format!("{name}_"), parts);
    let mut reaches = Vec::with_capacity(questions.len());
    let (mut asked_for, mut screened, mut passes) = (Vec::new(), Vec::new(), Vec::new());
    let (mut out, mut again) = (0, 0);
    for (at, question) in questions.iter().enumerate() {
        let mut reached = reached(parts, question);
        let rows = asked.examined[at] as i64 - asked.answers[at].len() as i64;
        let counted = asked.evaluations[at] as i64 - rows;
        let a_pass = reached.centres + reached.sketches;
        let made = match reached.sketches {
            0 => 1,
            _ => (counted / (a_pass + reached.unsketched)).max(1),
        };
        let left = counted - made * a_pass;
        let most = reached.unsketched + reached.sketches;
        out += usize::from(left < reached.unsketched || left > most);
        again += usize::from(made > 1);
        reached.centres *= made;
        reached.sketches *= made;
        asked_for.push(rows);
        screened.push(left);
        passes.push(made);
        reaches.push(reached);
    }
    let of = |what: fn(&Reached) -> i64| mean(reaches.iter().map(what));
    let name = |what: &str| format!("{name}_{what}");
    let hundred = format!("of_{}", questions.len());
    line(&name("passes_mean"), mean(passes.into_iter()), "passes");
    line(&name("searches_of_more_passes"), again, &hundred);
    line(&name("centres_compared_mean"), of(|r| r.centres), "centres");
    line(&name("groups_scanned_mean"), of(|r| r.groups), "groups");
    line(
        &name("slots_in_scanned_groups_mean"),
        of(|r| r.slots),
        "slots",
    );
    line(
        &name("sketches_estimated_mean"),
        of(|r| r.sketches),
        "sketches",
    );
    line(
        &name("slots_screened_mean"),
        mean(screened.into_iter()),
        "slots",
    );
    line(
        &name("rows_asked_mean"),
        mean(asked_for.into_iter()),
        "rows",
    );
    line(&name("work_out_of_range"), out, &hundred);
}
