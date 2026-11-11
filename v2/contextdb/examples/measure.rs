//! Prints the numbers this library is held to, one per line as `name value unit`.
//!
//! - `measure readme <folder>` inserts 20,000 notes of 1.6 KB, one
//!   commit per row, and prints the store's size on disk, how long opening it takes and how
//!   long `COUNT(*)` takes.
//! - `measure million <folder>` loads a million small rows under a 64 MiB limit in commits of
//!   10,000, scans them, and prints the most memory the process had allocated beside the most
//!   the operating system saw it hold.
//! - `measure vectors <folder>` loads 20,000 rows with a vector of 384 numbers made from the
//!   row number, 500 to a commit, seals them, searches for the 10 nearest of 100 further
//!   vectors after 5 searches that are not timed, and prints the search time and the store's
//!   size beside the bytes held.
//! - `measure sessions <corpus folder> <list file> <count> <folder>` loads every line that is
//!   not empty of the first `count` files the list names, 500 rows to a commit, and prints the
//!   files' bytes, the bytes put in (each line's and 8 for its number) and the store's size.
//!   It prints nothing of a file but those counts.
//! - `measure index <folder>` measures similarity search under each mode on a million generated
//!   vectors of 768 numbers in one partition, a row being an INTEGER key and the vector, and
//!   then on a million generated sightings: lines `synthetic_1000000_...` and
//!   `sightings_1000000_...`. `measure synthetic <rows> <folder>` and `measure sightings <rows>
//!   <folder>` do one of the two at another size. The first are the index pilot's vectors, to
//!   the number: a xorshift from `0x9E37_79B9_7F4A_7C15`, 300 hidden centres, each vector a
//!   centre plus 0.06 of noise a number, and 100 questions with four thirds of that noise. The
//!   second stand in for a camera store: three classes, a subject for every 2,500 rows with
//!   eight directions of its own for its appearance, tracks of ten sightings whose appearance
//!   drifts, a little noise on every number; 100 questions, eighty new sightings of stored
//!   subjects and twenty of subjects never stored. Each case loads two stores, 500 rows to a
//!   commit: one whose column declares `AUTO_INDEX_AT 1000000000`, so that nothing is grouped,
//!   sealed and searched under EXACT; and one that declares nothing, with every maintenance
//!   cycle run, opened and searched once in a new process (`measure first ...`, which the
//!   program starts itself as) and then searched, in one read session under 1 GiB, under
//!   EXACT, INDEXED and AUTO in turn: every question once after 5 searches of each mode that
//!   are not timed. On sightings a hundred stored vectors are then each searched for by itself.
//!
//! The store's folder must be missing or empty. A store's size is the bytes of every file in
//! its folder after it was closed.
//!
//! This program measures. It never passes or fails, no test reads what it prints, and
//! `numbers.sh` is what runs it.

mod support;

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use contextdb::{Database, DatabaseOpenOptions, Result, Uuid, params};
use support::{
    EXACT, Fallible, READER_LIMIT, Values, bytes_on_disk, line, mebibytes, milliseconds,
    percentile, ratio, seconds, timed,
};

#[global_allocator]
static ALLOCATOR: support::Counting = support::Counting;

/// The most memory the operating system has seen this process hold, where it says so.
fn peak_resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kibibytes: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kibibytes * 1024)
}

/// 20,000 notes of 1.6 KB, one commit per row.
fn readme(folder: &Path) -> Result<()> {
    const ROWS: u128 = 20_000;
    let db = Database::open(folder)?;
    db.execute(
        "CREATE TABLE notes (id UUID PRIMARY KEY, body TEXT)",
        &params! {},
    )?;
    let (inserted, insert) = timed(|| -> Result<()> {
        for n in 1..=ROWS {
            let body = format!("note {n:08} ").repeat(115)[..1600].to_owned();
            db.execute(
                "INSERT INTO notes (id, body) VALUES ($id, $body)",
                &params! { "id" => Uuid::from_u128(n), "body" => body },
            )?;
        }
        Ok(())
    });
    inserted?;
    db.close()?;
    line("readme_rows", ROWS, "rows");
    line("readme_insert", seconds(insert), "s");
    let on_disk = bytes_on_disk(folder).expect("the store's folder can be listed");
    line("readme_store_size", mebibytes(on_disk), "MiB");

    let (db, open) = timed(|| Database::open(folder));
    let db = db?;
    line("readme_open", milliseconds(open), "ms");
    let (count, counting) = timed(|| db.execute("SELECT COUNT(*) FROM notes", &params! {}));
    let count = count?;
    line("readme_count_star", milliseconds(counting), "ms");
    line(
        "readme_count_star_rows_examined",
        count.rows_examined(),
        "rows",
    );
    db.close()
}

/// A million small rows loaded and scanned under a 64 MiB limit.
fn million(folder: &Path) -> Result<()> {
    const ROWS: i64 = 1_000_000;
    const PER_COMMIT: i64 = 10_000;
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(64 << 20);
    let db = Database::open_with_options(folder, options)?;
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY, n INTEGER, word TEXT)",
        &params! {},
    )?;
    let (loaded, load) = timed(|| -> Result<()> {
        for first in (1..=ROWS).step_by(PER_COMMIT as usize) {
            let tx = db.transaction()?;
            for id in first..first + PER_COMMIT {
                tx.execute(
                    "INSERT INTO small (id, n, word) VALUES ($id, $n, $word)",
                    &params! { "id" => id, "n" => id % 1000, "word" => format!("w{id:07}") },
                )?;
            }
            tx.commit()?;
        }
        Ok(())
    });
    loaded?;
    let (sum, scan) = timed(|| db.execute("SELECT SUM(n) FROM small", &params! {}));
    let sum = sum?;
    line("million_rows_load", seconds(load), "s");
    line("million_rows_scan", milliseconds(scan), "ms");
    line(
        "million_rows_scan_rows_examined",
        sum.rows_examined(),
        "rows",
    );
    line(
        "million_rows_peak_allocated",
        mebibytes(support::take_peak_allocated()),
        "MiB",
    );
    if let Some(bytes) = peak_resident_bytes() {
        line("million_rows_peak_resident", mebibytes(bytes), "MiB");
    }
    db.close()
}

/// The numbers a vector is made of: the same for the same `seed` on every machine, each from
/// -1 up to 1.
fn vector_of(seed: u64, dimensions: usize) -> Vec<f32> {
    let mut state = seed;
    let mut next = || {
        // SplitMix64.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    };
    (0..dimensions)
        .map(|_| (next() >> 40) as f32 / (1u64 << 23) as f32 - 1.0)
        .collect()
}

/// 20,000 vectors of 384 numbers, sealed, and the 10 nearest of 100 others.
fn vectors(folder: &Path) -> Result<()> {
    const ROWS: i64 = 20_000;
    const PER_COMMIT: i64 = 500;
    const DIMENSIONS: usize = 384;
    let db = Database::open(folder)?;
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(384))",
        &params! {},
    )?;
    for first in (1..=ROWS).step_by(PER_COMMIT as usize) {
        let tx = db.transaction()?;
        for id in first..first + PER_COMMIT {
            tx.execute(
                "INSERT INTO items (id, embedding) VALUES ($id, $embedding)",
                &params! { "id" => id, "embedding" => vector_of(id as u64, DIMENSIONS) },
            )?;
        }
        tx.commit()?;
    }
    while db
        .run_maintenance_cycle()?
        .vector
        .maintenance_vectors_remaining
        > 0
    {}
    let mut times = Vec::new();
    for n in -5..100 {
        let q = vector_of((ROWS + 6 + n) as u64, DIMENSIONS);
        let (found, time) = timed(|| {
            db.execute(
                "SELECT id, embedding <=> $q AS distance FROM items ORDER BY distance LIMIT 10",
                &params! { "q" => q },
            )
        });
        found?;
        if n >= 0 {
            times.push(time);
        }
    }
    db.close()?;
    line(
        "vectors_20000_find_p95",
        milliseconds(percentile(&times, 0.95)),
        "ms",
    );
    let on_disk = bytes_on_disk(folder).expect("the store's folder can be listed");
    line("vectors_20000_store_size", mebibytes(on_disk), "MiB");
    let held = ROWS as u64 * (8 + 4 * DIMENSIONS as u64);
    line("vectors_20000_size_over_held", ratio(on_disk, held), "x");
    Ok(())
}

/// Stops the program with a sentence that holds nothing of a session file.
fn stop(sentence: &str, position: usize) -> ! {
    eprintln!("file {position} of the list {sentence}");
    std::process::exit(1);
}

/// The lines of the first `count` files the list names, one row each.
fn sessions(corpus: &Path, list: &Path, count: usize, folder: &Path) -> Result<()> {
    const PER_COMMIT: u32 = 500;
    let names = std::fs::read_to_string(list).expect("the list file can be read");
    let names = names.lines().filter(|name| !name.is_empty()).take(count);
    let db = Database::open(folder)?;
    db.execute(
        "CREATE TABLE session_lines (id INTEGER PRIMARY KEY, body TEXT NOT NULL)",
        &params! {},
    )?;
    let (mut files, mut source, mut put_in, mut id) = (0usize, 0u64, 0u64, 0i64);
    let (loaded, load) = timed(|| -> Result<()> {
        let mut tx = db.transaction()?;
        let mut in_commit = 0;
        for name in names {
            files += 1;
            let Ok(file) = File::open(corpus.join(name)) else {
                stop("cannot be opened", files);
            };
            let mut file = BufReader::new(file);
            loop {
                let mut bytes = Vec::new();
                match file.read_until(b'\n', &mut bytes) {
                    Ok(0) => break,
                    Ok(read) => source += read as u64,
                    Err(_) => stop("cannot be read", files),
                }
                if bytes.last() == Some(&b'\n') {
                    bytes.pop();
                }
                if bytes.is_empty() {
                    continue;
                }
                let Ok(body) = String::from_utf8(bytes) else {
                    stop("has a line that is not UTF-8", files);
                };
                id += 1;
                put_in += body.len() as u64 + 8;
                tx.execute(
                    "INSERT INTO session_lines (id, body) VALUES ($id, $body)",
                    &params! { "id" => id, "body" => body },
                )?;
                in_commit += 1;
                if in_commit == PER_COMMIT {
                    std::mem::replace(&mut tx, db.transaction()?).commit()?;
                    in_commit = 0;
                }
            }
        }
        tx.commit()
    });
    loaded?;
    db.close()?;
    let on_disk = bytes_on_disk(folder).expect("the store's folder can be listed");
    let name = |what: &str| format!("sessions_{files}_{what}");
    line(&name("source_size"), mebibytes(source), "MiB");
    line(&name("rows"), id, "rows");
    line(&name("load"), seconds(load), "s");
    line(&name("put_in_size"), mebibytes(put_in), "MiB");
    line(&name("store_size"), mebibytes(on_disk), "MiB");
    line(&name("size_over_source"), ratio(on_disk, source), "x");
    line(&name("size_over_put_in"), ratio(on_disk, put_in), "x");
    Ok(())
}

/// The index pilot's generator of numbers: a xorshift, and a number near a normal one made of
/// six of its draws.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn g(&mut self) -> f32 {
        let s: f32 = (0..6)
            .map(|_| (self.next() >> 40) as f32 / (1u64 << 24) as f32)
            .sum();
        (s - 3.0) * 1.414
    }
}

const DIMENSIONS: usize = 768;

/// What a case's rows are handed to, one vector after another.
type Each<'a> = &'a mut dyn FnMut(Vec<f32>) -> Fallible<()>;

/// The index pilot's vectors, to the number: hands `each` every row in order and answers the
/// 100 questions, which the generator makes after the rows.
fn synthetic(rows: usize, each: Each<'_>) -> Fallible<Vec<Vec<f32>>> {
    fn norm(v: &mut [f32]) {
        let n = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.iter_mut().for_each(|x| *x /= n);
    }
    let mut rng = Random(0x9E37_79B9_7F4A_7C15);
    let (nc, noise) = (300_u64, 0.06_f32);
    let mut centers = Vec::new();
    for _ in 0..nc {
        let mut c: Vec<f32> = (0..DIMENSIONS).map(|_| rng.g()).collect();
        norm(&mut c);
        centers.push(c);
    }
    for _ in 0..rows {
        let c = &centers[(rng.next() % nc) as usize];
        let mut v: Vec<f32> = c.iter().map(|x| x + noise * rng.g()).collect();
        norm(&mut v);
        each(v)?;
    }
    let mut questions = Vec::new();
    for _ in 0..100 {
        let c = &centers[(rng.next() % nc) as usize];
        let mut q: Vec<f32> = c.iter().map(|x| x + noise * 4.0 / 3.0 * rng.g()).collect();
        norm(&mut q);
        questions.push(q);
    }
    Ok(questions)
}

/// Generated sightings, to the number as the second measurement of the index made them. A
/// subject is a direction of its own beside its class's; its appearances vary along eight
/// directions of its own; a track is ten sightings in a row whose appearance drifts; every
/// sighting has a little noise on every number.
struct Sightings {
    subjects: usize,
    classes: Vec<Vec<f32>>,
    bases: Vec<Vec<f32>>,
    axes: Vec<Vec<f32>>,
    /// The generator's state after the directions, from which the questions are made.
    after: u64,
}

impl Sightings {
    const AXES: usize = 8;
    const TRACK: usize = 10;

    fn unit(v: &mut [f32]) {
        let n = v.iter().map(|x| f64::from(*x) * f64::from(*x));
        let n = n.sum::<f64>().sqrt();
        if n > 0.0 {
            for x in v {
                *x = (f64::from(*x) / n) as f32;
            }
        }
    }

    fn new(rows: usize) -> Sightings {
        let subjects = (rows / 2500).max(8);
        let mut random = Random(0x51A7_1265_0BAD_CAFE);
        let mut directions = |count: usize| -> Vec<Vec<f32>> {
            let direction = |_| {
                let mut c: Vec<f32> = (0..DIMENSIONS).map(|_| random.g()).collect();
                Sightings::unit(&mut c);
                c
            };
            (0..count).map(direction).collect()
        };
        Sightings {
            subjects,
            classes: directions(3),
            bases: directions(subjects + 20),
            axes: directions((subjects + 20) * Sightings::AXES),
            after: random.0,
        }
    }

    /// One sighting of a subject with this appearance.
    fn make(&self, subject: usize, appearance: &[f32; 8], random: &mut Random) -> Vec<f32> {
        let class = &self.classes[subject % 3];
        let base = &self.bases[subject];
        let mut out: Vec<f32> = (0..DIMENSIONS)
            .map(|d| {
                0.5 * class[d] + 0.6 * base[d] + 0.25 * random.g() / (DIMENSIONS as f32).sqrt()
            })
            .collect();
        for (j, a) in appearance.iter().enumerate() {
            let axis = &self.axes[subject * Sightings::AXES + j];
            let weight = 0.5 * a / (Sightings::AXES as f32).sqrt();
            for (x, along) in out.iter_mut().zip(axis) {
                *x += weight * along;
            }
        }
        Sightings::unit(&mut out);
        out
    }

    /// The first `count` sightings of one track.
    fn track(&self, track: usize, count: usize) -> Vec<Vec<f32>> {
        let seed = (track as u64 + 1).wrapping_mul(0x2545_F491_4F6C_DD1D);
        let mut random = Random(0x51A7_1265_0BAD_CAFE ^ seed);
        random.next();
        let subject = random.below(self.subjects);
        let mut appearance = [0f32; 8];
        for a in &mut appearance {
            *a = random.g();
        }
        let mut sightings = Vec::with_capacity(count);
        for _ in 0..count {
            sightings.push(self.make(subject, &appearance, &mut random));
            for a in &mut appearance {
                *a = 0.9 * *a + 0.436 * random.g();
            }
        }
        sightings
    }

    /// Hands `each` the first `rows` sightings, track after track.
    fn rows(&self, rows: usize, each: Each<'_>) -> Fallible<()> {
        for track in 0..rows.div_ceil(Sightings::TRACK) {
            let count = Sightings::TRACK.min(rows - track * Sightings::TRACK);
            for sighting in self.track(track, count) {
                each(sighting)?;
            }
        }
        Ok(())
    }

    /// The 100 questions: eighty new sightings of stored subjects, twenty of subjects never
    /// stored.
    fn questions(&self) -> Vec<Vec<f32>> {
        let mut random = Random(self.after);
        let mut questions = Vec::new();
        for j in 0..100 {
            let subject = if j % 5 == 4 {
                self.subjects + j / 5
            } else {
                random.below(self.subjects)
            };
            let mut appearance = [0f32; 8];
            for a in &mut appearance {
                *a = random.g();
            }
            questions.push(self.make(subject, &appearance, &mut random));
        }
        questions
    }
}

/// The search of a generated case: what follows `ORDER BY distance`, and the vector.
fn nearest(tail: &str, question: &[f32]) -> (String, Values) {
    let sql =
        format!("SELECT id, embedding <=> $query AS distance FROM items ORDER BY distance{tail}");
    (sql, params! { "query" => question.to_vec() })
}

/// A new store of the generated cases' table, its vector column declared with `clauses`, and
/// every row that `rows` hands over inserted, 500 to a commit, under the keys 0, 1, 2 and so
/// on. Answers the store and what `rows` answers.
fn filled<T>(
    store: &Path,
    clauses: &str,
    rows: &dyn Fn(Each<'_>) -> Fallible<T>,
) -> Fallible<(Database, T)> {
    let db = Database::open(store)?;
    db.execute(
        &format!("CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(768){clauses})"),
        &params! {},
    )?;
    let mut tx = db.transaction()?;
    let mut id = 0_i64;
    let answered = rows(&mut |vector| {
        tx.execute(
            "INSERT INTO items (id, embedding) VALUES ($id, $embedding)",
            &params! { "id" => id, "embedding" => vector },
        )?;
        id += 1;
        if id % 500 == 0 {
            std::mem::replace(&mut tx, db.transaction()?).commit()?;
        }
        Ok(())
    })?;
    tx.commit()?;
    Ok((db, answered))
}

/// One generated case: the two stores and every line taken on them. `rows` hands over the
/// case's rows and answers its questions; `stored` are rows to search for by themselves, each
/// with its key.
fn index(
    prefix: &str,
    folder: &Path,
    count: usize,
    rows: &dyn Fn(Each<'_>) -> Fallible<Vec<Vec<f32>>>,
    stored: &[(u128, Vec<f32>)],
) -> Fallible<()> {
    support::must_be_empty(folder);
    std::fs::create_dir_all(folder)?;
    let store = folder.join("sealed");
    let ((db, questions), load) = {
        let (filled, load) = timed(|| filled(&store, " AUTO_INDEX_AT 1000000000", rows));
        (filled?, load)
    };
    line(&format!("{prefix}load"), seconds(load), "s");
    let sealed = support::sealed(prefix, &db, &store)?;
    let reader = support::session(&store, READER_LIMIT)?;
    let before = support::ask(&reader, &nearest, &[EXACT], &questions, 1)?;
    let name = format!("{prefix}find_exact_before");
    support::setting(&name, READER_LIMIT);
    support::times(&name, &before[0]);
    support::counted(&name, &before[0]);
    drop(reader);

    let store = folder.join("built");
    let (db, _) = filled(&store, "", rows)?;
    let held = count as u64 * (8 + 4 * DIMENSIONS as u64);
    let parts = support::built(prefix, &db, &store, (held, sealed, DIMENSIONS))?;
    let question = folder.join("question.f32");
    let bytes = questions[0].iter().flat_map(|number| number.to_le_bytes());
    std::fs::write(&question, bytes.collect::<Vec<u8>>())?;
    let first = [OsStr::new("first"), store.as_os_str(), OsStr::new(prefix)];
    support::in_new_process(&[first[0], first[1], first[2], question.as_os_str()])?;
    let reader = support::session(&store, READER_LIMIT)?;
    let find = format!("{prefix}find");
    let every = support::selected(&parts, &[]);
    support::modes(&find, &reader, &nearest, &questions, (1, &every[..]))?;
    if !stored.is_empty() {
        let name = format!("{find}_own_grouped_found");
        support::own(&name, &reader, &nearest, stored)?;
    }
    Ok(())
}

/// The index pilot's vectors in one partition.
fn synthetic_case(rows: usize, folder: &Path) -> Fallible<()> {
    let prefix = format!("synthetic_{rows}_");
    index(&prefix, folder, rows, &|each| synthetic(rows, each), &[])
}

/// Generated sightings in one partition, and a hundred of them, evenly spread, each searched
/// for by itself.
fn sightings_case(rows: usize, folder: &Path) -> Fallible<()> {
    let sightings = Sightings::new(rows);
    let mut stored = Vec::new();
    for row in (0..100).map(|i| i * rows / 100) {
        let (track, place) = (row / Sightings::TRACK, row % Sightings::TRACK);
        if let Some(vector) = sightings.track(track, place + 1).pop() {
            stored.push((row as u128, vector));
        }
    }
    let load = |each: Each<'_>| {
        sightings.rows(rows, each)?;
        Ok(sightings.questions())
    };
    index(&format!("sightings_{rows}_"), folder, rows, &load, &stored)
}

/// What the program does when it starts itself: opens the built store and runs one search for
/// the vector the file holds.
fn first(store: &Path, prefix: &str, question: &Path) -> Fallible<()> {
    let question = std::fs::read(question)?;
    let (numbers, _) = question.as_chunks::<4>();
    let question: Vec<f32> = numbers
        .iter()
        .map(|four| f32::from_le_bytes(*four))
        .collect();
    let names = (format!("{prefix}open"), format!("{prefix}find_auto_first"));
    support::first(
        store,
        (&names.0, &names.1),
        &nearest(support::AUTO, &question),
    )
}

fn usage() -> ! {
    eprintln!(
        "usage: measure readme|million|vectors|index <folder>\n       \
         measure sessions <corpus folder> <list file> <count> <folder>\n       \
         measure synthetic|sightings <rows> <folder>"
    );
    std::process::exit(2);
}

fn main() -> Fallible<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    if let ["first", store, prefix, question] = arguments.as_slice() {
        return first(Path::new(store), prefix, Path::new(question));
    }
    let Some(folder) = arguments.last().map(Path::new) else {
        usage();
    };
    support::must_be_empty(folder);
    match arguments.as_slice() {
        ["readme", _] => readme(folder)?,
        ["million", _] => million(folder)?,
        ["vectors", _] => vectors(folder)?,
        ["sessions", corpus, list, count, _] => match count.parse() {
            Ok(count) => sessions(Path::new(corpus), Path::new(list), count, folder)?,
            Err(_) => usage(),
        },
        ["index", _] => {
            synthetic_case(1_000_000, &folder.join("synthetic"))?;
            sightings_case(1_000_000, &folder.join("sightings"))?;
        }
        [which @ ("synthetic" | "sightings"), rows, _] => match (*which, rows.parse()) {
            ("synthetic", Ok(rows)) => synthetic_case(rows, folder)?,
            (_, Ok(rows)) => sightings_case(rows, folder)?,
            _ => usage(),
        },
        _ => usage(),
    }
    Ok(())
}
