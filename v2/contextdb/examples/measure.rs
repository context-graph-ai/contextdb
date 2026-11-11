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
//!
//! The store's folder must be missing or empty. A store's size is the bytes of every file in
//! its folder after it was closed.
//!
//! This program measures. It never passes or fails, no test reads what it prints, and
//! `numbers.sh` is what runs it.

mod support;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use contextdb::{Database, DatabaseOpenOptions, Result, Uuid, params};
use support::{bytes_on_disk, line, mebibytes, milliseconds, percentile, ratio, seconds, timed};

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

fn usage() -> ! {
    eprintln!(
        "usage: measure readme|million|vectors <folder>\n       \
         measure sessions <corpus folder> <list file> <count> <folder>"
    );
    std::process::exit(2);
}

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let Some(folder) = arguments.last().map(Path::new) else {
        usage();
    };
    support::must_be_empty(folder);
    match arguments.as_slice() {
        ["readme", _] => readme(folder),
        ["million", _] => million(folder),
        ["vectors", _] => vectors(folder),
        ["sessions", corpus, list, count, _] => match count.parse() {
            Ok(count) => sessions(Path::new(corpus), Path::new(list), count, folder),
            Err(_) => usage(),
        },
        _ => usage(),
    }
}
