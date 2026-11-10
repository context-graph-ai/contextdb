//! Prints the numbers this library is held to, one per line as `name value unit`.
//!
//! `measure readme <folder>` runs the README's first program at 20,000 rows of 1.6 KB, one
//! commit per row, and prints the store's size on disk, how long opening it takes and how long
//! `COUNT(*)` takes. `measure million <folder>` loads a million small rows under a 64 MiB limit
//! in commits of 10,000, scans them, and prints the most memory the process had allocated beside
//! the most the operating system saw it hold. The folder must be missing or empty.
//!
//! This program measures. It never passes or fails, no test reads what it prints, and
//! `numbers.sh` is what runs it.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use contextdb::{Database, DatabaseOpenOptions, Result, Uuid, params};

/// The system allocator, counting the bytes handed out and not yet given back.
struct Counting;

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

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `work` and says how long it took. Apart from the library's `system` part, this is the
/// only place in the workspace that reads a clock.
#[allow(
    clippy::disallowed_methods,
    reason = "measuring elapsed time is what this program is for"
)]
fn timed<T>(work: impl FnOnce() -> T) -> (T, Duration) {
    let start = Instant::now();
    let result = work();
    (result, start.elapsed())
}

fn line(name: &str, value: impl Display, unit: &str) {
    println!("{name} {value} {unit}");
}

fn milliseconds(duration: Duration) -> String {
    format!("{:.3}", duration.as_secs_f64() * 1000.0)
}

fn mebibytes(bytes: u64) -> String {
    format!("{:.1}", bytes as f64 / 1_048_576.0)
}

/// The bytes of every file under a folder.
fn bytes_on_disk(folder: &Path) -> std::io::Result<u64> {
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

/// The most memory the operating system has seen this process hold, where it says so.
fn peak_resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
    let kibibytes: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kibibytes * 1024)
}

/// The README's first program, at 20,000 rows of 1.6 KB.
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
    line("readme_insert", format!("{:.1}", insert.as_secs_f64()), "s");
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
    line(
        "million_rows_load",
        format!("{:.1}", load.as_secs_f64()),
        "s",
    );
    line("million_rows_scan", milliseconds(scan), "ms");
    line(
        "million_rows_scan_rows_examined",
        sum.rows_examined(),
        "rows",
    );
    line(
        "million_rows_peak_allocated",
        mebibytes(PEAK.load(Ordering::Relaxed) as u64),
        "MiB",
    );
    if let Some(bytes) = peak_resident_bytes() {
        line("million_rows_peak_resident", mebibytes(bytes), "MiB");
    }
    db.close()
}

fn main() -> Result<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let [what, folder] = arguments.as_slice() else {
        eprintln!("usage: measure readme|million <folder>");
        std::process::exit(2);
    };
    let folder = Path::new(folder);
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
    match what.as_str() {
        "readme" => readme(folder),
        "million" => million(folder),
        _ => {
            eprintln!("usage: measure readme|million <folder>");
            std::process::exit(2);
        }
    }
}
