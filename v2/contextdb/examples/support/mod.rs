//! What the measuring programs of this folder share: an allocator that counts, the one timer,
//! the size of a folder, and the `name value unit` lines they print.

use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::Display;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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
