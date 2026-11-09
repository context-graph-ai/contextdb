//! What a bulk import actually holds, per passage, and whether the declared
//! memory limit knows about it.
//!
//! The consumer's import is reported to retain roughly 1.7x the store size in
//! anonymous memory, growing linearly for the whole build — about 110–170 KB
//! per passage, never released — so that a 100,000-passage import needs around
//! 18 GiB of RAM and a 14 GiB cgroup kills it. This probe reproduces the shape
//! at a scale that fits in a test: it writes passage-shaped rows into a
//! cg-shaped table, samples the process's own anonymous resident set from
//! `/proc/self/smaps_rollup` as it goes, and reports two numbers side by side —
//! the anonymous bytes the process gained per stored row, and what the
//! `MemoryAccountant` believes it is holding.
//!
//! The gap between those two is the finding. Memory the accountant does not
//! charge is memory a declared `MEMORY_LIMIT` cannot bound, whatever its value.

use contextdb_core::Value;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

const DIM: usize = 768;
/// Enough rows to separate a per-row slope from the fixed cost of opening a
/// store, and few enough that the probe stays inside an ordinary test budget.
const ROWS: usize = 2_000;

/// Anonymous resident bytes for this process: private, dirty, non-file memory —
/// the thing a cgroup kills for, and the thing the report measured.
fn anonymous_resident_bytes() -> u64 {
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup")
        .expect("this probe reads its own anonymous resident set from /proc");
    for line in rollup.lines() {
        if let Some(rest) = line.strip_prefix("Anonymous:") {
            let kb = rest
                .trim()
                .trim_end_matches(" kB")
                .parse::<u64>()
                .expect("smaps_rollup reports kilobytes");
            return kb * 1024;
        }
    }
    panic!("smaps_rollup has no Anonymous line:\n{rollup}");
}

fn row_uuid(row: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x9a55_0000_0000_0000_0000_0000_0000_0000 + row as u128)
}

fn context_uuid(context: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc0de_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}

/// Fixed finite unit vectors: no randomness between runs.
fn vector(row: usize) -> Vec<f32> {
    let mut state = row as u64 + 0x9e37_79b9;
    let mut values: Vec<f32> = (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state as u32 as f64 / u32::MAX as f64 - 0.5) as f32
        })
        .collect();
    let norm = values.iter().map(|x| x * x).sum::<f32>().sqrt();
    for value in &mut values {
        *value /= norm;
    }
    values
}

/// The bytes one passage genuinely carries: its vector and its text.
const TEXT_BYTES: usize = 512;

/// One arm of the matrix: how the table is declared, and what it is given.
struct Arm {
    what: &'static str,
    ddl: &'static str,
    index: Option<&'static str>,
    vector: bool,
}

const ARMS: &[Arm] = &[
    Arm {
        what: "rows only",
        ddl: "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
              body TEXT NOT NULL)",
        index: None,
        vector: false,
    },
    Arm {
        what: "rows + secondary index",
        ddl: "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
              body TEXT NOT NULL)",
        index: Some("CREATE INDEX passages_by_context ON passages (context_id, id)"),
        vector: false,
    },
    Arm {
        what: "rows + vector",
        ddl: "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
              body TEXT NOT NULL, embedding VECTOR(768) PARTITION_KEY (context_id) \
              MAX_PARTITIONS 8)",
        index: None,
        vector: true,
    },
    Arm {
        what: "rows + index + vector",
        ddl: "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
              body TEXT NOT NULL, embedding VECTOR(768) PARTITION_KEY (context_id) \
              MAX_PARTITIONS 8)",
        index: Some("CREATE INDEX passages_by_context ON passages (context_id, id)"),
        vector: true,
    },
];

struct Measured {
    anon_per_row: u64,
    charged_per_row: u64,
    anon_after_close: u64,
}

fn measure(arm: &Arm, rows: usize) -> Measured {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let before_open = anonymous_resident_bytes();
    let db = Database::open(root.path().join("writer-memory.db")).expect("open the writable store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(arm.ddl, &HashMap::new())
        .expect("declare the passage table");
    if let Some(index) = arm.index {
        db.execute(index, &HashMap::new())
            .expect("declare the index");
    }

    let baseline = anonymous_resident_bytes();
    let baseline_charged = db.accountant().usage().used;
    let body = "p".repeat(TEXT_BYTES);
    let statement = if arm.vector {
        "INSERT INTO passages VALUES ($id, $context, $body, $embedding)"
    } else {
        "INSERT INTO passages VALUES ($id, $context, $body)"
    };
    for row in 0..rows {
        let mut params = HashMap::from([
            ("id".to_owned(), Value::Uuid(row_uuid(row))),
            ("context".to_owned(), Value::Uuid(context_uuid(row % 8))),
            ("body".to_owned(), Value::Text(body.clone())),
        ]);
        if arm.vector {
            params.insert("embedding".to_owned(), Value::Vector(vector(row)));
        }
        db.execute(statement, &params).expect("store one passage");
    }

    let anon = anonymous_resident_bytes();
    let charged = db.accountant().usage().used;
    db.close().expect("close the store");
    drop(db);
    let after_close = anonymous_resident_bytes();

    Measured {
        anon_per_row: anon.saturating_sub(baseline) / rows as u64,
        charged_per_row: charged.saturating_sub(baseline_charged) as u64 / rows as u64,
        // What the process still holds once the store is gone, above where it
        // was before the store was opened. A store's retention comes back here;
        // a leak does not.
        anon_after_close: after_close.saturating_sub(before_open),
    }
}

const ARM_ENV: &str = "CONTEXTDB_WRITER_MEMORY_ARM";
const REUSE_ENV: &str = "CONTEXTDB_WRITER_MEMORY_REUSE";
const CG_ENV: &str = "CONTEXTDB_WRITER_MEMORY_CG";
/// Events, not rows: the consumer's unit of import. Fewer than the row arms
/// because each one writes three rows through the whole pinned cg schema.
const CG_EVENTS: usize = 500;
const ARM_RESULT: &str = "ARM_RESULT ";

/// One arm, in a process of its own.
///
/// The arms cannot share a process. A general-purpose allocator does not return
/// a freed heap to the kernel promptly, so a second arm measured after a first
/// one starts from an already-grown anonymous set and reports a per-row figure
/// below what it really costs — in the first attempt at this the last arm
/// reported fewer anonymous bytes per row than the accountant had charged,
/// which is impossible and is how the contamination announced itself.
#[test]
fn writer_memory_arm_child() {
    let Ok(index) = std::env::var(ARM_ENV) else {
        return;
    };
    let arm = &ARMS[index.parse::<usize>().expect("arm index")];
    let measured = measure(arm, ROWS);
    println!(
        "{ARM_RESULT}{} {} {}",
        measured.anon_per_row, measured.charged_per_row, measured.anon_after_close
    );
    use std::io::Write;
    std::io::stdout().flush().expect("flush the arm result");
}

/// Whether the memory a closed store held is FREE or LIVE.
///
/// The anonymous set barely falls when a store closes, but that alone does not
/// say which: a general-purpose allocator keeps a freed heap mapped rather than
/// handing it back to the kernel. What separates the two is whether the next
/// store can have it. Fill a store, close it, drop it, then fill a second store
/// exactly like it in the same process. If the first store's bytes were freed,
/// the allocator satisfies the second fill out of them and the process barely
/// grows. If they are still live, the process grows by the same amount again.
#[test]
fn writer_memory_reuse_child() {
    if std::env::var(REUSE_ENV).is_err() {
        return;
    }
    let arm = &ARMS[ARMS.len() - 1];
    let before_first = anonymous_resident_bytes();
    let first = measure(arm, ROWS);
    let after_first = anonymous_resident_bytes();
    let second = measure(arm, ROWS);
    let after_second = anonymous_resident_bytes();
    println!(
        "{ARM_RESULT}{} {} {}",
        after_first.saturating_sub(before_first),
        after_second.saturating_sub(after_first),
        first.anon_per_row.saturating_sub(second.anon_per_row),
    );
    use std::io::Write;
    std::io::stdout().flush().expect("flush the reuse result");
}

/// What one cg EVENT costs the writer, through the consumer's own pinned
/// startup schema rather than a two-column probe table.
///
/// This is the arm that reconciles a per-row figure with a per-passage one: the
/// benchmark's "passage" is a group of events, and one event is three rows
/// across `events`, `event_anchors` and `event_local_commits` — through every
/// index those tables declare.
#[test]
fn writer_memory_cg_event_child() {
    if std::env::var(CG_ENV).is_err() {
        return;
    }
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db =
        Database::open(root.path().join("cg-writer-memory.db")).expect("open the writable store");
    crate::one_event_append_work_by_family::declare(&db);
    crate::one_event_append_work_by_family::append_event(&db, 0);

    let baseline = anonymous_resident_bytes();
    let baseline_charged = db.accountant().usage().used;
    contextdb_engine::append_work::reset();
    for event in 1..=CG_EVENTS {
        crate::one_event_append_work_by_family::append_event(&db, event);
    }
    let families = contextdb_engine::append_work::take_family_work();
    let anon = anonymous_resident_bytes();
    let charged = db.accountant().usage().used;

    let family = |name: &str| {
        families
            .iter()
            .find(|(_, family, _)| *family == name)
            .map(|(_, _, work)| work.mutations)
            .unwrap_or(0)
    };
    println!(
        "{ARM_RESULT}{} {} {} {} {}",
        anon.saturating_sub(baseline) / CG_EVENTS as u64,
        charged.saturating_sub(baseline_charged) as u64 / CG_EVENTS as u64,
        family("Relational") / CG_EVENTS as u64,
        family("SchemaAndIndexes") / CG_EVENTS as u64,
        family("F32Vectors") / CG_EVENTS as u64,
    );
    use std::io::Write;
    std::io::stdout().flush().expect("flush the cg result");
    db.close().expect("close the store");
}

fn run_arm(index: usize) -> Measured {
    let output = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "writer_memory_arm_child",
        ))
        .arg("--nocapture")
        .env(ARM_ENV, index.to_string())
        .output()
        .expect("run one arm in a process of its own");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix(ARM_RESULT))
        .unwrap_or_else(|| {
            panic!(
                "arm {index} printed no result:\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    let mut parts = line.split_whitespace();
    let mut next = || {
        parts
            .next()
            .expect("three numbers")
            .parse::<u64>()
            .expect("a number")
    };
    Measured {
        anon_per_row: next(),
        charged_per_row: next(),
        anon_after_close: next(),
    }
}

/// The matrix. Each arm adds one thing, so the difference between two arms is
/// what that one thing costs per row.
#[test]
#[ignore = "measuring writer memory per stored passage does not finish within five minutes"]
fn writer_memory_per_stored_passage() {
    eprintln!(
        "writer memory per stored row, {ROWS} rows per arm; content per row is \
         {TEXT_BYTES} B of text plus {} B of vector where declared",
        DIM * 4
    );
    eprintln!(
        "{:<26}  {:>10}  {:>10}  {:>11}  {:>16}",
        "arm", "anon/row", "charged/row", "uncharged/row", "held after close"
    );
    let mut results = Vec::new();
    for (index, arm) in ARMS.iter().enumerate() {
        let measured = run_arm(index);
        eprintln!(
            "{:<26}  {:>10}  {:>10}  {:>11}  {:>16}",
            arm.what,
            measured.anon_per_row,
            measured.charged_per_row,
            measured
                .anon_per_row
                .saturating_sub(measured.charged_per_row),
            measured.anon_after_close,
        );
        results.push((arm.what, measured));
    }

    let by = |what: &str| -> &Measured {
        &results
            .iter()
            .find(|(name, _)| *name == what)
            .expect("every arm ran")
            .1
    };
    let rows_only = by("rows only");
    let with_index = by("rows + secondary index");
    let with_vector = by("rows + vector");
    let everything = by("rows + index + vector");
    eprintln!(
        "per-row deltas: a secondary index adds {} B anon ({} B charged); a 768-dim vector column \
         adds {} B anon ({} B charged) against {} B of vector content; both together reach {} B \
         anon ({} B charged)",
        with_index
            .anon_per_row
            .saturating_sub(rows_only.anon_per_row),
        with_index
            .charged_per_row
            .saturating_sub(rows_only.charged_per_row),
        with_vector
            .anon_per_row
            .saturating_sub(rows_only.anon_per_row),
        with_vector
            .charged_per_row
            .saturating_sub(rows_only.charged_per_row),
        DIM * 4,
        everything.anon_per_row,
        everything.charged_per_row,
    );

    assert!(
        everything.anon_per_row > 0,
        "the probe measured no anonymous growth; the sampling is broken, not the writer"
    );

    // Free or live: does a second store get the first one's bytes back?
    let output = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "writer_memory_reuse_child",
        ))
        .arg("--nocapture")
        .env(REUSE_ENV, "1")
        .output()
        .expect("run the reuse arm in a process of its own");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix(ARM_RESULT))
        .unwrap_or_else(|| {
            panic!(
                "the reuse arm printed no result:\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    let numbers = line
        .split_whitespace()
        .map(|part| part.parse::<u64>().expect("a number"))
        .collect::<Vec<_>>();
    let (first, second) = (numbers[0], numbers[1]);
    eprintln!(
        "after one store of {ROWS} rows closed and dropped, a second identical store in the same \
         process grew the process by {second} B against the first store's {first} B ({:.0}% of it). \
         A freed heap would be reused and the second figure would be near zero.",
        second as f64 * 100.0 / first.max(1) as f64,
    );

    // And what one cg EVENT costs, through the consumer's pinned startup
    // schema: the unit the import is counted in.
    let output = std::process::Command::new(std::env::current_exe().expect("current test binary"))
        .arg("--exact")
        .arg(crate::child_test_filter(
            module_path!(),
            "writer_memory_cg_event_child",
        ))
        .arg("--nocapture")
        .env(CG_ENV, "1")
        .output()
        .expect("run the cg-event arm in a process of its own");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix(ARM_RESULT))
        .unwrap_or_else(|| {
            panic!(
                "the cg-event arm printed no result:\nstdout:\n{stdout}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            )
        });
    let cg = line
        .split_whitespace()
        .map(|part| part.parse::<u64>().expect("a number"))
        .collect::<Vec<_>>();
    eprintln!(
        "one cg event through the pinned startup schema: {} B anonymous, {} B charged, writing {} \
         rows, {} ordered index postings and {} stored vectors",
        cg[0], cg[1], cg[2], cg[3], cg[4],
    );
}
