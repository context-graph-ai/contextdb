//! Loads a dump of search passages into a store and measures searches on them, printing one
//! number per line as `name value unit`.
//!
//! `passages <dump folder> <answers folder> <store folder>`
//!
//! The dump folder holds `rows.bin` (36 bytes for each passage, numbers low byte first: a row
//! number `u64`, the passage's id, the index `u32` of its Context, the index `u32` of its
//! (Context, generation) pair, and flags `u32`: 1 current, 2 searchable, 4 has a vector),
//! `vectors.f32` (768 `f32` for each passage, in the same order), `contexts.txt` (`index uuid
//! name` on each line), `pairs.txt` (`index context-index generation-uuid active`) and
//! `queries.f32` (768 `f32` for each question). The answers folder holds `engine-one.jsonl`,
//! `engine-several.jsonl` and `engine-all.jsonl`: on each line the ten answers recorded for one
//! question, `{"q": n, "ids": [...], "scores": [...]}`, a score being one minus the distance,
//! rounded to 32 bits. The store folder must be missing or empty.
//!
//! What it does, in order:
//!
//! 1. opens the store with no memory limit and inserts the passages, 500 to a commit;
//! 2. searches all Contexts for every question once through a read session, with every vector
//!    still waiting to be sealed;
//! 3. calls `run_maintenance_cycle` until nothing waits, closes the store and takes its size;
//! 4. through a read session with no memory limit, then one under 1 GiB, then one under
//!    64 MiB, searches one Context (the first), several (the first three) and all of them: 5
//!    searches that are not timed, then every question, each `execute` call timed whole;
//! 5. compares the answers of the session with no limit with the recorded ones.
//!
//! A time named `p50` or `p95` is the one at position `round((count - 1) * share)` of the
//! times in order. "Held" is the bytes of the values put in: 50 for a passage's five other
//! columns and 3,072 for its vector.
//!
//! This program measures. It never passes or fails, no test reads what it prints, and
//! `numbers.sh` is what runs it. It prints nothing of a passage but counts, sizes and times.

mod support;

use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::Duration;

use contextdb::serde_json::Value as Json;
use contextdb::{Database, ReadSession, ReadSessionOptions, Uuid, Value, params};
use support::{bytes_on_disk, line, mebibytes, milliseconds, percentile, ratio, seconds, timed};

#[global_allocator]
static ALLOCATOR: support::Counting = support::Counting;

type Fallible<T> = Result<T, Box<dyn std::error::Error>>;

const DIMENSIONS: usize = 768;
const PER_COMMIT: usize = 500;
const UNTIMED: usize = 5;
const SCOPES: [&str; 3] = ["one", "several", "all"];

/// What the dump says beside its passages.
struct Dump {
    /// Each Context's id, by index.
    contexts: Vec<Uuid>,
    /// Each pair's Context index, generation, and whether it is the Context's active one.
    pairs: Vec<(usize, Uuid, bool)>,
    /// The question vectors.
    questions: Vec<Vec<f32>>,
}

/// What the searches of one scope gave: for each question the time, the rows read, and the
/// ids and distances returned.
#[derive(Default)]
struct Finds {
    first: Duration,
    times: Vec<Duration>,
    examined: Vec<u64>,
    answers: Vec<Vec<(Uuid, f64)>>,
}

fn floats(bytes: &[u8]) -> Vec<f32> {
    let four = bytes.chunks_exact(4);
    four.map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        .collect()
}

fn read_dump(folder: &Path) -> Fallible<Dump> {
    let mut contexts = Vec::new();
    for text in std::fs::read_to_string(folder.join("contexts.txt"))?.lines() {
        contexts.push(text.split(' ').nth(1).ok_or("contexts.txt")?.parse()?);
    }
    let mut pairs = Vec::new();
    for text in std::fs::read_to_string(folder.join("pairs.txt"))?.lines() {
        let fields: Vec<&str> = text.split(' ').collect();
        let [_, context, generation, active] = fields.as_slice() else {
            return Err("pairs.txt".into());
        };
        pairs.push((context.parse()?, generation.parse()?, *active == "1"));
    }
    let questions = floats(&std::fs::read(folder.join("queries.f32"))?);
    let questions = questions.chunks_exact(DIMENSIONS).map(<[f32]>::to_vec);
    Ok(Dump {
        contexts,
        pairs,
        questions: questions.collect(),
    })
}

/// Inserts every passage of the dump. Returns how many there were and the bytes held.
fn load(db: &Database, folder: &Path, dump: &Dump) -> Fallible<(u64, u64)> {
    let rows = File::open(folder.join("rows.bin"))?;
    let passages = rows.metadata()?.len() / 36;
    let mut rows = BufReader::new(rows);
    let mut vectors = BufReader::new(File::open(folder.join("vectors.f32"))?);
    let (mut record, mut vector) = ([0u8; 36], vec![0u8; 4 * DIMENSIONS]);
    let mut held = 0;
    let mut tx = db.transaction()?;
    for passage in 1..=passages as usize {
        rows.read_exact(&mut record)?;
        vectors.read_exact(&mut vector)?;
        let number = |at: usize| {
            u32::from_le_bytes([record[at], record[at + 1], record[at + 2], record[at + 3]])
        };
        let (context, generation, _) = dump.pairs[number(28) as usize];
        let flags = number(32);
        let embedding = if flags & 4 == 0 {
            Value::Null
        } else {
            Value::Vector(floats(&vector))
        };
        held += 3 * 16
            + 2
            + if flags & 4 == 0 {
                0
            } else {
                4 * DIMENSIONS as u64
            };
        tx.execute(
            "INSERT INTO event_search_passages \
             (id, context_id, generation_id, vector_text, is_current, is_searchable) \
             VALUES ($id, $context, $generation, $vector, $current, $searchable)",
            &params! {
                "id" => Uuid::from_slice(&record[8..24])?,
                "context" => dump.contexts[context],
                "generation" => generation,
                "vector" => embedding,
                "current" => flags & 1 != 0,
                "searchable" => flags & 2 != 0,
            },
        )?;
        if passage % PER_COMMIT == 0 {
            std::mem::replace(&mut tx, db.transaction()?).commit()?;
        }
    }
    tx.commit()?;
    Ok((passages, held))
}

/// The search statement for the active pairs of the scope's Contexts, with its values.
fn statement(dump: &Dump, scope: &[usize], question: &[f32]) -> (String, HashMap<String, Value>) {
    let mut values = params! {
        "query" => question.to_vec(), "is_current" => true, "is_searchable" => true,
    };
    let active = dump.pairs.iter().filter(|pair| pair.2);
    let wanted = active.filter(|pair| scope.contains(&pair.0));
    let pairs: BTreeSet<(Uuid, Uuid)> =
        wanted.map(|pair| (dump.contexts[pair.0], pair.1)).collect();
    let mut names = Vec::new();
    for (i, (context, generation)) in pairs.into_iter().enumerate() {
        values.insert(format!("search_context_{i}"), Value::Uuid(context));
        values.insert(format!("search_generation_{i}"), Value::Uuid(generation));
        names.push(format!("($search_context_{i}, $search_generation_{i})"));
    }
    let filter = if names.len() == 1 {
        "context_id = $search_context_0 AND generation_id = $search_generation_0".to_owned()
    } else {
        format!("(context_id, generation_id) IN ({})", names.join(", "))
    };
    let sql = format!(
        "SELECT id, context_id, generation_id, vector_text <=> $query AS distance \
         FROM event_search_passages WHERE {filter} \
         AND is_current = $is_current AND is_searchable = $is_searchable \
         ORDER BY distance LIMIT 10"
    );
    (sql, values)
}

/// The Contexts of a scope, by index.
fn contexts_of(scope: &str, dump: &Dump) -> Vec<usize> {
    match scope {
        "one" => vec![0],
        "several" => vec![0, 1, 2],
        _ => (0..dump.contexts.len()).collect(),
    }
}

/// Searches a scope for every question, after `untimed` searches that are not recorded.
fn find(session: &ReadSession, dump: &Dump, scope: &str, untimed: usize) -> Fallible<Finds> {
    let scope = contexts_of(scope, dump);
    let mut finds = Finds::default();
    for run in 0..untimed + dump.questions.len() {
        let question = run.checked_sub(untimed);
        let asked = question.unwrap_or(run % dump.questions.len());
        let (sql, values) = statement(dump, &scope, &dump.questions[asked]);
        let (found, time) = timed(|| session.execute(&sql, &values));
        let found = found?;
        if run == 0 {
            finds.first = time;
        }
        if question.is_none() {
            continue;
        }
        let answer = found.rows().iter().map(|row| match (&row[0], &row[3]) {
            (Value::Uuid(id), Value::Float64(distance)) => Ok((*id, *distance)),
            _ => Err("a search row is not an id and a distance"),
        });
        finds.answers.push(answer.collect::<Result<_, _>>()?);
        finds.times.push(time);
        finds.examined.push(found.rows_examined());
    }
    Ok(finds)
}

/// The three scopes through a read session opened under `limit`.
fn find_under(store: &Path, limit: Option<usize>, dump: &Dump) -> Fallible<Vec<Finds>> {
    let mut options = ReadSessionOptions::default();
    options.memory_limit = limit;
    let session = ReadSession::open_with_options(store, options)?;
    let each = SCOPES.iter();
    each.map(|scope| find(&session, dump, scope, UNTIMED))
        .collect()
}

/// Compares a scope's answers with the recorded ones: how many questions have the same ten
/// ids, how many have them in the same order, and the largest difference between a recorded
/// score and one minus the distance, rounded to 32 bits, of the same id.
fn compare(answers: &[Vec<(Uuid, f64)>], recorded: &Path) -> Fallible<(usize, usize, f64)> {
    let (mut same_ten, mut same_order, mut gap) = (0, 0, 0f64);
    for text in std::fs::read_to_string(recorded)?.lines() {
        let recorded: Json = contextdb::serde_json::from_str(text)?;
        let (Some(question), Some(ids), Some(scores)) = (
            recorded["q"].as_u64(),
            recorded["ids"].as_array(),
            recorded["scores"].as_array(),
        ) else {
            continue; // the summary line
        };
        let ids = ids.iter().map(|id| id.as_str().unwrap_or_default().parse());
        let ids: Vec<Uuid> = ids.collect::<Result<_, _>>()?;
        let answer = answers
            .get(question as usize)
            .ok_or("a question out of range")?;
        let ours: Vec<Uuid> = answer.iter().map(|(id, _)| *id).collect();
        same_order += usize::from(ours == ids);
        let as_set = |ids: &[Uuid]| ids.iter().copied().collect::<BTreeSet<Uuid>>();
        same_ten += usize::from(as_set(&ours) == as_set(&ids));
        for (id, distance) in answer {
            if let Some(at) = ids.iter().position(|recorded| recorded == id) {
                let score = scores[at].as_f64().ok_or("a score that is not a number")?;
                gap = gap.max((f64::from((1.0 - distance) as f32) - score).abs());
            }
        }
    }
    Ok((same_ten, same_order, gap))
}

fn main() -> Fallible<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let [dump_folder, answers, store] = arguments.as_slice() else {
        eprintln!("usage: passages <dump folder> <answers folder> <store folder>");
        std::process::exit(2);
    };
    let (dump_folder, answers, store) =
        (Path::new(dump_folder), Path::new(answers), Path::new(store));
    support::must_be_empty(store);
    let dump = read_dump(dump_folder)?;

    let db = Database::open(store)?;
    db.execute(
        "CREATE TABLE event_search_passages (
           id UUID PRIMARY KEY,
           context_id UUID NOT NULL,
           generation_id UUID NOT NULL,
           vector_text VECTOR(768) PARTITION_KEY (context_id, generation_id)
             MAX_PARTITIONS 4096 SEARCH_MODE AUTO,
           is_current BOOLEAN NOT NULL,
           is_searchable BOOLEAN NOT NULL DEFAULT true
         )",
        &params! {},
    )?;
    support::take_peak_allocated();
    let (loaded, load_time) = timed(|| load(&db, dump_folder, &dump));
    let (passages, held) = loaded?;
    let load_peak = support::take_peak_allocated();
    let name = |what: &str| format!("passages_{passages}_{what}");
    line(&name("load"), seconds(load_time), "s");

    let unsealed = find(&ReadSession::open(store)?, &dump, "all", 0)?;

    let mut cycles = 0;
    let (sealed, seal_time) = timed(|| -> contextdb::Result<()> {
        loop {
            cycles += 1;
            let report = db.run_maintenance_cycle()?;
            if report.vector.maintenance_vectors_remaining == 0 {
                return Ok(());
            }
        }
    });
    sealed?;
    db.close()?;
    let on_disk = bytes_on_disk(store)?;
    line(&name("seal"), seconds(seal_time), "s");
    line(&name("seal_cycles"), cycles, "cycles");
    line(&name("store_size"), mebibytes(on_disk), "MiB");
    line(
        &name("bytes_per_passage"),
        on_disk / passages.max(1),
        "bytes",
    );
    line(&name("size_over_held"), ratio(on_disk, held), "x");
    let p95 = |times: &[Duration]| milliseconds(percentile(times, 0.95));
    line(&name("find_unsealed_all_p95"), p95(&unsealed.times), "ms");
    let rows = percentile(&unsealed.examined, 0.5);
    line(&name("find_unsealed_all_rows_examined_p50"), rows, "rows");

    support::take_peak_allocated();
    let unlimited = find_under(store, None, &dump)?;
    for (scope, finds) in SCOPES.iter().zip(&unlimited) {
        let p50 = milliseconds(percentile(&finds.times, 0.5));
        line(&name(&format!("find_{scope}_p50")), p50, "ms");
        line(&name(&format!("find_{scope}_p95")), p95(&finds.times), "ms");
    }
    let all = &unlimited[2];
    line(&name("find_all_first"), milliseconds(all.first), "ms");
    line(
        &name("find_all_max"),
        milliseconds(percentile(&all.times, 1.0)),
        "ms",
    );
    for (limit, bytes) in [("1g", 1 << 30), ("64m", 64 << 20)] {
        let limited = find_under(store, Some(bytes), &dump)?;
        let name = name(&format!("find_all_p95_limit_{limit}"));
        line(&name, p95(&limited[2].times), "ms");
    }
    for (what, share) in [("p50", 0.5), ("p95", 0.95), ("max", 1.0)] {
        let rows = percentile(&all.examined, share);
        line(
            &name(&format!("find_all_rows_examined_{what}")),
            rows,
            "rows",
        );
    }

    let mut compared = Vec::new();
    for (scope, finds) in SCOPES.iter().zip(&unlimited) {
        let recorded = answers.join(format!("engine-{scope}.jsonl"));
        compared.push(compare(&finds.answers, &recorded)?);
    }
    for (scope, (same_ten, _, _)) in SCOPES.iter().zip(&compared) {
        line(&name(&format!("same_ten_{scope}")), same_ten, "of_100");
    }
    for (scope, (_, same_order, _)) in SCOPES.iter().zip(&compared) {
        line(&name(&format!("same_order_{scope}")), same_order, "of_100");
    }
    let gap = compared.iter().map(|(_, _, gap)| *gap).fold(0.0, f64::max);
    line(&name("distance_gap_max"), gap, "-");
    line(&name("load_peak_allocated"), mebibytes(load_peak), "MiB");
    let find_peak = support::take_peak_allocated();
    line(&name("find_peak_allocated"), mebibytes(find_peak), "MiB");
    Ok(())
}
