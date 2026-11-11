//! Loads a dump of search passages into stores and measures searches on them under each mode,
//! printing one number per line as `name value unit`.
//!
//! The program is called in three ways:
//!
//! - `passages <dump folder> <answers folder> <folder> [sq8][,tail]`: every passage of the
//!   dump, declared as Context Graph declares its table. Lines `passages_<count>_...`.
//! - `passages single <dump folder> <folder>`: the first 10,000 passages, in dump order, of
//!   the dump's (Context, generation) pair of index 1, in a table whose vector column also
//!   declares `AUTO_INDEX_AT 1000`. Lines `single_<count>_...`.
//! - `passages first single|store <dump folder> <store folder>`: what the program starts
//!   itself as, to open a built store in a new process and run one search.
//!
//! The dump folder holds `rows.bin` (36 bytes for each passage, numbers low byte first: a row
//! number `u64`, the passage's id, the index `u32` of its Context, the index `u32` of its
//! (Context, generation) pair, and flags `u32`: 1 current, 2 searchable, 4 has a vector),
//! `vectors.f32` (768 `f32` for each passage, in the same order), `contexts.txt` (`index uuid
//! name` on each line), `pairs.txt` (`index context-index generation-uuid active`) and
//! `queries.f32` (768 `f32` for each question). The answers folder holds `engine-one.jsonl`,
//! `engine-several.jsonl` and `engine-all.jsonl`: on each line the ten answers recorded for one
//! question, `{"q": n, "ids": [...], "scores": [...]}`, a score being one minus the distance,
//! rounded to 32 bits. The folder must be missing or empty; each store is a folder inside it.
//!
//! What it does, in order, each store loaded 500 passages to a commit with no memory limit
//! and every read session opened under 1 GiB unless a line's name says another limit:
//!
//! 1. `sealed`: a store whose column declares `AUTO_INDEX_AT 1000000000`, so that no partition
//!    is ever grouped. One search of every question with every vector still waiting; cycles
//!    until nothing waits; its size; EXACT in each scope (`exact_before`).
//! 2. `built`: the store as its case declares it. Cycles until nothing remains; what
//!    inspection says of the index; its size; in a new process, opening it and one search;
//!    then, in one session, each scope under EXACT, INDEXED and AUTO in turn within each of
//!    five rounds, the two indexed modes compared with EXACT of the same run; the answers
//!    beside the recorded ones; INDEXED again under 64 MiB; the statement with one condition
//!    more that passes one passage in a hundred, under EXACT and INDEXED; and a hundred stored
//!    vectors each searched for by itself.
//! 3. with `sq8`: a store whose column is declared `WITH (quantization = 'SQ8')` and never
//!    grouped, searched under EXACT, its answers beside the first store's.
//! 4. with `tail`: a store built from all but the last tenth of each partition's passages, in
//!    dump order, which are written after the build and sealed, so that they stay in each
//!    partition's tail; EXACT and INDEXED over all of it, and a hundred of the tail's vectors
//!    each searched for by itself.
//!
//! The scopes are one Context (the first), several (the first three) and all of them; the
//! second way of calling has the one scope of its pair and leaves the scope out of its names.
//! The search is Context Graph's statement as it sends it, its two flags included. The added
//! condition is on the column `id`: sixteen ranges of ids, evenly spread through the ids in
//! order, that hold a hundredth of the passages together.
//!
//! A time named `p50` or `p95` is the middle of the five rounds' values, each the time at
//! position `round((count - 1) * share)` of a round's times in order; `p95_spread` is the
//! largest of the five less the smallest. "Held" is the bytes of the values put in: 50 for a
//! passage's five other columns and 3,072 for its vector.
//!
//! This program measures. It never passes or fails, no test reads what it prints, and
//! `numbers.sh` is what runs it. It prints nothing of a passage but counts, sizes and times.

mod support;

use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use contextdb::serde_json::Value as Json;
use contextdb::{Database, Uuid, Value, params};
use support::{
    AUTO, Asked, EXACT, Fallible, INDEXED, READER_LIMIT, Values, against, ask, counted, line,
    mebibytes, ratio, seconds, selected, session, setting, timed, times, work,
};

#[global_allocator]
static ALLOCATOR: support::Counting = support::Counting;

const DIMENSIONS: usize = 768;
const PER_COMMIT: usize = 500;
const ROUNDS: usize = 5;
/// The clause of a column no partition of which is ever grouped.
const NEVER_GROUPED: &str = " AUTO_INDEX_AT 1000000000";

struct Passage {
    id: Uuid,
    pair: usize,
    flags: u32,
}

/// A dump: its passages without their vectors, and what it says beside them.
struct Dump {
    folder: PathBuf,
    /// Each Context's id, by index.
    contexts: Vec<Uuid>,
    /// Each pair's Context index, generation, and whether it is the Context's active one.
    pairs: Vec<(usize, Uuid, bool)>,
    /// The question vectors.
    questions: Vec<Vec<f32>>,
    passages: Vec<Passage>,
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
    let mut passages = Vec::new();
    for record in std::fs::read(folder.join("rows.bin"))?.chunks_exact(36) {
        let number = |at: usize| {
            u32::from_le_bytes([record[at], record[at + 1], record[at + 2], record[at + 3]])
        };
        passages.push(Passage {
            id: Uuid::from_slice(&record[8..24])?,
            pair: number(28) as usize,
            flags: number(32),
        });
    }
    Ok(Dump {
        folder: folder.to_owned(),
        contexts,
        pairs,
        questions: questions.collect(),
        passages,
    })
}

impl Dump {
    /// The Context and generation of a pair.
    fn pair(&self, pair: usize) -> (Uuid, Uuid) {
        (self.contexts[self.pairs[pair].0], self.pairs[pair].1)
    }

    /// The active pairs of these Contexts, each once, in the order of their ids.
    fn active(&self, contexts: &[usize]) -> Vec<(Uuid, Uuid)> {
        let active = self.pairs.iter().filter(|pair| pair.2);
        let wanted = active.filter(|pair| contexts.contains(&pair.0));
        let pairs: BTreeSet<(Uuid, Uuid)> =
            wanted.map(|pair| (self.contexts[pair.0], pair.1)).collect();
        pairs.into_iter().collect()
    }

    /// As many as `count` of these passages that have a vector, evenly spread, each with its
    /// id as a number and its vector.
    fn stored(&self, places: &[usize], count: usize) -> Fallible<Vec<(u128, Vec<f32>)>> {
        let with = places.iter().copied();
        let with: Vec<usize> = with
            .filter(|at| self.passages[*at].flags & 4 != 0)
            .collect();
        let count = count.min(with.len());
        let mut vectors = File::open(self.folder.join("vectors.f32"))?;
        let mut bytes = vec![0u8; 4 * DIMENSIONS];
        let mut stored = Vec::with_capacity(count);
        for at in (0..count).map(|i| with[i * with.len() / count]) {
            vectors.seek(SeekFrom::Start((at * 4 * DIMENSIONS) as u64))?;
            vectors.read_exact(&mut bytes)?;
            stored.push((self.passages[at].id.as_u128(), floats(&bytes)));
        }
        Ok(stored)
    }
}

/// What is loaded and how it is declared and searched.
struct Case<'a> {
    dump: &'a Dump,
    /// What every line's name begins with.
    prefix: String,
    /// The passages loaded, by their place in the dump, in its order.
    loaded: Vec<usize>,
    /// What the vector column declares after `SEARCH_MODE AUTO`.
    declared: &'static str,
    /// Each scope: what its lines' names hold after `find_`, and the pairs it selects. The
    /// last is the widest.
    scopes: Vec<(&'static str, Vec<(Uuid, Uuid)>)>,
    /// The folder of recorded answers, for a case that has them.
    answers: Option<&'a Path>,
}

impl<'a> Case<'a> {
    /// Every passage of the dump, as Context Graph declares its table.
    fn store(dump: &'a Dump, answers: Option<&'a Path>) -> Case<'a> {
        let every: Vec<usize> = (0..dump.contexts.len()).collect();
        Case {
            dump,
            prefix: format!("passages_{}_", dump.passages.len()),
            loaded: (0..dump.passages.len()).collect(),
            declared: "",
            scopes: vec![
                ("one_", dump.active(&[0])),
                ("several_", dump.active(&[0, 1, 2])),
                ("all_", dump.active(&every)),
            ],
            answers,
        }
    }

    /// The first 10,000 passages of the pair of index 1, in a column that declares
    /// `AUTO_INDEX_AT 1000`.
    fn single(dump: &'a Dump) -> Case<'a> {
        let places = 0..dump.passages.len();
        let of_pair = places.filter(|at| dump.passages[*at].pair == 1);
        let loaded: Vec<usize> = of_pair.take(10_000).collect();
        Case {
            dump,
            prefix: format!("single_{}_", loaded.len()),
            loaded,
            declared: " AUTO_INDEX_AT 1000",
            scopes: vec![("", vec![dump.pair(1)])],
            answers: None,
        }
    }

    /// The name of a scope's lines up to the mode: `<prefix>find_<scope>`, or `<prefix>find`.
    fn find(&self, scope: &str) -> String {
        format!("{}find_{scope}", self.prefix)
            .trim_end_matches('_')
            .to_owned()
    }

    /// The name of one line of the case.
    fn name(&self, what: &str) -> String {
        format!("{}{what}", self.prefix)
    }

    /// The widest scope.
    fn widest(&self) -> &(&'static str, Vec<(Uuid, Uuid)>) {
        &self.scopes[self.scopes.len() - 1]
    }
}

/// Opens a new store with Context Graph's table, its vector column declared `with` this
/// quantization clause and with these clauses after `SEARCH_MODE AUTO`.
fn create(store: &Path, with: &str, clauses: &str) -> Fallible<Database> {
    let db = Database::open(store)?;
    db.execute(
        &format!(
            "CREATE TABLE event_search_passages (
               id UUID PRIMARY KEY,
               context_id UUID NOT NULL,
               generation_id UUID NOT NULL,
               vector_text VECTOR(768){with} PARTITION_KEY (context_id, generation_id)
                 MAX_PARTITIONS 4096 SEARCH_MODE AUTO{clauses},
               is_current BOOLEAN NOT NULL,
               is_searchable BOOLEAN NOT NULL DEFAULT true
             )"
        ),
        &params! {},
    )?;
    Ok(db)
}

/// Inserts these passages of the dump, given by their places in rising order. Returns the
/// bytes held.
fn load(db: &Database, dump: &Dump, places: &[usize]) -> Fallible<u64> {
    let mut vectors = BufReader::new(File::open(dump.folder.join("vectors.f32"))?);
    let mut vector = vec![0u8; 4 * DIMENSIONS];
    let (mut held, mut next) = (0, 0);
    let mut tx = db.transaction()?;
    for (count, at) in places.iter().enumerate() {
        vectors.seek_relative(((at - next) * 4 * DIMENSIONS) as i64)?;
        vectors.read_exact(&mut vector)?;
        next = at + 1;
        let passage = &dump.passages[*at];
        let (context, generation) = dump.pair(passage.pair);
        let has_vector = passage.flags & 4 != 0;
        let embedding = if has_vector {
            Value::Vector(floats(&vector))
        } else {
            Value::Null
        };
        held += 3 * 16 + 2 + if has_vector { 4 * DIMENSIONS as u64 } else { 0 };
        tx.execute(
            "INSERT INTO event_search_passages \
             (id, context_id, generation_id, vector_text, is_current, is_searchable) \
             VALUES ($id, $context, $generation, $vector, $current, $searchable)",
            &params! {
                "id" => passage.id,
                "context" => context,
                "generation" => generation,
                "vector" => embedding,
                "current" => passage.flags & 1 != 0,
                "searchable" => passage.flags & 2 != 0,
            },
        )?;
        if (count + 1) % PER_COMMIT == 0 {
            std::mem::replace(&mut tx, db.transaction()?).commit()?;
        }
    }
    tx.commit()?;
    Ok(held)
}

/// Context Graph's search statement for these pairs, with its values: `tail` is what follows
/// `ORDER BY distance`, and `among`, when it holds any, adds the condition that the passage's
/// id lies in one of these ranges, at or after a range's first id and before its second.
fn statement(
    pairs: &[(Uuid, Uuid)],
    among: &[(Uuid, Uuid)],
    tail: &str,
    question: &[f32],
) -> (String, Values) {
    let mut values = params! {
        "query" => question.to_vec(), "is_current" => true, "is_searchable" => true,
    };
    let mut names = Vec::new();
    for (i, (context, generation)) in pairs.iter().enumerate() {
        values.insert(format!("search_context_{i}"), Value::Uuid(*context));
        values.insert(format!("search_generation_{i}"), Value::Uuid(*generation));
        names.push(format!("($search_context_{i}, $search_generation_{i})"));
    }
    let filter = if names.len() == 1 {
        "context_id = $search_context_0 AND generation_id = $search_generation_0".to_owned()
    } else {
        format!("(context_id, generation_id) IN ({})", names.join(", "))
    };
    let mut ranges = Vec::new();
    for (i, (from, before)) in among.iter().enumerate() {
        values.insert(format!("id_from_{i}"), Value::Uuid(*from));
        values.insert(format!("id_before_{i}"), Value::Uuid(*before));
        ranges.push(format!("(id >= $id_from_{i} AND id < $id_before_{i})"));
    }
    let condition = if ranges.is_empty() {
        String::new()
    } else {
        format!(" AND ({})", ranges.join(" OR "))
    };
    let sql = format!(
        "SELECT id, context_id, generation_id, vector_text <=> $query AS distance \
         FROM event_search_passages WHERE {filter} \
         AND is_current = $is_current AND is_searchable = $is_searchable{condition} \
         ORDER BY distance{tail}"
    );
    (sql, values)
}

/// The key tuples of these pairs as the store writes them: for each of the two ids the tag
/// of a UUID and its bytes.
fn tuples(pairs: &[(Uuid, Uuid)]) -> Vec<Vec<u8>> {
    let tuple = |(context, generation): &(Uuid, Uuid)| {
        [
            &[5][..],
            &context.as_bytes()[..],
            &[5],
            &generation.as_bytes()[..],
        ]
        .concat()
    };
    pairs.iter().map(tuple).collect()
}

/// Compares a scope's answers with the recorded ones: how many questions have the same ten
/// ids, how many have them in the same order, and the largest difference between a recorded
/// score and one minus the distance, rounded to 32 bits, of the same id.
fn compare(answers: &[Vec<(u128, f64)>], recorded: &Path) -> Fallible<(usize, usize, f64)> {
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
        let ids: Vec<u128> = ids.iter().map(Uuid::as_u128).collect();
        let answer = answers
            .get(question as usize)
            .ok_or("a question out of range")?;
        let ours: Vec<u128> = answer.iter().map(|(id, _)| *id).collect();
        same_order += usize::from(ours == ids);
        let as_set = |ids: &[u128]| ids.iter().copied().collect::<BTreeSet<u128>>();
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

/// The first store: never grouped. Returns the bytes held, the store's bytes, and EXACT's
/// answers in each scope.
fn sealed(case: &Case<'_>, store: &Path) -> Fallible<(u64, u64, Vec<Asked>)> {
    let dump = case.dump;
    let db = create(store, "", NEVER_GROUPED)?;
    support::take_peak_allocated();
    let (held, load_time) = timed(|| load(&db, dump, &case.loaded));
    let held = held?;
    line(&case.name("load"), seconds(load_time), "s");
    let load_peak = support::take_peak_allocated();
    line(
        &case.name("load_peak_allocated"),
        mebibytes(load_peak),
        "MiB",
    );

    let (scope, pairs) = case.widest();
    let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
    let unsealed = session(store, READER_LIMIT)?;
    let unsealed = ask(&unsealed, &find, &[AUTO], &dump.questions, 1)?;
    let name = format!("{}find_unsealed_{scope}", case.prefix);
    let name = name.trim_end_matches('_');
    setting(name, READER_LIMIT);
    times(name, &unsealed[0]);
    counted(name, &unsealed[0]);

    let bytes = support::sealed(&case.prefix, &db, store)?;
    let reader = session(store, READER_LIMIT)?;
    let mut exact = Vec::new();
    for (scope, pairs) in &case.scopes {
        let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
        let mut asked = ask(&reader, &find, &[EXACT], &dump.questions, ROUNDS)?;
        let name = format!("{}_exact_before", case.find(scope));
        setting(&name, READER_LIMIT);
        times(&name, &asked[0]);
        counted(&name, &asked[0]);
        exact.append(&mut asked);
    }
    Ok((held, bytes, exact))
}

/// Ranges of ids: the first id of each and the first id after it.
type Ranges = Vec<(Uuid, Uuid)>;

/// Sixteen ranges of ids, evenly spread through the loaded passages' ids in order, that hold
/// a hundredth of the passages together: each range's first id and the first id after it,
/// how many passages they hold, and in how many pairs those lie. `None` for fewer than 1,600
/// passages.
fn hundredth(case: &Case<'_>) -> Option<(Ranges, usize, usize)> {
    const RANGES: usize = 16;
    let passages = case.loaded.iter().map(|at| &case.dump.passages[*at]);
    let mut ids: Vec<(Uuid, usize)> = passages.map(|passage| (passage.id, passage.pair)).collect();
    ids.sort_unstable();
    let each = ids.len() / (100 * RANGES);
    let (mut ranges, mut pairs) = (Vec::new(), BTreeSet::new());
    for range in 0..RANGES {
        let from = (2 * range + 1) * ids.len() / (2 * RANGES) - each / 2;
        let inside = ids.get(from..from + each).filter(|_| each > 0)?;
        pairs.extend(inside.iter().map(|(_, pair)| *pair));
        ranges.push((inside[0].0, ids.get(from + each)?.0));
    }
    Some((ranges, each * RANGES, pairs.len()))
}

/// The second store, declared as the case says, and every line taken on it.
fn built(case: &Case<'_>, folder: &Path, held: u64, sealed: u64) -> Fallible<()> {
    let (dump, store) = (case.dump, folder.join("built"));
    let db = create(&store, "", case.declared)?;
    load(&db, dump, &case.loaded)?;
    let parts = support::built(&case.prefix, &db, &store, (held, sealed, DIMENSIONS))?;
    let kind = if case.answers.is_some() {
        "store"
    } else {
        "single"
    };
    let first = [OsStr::new("first"), OsStr::new(kind)];
    support::in_new_process(&[
        first[0],
        first[1],
        dump.folder.as_os_str(),
        store.as_os_str(),
    ])?;

    support::take_peak_allocated();
    let reader = session(&store, READER_LIMIT)?;
    let mut asked = Vec::new();
    for (scope, pairs) in &case.scopes {
        let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
        let parts = selected(&parts, &tuples(pairs));
        let how = (ROUNDS, &parts[..]);
        asked.push(support::modes(
            &case.find(scope),
            &reader,
            &find,
            &dump.questions,
            how,
        )?);
    }
    if let Some(answers) = case.answers {
        let mut gap = 0.0;
        for (mode, at) in [("", 0), ("auto_", 2)] {
            for (what, order) in [("same_ten", false), ("same_order", true)] {
                for ((scope, _), asked) in case.scopes.iter().zip(&asked) {
                    let scope = scope.trim_end_matches('_');
                    let recorded = answers.join(format!("engine-{scope}.jsonl"));
                    let (same_ten, same_order, far) = compare(&asked[at].answers, &recorded)?;
                    let same = if order { same_order } else { same_ten };
                    line(&case.name(&format!("{mode}{what}_{scope}")), same, "of_100");
                    gap = f64::max(gap, far);
                }
            }
        }
        line(&case.name("distance_gap_max"), gap, "-");

        // INDEXED again where the blocks do not stay in the cache.
        let small = session(&store, 64 << 20)?;
        for ((scope, pairs), asked) in case.scopes.iter().zip(&asked) {
            let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
            let under = ask(&small, &find, &[INDEXED], &dump.questions, ROUNDS)?;
            let name = format!("{}_indexed_under_64m", case.find(scope));
            setting(&name, 64 << 20);
            times(&name, &under[0]);
            counted(&name, &under[0]);
            against(&name, &under[0], &asked[0]);
        }

        // The statement with a condition that passes a passage in a hundred. INDEXED is
        // refused where the filter leaves the index nothing to choose among, as in a store
        // none of whose partitions is grouped: EXACT is then timed alone.
        let (scope, pairs) = case.widest();
        let find = case.find(scope);
        if let Some((among, passing, in_pairs)) = hundredth(case) {
            let name = format!("{find}_filtered");
            line(&format!("{name}_rows_passing"), passing, "rows");
            let in_pairs = (format!("{name}_pairs_with_passing_rows"), in_pairs);
            line(&in_pairs.0, in_pairs.1, "pairs");
            let filtered = |tail: &str, question: &[f32]| statement(pairs, &among, tail, question);
            let both = ask(
                &reader,
                &filtered,
                &[EXACT, INDEXED],
                &dump.questions,
                ROUNDS,
            );
            let asked = match both {
                Ok(asked) => asked,
                Err(error) => {
                    eprintln!("{find}_indexed_filtered: {error}");
                    ask(&reader, &filtered, &[EXACT], &dump.questions, ROUNDS)?
                }
            };
            setting(&name, READER_LIMIT);
            let name = format!("{find}_exact_filtered");
            times(&name, &asked[0]);
            counted(&name, &asked[0]);
            let name = format!("{find}_indexed_filtered");
            match asked.get(1) {
                Some(indexed) => {
                    times(&name, indexed);
                    counted(&name, indexed);
                    against(&name, indexed, &asked[0]);
                }
                None => line(&name, "refused", "-"),
            }
        } else {
            line(&format!("{find}_filtered"), "unavailable", "-");
        }
    }

    let (scope, pairs) = case.widest();
    let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
    let name = format!("{}_own_grouped_found", case.find(scope));
    support::own(&name, &reader, &find, &dump.stored(&case.loaded, 100)?)?;
    let find_peak = support::take_peak_allocated();
    line(
        &case.name("find_peak_allocated"),
        mebibytes(find_peak),
        "MiB",
    );
    Ok(())
}

/// A store with a tail: built from all but the last tenth of each pair's passages, which are
/// written after the build and sealed.
fn tail(case: &Case<'_>, store: &Path) -> Fallible<()> {
    let dump = case.dump;
    let mut of_pair = vec![0_usize; dump.pairs.len()];
    for at in &case.loaded {
        of_pair[dump.passages[*at].pair] += 1;
    }
    let mut seen = vec![0_usize; dump.pairs.len()];
    let (mut first, mut last) = (Vec::new(), Vec::new());
    for at in &case.loaded {
        let pair = dump.passages[*at].pair;
        seen[pair] += 1;
        if seen[pair] > of_pair[pair] - of_pair[pair] / 10 {
            last.push(*at);
        } else {
            first.push(*at);
        }
    }
    let db = create(store, "", case.declared)?;
    load(&db, dump, &first)?;
    support::upkeep(&db)?;
    load(&db, dump, &last)?;
    let cycles = support::upkeep(&db)?;
    db.close()?;
    line(&case.name("tail_passages"), last.len(), "passages");
    line(&case.name("tail_cycles"), cycles, "cycles");

    let parts = support::parts(store, DIMENSIONS)?;
    let (scope, pairs) = case.widest();
    let parts = selected(&parts, &tuples(pairs));
    let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
    let reader = session(store, READER_LIMIT)?;
    let asked = ask(&reader, &find, &[EXACT, INDEXED], &dump.questions, ROUNDS)?;
    let scope_find = case.find(scope);
    setting(&format!("{scope_find}_tail"), READER_LIMIT);
    let name = format!("{scope_find}_exact_tail");
    times(&name, &asked[0]);
    counted(&name, &asked[0]);
    if let Some(answers) = case.answers {
        let scope = scope.trim_end_matches('_');
        let recorded = answers.join(format!("engine-{scope}.jsonl"));
        let same_ten = compare(&asked[0].answers, &recorded)?.0;
        line(&format!("{name}_same_ten"), same_ten, "of_100");
    }
    let name = format!("{scope_find}_indexed_tail");
    times(&name, &asked[1]);
    counted(&name, &asked[1]);
    against(&name, &asked[1], &asked[0]);
    work(&name, &asked[1], &parts, &dump.questions);
    let name = format!("{scope_find}_own_tail_found");
    support::own(&name, &reader, &find, &dump.stored(&last, 100)?)
}

/// A store that keeps only the reduced vectors, never grouped, searched under EXACT beside
/// the first store's answers under EXACT.
fn reduced(case: &Case<'_>, store: &Path, sealed: u64, exact: &[Asked]) -> Fallible<()> {
    let dump = case.dump;
    let db = create(store, " WITH (quantization = 'SQ8')", NEVER_GROUPED)?;
    load(&db, dump, &case.loaded)?;
    support::upkeep(&db)?;
    db.close()?;
    let reader = session(store, READER_LIMIT)?;
    let mut compared = Vec::new();
    for ((_, pairs), exact) in case.scopes.iter().zip(exact) {
        let find = |tail: &str, question: &[f32]| statement(pairs, &[], tail, question);
        let asked = ask(&reader, &find, &[EXACT], &dump.questions, 1)?;
        let (mut same_ten, mut same_order, mut shared) = (0, 0, 0);
        for (ours, theirs) in asked[0].answers.iter().zip(&exact.answers) {
            let ids = |answer: &[(u128, f64)]| answer.iter().map(|row| row.0).collect::<Vec<_>>();
            let (ours, theirs) = (ids(ours), ids(theirs));
            let found = theirs.iter().filter(|id| ours.contains(id)).count();
            shared += found;
            same_ten += usize::from(found == theirs.len() && ours.len() == theirs.len());
            same_order += usize::from(ours == theirs);
        }
        compared.push((same_ten, same_order, shared));
    }
    let scopes = || {
        case.scopes
            .iter()
            .map(|scope| scope.0.trim_end_matches('_'))
    };
    for (scope, (same_ten, _, _)) in scopes().zip(&compared) {
        line(
            &case.name(&format!("sq8_same_ten_{scope}")),
            same_ten,
            "of_100",
        );
    }
    for (scope, (_, same_order, _)) in scopes().zip(&compared) {
        line(
            &case.name(&format!("sq8_same_order_{scope}")),
            same_order,
            "of_100",
        );
    }
    for (scope, (_, _, shared)) in scopes().zip(&compared) {
        line(
            &case.name(&format!("sq8_answers_shared_{scope}")),
            shared,
            "of_1000",
        );
    }
    let bytes = support::bytes_on_disk(store)?;
    let passages = case.loaded.len().max(1) as u64;
    line(&case.name("sq8_store_size"), mebibytes(bytes), "MiB");
    line(
        &case.name("sq8_bytes_per_passage"),
        bytes / passages,
        "bytes",
    );
    line(&case.name("sq8_size_over_f32"), ratio(bytes, sealed), "x");
    Ok(())
}

/// Every store of a case, each a folder inside `folder`.
fn measure(case: &Case<'_>, folder: &Path, extras: &str) -> Fallible<()> {
    support::must_be_empty(folder);
    std::fs::create_dir_all(folder)?;
    if case.declared == " AUTO_INDEX_AT 1000" {
        line(&case.name("auto_index_at"), 1000, "vectors");
    }
    let (held, sealed_bytes, exact) = sealed(case, &folder.join("sealed"))?;
    built(case, folder, held, sealed_bytes)?;
    if extras.split(',').any(|extra| extra == "sq8") {
        reduced(case, &folder.join("sq8"), sealed_bytes, &exact)?;
    }
    if extras.split(',').any(|extra| extra == "tail") {
        tail(case, &folder.join("tail"))?;
    }
    Ok(())
}

fn main() -> Fallible<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    match arguments.as_slice() {
        ["first", kind, dump, store] => {
            let dump = read_dump(Path::new(dump))?;
            let case = if *kind == "single" {
                Case::single(&dump)
            } else {
                Case::store(&dump, None)
            };
            let (scope, pairs) = case.widest();
            let first = format!("{}_auto_first", case.find(scope));
            let search = statement(pairs, &[], AUTO, &dump.questions[0]);
            support::first(Path::new(store), (&case.name("open"), &first), &search)
        }
        ["single", dump, folder] => {
            let dump = read_dump(Path::new(dump))?;
            measure(&Case::single(&dump), Path::new(folder), "")
        }
        [dump, answers, folder] | [dump, answers, folder, _] => {
            let dump = read_dump(Path::new(dump))?;
            let case = Case::store(&dump, Some(Path::new(answers)));
            measure(&case, Path::new(folder), arguments.get(3).unwrap_or(&""))
        }
        _ => {
            eprintln!(
                "usage: passages <dump folder> <answers folder> <folder> [sq8][,tail]\n       \
                 passages single <dump folder> <folder>"
            );
            std::process::exit(2);
        }
    }
}
