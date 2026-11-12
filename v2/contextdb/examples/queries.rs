//! Measures Context Graph's own declarations and statements, printing one number per line as
//! `name value unit`. Every store is declared with the tables and indexes of
//! `tests/fixtures/consumer_schema.sql`, the declarations Context Graph runs, and nothing else.
//!
//! - `queries declarations <folder>`: every table and index text of the fixture, each second
//!   text in a store of its own after the tables written before its first; how many are
//!   accepted, and of the tables, how many print back unchanged: the printed text, declared
//!   again in an empty store, prints the same text. (That it says every clause its author wrote
//!   and no other is `tests/declarations.rs`.)
//! - `queries declared <dump folder> <answers folder> <folder>`: the dump `passages.rs` reads,
//!   loaded into `event_search_passages` with the `contexts` and `event_search_generations`
//!   rows it refers to, 500 passages to a commit, then every maintenance cycle. A column the
//!   dump does not hold is written short: the connector's name, a session named by the pair,
//!   the place in the dump as ordinal and sequences, an empty text, the id's digits as the
//!   content's digest. Then Context Graph's find, as its branch writes it, in a read session
//!   under 1 GiB (`declared_find_<scope>_...`, five rounds after 5 untimed searches, and its
//!   ten beside the recorded ones); a count by an index range and by a kept count; three
//!   flips of `is_current` over the passages of the dump's pair of index 1, with the bytes the
//!   storage door wrote; and the DELETE of that pair's passages under 64 MiB.
//! - `queries anchors <corpus folder> <folder>`: every file of the folder read as a session
//!   whose lines are JSON, as Context Graph's connector reads one: each tool call of a message
//!   is an event, with a `command` anchor for its `input.command`, a `file` anchor for its file
//!   path and a `working_directory` anchor for its line's `cwd`, all in one Context, 500 events
//!   and their anchors to a commit. The size beside the bytes put in, the index entries as the
//!   store holds them, and the exact-match read for 100 commands, 50 of them longer than an
//!   entry holds whole, five rounds. It prints nothing of a session but counts, sizes and
//!   times.
//! - `queries dag <folder>`: 300,000 generated edges of `edges` (`DAG('CITES')`) over 100,000
//!   nodes in groups of 100, each from a node drawn at random to a later node of its group
//!   (fixed seed), 500 to a commit; each INSERT timed. Synthetic.
//! - `queries grants <folder>`: 100,000 rows in ten Contexts, each under one of 20,000 access
//!   ids, of which an agent holds every twentieth: a Context's rows are under 2,000 ids, and the
//!   agent may see half its rows. The rows of one Context are read through a handle narrowed to
//!   that Context and that agent, and through the handle narrowed to nothing, five rounds of
//!   twenty reads each. Synthetic.
//!
//! "Held" is the bytes of the values put in: a UUID 16, a number or TXID 8, a flag 1, a text or
//! JSON its bytes, a vector four a number. The folder must be missing or empty. This program
//! measures. It never passes or fails, no test reads what it prints, and `numbers.sh` is what
//! runs it.

// Public, so that what this program does not use of what the three programs share is not
// read as unused.
pub mod support;

use std::collections::BTreeSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use contextdb::serde_json::{self, Value as Json};
use contextdb::{
    ContextId, Database, DatabaseOpenOptions, Principal, StorageBackend, Uuid, Value, params,
};
use support::{
    AUTO, Asked, Fallible, READER_LIMIT, Values, ask, bytes_on_disk, line, mebibytes, milliseconds,
    must_be_empty, percentile, ratio, seconds, session, setting, timed, times,
};

#[global_allocator]
static ALLOCATOR: support::Counting = support::Counting;

const SCHEMA: &str = include_str!("../tests/fixtures/consumer_schema.sql");
const PER_COMMIT: usize = 500;
const ROUNDS: usize = 5;
const DIMENSIONS: usize = 768;

/// The statements of a text: each ends with `;` at the end of a line; `--` lines are comments.
fn statements(text: &str) -> Vec<String> {
    let (mut found, mut statement) = (Vec::new(), String::new());
    for line in text.lines().filter(|line| !line.starts_with("--")) {
        statement.push_str(line);
        statement.push('\n');
        if let Some(whole) = statement.trim().strip_suffix(';') {
            found.push(whole.to_owned());
            statement.clear();
        }
    }
    found
}

/// The table a `CREATE TABLE t` or a `CREATE INDEX i ON t` is about.
fn table_of(sql: &str) -> String {
    let rest = sql.strip_prefix("CREATE TABLE ").unwrap_or_else(|| {
        let (_, on) = sql.split_once(" ON ").unwrap_or_default();
        on
    });
    rest.chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect()
}

/// The fixture's statements a single store takes, and its second texts, which follow the line
/// that begins `-- Second texts`.
fn schema() -> (Vec<String>, Vec<String>) {
    let (first, second) = SCHEMA
        .split_once("\n-- Second texts")
        .unwrap_or((SCHEMA, ""));
    let (_, second) = second.split_once('\n').unwrap_or_default();
    (statements(first), statements(second))
}

/// A new store with every table and index Context Graph declares in one store.
fn declared_store(folder: &Path, options: DatabaseOpenOptions) -> Fallible<Database> {
    let db = Database::open_with_options(folder, options)?;
    for sql in schema().0 {
        db.execute(&sql, &params! {})?;
    }
    Ok(db)
}

/// Declares the statements in a new store: whether each was accepted, and for each table in
/// the order declared whether its printed text, declared in another new store, prints the
/// same.
fn declare(folder: &Path, texts: &[String]) -> Fallible<(Vec<bool>, Vec<bool>)> {
    let db = Database::open(folder.join("written"))?;
    let accepted: Vec<bool> = texts
        .iter()
        .map(|sql| db.execute(sql, &params! {}).is_ok())
        .collect();
    let again = Database::open(folder.join("printed"))?;
    let mut same = Vec::new();
    for sql in texts.iter().filter(|sql| sql.starts_with("CREATE TABLE")) {
        let Ok(printed) = db.declaration(&table_of(sql)) else {
            same.push(false);
            continue;
        };
        let declared = statements(&printed)
            .iter()
            .all(|sql| again.execute(sql, &params! {}).is_ok());
        same.push(declared && again.declaration(&table_of(sql)).ok() == Some(printed));
    }
    Ok((accepted, same))
}

fn declarations(folder: &Path) -> Fallible<()> {
    let (first, second) = schema();
    let (mut accepted, mut same) = declare(&folder.join("first"), &first)?;
    for (n, text) in second.iter().enumerate() {
        let table = table_of(text);
        let before = first
            .iter()
            .take_while(|sql| table_of(sql) != table || !sql.starts_with("CREATE TABLE"));
        let texts: Vec<String> = before
            .filter(|sql| sql.starts_with("CREATE TABLE"))
            .cloned()
            .chain([text.clone()])
            .collect();
        let (taken, printed) = declare(&folder.join(format!("second-{n}")), &texts)?;
        accepted.push(taken[taken.len() - 1]);
        same.push(printed[printed.len() - 1]);
    }
    let count = |of: &[bool]| {
        (
            of.iter().filter(|ok| **ok).count(),
            format!("of_{}", of.len()),
        )
    };
    let (n, of) = count(&accepted);
    line("declarations_accepted", n, &of);
    let (n, of) = count(&same);
    line("declarations_print_back_unchanged", n, &of);
    Ok(())
}

/// The bytes of the values put in, as the head of this file counts them.
fn held(values: &Values) -> u64 {
    let size = |value: &Value| match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Uuid(_) => 16,
        Value::Text(text) => text.len(),
        Value::Json(json) => json.to_string().len(),
        Value::Vector(numbers) => 4 * numbers.len(),
        _ => 8,
    };
    values.values().map(|value| size(value) as u64).sum()
}

/// Inserts a row of these values into a table.
fn insert(tx: &contextdb::Transaction<'_>, table: &str, values: &Values) -> Fallible<u64> {
    let names: Vec<&str> = values.keys().map(String::as_str).collect();
    let places: Vec<String> = names.iter().map(|name| format!("${name}")).collect();
    let sql = format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        names.join(", "),
        places.join(", ")
    );
    tx.execute(&sql, values)?;
    Ok(held(values))
}

/// A read statement asked of every value in `rounds` rounds after 5 untimed: the times of each
/// round as `ask` gives them, and from the first round the rows each examined.
fn reads(
    run: &dyn Fn(&Values) -> contextdb::Result<contextdb::QueryResult>,
    asked: &[Values],
) -> Fallible<Asked> {
    for values in asked.iter().take(support::UNTIMED) {
        run(values)?;
    }
    let mut out = Asked::default();
    for round in 0..ROUNDS {
        let mut spent = Vec::new();
        for values in asked {
            let (found, time) = timed(|| run(values));
            let found = found?;
            spent.push(time);
            if round == 0 {
                out.examined.push(found.rows_examined());
                out.evaluations.push(found.rows().len() as u64);
            }
        }
        out.p50.push(percentile(&spent, 0.5));
        out.p95.push(percentile(&spent, 0.95));
    }
    Ok(out)
}

/// Prints the times of `asked` and the rows it examined at the middle and the 95th percentile.
fn read_lines(name: &str, asked: &Asked) {
    times(name, asked);
    line(
        &format!("{name}_rows_examined_p50"),
        percentile(&asked.examined, 0.5),
        "rows",
    );
    line(
        &format!("{name}_rows_examined_p95"),
        percentile(&asked.examined, 0.95),
        "rows",
    );
}

// ---------------------------------------------------------------------------------------------
// The passages, declared as Context Graph declares them

/// A dump as `passages.rs` reads it: each Context, each pair's Context and generation, the
/// questions, and each passage's id, pair and flags.
struct Dump {
    contexts: Vec<Uuid>,
    pairs: Vec<(usize, Uuid)>,
    questions: Vec<Vec<f32>>,
    passages: Vec<(Uuid, usize, u32)>,
}

fn floats(bytes: &[u8]) -> Vec<f32> {
    let (fours, _) = bytes.as_chunks::<4>();
    fours.iter().map(|four| f32::from_le_bytes(*four)).collect()
}

fn read_dump(folder: &Path) -> Fallible<Dump> {
    let text = |name: &str| std::fs::read_to_string(folder.join(name));
    let field = |line: &str, at: usize| line.split(' ').nth(at).unwrap_or_default().to_owned();
    let contexts = text("contexts.txt")?
        .lines()
        .map(|l| field(l, 1).parse())
        .collect::<Result<_, _>>()?;
    let mut pairs = Vec::new();
    for l in text("pairs.txt")?.lines() {
        pairs.push((field(l, 1).parse()?, field(l, 2).parse()?));
    }
    let questions = floats(&std::fs::read(folder.join("queries.f32"))?);
    let mut passages = Vec::new();
    for record in std::fs::read(folder.join("rows.bin"))?.chunks_exact(36) {
        let number = |at: usize| u32::from_le_bytes(std::array::from_fn(|b| record[at + b]));
        passages.push((
            Uuid::from_slice(&record[8..24])?,
            number(28) as usize,
            number(32),
        ));
    }
    let questions = questions
        .chunks_exact(DIMENSIONS)
        .map(<[f32]>::to_vec)
        .collect();
    Ok(Dump {
        contexts,
        pairs,
        questions,
        passages,
    })
}

/// Of the recorded answers in a file, how many questions have the same ten ids as `asked`.
fn same_ten(asked: &Asked, recorded: &Path) -> Fallible<usize> {
    let mut same = 0;
    for text in std::fs::read_to_string(recorded)?.lines() {
        let recorded: Json = serde_json::from_str(text)?;
        let (Some(question), Some(ids)) = (recorded["q"].as_u64(), recorded["ids"].as_array())
        else {
            continue; // the summary line
        };
        let ids: BTreeSet<u128> = ids
            .iter()
            .filter_map(|id| id.as_str()?.parse::<Uuid>().ok())
            .map(|id| id.as_u128())
            .collect();
        let ours = asked
            .answers
            .get(question as usize)
            .ok_or("a question out of range")?;
        same += usize::from(ours.iter().map(|row| row.0).collect::<BTreeSet<_>>() == ids);
    }
    Ok(same)
}

/// The longest keys a closed store holds, in bytes: an index entry (its row key left out), a
/// row key, a kept count's group and a partition's key tuple, each without the bytes that name
/// its table, index, count or column; and how many index entries hold a cut value. Read from
/// the data file with the storage library, as `contextdb/src/store/disk.rs` lays it out: an
/// index entry's record is its length in four bytes, and for an entry that holds a cut value
/// four more; a partition's record is kept under the byte 1, its column and its key tuple.
struct Keys {
    entry: u32,
    cut: u64,
    row: usize,
    count: usize,
    tuple: usize,
}

fn stored_keys(store: &Path) -> Fallible<Keys> {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};
    let data = redb::ReadOnlyDatabase::open(store.join("data"))?;
    let read = data.begin_read()?;
    let table = |name: &str| read.open_table(TableDefinition::<&[u8], &[u8]>::new(name));
    let mut keys = Keys {
        entry: 0,
        cut: 0,
        row: 0,
        count: 0,
        tuple: 0,
    };
    for record in table("index")?.iter()? {
        let record = record?.1.value().to_vec();
        keys.entry = keys
            .entry
            .max(u32::from_le_bytes(std::array::from_fn(|b| record[b])));
        keys.cut += u64::from(record.len() == 8);
    }
    let longest = |name: &str, named_by: usize| -> Fallible<usize> {
        let table = table(name)?;
        let lengths = table
            .iter()?
            .map(|record| record.map(|(key, _)| key.value().len()));
        let longest = lengths.collect::<Result<Vec<_>, _>>()?.into_iter().max();
        Ok(longest.unwrap_or(named_by) - named_by)
    };
    keys.row = longest("rows", 4)?;
    keys.count = longest("counts", 4)?;
    for record in table("vectors")?.range::<&[u8]>(&[1_u8][..]..&[2_u8][..])? {
        keys.tuple = keys.tuple.max(record?.0.value().len() - 5);
    }
    Ok(keys)
}

/// The storage door of a data file that counts the bytes written through it.
#[derive(Debug)]
struct Written(File, Arc<AtomicU64>);

impl StorageBackend for Written {
    fn len(&self) -> std::io::Result<u64> {
        Ok(self.0.metadata()?.len())
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> std::io::Result<()> {
        self.0.read_exact_at(out, offset)
    }
    fn set_len(&self, len: u64) -> std::io::Result<()> {
        self.0.set_len(len)
    }
    fn sync_data(&self) -> std::io::Result<()> {
        self.0.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> std::io::Result<()> {
        self.1.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.0.write_all_at(data, offset)
    }
}

fn declared(dump_folder: &Path, answers: &Path, folder: &Path) -> Fallible<()> {
    let dump = read_dump(dump_folder)?;
    let store = folder.join("declared");
    let db = declared_store(&store, DatabaseOpenOptions::default())?;
    let prefix = format!("declared_{}_", dump.passages.len());
    let name = |what: &str| format!("{prefix}{what}");
    let (held_bytes, load) = timed(|| -> Fallible<u64> {
        let tx = db.transaction()?;
        let space = params! { "embedding_space_id" => "space", "dimension" => DIMENSIONS as i64, "metric" => "cosine", "created_at" => Value::TxId(tx.id()) };
        let mut held_bytes = insert(&tx, "embedding_spaces", &space)?;
        for (n, context) in dump.contexts.iter().enumerate() {
            let row = params! { "id" => *context, "name" => format!("context {n}"), "created_at" => Value::TxId(tx.id()) };
            held_bytes += insert(&tx, "contexts", &row)?;
        }
        for (context, generation) in &dump.pairs {
            let mut row = params! {
                "id" => *generation, "context_id" => dump.contexts[*context],
                "interpretation_snapshot" => Json::Object(Default::default()), "interpretation_snapshot_sha256" => "",
                "event_count" => 0_i64, "ordered_source_set_sha256" => "", "renderer_id" => "r",
                "renderer_version" => "1", "renderer_profile" => Json::Object(Default::default()),
                "embedding_space_id" => "space", "encoding_profile" => "e", "state" => "active",
                "last_attempt_token" => Uuid::nil(),
            };
            row.insert("started_at".to_owned(), Value::TxId(tx.id()));
            held_bytes += insert(&tx, "event_search_generations", &row)?;
        }
        tx.commit()?;
        let mut vectors = BufReader::new(File::open(dump_folder.join("vectors.f32"))?);
        let mut vector = vec![0u8; 4 * DIMENSIONS];
        let mut tx = db.transaction()?;
        for (at, (id, pair, flags)) in dump.passages.iter().enumerate() {
            vectors.read_exact(&mut vector)?;
            let (context, generation) = dump.pairs[*pair];
            let mut row = params! {
                "id" => *id, "context_id" => dump.contexts[context], "generation_id" => generation,
                "connector_id" => "claude-code", "session_ref" => format!("pair {pair}"),
                "source_instance_ref" => format!("pair {pair}"), "passage_ordinal" => at as i64,
                "first_event_sequence" => at as i64, "last_event_sequence" => at as i64,
                "rendered_text" => "", "content_sha256" => id.simple().to_string(),
                "dimension" => DIMENSIONS as i64, "is_current" => flags & 1 != 0,
                "is_searchable" => flags & 2 != 0,
            };
            row.insert("created_at".to_owned(), Value::TxId(tx.id()));
            let vector = if flags & 4 != 0 {
                Value::Vector(floats(&vector))
            } else {
                Value::Null
            };
            row.insert("vector_text".to_owned(), vector);
            held_bytes += insert(&tx, "event_search_passages", &row)?;
            if (at + 1) % PER_COMMIT == 0 {
                std::mem::replace(&mut tx, db.transaction()?).commit()?;
            }
        }
        tx.commit()?;
        support::upkeep(&db)?;
        Ok(held_bytes)
    });
    let held_bytes = held_bytes?;
    db.close()?;
    let bytes = bytes_on_disk(&store)?;
    line(&name("load"), seconds(load), "s");
    line(&name("store_size"), mebibytes(bytes), "MiB");
    line(
        &name("bytes_per_passage"),
        bytes / dump.passages.len() as u64,
        "bytes",
    );
    line(&name("size_over_held"), ratio(bytes, held_bytes), "x");
    line(
        "key_longest_partition_tuple",
        stored_keys(&store)?.tuple,
        "bytes",
    );

    // The find, in three scopes, under AUTO as Context Graph sends it.
    let reader = session(&store, READER_LIMIT)?;
    let every: Vec<usize> = (0..dump.contexts.len()).collect();
    for (scope, contexts) in [
        ("one", &every[..1]),
        ("several", &every[..3]),
        ("all", &every[..]),
    ] {
        let wanted: BTreeSet<usize> = contexts.iter().copied().collect();
        let mut values = params! {};
        let (mut context_names, mut generation_names) = (Vec::new(), Vec::new());
        for (n, context) in contexts.iter().enumerate() {
            values.insert(format!("c{n}"), Value::Uuid(dump.contexts[*context]));
            context_names.push(format!("$c{n}"));
        }
        for (n, (_, generation)) in dump
            .pairs
            .iter()
            .filter(|(context, _)| wanted.contains(context))
            .enumerate()
        {
            values.insert(format!("g{n}"), Value::Uuid(*generation));
            generation_names.push(format!("$g{n}"));
        }
        let sql = format!(
            "SELECT id, vector_text <=> $q AS distance FROM event_search_passages \
             WHERE context_id IN ({}) AND generation_id IN ({}) AND is_current = TRUE \
             AND is_searchable = TRUE ORDER BY distance",
            context_names.join(", "),
            generation_names.join(", ")
        );
        let find = |tail: &str, question: &[f32]| {
            let mut values = values.clone();
            values.insert("q".to_owned(), Value::Vector(question.to_vec()));
            (format!("{sql}{tail}"), values)
        };
        let asked = ask(&reader, &find, &[AUTO], &dump.questions, ROUNDS)?.remove(0);
        let find_name = format!("declared_find_{scope}");
        setting(&find_name, READER_LIMIT);
        read_lines(&find_name, &asked);
        let same = same_ten(&asked, &answers.join(format!("engine-{scope}.jsonl")))?;
        line(&format!("declared_same_ten_{scope}"), same, "of_100");
    }

    // Counts of one pair's current passages: by the index range, then by a kept count.
    let (context, generation) = dump.pairs[1];
    let pair = params! { "c" => dump.contexts[context], "g" => generation };
    let count = "SELECT COUNT(*) FROM event_search_passages WHERE context_id = $c AND generation_id = $g AND is_current = TRUE";
    let asked = reads(
        &|values| reader.execute(count, values),
        &vec![pair.clone(); 100],
    )?;
    read_lines("declared_count_context", &asked);
    drop(reader);
    let db = Database::open(&store)?;
    db.execute("ALTER TABLE event_search_passages SET MAINTAIN COUNT (context_id, generation_id, is_current)", &params! {})?;
    let asked = reads(
        &|values| db.execute(count, values),
        &vec![pair.clone(); 100],
    )?;
    read_lines("kept_count_context", &asked);
    db.close()?;

    // Three flips of the pair's current flag, through a door that counts the bytes written.
    let before = bytes_on_disk(&store)?;
    let written = Arc::new(AtomicU64::new(0));
    let counter = written.clone();
    let mut options = DatabaseOpenOptions::default();
    options.storage = Some(Arc::new(move |file| {
        Ok(Box::new(Written(file, counter.clone())) as Box<dyn StorageBackend>)
    }));
    let db = Database::open_with_options(&store, options)?;
    let flip = "UPDATE event_search_passages SET is_current = $to WHERE context_id = $c AND generation_id = $g";
    let (mut spent, mut changed) = (Vec::new(), 0);
    written.store(0, Ordering::Relaxed);
    for to in [false, true, false] {
        let mut values = pair.clone();
        values.insert("to".to_owned(), Value::Bool(to));
        let (found, time) = timed(|| db.execute(flip, &values));
        changed = found?.rows_affected();
        spent.push(time);
    }
    let flips = written.load(Ordering::Relaxed);
    db.close()?;
    let flip_name = format!("flip_current_{changed}");
    line(&flip_name, milliseconds(percentile(&spent, 0.5)), "ms");
    line(
        &format!("{flip_name}_bytes_written"),
        mebibytes(flips / 3),
        "MiB",
    );
    line(
        &format!("{flip_name}_store_size_before"),
        mebibytes(before),
        "MiB",
    );
    line(
        &format!("{flip_name}_store_size_after_3_flips"),
        mebibytes(bytes_on_disk(&store)?),
        "MiB",
    );

    // The pair's passages deleted under 64 MiB.
    let mut options = DatabaseOpenOptions::default();
    options.memory_limit = Some(64 << 20);
    let db = Database::open_with_options(&store, options)?;
    support::take_peak_allocated();
    let deleted = db
        .execute(
            "DELETE FROM event_search_passages WHERE context_id = $c AND generation_id = $g",
            &pair,
        )?
        .rows_affected();
    line(
        &format!("delete_generation_{deleted}_peak_allocated"),
        mebibytes(support::take_peak_allocated()),
        "MiB",
    );
    db.close()?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// The anchors of the sealed sessions

/// An anchor as the connector makes one: its key and its value.
fn anchors_of(line: &Json) -> Vec<Vec<(&'static str, String)>> {
    let cwd = line["cwd"].as_str();
    let blocks = line["message"]["content"].as_array().into_iter().flatten();
    let calls = blocks.filter(|block| block["type"] == "tool_use");
    calls
        .map(|call| {
            let input = &call["input"];
            let file = ["file_path", "path", "notebook_path"]
                .iter()
                .find_map(|field| input[*field].as_str());
            [
                ("command", input["command"].as_str()),
                ("file", file),
                ("working_directory", cwd),
            ]
            .into_iter()
            .filter_map(|(key, value)| Some((key, value?.to_owned())))
            .collect()
        })
        .collect()
}

fn anchors(corpus: &Path, folder: &Path) -> Fallible<()> {
    let store = folder.join("anchors");
    let db = declared_store(&store, DatabaseOpenOptions::default())?;
    let context = Uuid::from_u128(1);
    let mut names: Vec<_> = std::fs::read_dir(corpus)?.collect::<Result<Vec<_>, _>>()?;
    names.sort_by_key(|entry| entry.file_name());
    let (mut events, mut rows, mut commands) = (0_u128, 0_u64, Vec::new());
    let (held_bytes, load) = timed(|| -> Fallible<u64> {
        let mut tx = db.transaction()?;
        let row =
            params! { "id" => context, "name" => "sessions", "created_at" => Value::TxId(tx.id()) };
        let mut held_bytes = insert(&tx, "contexts", &row)?;
        for entry in &names {
            let session = entry.file_name().to_string_lossy().into_owned();
            let mut seq = 0_i64;
            for text in BufReader::new(File::open(entry.path())?).lines() {
                let Ok(json) = serde_json::from_str::<Json>(&text?) else {
                    continue;
                };
                for anchors in anchors_of(&json) {
                    events += 1;
                    seq += 1;
                    let event = Uuid::from_u128(1 << 64 | events);
                    let mut row = params! {
                        "id" => event, "context_id" => context, "connector_id" => "claude-code",
                        "adapter_version" => "1", "anchor_declaration_version" => "anc1",
                        "source_instance_ref" => session.as_str(), "session_ref" => session.as_str(),
                        "source_event_key" => seq.to_string(), "source_position" => Json::Object(Default::default()),
                        "seq" => seq, "content_hash" => "", "anchor_count" => anchors.len() as i64,
                        "anchor_set_hash" => "", "author" => "assistant", "kind" => "tool_call",
                        "render_role" => "tool", "coverage" => "full",
                    };
                    held_bytes += insert(&tx, "events", &row)?;
                    for (key, value) in anchors {
                        rows += 1;
                        let identity = serde_json::to_string(&value)?;
                        row = params! {
                            "id" => Uuid::from_u128(2 << 64 | u128::from(rows)), "context_id" => context,
                            "event_id" => event, "correlation_scope_hash" => session.as_str(),
                            "declaration_version" => "anc1", "connector_id" => "claude-code",
                            "source_instance_ref" => session.as_str(), "session_ref" => session.as_str(),
                            "render_role" => "tool", "key" => key, "value" => value.as_str(),
                            "identity_value" => identity, "original_value" => Json::String(value.clone()),
                        };
                        held_bytes += insert(&tx, "event_anchors", &row)?;
                        if key == "command" {
                            commands.push(value);
                        }
                    }
                    if events % PER_COMMIT as u128 == 0 {
                        std::mem::replace(&mut tx, db.transaction()?).commit()?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(held_bytes)
    });
    let held_bytes = held_bytes?;
    db.close()?;
    let bytes = bytes_on_disk(&store)?;
    let name = |what: &str| format!("anchors_{}_{what}", commands.len());
    line(&name("load"), seconds(load), "s");
    line(&name("rows"), rows, "rows");
    line(&name("events"), events, "rows");
    line(&name("store_size"), mebibytes(bytes), "MiB");
    line(&name("size_over_held"), ratio(bytes, held_bytes), "x");
    let keys = stored_keys(&store)?;
    line("anchor_entry_longest", keys.entry, "bytes");
    line("anchor_entries_cut", keys.cut, "entries");
    line("key_longest_row_key", keys.row, "bytes");
    line("key_longest_count_key", keys.count, "bytes");

    // 100 commands, evenly spread through the distinct ones: 50 longer than the 876 bytes the
    // exact index keeps whole, and 50 not.
    let distinct: BTreeSet<&String> = commands.iter().collect();
    let (long, short): (Vec<&String>, Vec<&String>) =
        distinct.into_iter().partition(|c| c.len() > 876);
    let spread = |of: &[&String]| {
        (0..50)
            .map(|n| of[n * of.len() / 50].clone())
            .collect::<Vec<_>>()
    };
    let asked: Vec<Values> = [spread(&long), spread(&short)]
        .concat()
        .into_iter()
        .map(|value| params! { "c" => context, "v" => value })
        .collect();
    let reader = session(&store, READER_LIMIT)?;
    let exact =
        "SELECT id FROM event_anchors WHERE context_id = $c AND key = 'command' AND value = $v";
    let asked = reads(&|values| reader.execute(exact, values), &asked)?;
    setting("anchor_exact", READER_LIMIT);
    times("anchor_exact", &asked);
    line(
        "anchor_exact_rows_examined_p95",
        percentile(&asked.examined, 0.95),
        "rows",
    );
    line(
        "anchor_exact_rows_returned_p95",
        percentile(&asked.evaluations, 0.95),
        "rows",
    );
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Synthetic: edges of a DAG, and a read narrowed by a grant

fn dag(folder: &Path) -> Fallible<()> {
    let db = declared_store(&folder.join("dag"), DatabaseOpenOptions::default())?;
    let tx = db.transaction()?;
    let context = Uuid::from_u128(1);
    insert(
        &tx,
        "contexts",
        &params! { "id" => context, "name" => "graph", "created_at" => Value::TxId(tx.id()) },
    )?;
    tx.commit()?;
    let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
    let mut next = |below: u64| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed % below
    };
    let (mut spent, mut tx) = (Vec::with_capacity(300_000), db.transaction()?);
    // Edges in the order drawn, each once: a node cites a later node of its group of 100.
    let (mut edges, mut drawn) = (Vec::with_capacity(300_000), BTreeSet::new());
    while edges.len() < 300_000 {
        let source = next(100_000);
        let room = 99 - source % 100;
        let edge = (source, source + 1 + next(room.max(1)));
        if room > 0 && drawn.insert(edge) {
            edges.push(edge);
        }
    }
    for (n, (source, target)) in edges.iter().enumerate() {
        let mut row = params! {
            "id" => Uuid::from_u128(n as u128 + 1), "source_id" => Uuid::from_u128(u128::from(*source) + (1 << 64)),
            "target_id" => Uuid::from_u128(u128::from(*target) + (1 << 64)), "edge_type" => "CITES", "context_id" => context,
        };
        row.insert("valid_from".to_owned(), Value::TxId(tx.id()));
        let (done, time) = timed(|| insert(&tx, "edges", &row));
        done?;
        spent.push(time);
        if (n + 1) % PER_COMMIT == 0 {
            std::mem::replace(&mut tx, db.transaction()?).commit()?;
        }
    }
    tx.commit()?;
    line(
        "dag_insert_p95",
        milliseconds(percentile(&spent, 0.95)),
        "ms",
    );
    Ok(())
}

fn grants(folder: &Path) -> Fallible<()> {
    let db = Database::open(folder.join("grants"))?;
    for sql in [
        "CREATE TABLE grants (id INTEGER PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID)",
        "CREATE INDEX grants_acl ON grants (acl_id)",
        "CREATE TABLE guarded (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, acl_id UUID ACL REFERENCES grants(acl_id), body TEXT)",
        "CREATE INDEX guarded_context ON guarded (context_id)",
    ] {
        db.execute(sql, &params! {})?;
    }
    let mut tx = db.transaction()?;
    for n in 0..100_000_i64 {
        let row = params! { "id" => n, "context_id" => Uuid::from_u128((n % 10) as u128 + 1), "acl_id" => Uuid::from_u128((n % 20_000) as u128 + 1), "body" => "kept" };
        insert(&tx, "guarded", &row)?;
        if n < 20_000 && n % 20 == 0 {
            let grant = params! { "id" => n, "principal_kind" => "Agent", "principal_id" => "a", "acl_id" => Uuid::from_u128(n as u128 + 1) };
            insert(&tx, "grants", &grant)?;
        }
        if (n + 1) % PER_COMMIT as i64 == 0 {
            std::mem::replace(&mut tx, db.transaction()?).commit()?;
        }
    }
    tx.commit()?;
    let context = Uuid::from_u128(1);
    let narrowed = db.scoped_with_constraints(
        Some(BTreeSet::from([ContextId::new(context)])),
        None,
        Some(Principal::Agent("a".to_owned())),
    );
    let read = "SELECT id, body FROM guarded WHERE context_id = $c";
    let asked = vec![params! { "c" => context }; 20];
    for (name, handle) in [
        ("grants_read_narrowed", &narrowed),
        ("grants_read_whole", &db),
    ] {
        let asked = reads(&|values| handle.execute(read, values), &asked)?;
        read_lines(name, &asked);
        line(
            &format!("{name}_rows_returned"),
            asked.evaluations[0],
            "rows",
        );
    }
    Ok(())
}

fn main() -> Fallible<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    let folder = Path::new(arguments.last().copied().unwrap_or_default());
    must_be_empty(folder);
    match arguments.as_slice() {
        ["declarations", _] => declarations(folder),
        ["declared", dump, answers, _] => declared(Path::new(dump), Path::new(answers), folder),
        ["anchors", corpus, _] => anchors(Path::new(corpus), folder),
        ["dag", _] => dag(folder),
        ["grants", _] => grants(folder),
        _ => {
            eprintln!(
                "usage: queries declarations|dag|grants <folder>\n       queries declared <dump folder> <answers folder> <folder>\n       queries anchors <corpus folder> <folder>"
            );
            std::process::exit(2);
        }
    }
}
