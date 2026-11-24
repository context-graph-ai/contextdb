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
//!   the number: a xorshift from `0x9E37_79B9_7F4A_7C15`, 300 hidden centroids, each vector a
//!   centroid plus 0.06 of noise a number, and 100 questions with four thirds of that noise. The
//!   second stand in for a camera store: three classes, a subject for every 2,500 rows with
//!   eight directions of its own for its appearance, tracks of ten sightings whose appearance
//!   drifts, a little noise on every number; 100 questions, eighty new sightings of stored
//!   subjects and twenty of subjects never stored. Each case loads two stores, 500 rows to a
//!   commit: one whose column declares `INDEXING_THRESHOLD 1000000000`, so that nothing is grouped,
//!   sealed and searched under EXACT; and one that declares nothing, with every maintenance
//!   cycle run, opened and searched once in a new process (`measure first ...`, which the
//!   program starts itself as) and then searched, in one read session under 1 GiB, under
//!   EXACT, INDEXED and AUTO in turn: every question once after 5 searches of each mode that
//!   are not timed. On sightings a hundred stored vectors are then each searched for by itself.
//! - `measure commits <count> <folder>`, `measure opens <size> <folder>`, `measure camera
//!   <rows a second> <seconds> [period seconds] <folder>` and `measure sightings <rows>
//!   --contexts <n> <folder>`: the timed lines of the engine's bars on commits, opens after a
//!   kill, a camera's day and a find over several Contexts; each function says what it does.
//! - `measure bound <rows> <store>`: a find with a bound on the distance, on a store `measure
//!   sightings <rows>` built, beside the same find without it (see `bound`).
//!
//! The store's folder must be missing or empty. A store's size is the bytes of every file in
//! its folder after it was closed.
//!
//! This program measures. Only `measure bound` passes or fails, on its bar; no test reads what
//! it prints, and `numbers.sh` is what runs it.

#[path = "../tests/support/sightings.rs"]
mod sightings;
mod support;

use std::ffi::OsStr;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use contextdb::{Database, DatabaseOpenOptions, Result, Uuid, Value, params};
use sightings::{DIMENSIONS, Random, Sightings};
use support::{
    EXACT, Fallible, READER_LIMIT, Values, bytes_on_disk, line, mebibytes, milliseconds,
    peak_resident_bytes, percentile, ratio, seconds, timed,
};

#[global_allocator]
static ALLOCATOR: support::Counting = support::Counting;

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
    line(
        "readme_idle_rss_anon",
        mebibytes(support::status_bytes("RssAnon:").unwrap_or(0)),
        "MiB",
    );
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
        > Some(0)
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
        let (filled, load) = timed(|| filled(&store, " INDEXING_THRESHOLD 1000000000", rows));
        (filled?, load)
    };
    line(&format!("{prefix}load"), seconds(load), "s");
    support::sealed(prefix, &db, &store)?;
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
    support::built(prefix, &db, &store, held)?;
    let question = folder.join("question.f32");
    let bytes = questions[0].iter().flat_map(|number| number.to_le_bytes());
    std::fs::write(&question, bytes.collect::<Vec<u8>>())?;
    let first = [OsStr::new("first"), store.as_os_str(), OsStr::new(prefix)];
    support::in_new_process(&[first[0], first[1], first[2], question.as_os_str()])?;
    let reader = support::session(&store, READER_LIMIT)?;
    let find = format!("{prefix}find");
    support::modes(&find, &reader, &nearest, &questions, 1)?;
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
        sightings.rows(rows).try_for_each(each)?;
        Ok(sightings.questions())
    };
    index(&format!("sightings_{rows}_"), folder, rows, &load, &stored)
}

/// Generated sightings over `contexts` partitions keyed by Context, loaded with no maintenance,
/// grouped by a backfill while another thread searches all of them under AUTO, then each
/// question under each mode over the first Context and over all.
fn contexts_case(rows: usize, contexts: usize, folder: &Path) -> Fallible<()> {
    let store = folder.join("store");
    let mut options = DatabaseOpenOptions::default();
    options.maintenance = contextdb::Maintenance::Caller;
    let db = Database::open_with_options(&store, options)?;
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, context INTEGER NOT NULL, embedding VECTOR(768) PARTITION_KEY (context) MAX_PARTITIONS 4096)", &params! {})?;
    let sightings = Sightings::new(rows);
    let (loaded, load) = timed(|| -> Fallible<()> {
        let mut tx = db.transaction()?;
        for (row, vector) in sightings.rows(rows).enumerate() {
            let values = params! { "id" => row as i64, "c" => (row / Sightings::TRACK % contexts) as i64, "v" => vector };
            tx.execute("INSERT INTO items VALUES ($id, $c, $v)", &values)?;
            if (row + 1) % 500 == 0 {
                std::mem::replace(&mut tx, db.transaction()?).commit()?;
            }
        }
        Ok(tx.commit()?)
    });
    loaded?;
    let name = |what: &str| format!("sightings_{rows}_contexts_{contexts}_{what}");
    line(&name("load"), seconds(load), "s");
    let find = |among: usize, tail: &str, question: &[f32]| {
        let list: Vec<_> = (0..among).map(|c| c.to_string()).collect();
        let sql = format!(
            "SELECT id, embedding <=> $query AS distance FROM items WHERE context IN ({}) ORDER BY distance{tail}",
            list.join(", ")
        );
        (sql, params! { "query" => question.to_vec() })
    };
    let questions = sightings.questions();
    let auto = support::AUTO;
    let searches: Vec<_> = questions.iter().map(|q| find(contexts, auto, q)).collect();
    let backfill = || timed(|| support::maintenance(&db));
    let ((cycles, time), searched) = support::during(&store, &searches, backfill)?;
    line(&name("backfill"), seconds(time), "s");
    line(&name("backfill_cycles"), cycles?, "cycles");
    support::spread(&name("find_during_backfill"), &searched);
    let reader = support::session(&store, READER_LIMIT)?;
    for (what, among) in [("find_one", 1), ("find_all", contexts)] {
        let each = |tail: &str, question: &[f32]| find(among, tail, question);
        support::modes(&name(what), &reader, &each, &questions, 1)?;
    }
    Ok(db.close()?)
}

/// `measure bound <rows> <store>`: on the store `measure sightings <rows>` built, in one read
/// session under the reader's limit, the fixture's hundred questions with `LIMIT 8` and a bound
/// on the distance at 0.5, under AUTO and under INDEXED (design section 6, review 13 R7). First,
/// untimed, each is asked under EXACT with no bound: that gives its rows inside the bound and
/// the premise that 0.5 parts the two kinds (a stored subject's nearest row inside it, a never
/// stored one's outside); the run stops if the premise fails. Then each question is asked with
/// the bound and timed by itself. It counts when it came back within 200 ms and is right: no row
/// for a subject never stored; for a stored one at least one row, every row inside the bound. In
/// each mode the run fails unless at least 76 of the 80 stored and 19 of the 20 never stored
/// count, and the answers together hold at least 95 in 100 of EXACT's rows inside the bound, a
/// refused question holding none. The same find without the bound is asked after; its lines
/// decide nothing.
fn bound(rows: usize, store: &Path) -> Fallible<()> {
    const BOUND: f64 = 0.5;
    let questions = Sightings::new(rows).questions();
    let reader = support::session(store, READER_LIMIT)?;
    let ask = |filter: &str, tail: &str, question: &[f32]| -> Fallible<_> {
        let sql = format!(
            "SELECT id, embedding <=> $query AS distance FROM items{filter} ORDER BY distance{tail}"
        );
        let values = params! { "query" => question.to_vec() };
        let (found, time) = timed(|| reader.execute(&sql, &values));
        let found = found?;
        let rows = found
            .rows()
            .iter()
            .map(|row| match (row.first(), row.last()) {
                (Some(Value::Int64(id)), Some(Value::Float64(distance))) => (*id, *distance),
                _ => (-1, f64::NAN),
            });
        let counted = (found.distance_evaluations(), found.rows_examined());
        Ok((time, counted, rows.collect::<Vec<_>>()))
    };
    let never = |j: usize| j % 5 == 4;
    let mut inside: Vec<Vec<i64>> = Vec::new();
    for (j, question) in questions.iter().enumerate() {
        let (_, _, exact) = ask("", " USE VECTOR EXACT LIMIT 8", question)?;
        let nearest = exact.first().map_or(f64::INFINITY, |row| row.1);
        if (nearest < BOUND) == never(j) {
            let kind = if never(j) { "never stored" } else { "stored" };
            let premise = format!("EXACT's nearest at {nearest}: {BOUND} does not part the kinds");
            return Err(format!("question {j} ({kind}): {premise}").into());
        }
        let within = exact.iter().filter(|row| row.1 < BOUND);
        inside.push(within.map(|row| row.0).collect());
    }
    let total = inside.iter().map(Vec::len).sum::<usize>();
    let name = |what: &str| format!("sightings_{rows}_bound_{what}");
    support::setting(&name("search"), READER_LIMIT);
    line(&name("distance"), BOUND, "-");
    let within = format!(" WHERE embedding <=> $query < {BOUND}");
    let mut failed = Vec::new();
    for (mode, tail) in [
        ("auto", " LIMIT 8"),
        ("indexed", " USE VECTOR INDEXED LIMIT 8"),
    ] {
        let name = |what: &str| name(&format!("{mode}_{what}"));
        for (filter, which) in [(within.as_str(), "with"), ("", "without")] {
            let (mut right, mut refused, mut held) = ([0; 2], 0, 0);
            let mut asked = support::Asked::default();
            for (j, question) in questions.iter().enumerate() {
                let Ok((time, (evaluations, examined), found)) = ask(filter, tail, question)
                    .map_err(|error| eprintln!("{mode} {which} the bound, question {j}: {error}"))
                else {
                    refused += 1;
                    continue;
                };
                let good = match never(j) {
                    true => found.is_empty(),
                    false => !found.is_empty() && found.iter().all(|row| row.1 < BOUND),
                };
                let late = time > std::time::Duration::from_millis(200);
                right[usize::from(never(j))] += usize::from(good && !late);
                let ids: Vec<_> = found.iter().map(|row| row.0).collect();
                held += inside[j].iter().filter(|id| ids.contains(id)).count();
                asked.p50.push(time);
                asked.evaluations.push(evaluations);
                asked.examined.push(examined);
            }
            line(&name(&format!("{which}_refused")), refused, "statements");
            // Every question refused leaves nothing to time.
            if !asked.p50.is_empty() {
                asked.p95 = vec![percentile(&asked.p50, 0.95)];
                asked.p50 = vec![percentile(&asked.p50, 0.5)];
                support::times(&name(which), &asked);
                support::counted(&name(which), &asked);
            }
            if which == "without" {
                continue;
            }
            let share = format!("{:.1}", 100.0 * held as f64 / total as f64);
            line(&name("with_stored_right"), right[0], "of_80_against_76");
            line(&name("with_never_right"), right[1], "of_20_against_19");
            line(&name("with_exact_rows_held"), share, "in_100_against_95");
            if right[0] < 76 || right[1] < 19 || held * 100 < 95 * total {
                let rows = format!("{held} of {total} rows");
                failed.push(format!("{mode}: {right:?} right of [80, 20], {rows}"));
            }
        }
    }
    match failed.is_empty() {
        true => Ok(()),
        false => Err(format!("the bound's bar failed: {}", failed.join("; ")).into()),
    }
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

struct ExpiryClock;
impl contextdb::Clock for ExpiryClock {
    fn now(&self) -> std::time::SystemTime {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(100)
    }
}
fn statement_options(parent: bool) -> DatabaseOpenOptions {
    let mut options = DatabaseOpenOptions::default();
    options.maintenance = contextdb::Maintenance::Caller;
    options.clock = parent.then_some(std::sync::Arc::new(ExpiryClock));
    options
}

/// A fixed consumer store, built outside the instruction counter.
fn statement_store(folder: &Path) -> Fallible<()> {
    support::must_be_empty(folder);
    let db = Database::open(folder)?;
    let fixture = include_str!("../tests/fixtures/consumer_schema.sql");
    for sql in fixture
        .split("\n-- Second texts")
        .next()
        .unwrap()
        .lines()
        .filter(|s| s.starts_with("CREATE "))
    {
        db.execute(sql, &params! {})?;
    }
    for c in 1..=2_u128 {
        let values = params! { "c" => Uuid::from_u128(c), "name" => format!("context-{c}"), "at" => contextdb::Value::TxId(db.committed_watermark()?) };
        db.execute(
            "INSERT INTO contexts (id, name, created_at) VALUES ($c, $name, $at)",
            &values,
        )?;
        db.execute("INSERT INTO events (id, context_id, connector_id, adapter_version, anchor_declaration_version, source_instance_ref, session_ref, source_event_key, source_position, seq, content_hash, anchor_count, anchor_set_hash, author, kind, render_role, coverage) VALUES ($c, $c, 'camera', '1', '1', 'source', 'session', 'event', '{}', 1, 'hash', 1, 'hash', 'author', 'text', 'user', 'whole')", &values)?;
    }
    db.execute("INSERT INTO embedding_spaces (embedding_space_id, dimension, metric, created_at) VALUES ('fixed', 768, 'cosine', $at)", &params! { "at" => contextdb::Value::TxId(db.committed_watermark()?) })?;
    for c in 1..=2_u128 {
        db.execute("INSERT INTO event_search_generations (id, context_id, interpretation_snapshot, interpretation_snapshot_sha256, event_count, ordered_source_set_sha256, renderer_id, renderer_version, renderer_profile, embedding_space_id, encoding_profile, state, started_at, last_attempt_token) VALUES ($g, $c, '{}', 'hash', 20000, 'hash', 'fixed', '1', '{}', 'fixed', 'fixed', 'ready', $at, $g)", &params! { "c" => Uuid::from_u128(c), "g" => Uuid::from_u128(100 + c), "at" => contextdb::Value::TxId(db.committed_watermark()?) })?;
    }
    for start in (0..20_000_u128).step_by(500) {
        let tx = db.transaction()?;
        for n in start..start + 500 {
            let c = Uuid::from_u128(1 + n % 2);
            let values = params! { "id" => Uuid::from_u128(1000 + n), "c" => c, "v" => format!("command-{n}"), "n" => n as i64 };
            tx.execute("INSERT INTO event_anchors (id, context_id, event_id, correlation_scope_hash, declaration_version, connector_id, source_instance_ref, session_ref, render_role, key, value, identity_value, original_value) VALUES ($id, $c, $c, 'hash', '1', 'camera', 'source', 'session', 'user', 'command', $v, $v, '{\"payload\":{\"active\":true,\"rank\":42}}')", &values)?;
            // Keep 100 current rows in the searched pair so ten hit reads remain visible.
            let pair = if n < 100 { 1 } else { 2 };
            let vector = if n < 4000 {
                contextdb::Value::Vector(
                    (0..768).map(|at| ((n + at) % 127) as f32 / 127.0).collect(),
                )
            } else {
                contextdb::Value::Null
            };
            tx.execute("INSERT INTO event_search_passages (id, context_id, generation_id, connector_id, session_ref, source_instance_ref, passage_ordinal, first_event_sequence, last_event_sequence, rendered_text, content_sha256, vector_text, dimension, is_current, created_at) VALUES ($id, $c, $g, 'camera', 'session', 'source', $n, $n, $n, 'text', 'hash', $q, 768, TRUE, $at)", &params! { "id" => Uuid::from_u128(1000+n), "c" => Uuid::from_u128(pair), "g" => Uuid::from_u128(100 + pair), "n" => n as i64, "q" => vector, "at" => contextdb::Value::TxId(tx.id()) })?;
        }
        tx.commit()?;
    }
    db.execute("ALTER TABLE event_search_passages SET MAINTAIN COUNT (context_id, generation_id, is_current)", &params! {})?;
    db.close()?;
    for (name, rows) in [
        ("vector_purge_small", 2_048),
        ("vector_purge_large", 4_096),
        ("parent_expiry_small", 512),
        ("parent_expiry_large", 1024),
    ] {
        let parent = name.starts_with("parent");
        let db = Database::open_with_options(folder.join(name), statement_options(parent))?;
        let source = [
            "CREATE TABLE removed(id INTEGER PRIMARY KEY, parent INTEGER REFERENCES removed(id), due TIMESTAMP EXPIRES) RETAIN 1 SECONDS\nINSERT INTO removed VALUES($id,NULL,'1970-01-01T00:00:00.001Z')",
            "CREATE TABLE removed(id INTEGER PRIMARY KEY, v VECTOR(1) INDEXING_THRESHOLD 1000000000)\nINSERT INTO removed VALUES($id,[1.0])",
        ][usize::from(!parent)];
        let (ddl, insert) = source.split_once('\n').unwrap();
        db.execute(ddl, &params! {})?;
        let end = rows + i64::from(!parent);
        for first in (0..end).step_by(1024) {
            let tx = db.transaction()?;
            for id in first..(first + 1024).min(end) {
                tx.execute(insert, &params! {"id"=>id})?;
            }
            tx.commit()?;
        }
        if !parent {
            support::maintenance(&db)?;
        }
        db.close()?;
    }
    Ok(())
}

/// Open once: a destructive workload runs zero or once, other statements zero or 200 times.
fn statements(which: &str, folder: &Path, count: usize) -> Fallible<()> {
    let removal = which.starts_with("vector_purge") || which.starts_with("parent_expiry");
    let folder = folder.join(if removal { which } else { "." });
    let db = Database::open_with_options(
        folder,
        statement_options(which.starts_with("parent_expiry")),
    )?;
    let narrowed = db.scoped_with_constraints(
        Some([contextdb::ContextId(Uuid::from_u128(1))].into()),
        None,
        None,
    );
    let values = params! { "c" => Uuid::from_u128(1), "g" => Uuid::from_u128(101), "v" => "command-100", "query" => vec![0.5_f32; 768] };
    let sql = match which {
        "vector_purge_small" | "vector_purge_large" => "PURGE FROM removed WHERE id>0",
        "parent_expiry_small" | "parent_expiry_large" => {
            for _ in 0..count {
                std::hint::black_box(db.run_maintenance_cycle()?);
            }
            return Ok(());
        }
        "keyed_find" | "narrowed_find" => {
            "SELECT id FROM event_anchors WHERE context_id=$c AND key='command' AND value=$v"
        }
        "kept_count" => {
            "SELECT COUNT(*) FROM event_search_passages WHERE context_id=$c AND generation_id=$g AND is_current=TRUE"
        }
        "json_filter" => {
            r#"SELECT id FROM event_anchors WHERE id < '00000000-0000-0000-0000-00000000044c' AND original_value -> 'payload' = '{"active":true,"rank":42}'"#
        }
        "search_find" => {
            "SELECT id, context_id, generation_id, vector_text <=> $query AS distance FROM event_search_passages WHERE context_id=$c AND generation_id=$g AND is_current=TRUE ORDER BY distance USE VECTOR EXACT LIMIT 10"
        }
        _ => usage(),
    };
    let reader = if which == "narrowed_find" {
        &narrowed
    } else {
        &db
    };
    for _ in 0..count {
        std::hint::black_box(reader.execute(sql, &values)?);
    }
    Ok(())
}

/// `count` one-row commits of a vector of 768 numbers, each timed whole; then as many writes of
/// 4 KiB to a file in the same folder, so on the same drive, each followed by the drive's own
/// sync, which alone is timed.
fn commits(count: usize, folder: &Path) -> Fallible<()> {
    let db = Database::open(folder.join("store"))?;
    db.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(768))",
        &params! {},
    )?;
    let mut commits = Vec::with_capacity(count);
    for id in 0..count as i64 {
        let values = params! { "id" => id, "embedding" => vector_of(id as u64, DIMENSIONS) };
        let sql = "INSERT INTO items (id, embedding) VALUES ($id, $embedding)";
        let (done, time) = timed(|| db.execute(sql, &values));
        done?;
        commits.push(time);
    }
    db.close()?;
    let file = File::create(folder.join("sync"))?;
    let mut syncs = Vec::with_capacity(count);
    for at in 0..count as u64 {
        std::os::unix::fs::FileExt::write_all_at(&file, &[at as u8; 4096], at * 4096)?;
        let (done, time) = timed(|| file.sync_data());
        done?;
        syncs.push(time);
    }
    for (name, times) in [("commit", &commits), ("drive_sync", &syncs)] {
        for (at, share) in [("p50", 0.5), ("p95", 0.95), ("max", 1.0)] {
            let value = milliseconds(percentile(times, share));
            line(&format!("commits_{count}_{name}_{at}"), value, "ms");
        }
    }
    Ok(())
}

/// Drops the operating system's cached pages of every file in a store's folder
/// (`posix_fadvise` with `POSIX_FADV_DONTNEED`, which needs no privilege), so that the next open
/// reads from the drive.
fn evict(store: &Path) -> Fallible<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = store;
        return Err("unsupported: cache eviction requires Linux posix_fadvise".into());
    }
    #[cfg(target_os = "linux")]
    {
        unsafe extern "C" {
            fn posix_fadvise(descriptor: i32, offset: i64, length: i64, advice: i32) -> i32;
        }
        for entry in std::fs::read_dir(store)? {
            let path = entry?.path();
            if path.is_file() {
                let file = File::open(&path)?;
                file.sync_all()?;
                let descriptor = std::os::fd::AsRawFd::as_raw_fd(&file);
                // SAFETY: the descriptor stays open for the call; 4 is POSIX_FADV_DONTNEED on Linux.
                let failed = unsafe { posix_fadvise(descriptor, 0, 0, 4) };
                if failed != 0 {
                    return Err(format!("posix_fadvise answered {failed}").into());
                }
            }
        }
        Ok(())
    }
}

/// Opens the store from cold pages and reads one row by its key: the time of both together.
fn cold_open(store: &Path) -> Fallible<std::time::Duration> {
    evict(store)?;
    let (opened, time) = timed(|| -> Fallible<Database> {
        let db = Database::open(store)?;
        db.execute("SELECT id FROM items WHERE id = 0", &params! {})?;
        Ok(db)
    });
    opened?.close()?;
    Ok(time)
}

/// A store of vectors filled, 500 to a commit, until its folder holds `size` bytes; then the
/// open from cold pages after a clean close, and again after the process that held the store
/// had committed one row and was killed.
fn opens(size: u64, folder: &Path) -> Fallible<()> {
    let store = folder.join("store");
    let db = Database::open(&store)?;
    let table = "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding VECTOR(768))";
    db.execute(table, &params! {})?;
    let mut id = 0_i64;
    while bytes_on_disk(&store)? < size {
        let tx = db.transaction()?;
        for _ in 0..500 {
            let values = params! { "id" => id, "embedding" => vector_of(id as u64, DIMENSIONS) };
            tx.execute("INSERT INTO items VALUES ($id, $embedding)", &values)?;
            id += 1;
        }
        tx.commit()?;
    }
    db.close()?;
    let name = |what: &str| format!("opens_{size}_{what}");
    line(
        &name("store_size"),
        mebibytes(bytes_on_disk(&store)?),
        "MiB",
    );
    line(&name("rows"), id, "rows");
    line(&name("clean_open"), milliseconds(cold_open(&store)?), "ms");
    let mut holder = std::process::Command::new(std::env::current_exe()?)
        .args([OsStr::new("held"), store.as_os_str()])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()?;
    let mut said = String::new();
    BufReader::new(holder.stdout.take().ok_or("no output")?).read_line(&mut said)?;
    holder.kill()?;
    holder.wait()?;
    if said.trim() != "ready" {
        return Err("the holding process did not commit its row".into());
    }
    line(&name("killed_open"), milliseconds(cold_open(&store)?), "ms");
    Ok(())
}

/// What the program starts itself as for `opens`: holds the store, commits one row, says so,
/// and waits on its input until it is killed.
fn held(store: &Path) -> Fallible<()> {
    let db = Database::open(store)?;
    let values = params! { "embedding" => vector_of(0, DIMENSIONS) };
    db.execute("INSERT INTO items VALUES (-1, $embedding)", &values)?;
    println!("ready");
    std::io::stdin().read_line(&mut String::new())?;
    Ok(())
}

/// Processor time in ticks of 1/100 s (Linux's `USER_HZ`): the whole process's, its ended
/// threads included, and its first thread's.
fn ticks() -> Fallible<(u64, u64)> {
    let read = |path: &str| -> Fallible<u64> {
        let stat = std::fs::read_to_string(path)?;
        let after = stat.rsplit_once(')').ok_or("no stat")?.1;
        let fields: Vec<&str> = after.split_whitespace().collect();
        Ok(fields[11].parse::<u64>()? + fields[12].parse::<u64>()?)
    };
    let first = format!("/proc/self/task/{}/stat", std::process::id());
    Ok((read("/proc/self/stat")?, read(&first)?))
}

/// A camera store's day: 20 cameras writing `rate` sightings a second in all, one commit a
/// second, for `seconds`. The table keeps a row for a turnover, an eighth of the run, so that
/// the run turns the table over eight times; its vectors are partitioned by camera and by
/// period, `period` seconds long (a turnover when not given). Before the paced writes it is
/// filled at full speed to a turnover's rows, their due times spread over the first turnover as
/// if written at the rate before it, and its maintenance drained: the table's first fill.
/// Prints, at the first fill and at the end of each turnover, the store's size, the memory held
/// and one fixed find's work; at the end the statements refused, how far the writer fell
/// behind its second, and the share of a core the threads other than the writer's were busy.
fn camera(rate: usize, seconds: u64, period: Option<u64>, folder: &Path) -> Fallible<()> {
    use std::sync::mpsc::channel;
    use std::time::Duration;
    let turnover = (seconds / 8).max(1);
    let period = 1000 * period.unwrap_or(turnover).max(1) as i64;
    let live = rate * turnover as usize;
    let store = folder.join("store");
    let db = Database::open(&store)?;
    db.execute(&format!("CREATE TABLE sightings (id INTEGER PRIMARY KEY, camera INTEGER NOT NULL, period INTEGER NOT NULL, due TIMESTAMP EXPIRES, embedding VECTOR(768) PARTITION_KEY (camera, period) MAX_PARTITIONS 4096) RETAIN {turnover} SECONDS"), &params! {})?;
    let now = || -> Fallible<i64> {
        match db.execute("SELECT NOW() AS now", &params! {})?.rows()[0].first() {
            Some(contextdb::Value::Timestamp(at)) => Ok(*at),
            _ => Err("NOW() is not a timestamp".into()),
        }
    };
    let start = now()?;
    let sightings = Sightings::new(live);
    let mut vectors = sightings.rows(usize::MAX);
    let mut row = 0_usize;
    // Writes `count` rows in one commit, as if `at` milliseconds after the start; a row of the
    // first fill names its due time.
    let mut write = |count: usize, at: i64, due: bool| -> Fallible<()> {
        let tx = db.transaction()?;
        for _ in 0..count {
            let at = at + (row % rate * 1000 / rate) as i64;
            let due = due.then(|| contextdb::Value::Timestamp(start + at + 1000 * turnover as i64));
            let values = params! { "id" => row as i64, "camera" => (row / 10 % 20) as i64, "period" => at.div_euclid(period), "due" => due.unwrap_or(contextdb::Value::Null), "embedding" => vectors.next().ok_or("no sighting")? };
            tx.execute(
                "INSERT INTO sightings VALUES ($id, $camera, $period, $due, $embedding)",
                &values,
            )?;
            row += 1;
        }
        Ok(tx.commit()?)
    };
    for second in 0..turnover as i64 {
        write(rate, 1000 * (second - turnover as i64), true)?;
    }
    support::maintenance(&db)?;
    let name = |what: &str| format!("camera_{rate}_{seconds}_{what}");
    line(&name("turnover"), turnover, "s");
    line(&name("period"), period / 1000, "s");
    let question = sightings.questions().swap_remove(0);
    let find = "SELECT id, embedding <=> $q AS distance FROM sightings ORDER BY distance LIMIT 10";
    let figures = |mark: u64| -> Fallible<[u64; 3]> {
        let label = |what: &str| name(&format!("turnover_{mark}_{what}"));
        let rows = db.execute("SELECT COUNT(*) AS n FROM sightings", &params! {})?;
        let rows = match rows.rows()[0].first() {
            Some(contextdb::Value::Int64(n)) => *n as u64,
            _ => 0,
        };
        let size = bytes_on_disk(&store)?;
        let (found, time) = timed(|| db.execute(find, &params! { "q" => question.clone() }));
        let found = found?;
        let used = db.memory_used()? as u64;
        line(&label("live_rows"), rows, "rows");
        line(&label("store_size"), mebibytes(size), "MiB");
        let held = rows * (8 * 4 + 4 * DIMENSIONS as u64);
        line(&label("size_over_held"), ratio(size, held.max(1)), "x");
        line(&label("memory_used"), mebibytes(used), "MiB");
        let resident = support::status_bytes("VmRSS:").unwrap_or(0);
        line(&label("resident"), mebibytes(resident), "MiB");
        let evaluations = found.distance_evaluations();
        line(
            &label("find_distance_evaluations"),
            evaluations,
            "evaluations",
        );
        line(&label("find_rows_examined"), found.rows_examined(), "rows");
        line(&label("find"), milliseconds(time), "ms");
        let c = db.last_maintenance_cycle()?.unwrap_or_default();
        let show = |what: &str, x: Option<f64>| line(&label(what), x.unwrap_or(f64::NAN), "");
        show("compaction_free_ratio", c.compaction_free_ratio);
        show("page_slack_ratio", db.page_slack_ratio());
        show("cycle_compacted_pages", c.compacted_pages.map(|p| p as f64));
        Ok([size, used, evaluations])
    };
    let first = figures(0)?;
    let (mut last, mut base, mut refused, mut lag) = ([0; 3], first, 0_u64, Duration::ZERO);
    let (_keep, idle) = channel::<()>();
    let (from, before) = (now()? - start, ticks()?);
    let mut elapsed = Duration::ZERO;
    for second in 0..seconds {
        let (written, spent) = timed(|| write(rate, from + 1000 * second as i64, false));
        elapsed += spent;
        if let Err(error) = written {
            match error.downcast_ref::<contextdb::Error>() {
                Some(refusal) if refused == 0 => eprintln!("refused: {refusal}"),
                Some(_) => {}
                None => return Err(error),
            }
            refused += 1;
        }
        if (second + 1) % turnover == 0 {
            let (taken, spent) = timed(|| figures((second + 1) / turnover));
            elapsed += spent;
            let mark = taken?;
            base = if second + 1 == turnover { mark } else { base };
            if second + 1 > turnover {
                last = std::array::from_fn(|i| last[i].max(mark[i]));
            }
        }
        let target = Duration::from_secs(second + 1);
        lag = lag.max(elapsed.saturating_sub(target));
        let (_, waited) = timed(|| idle.recv_timeout(target.saturating_sub(elapsed)));
        elapsed += waited;
    }
    let after = ticks()?;
    let others = (after.0 - before.0).saturating_sub(after.1 - before.1);
    line(&name("refused"), refused, "statements");
    line(&name("wall"), support::seconds(elapsed), "s");
    line(&name("writer_lag_max"), milliseconds(lag), "ms");
    let busy = others as f64 / 100.0 / elapsed.as_secs_f64();
    line(&name("worker_busy_host"), format!("{busy:.3}"), "cores");
    let khz = std::env::var("BOX_HOST_KHZ").unwrap_or_default();
    let at = busy * khz.parse().unwrap_or(f64::NAN) / 2_300_000.0;
    line(&name("worker_busy_2300mhz"), format!("{at:.3}"), "cores");
    for (at, what) in ["store_size", "memory_used", "find_distance_evaluations"]
        .iter()
        .enumerate()
    {
        line(
            &name(&format!("{what}_last_over_first")),
            ratio(last[at], base[at].max(1)),
            "x",
        );
    }
    Ok(db.close()?)
}

/// Makes the folder and answers it.
fn create(folder: &Path) -> Fallible<&Path> {
    std::fs::create_dir_all(folder)?;
    Ok(folder)
}

fn usage() -> ! {
    eprintln!(
        "usage: measure readme|million|vectors|index <folder>\n       \
         measure sessions <corpus folder> <list file> <count> <folder>\n       \
         measure synthetic|sightings <rows> <folder> | sightings <rows> --contexts <n> <folder>\n       \
         measure statements build <folder> | <which> <folder> <n>\n       \
         measure commits <count> <folder> | opens <size> <folder> | bound <rows> <store>\n       \
         measure camera <rows a second> <seconds> [period seconds] <folder>"
    );
    std::process::exit(2);
}

fn main() -> Fallible<()> {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    let arguments: Vec<&str> = arguments.iter().map(String::as_str).collect();
    if let ["first", store, prefix, question] = arguments.as_slice() {
        return first(Path::new(store), prefix, Path::new(question));
    }
    if let ["held", store] = arguments.as_slice() {
        return held(Path::new(store));
    }
    match arguments.as_slice() {
        ["statements", "build", folder] => return statement_store(Path::new(folder)),
        ["statements", which, folder, count] => {
            return statements(which, Path::new(folder), count.parse()?);
        }
        ["bound", rows, store] => return bound(rows.parse()?, Path::new(store)),
        _ => {}
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
        ["sightings", rows, "--contexts", contexts, _] => {
            contexts_case(rows.parse()?, contexts.parse()?, create(folder)?)?;
        }
        [which @ ("synthetic" | "sightings"), rows, _] => match (*which, rows.parse()) {
            ("synthetic", Ok(rows)) => synthetic_case(rows, folder)?,
            (_, Ok(rows)) => sightings_case(rows, folder)?,
            _ => usage(),
        },
        ["commits", count, _] => commits(count.parse()?, create(folder)?)?,
        ["opens", size, _] => opens(contextdb::parse_size(size)? as u64, create(folder)?)?,
        ["camera", rate, seconds, _] => {
            camera(rate.parse()?, seconds.parse()?, None, create(folder)?)?
        }
        ["camera", rate, seconds, period, _] => {
            let period = Some(period.parse()?);
            camera(rate.parse()?, seconds.parse()?, period, create(folder)?)?;
        }
        _ => usage(),
    }
    Ok(())
}
