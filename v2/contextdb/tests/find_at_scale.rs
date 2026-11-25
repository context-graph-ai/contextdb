//! Find at scale, in a release build: generated sightings in one partition, grouped by
//! maintenance, then the generator's 100 questions searched INDEXED and EXACT. The engine draws
//! no random number and starts no thread, so every count is the same on every run. The data
//! file's length, the distance evaluations and the share of EXACT's rows found are recorded; a
//! different count is a change of the design, recorded by the pull request that makes it. What a
//! unit of work counts is the owner's to say (design section 6, F4): until then a unit is a row
//! read or a distance evaluation, as the page says.

#[path = "support/sightings.rs"]
mod sightings;
mod support;

use contextdb::{Database, QueryResult};
use sightings::{DIMENSIONS, Sightings};

fn units(found: &QueryResult) -> u64 {
    found.rows_examined() + found.distance_evaluations()
}

/// A store of `rows` sightings, a thousand to a commit, with maintenance run to rest, which
/// seals them with no row read back: each pending record carries its slot (F1, bar 6).
fn loaded(rows: usize) -> (std::path::PathBuf, Database, Sightings) {
    let sightings = Sightings::new(rows);
    let folder = support::folder(&format!("find-at-scale-{rows}"));
    let db = support::open(&folder);
    // Not about the interval: MIN_INTERVAL 0 lets the space freed during the build be given back
    // within this short run, so the length pinned is the store's length at rest (design 4, S4).
    support::run(&db, "SET STORAGE_COMPACTION MIN_INTERVAL 0 SECONDS");
    support::declare_docs(&db, DIMENSIONS, "");
    {
        let mut left = sightings.rows(rows).zip(1_i64..).peekable();
        while left.peek().is_some() {
            let tx = db.transaction().expect("begin");
            for (vector, n) in left.by_ref().take(1000) {
                support::insert_doc(&tx, n, "a", Some(vector)).expect("insert a sighting");
            }
            tx.commit().expect("commit a thousand sightings");
        }
    }
    let before = db.work_done().unwrap().rows_read;
    support::maintenance(&db);
    let read = db.work_done().unwrap().rows_read - before;
    assert_eq!(read, 0, "sealing {rows} sightings read {read} rows back");
    (folder, db, sightings)
}

/// Asks the 100 questions INDEXED and EXACT: the INDEXED answers together hold at least 95 of
/// every 100 of EXACT's rows across the run, not each answer alone (bar 3, design 8.1; root
/// intent 702-703), at least `whole` of them, and do less work than EXACT's. Their distance
/// evaluations are the recorded `work`, and the units of the 95th cheapest question the recorded
/// `slow`: bar 4 is a time for 95 questions of 100, and a sum can hold while the slow ones grow.
fn asked(db: &Database, sightings: &Sightings, [work_pin, slow_pin, whole]: [u64; 3]) {
    let (mut evaluations, mut work, mut exhaustive, mut held, mut total) = (0, 0, 0, 0, 0);
    let mut each = Vec::new();
    for question in sightings.questions() {
        let [exact, indexed] = ["EXACT", "INDEXED"]
            .map(|mode| support::answer(db, &support::by_mode(None, mode, 10), &question));
        evaluations += indexed.distance_evaluations();
        (work, exhaustive) = (work + units(&indexed), exhaustive + units(&exact));
        each.push(units(&indexed));
        let ours: Vec<_> = indexed.rows().iter().map(|row| &row[0]).collect();
        let wanted = exact.rows();
        held += wanted.iter().filter(|row| ours.contains(&&row[0])).count() as u64;
        total += wanted.len() as u64;
    }
    let bar = "found of EXACT's rows; bar 3 wants 95 in 100 across all the questions";
    assert!(
        held * 100 >= 95 * total && held >= whole,
        "{held}/{total} {bar}, at least {whole}"
    );
    assert!(
        work < exhaustive,
        "INDEXED's units {work}, EXACT's {exhaustive}"
    );
    println!("each question's units: {each:?}");
    each.sort_unstable();
    let counted = "distance evaluations of the 100 INDEXED searches, the 95th cheapest's units";
    assert_eq!((evaluations, each[94]), (work_pin, slow_pin), "{counted}");
}

/// At 100,000: a walk with no filter reads at most `R` = 3 rows for each
/// row asked for (F3); and the narrowed grid at 0.5, 2, 12 and 50% of the rows, by key, is answered by
/// AUTO in no more units than EXACT, falling as the filter passes more past the crossing, while
/// EXACT compares each vector at most once besides the rows it read and holds the same memory
/// at 12 and 50% (bar 5).
#[test]
fn a_hundred_thousand_sightings_are_found_with_the_recorded_work_recall_and_length() {
    // Ordered allocation writes two fewer pages. Lowest-address recovery bookkeeping lets small stores shrink after deletes. This file has 16 more free, reusable interior pages, with identical live user and system page counts.
    // LEAN-B82 (edf6294fc) keeps replaced system-tree pages a commit longer: +8 pages, 433,819,648 at f00519315.
    // Space (design 4) gives back what the build freed once MIN_INTERVAL 0 lets a pass start: 216 fewer pages, the same in two runs.
    // Leaves joining only into one page (5ee065d11) leave one 64-page block above; pages in use are unchanged.
    // No centroid record kept past a shrink (e8162ab0f): 8 fewer pages, 439,291,904, the same in three runs.
    // The Find lane's slot screening and the reach (7364fc08b) give 971,425 and 38,544, the same in three runs.
    // Asking at most k kept slots after each group (e9815ada5) gives 970,918 and 38,500, the same in two runs.
    let (folder, db, sightings) = loaded(100_000);
    asked(&db, &sightings, [970_918, 38_500, 999]);
    let questions = sightings.questions();
    for k in [10, 100] {
        let walked = support::answer(&db, &support::by_mode(None, "INDEXED", k), &questions[0]);
        let read = walked.rows_examined();
        assert!(read <= 3 * k as u64, "{read} rows read for {k} asked for");
    }
    for question in &questions[..3] {
        let mut cells = Vec::new();
        for share in [500, 2_000, 12_000, 50_000] {
            let filter = format!("id <= {share}");
            let [auto, exact] = ["AUTO", "EXACT"].map(|mode| {
                support::answer(&db, &support::by_mode(Some(&filter), mode, 10), question)
            });
            assert!(
                units(&auto) <= units(&exact),
                "{filter}: AUTO {auto:?}, EXACT {exact:?}"
            );
            cells.push((
                units(&auto),
                exact.distance_evaluations(),
                exact.rows_examined(),
                exact.peak_bytes(),
            ));
        }
        // id <= 2000 is below the crossover and served directly at EXACT's cost (4,010), so it can be cheaper than the 12000 walk (4,135).
        assert!(cells[2].0 >= cells[3].0, "AUTO's units falling {cells:?}");
        let once = |(_, compared, read, _): (u64, u64, u64, _)| compared <= 100_000 + read;
        assert!(
            once(cells[2]) && once(cells[3]),
            "EXACT compares each vector once {cells:?}"
        );
        assert_eq!(
            cells[2].3, cells[3].3,
            "EXACT's memory at 12 and 50% {cells:?}"
        );
    }
    support::close(db);
    assert_eq!(
        support::data_file(&folder),
        439_291_904,
        "the data file's length"
    );
}

#[test]
fn a_million_sightings_are_found_with_the_recorded_work_recall_and_length() {
    let (folder, db, sightings) = loaded(1_000_000);
    // The reach (7364fc08b) gives 6,883,657 and 348,651, the same in two runs; at the Find merge 7,556,029 and 368,157.
    // Asking at most k kept slots after each group (e9815ada5) gives 6,883,270 and 348,596, the same in two runs.
    asked(&db, &sightings, [6_883_270, 348_596, 1_000]);
    support::close(db);
    // The length was measured at e1e2fcc37, the same in two runs: 1.41 times the data (bar 1).
    // +1.1% at a7c3cd8ae is the slot reverse index PURGE reads (PR 5B); work and recall unchanged.
    assert_eq!(
        support::data_file(&folder),
        4_378_333_184,
        "the data file's length"
    );
}

#[test]
fn the_questions_asked_are_a_hundred_or_more() {
    let questions = Sightings::new(1_000).questions().len();
    assert!(
        questions >= 100,
        "recall is a count of 100 held-out questions: {questions}"
    );
}
