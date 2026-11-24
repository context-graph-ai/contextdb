//! Find at scale, in a release build: generated sightings in one partition, grouped by
//! maintenance, then the generator's 100 questions searched INDEXED and EXACT. The work, the
//! recall and the data file's length are recorded from the first run; the engine draws no
//! random number and starts no thread, so they are the same on every run. A different count is
//! a change of the index's design: the pull request that makes it records the new number.

#[path = "support/sightings.rs"]
mod sightings;
mod support;

use sightings::{DIMENSIONS, Sightings};

/// Loads `rows` sightings, a thousand to a commit, runs maintenance to rest and asks the 100
/// questions, the INDEXED answers together holding at least 95 of every 100 of EXACT's rows
/// across the run, not each answer alone (bar 3, design 8.1; root intent 702-703).
/// With `pins`, asserts the distance evaluations of the 100 INDEXED searches, how many of EXACT's
/// 1,000 rows they return (at least the recorded number), and the data file's length once the
/// store is closed. Returns the distance evaluations.
fn found_at_scale(rows: usize, pins: Option<[u64; 3]>) -> u64 {
    let sightings = Sightings::new(rows);
    let folder = support::folder(&format!("find-at-scale-{rows}"));
    let db = support::open(&folder);
    support::declare_docs(&db, DIMENSIONS, "");
    let mut left = sightings.rows(rows).zip(1_i64..).peekable();
    while left.peek().is_some() {
        let tx = db.transaction().expect("begin");
        for (vector, n) in left.by_ref().take(1000) {
            support::insert_doc(&tx, n, "a", Some(vector)).expect("insert a sighting");
        }
        tx.commit().expect("commit a thousand sightings");
    }
    support::maintenance(&db);
    let what = format!("{rows} sightings");
    let (mut evaluations, mut held, mut total) = (0, 0, 0);
    for question in sightings.questions() {
        let [exact, indexed] = ["EXACT", "INDEXED"]
            .map(|mode| support::answer(&db, &support::by_mode(None, mode, 10), &question));
        evaluations += indexed.distance_evaluations();
        let ours: Vec<_> = indexed.rows().iter().map(|row| &row[0]).collect();
        let wanted = exact.rows();
        held += wanted.iter().filter(|row| ours.contains(&&row[0])).count() as u64;
        total += wanted.len() as u64;
    }
    let bar = "found of EXACT's rows; bar 3 wants 95 in 100 across all the questions";
    assert!(held * 100 >= 95 * total, "{what}, {held}/{total} {bar}");
    support::close(db);
    if let Some([work, whole, length]) = pins {
        let file = support::data_file(&folder);
        let counted = "distance evaluations of the 100 INDEXED searches, the data file's length";
        assert_eq!((evaluations, file), (work, length), "{what}: {counted}");
        assert!(held >= whole, "{what}: {held} of EXACT's 1,000 rows found");
    }
    evaluations
}

#[test]
fn a_hundred_thousand_sightings_are_found_with_the_recorded_work_recall_and_length() {
    // Ordered allocation writes two fewer pages. Lowest-address recovery bookkeeping lets small stores shrink after deletes. This file has 16 more free, reusable interior pages, with identical live user and system page counts.
    // LEAN-B82 (edf6294fc) keeps replaced system-tree pages a commit longer: +8 pages, 433,819,648 at f00519315.
    let work = found_at_scale(100_000, Some([916_228, 999, 440_209_408]));
    // Bar 4: four times the rows costs at most 2.2 times the work, the square-root law the
    // 5 million figure rests on (design 8.1).
    let quarter = found_at_scale(25_000, None);
    println!("bar 4: {quarter} distance evaluations at 25,000 sightings, {work} at 100,000");
    assert!(
        work * 10 <= quarter * 22,
        "{quarter} distance evaluations at 25,000 sightings, {work} at 100,000"
    );
}

#[test]
fn a_million_sightings_are_found_with_the_recorded_work_recall_and_length() {
    // The length was measured at e1e2fcc37, the same in two runs: 1.41 times the data (bar 1).
    found_at_scale(1_000_000, Some([7_421_025, 1_000, 4_329_050_112]));
}

#[test]
fn the_questions_asked_are_a_hundred_or_more() {
    let questions = Sightings::new(1_000).questions().len();
    assert!(
        questions >= 100,
        "recall is a count of 100 held-out questions: {questions}"
    );
}
