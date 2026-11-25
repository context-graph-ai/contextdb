//! Find with a bound on the distance, on generated sightings: a stored face is found within its
//! bound, a never-stored one gives no row, bare and with two row conditions, plain and under
//! USE VECTOR INDEXED, with the vector index built.

#[path = "support/sightings.rs"]
mod sightings;
mod support;

use sightings::{DIMENSIONS, Sightings};

fn distance_of(row: &[contextdb::Value]) -> f64 {
    match row.last() {
        Some(contextdb::Value::Float64(d)) => *d,
        other => panic!("a distance was wanted: {other:?}"),
    }
}

/// The rows nearer than `bound`, in their order.
fn nearer_than(rows: &[Vec<contextdb::Value>], bound: f64) -> Vec<&Vec<contextdb::Value>> {
    rows.iter().filter(|r| distance_of(r) < bound).collect()
}

/// Sightings of one connector, the vector index built, and an index on `tag` so that a filter
/// on it is countable.
fn stored_sightings(name: &str) -> (Sightings, contextdb::Database) {
    const ROWS: usize = 4000;
    let sightings = Sightings::new(ROWS);
    let db = support::open(&support::folder(name));
    support::declare_docs(&db, DIMENSIONS, "INDEXING_THRESHOLD 1000");
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    {
        let mut left = sightings.rows(ROWS).zip(1_i64..).peekable();
        while left.peek().is_some() {
            let tx = db.transaction().expect("begin");
            for (vector, n) in left.by_ref().take(1000) {
                support::insert_doc(&tx, n, "a", Some(vector)).expect("insert a sighting");
            }
            tx.commit().expect("commit a thousand sightings");
        }
    }
    support::maintenance(&db);
    (sightings, db)
}

/// The promise of a distance bound (sheet section 5, point 4): every row answered is inside the
/// bound at its exact distance, nearest first; EXACT is the exact scan cut at the bound; and
/// across the questions the AUTO and INDEXED answers hold at least 95 in 100 of the exact scan's
/// rows inside the bound (contextdb-intent.md:863-865), the share taken over all questions
/// together, bare and with row conditions apart.
#[test]
fn a_bound_finds_what_is_stored_and_nothing_for_a_face_never_stored() {
    let (sightings, db) = stored_sightings("bound-find");
    // Rows hit and rows of the exact scan inside the bound, for the bare form and the form with
    // two row conditions.
    let mut share = [(0_usize, 0_usize); 2];
    for (j, question) in sightings.questions().iter().enumerate() {
        let stored = j % 5 != 4;
        let top = support::answer(&db, &support::by_mode(None, "EXACT", 8), question);
        let nearest = distance_of(&top.rows()[0]);
        let eighth = distance_of(&top.rows()[7]);
        // A bound just above the eighth distance keeps the exact top eight; one below half the
        // nearest distance keeps no row.
        let (wide, narrow) = (eighth * (1.0 + 1e-6) + 1e-9, nearest * 0.5);
        for (bound, expected) in [(wide, true), (narrow, false)] {
            for (form, extra) in [(0, ""), (1, " AND tag >= 0 AND id > 0")] {
                // Below half the nearest distance no row passes: the bare form shows it.
                if !expected && form == 1 {
                    continue;
                }
                for mode in ["", "INDEXED", "EXACT"] {
                    let sql = format!(
                        "SELECT id, v <=> $q AS distance FROM docs WHERE v <=> $q < {bound:?}{extra} ORDER BY distance {}LIMIT 8",
                        if mode.is_empty() {
                            String::new()
                        } else {
                            format!("USE VECTOR {mode} ")
                        }
                    );
                    let got = support::search(&db, &sql, question)
                        .unwrap_or_else(|e| panic!("question {j}, {mode:?}{extra}: {e:?}"));
                    // With nothing inside it, the answer is empty, and under AUTO and INDEXED
                    // the bound stands for the last row and ends the walk short of the last
                    // group (section 6, the bound on the distance).
                    if !expected {
                        assert!(
                            got.rows().is_empty(),
                            "question {j} {mode:?}: nothing inside"
                        );
                        let units = got.rows_examined() + got.distance_evaluations();
                        assert!(
                            mode == "EXACT" || units < 4000,
                            "question {j} {mode:?}: {units} units under the bound, of 4,000 vectors"
                        );
                    }
                    let within = nearer_than(top.rows(), bound);
                    let mut last = f64::MIN;
                    for row in got.rows() {
                        assert!(
                            within.contains(&row),
                            "question {j} {mode:?}{extra}: every row answered is inside the bound, at its exact distance"
                        );
                        assert!(
                            distance_of(row) >= last,
                            "question {j} {mode:?}{extra}: nearest first"
                        );
                        last = distance_of(row);
                    }
                    if mode == "EXACT" {
                        assert_eq!(
                            got.rows().iter().collect::<Vec<_>>(),
                            within,
                            "question {j}{extra}: EXACT is the exact scan cut at the bound"
                        );
                    } else {
                        share[form].0 += got.rows().len();
                        share[form].1 += within.len();
                    }
                    if expected && stored && mode == "EXACT" {
                        assert!(
                            !got.rows().is_empty(),
                            "question {j} {mode:?}{extra}: a stored face is found within its bound"
                        );
                    }
                }
            }
        }
    }
    for (form, (hit, of)) in ["bare", "with row conditions"].iter().zip(share) {
        eprintln!("bound share, {form}: {hit} of {of} rows of the exact scan inside the bound");
        assert!(
            hit * 100 >= of * 95,
            "{form}: AUTO and INDEXED hold {hit} of the exact scan's {of} rows inside the bound, under 95 in 100"
        );
    }
    // A condition that passes one row in fifty, through its index, turns rows away: the bound
    // is set just above the tenth exact distance among the rows that pass, the root's ten.
    let mut rare = [(0_usize, 0_usize); 2];
    let mut worst = Vec::new();
    for (j, question) in sightings.questions().iter().enumerate() {
        let scan = support::answer(
            &db,
            &support::by_mode(Some("tag = 3"), "EXACT", 10),
            question,
        );
        let bound = distance_of(&scan.rows()[9]) * (1.0 + 1e-6) + 1e-9;
        let within = nearer_than(scan.rows(), bound);
        for (at, mode) in ["", "USE VECTOR INDEXED "].into_iter().enumerate() {
            let sql = format!(
                "SELECT id, v <=> $q AS distance FROM docs WHERE v <=> $q < {bound:?} AND tag = 3 ORDER BY distance {mode}LIMIT 10"
            );
            let got = support::answer(&db, &sql, question);
            assert_bounded(
                got.rows(),
                &within,
                &format!("question {j} {mode:?} tag = 3"),
            );
            rare[at].0 += got.rows().len();
            rare[at].1 += within.len();
            if got.rows().len() < within.len() {
                worst.push((within.len() - got.rows().len(), j, at));
            }
        }
    }
    worst.sort_by(|a, b| b.cmp(a));
    for (name, (hit, of)) in ["AUTO", "INDEXED"].iter().zip(rare) {
        eprintln!("bound share, tag = 3, {name}: {hit} of {of}");
        assert!(
            hit * 100 >= of * 95,
            "tag = 3, {name}: {hit} of the exact scan's {of} rows inside the bound, under 95 in 100; most rows missed (rows, question, mode 0 AUTO 1 INDEXED): {:?}",
            &worst[..worst.len().min(8)]
        );
    }
}

const MODES: [&str; 3] = ["", "USE VECTOR INDEXED ", "USE VECTOR EXACT "];

/// Every row of a bounded answer is one of the exact scan's rows inside the bound, at its exact
/// distance, nearest first, and EXACT gives all of them.
fn assert_bounded(got: &[Vec<contextdb::Value>], within: &[&Vec<contextdb::Value>], what: &str) {
    let mut last = f64::MIN;
    for row in got {
        assert!(
            within.contains(&row),
            "{what}: a row inside the bound at its exact distance"
        );
        assert!(distance_of(row) >= last, "{what}: nearest first");
        last = distance_of(row);
    }
}

/// A search asked for far more rows than its bound lets through keeps fewer rows than it was
/// asked for, so what it holds to stand in for the last row is short (review 10, question 3.1).
/// In each mode its rows are rows of the exact scan inside the bound, at their exact distances;
/// EXACT gives all of them.
fn short_bound_answers_rows_inside_the_bound(sightings: &Sightings, db: &contextdb::Database) {
    let (wanted_rows, asked) = (8, 50);
    for (j, question) in sightings.questions().iter().enumerate().skip(8).take(8) {
        let top = support::answer(db, &support::by_mode(None, "EXACT", wanted_rows), question);
        // The bound just above the eighth distance lets eight rows through; fifty are asked.
        let bound = distance_of(&top.rows()[wanted_rows - 1]) * (1.0 + 1e-6) + 1e-9;
        let within = nearer_than(top.rows(), bound);
        assert!(
            within.len() < asked,
            "question {j}: the bound lets fewer rows through than asked"
        );
        for mode in MODES {
            let sql = format!(
                "SELECT id, v <=> $q AS distance FROM docs WHERE v <=> $q < {bound:?} ORDER BY distance {mode}LIMIT {asked}"
            );
            let got = support::answer(db, &sql, question);
            assert_bounded(got.rows(), &within, &format!("question {j} {mode:?}"));
            if mode.contains("EXACT") {
                assert_eq!(
                    got.rows().len(),
                    within.len(),
                    "question {j}: EXACT gives every row inside the bound"
                );
            }
        }
    }
}

/// Review 13, item 1.2: a pending row the bound cuts is a row the bound cut. One group holds
/// four sealed rows at distance 0.1 and a row still pending at 0.8, all of tag 3; a bound of 0.3
/// under INDEXED answers its four rows, as the exact scan does, and is not refused.
#[test]
fn a_pending_row_beyond_the_bound_does_not_refuse_an_indexed_search() {
    let db = support::open(&support::folder("bound-find-pending"));
    support::declare_docs(&db, 2, "INDEXING_THRESHOLD 1");
    support::run(&db, "CREATE INDEX docs_tag ON docs (tag)");
    let at = |cosine: f32| vec![cosine, (1.0 - cosine * cosine).sqrt()];
    for (id, near) in [(3, 0.9), (53, 0.9), (103, 0.9), (153, 0.9)] {
        support::insert_doc(&db, id, "a", Some(at(near))).expect("a sealed row");
    }
    support::maintenance(&db);
    support::insert_doc(&db, 203, "a", Some(at(0.2))).expect("a pending row");
    let sql = |order: &str, mode: &str| {
        format!(
            "SELECT id, v <=> $q AS distance FROM docs WHERE tag = 3 AND v <=> $q < 0.3 ORDER BY {order} {mode}LIMIT 8"
        )
    };
    let question = [1.0, 0.0];
    let twin = support::answer(&db, &sql("(v <=> $q) + 0", ""), &question);
    assert_eq!(twin.rows().len(), 4, "the four rows within the bound");
    let got = support::answer(&db, &sql("distance", "USE VECTOR INDEXED "), &question);
    assert_eq!(
        got.rows(),
        twin.rows(),
        "the pending row at 0.8 is cut by the bound: INDEXED answers the four rows"
    );
}

/// Review 13, item 1.3: two bounds on one distance are one filter however they are written. A
/// bound that cuts nothing and one that cuts give the same answer in either order, bare and with
/// `tag = 3`, in each mode, and its rows are rows of the exact scan inside the tighter bound.
fn two_bounds_give_one_answer_in_either_order(sightings: &Sightings, db: &contextdb::Database) {
    for (j, question) in sightings.questions().iter().enumerate().skip(8).take(8) {
        let top = support::answer(db, &support::by_mode(None, "EXACT", 8), question);
        let tight = distance_of(&top.rows()[7]) * (1.0 + 1e-6) + 1e-9;
        let within: Vec<_> = top
            .rows()
            .iter()
            .filter(|r| distance_of(r) < tight)
            .collect();
        for extra in ["", " AND tag = 3"] {
            let (loose, written) = ("2.0", format!("{tight:?}"));
            let sql = |bounds: [&str; 2], mode: &str| {
                format!(
                    "SELECT id, v <=> $q AS distance FROM docs WHERE v <=> $q < {} AND v <=> $q < {}{extra} ORDER BY distance {mode}LIMIT 8",
                    bounds[0], bounds[1]
                )
            };
            for mode in MODES {
                let first = support::answer(db, &sql([loose, &written], mode), question);
                let second = support::answer(db, &sql([&written, loose], mode), question);
                let what = format!("question {j}{extra} {mode:?}");
                assert_eq!(
                    first.rows(),
                    second.rows(),
                    "{what}: the order changes the answer"
                );
                assert_bounded(first.rows(), &within, &what);
                if mode.contains("EXACT") && extra.is_empty() {
                    assert_eq!(
                        first.rows().len(),
                        within.len(),
                        "{what}: EXACT gives every row inside the bound"
                    );
                }
            }
        }
    }
}

/// Both rows above, on one store of sightings: loading it is most of their time.
#[test]
fn a_bound_over_sightings_answers_rows_inside_it_however_asked_or_written() {
    let (sightings, db) = stored_sightings("bound-find-rows");
    short_bound_answers_rows_inside_the_bound(&sightings, &db);
    two_bounds_give_one_answer_in_either_order(&sightings, &db);
}
