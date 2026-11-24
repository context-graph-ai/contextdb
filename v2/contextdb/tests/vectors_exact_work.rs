//! An exact search with a row condition over a narrowed handle: where the table's vectors are at
//! least 16 times the key set's rows it walks those rows and compares them; under that it screens
//! every slot by its one-byte copy and measures only the key set's slots (sheet section 10).

mod support;
use contextdb::Value;

const DIMENSION: usize = 16;
const EXACT: &str = "USE VECTOR EXACT LIMIT 10";

#[test]
fn an_exact_search_compares_the_vectors_of_the_rows_its_condition_reaches() {
    for (rows, c) in [
        (2000_i64, 200_i64),
        (2000, 2000),
        (20_000, 200),
        (20_000, 1000),
        (20_000, 2000),
    ] {
        let db = support::open(&support::folder(&format!("exact-work-{rows}-{c}")));
        support::declare_docs(&db, DIMENSION, "");
        support::run(&db, "CREATE INDEX docs_ctx ON docs (ctx)");
        for first in (0..rows / c).map(|j| j * c) {
            support::load_docs(
                &db,
                first + 1..=first + c,
                &format!("c{}", first / c),
                |n| support::gathered(n, DIMENSION),
            );
        }
        let asked = rows / c / 2;
        let ctx = format!("c{asked}");
        let query = support::gathered(asked * c + 7, DIMENSION);
        let sql = format!(
            "SELECT id, v <=> $q AS distance FROM docs WHERE ctx = '{ctx}' ORDER BY distance USE VECTOR EXACT LIMIT 10"
        );
        let wanted = support::stored(&db, "SELECT id, ctx, tag, v FROM docs").nearest(
            |row| row[1] == support::text(&ctx),
            &query,
            10,
        );
        for (when, sealed) in [("loose", false), ("sealed", true)] {
            if sealed {
                support::seal_all(&db);
            }
            let found = support::answer(&db, &sql, &query);
            support::assert_same_rows(&support::found(&found, when), &wanted, when);
            // The designer's rule (sheet section 10). Where the table's vectors N are at least 16
            // times the key set's m rows, the search walks those rows: m compared, and m rows plus
            // the 10 answer rows examined. Under 16m it takes the key-set route: it gathers the
            // key set's c entries (each charged), screens every sealed vector by its one-byte copy
            // (one compared each), and measures exactly only the key set's rows: e exact distances,
            // each of a row read for it. Compared = screens + e, rows examined = c + e + LIMIT, and
            // e is at most m, because every exact distance passes the key-set check first.
            let (n, m) = (rows as u64, c as u64);
            let compared = found.distance_evaluations();
            let examined = found.rows_examined();
            if n >= 16 * m {
                assert_eq!(
                    compared, m,
                    "table {rows}, Context {c}, {when}: N is at least 16m, so the search walks the key set's rows and compares m"
                );
                assert_eq!(
                    examined,
                    m + 10,
                    "table {rows}, Context {c}, {when}: a walk reads the m rows and the 10 answer rows"
                );
            } else {
                assert!(
                    examined >= m + 10,
                    "table {rows}, Context {c}, {when}: the key-set route gathers its {m} entries and the 10 answer rows are read: {examined} examined"
                );
                let exact = examined - m - 10;
                assert!(
                    exact <= m,
                    "table {rows}, Context {c}, {when}: {exact} exact distances, of a key set of {m}: a vector outside the set was measured"
                );
                assert!(
                    compared <= n + m,
                    "table {rows}, Context {c}, {when}: {compared} compared, over one screen for each of the {n} vectors and one exact distance for each of the {m} in the set"
                );
                // The vectors sealed are screened, and the pending ones are not: all or none where
                // the test knows which. At 20,000 rows 14,166 are sealed by the load itself, a
                // count the engine's block size decides, so that one case asserts the bounds only.
                let screens = if sealed {
                    Some(n)
                } else if rows <= 2000 {
                    Some(0)
                } else {
                    None
                };
                if let Some(screens) = screens {
                    assert_eq!(
                        compared,
                        screens + exact,
                        "table {rows}, Context {c}, {when}: compared is the screens plus the exact distances"
                    );
                }
            }
        }
    }
}

/// Through the built index, 95 in 100 kept at under 2,000 compared; a key list of 50 reads 60.
#[test]
fn a_search_through_the_index_and_over_a_key_list_reads_what_each_was_built_for() {
    let (_, db) = support::built("exact-index", DIMENSION, "INDEXING_THRESHOLD 1000", 2000);
    let every = support::stored(&db, "SELECT id, ctx, tag, v FROM docs");
    let sql =
        "SELECT id, v <=> $q AS distance FROM docs WHERE v IS NOT NULL ORDER BY distance LIMIT 10";
    let mut kept = 0;
    for i in 0..100 {
        let query = support::gathered(i * 13 + 1, DIMENSION);
        let found = support::answer(&db, sql, &query);
        let exact = every.nearest(|_| true, &query, 10);
        let got = support::found(&found, sql);
        assert!(found.distance_evaluations() < 2000, "question {i}");
        for e in &exact {
            kept += usize::from(got.iter().any(|g| g.key == e.key));
        }
    }
    assert!(kept >= 950, "kept {kept} of 1000");
    let ids: Vec<i64> = (1..=50).map(|i| i * 40).collect();
    let list = format!("{ids:?}").replace(['[', ']'], "");
    let query = support::gathered(7, DIMENSION);
    let named = |r: &[Value]| ids.iter().any(|i| r[0] == Value::Int64(*i));
    let wanted = every.nearest(named, &query, 10);
    let by = format!("WHERE id IN ({list}) ORDER BY distance {EXACT}");
    for (vector, extra) in [("$q", 0), ("(SELECT v FROM docs WHERE id = 7)", 1)] {
        let sql = format!("SELECT id, v <=> {vector} AS distance FROM docs {by}");
        let before = db.work_done().unwrap().rows_read;
        let found = support::answer(&db, &sql, &query);
        let read = db.work_done().unwrap().rows_read - before;
        support::assert_same_rows(&support::found(&found, &sql), &wanted, &sql);
        let got = (found.distance_evaluations(), found.rows_examined(), read);
        assert_eq!(got, (50, 60 + extra, 60 + extra), "{sql}");
    }
}
