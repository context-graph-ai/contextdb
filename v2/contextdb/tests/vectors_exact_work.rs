//! An exact search with a row condition: it compares the rows its condition reaches where that
//! costs no more than its walk, and otherwise screens every vector once (design F3).

mod support;
#[path = "support/vecsearch.rs"]
mod vecsearch;
use contextdb::Value;

const DIMENSION: usize = 16;
const EXACT: &str = "USE VECTOR EXACT LIMIT 10";

#[test]
fn an_exact_search_compares_the_vectors_of_the_rows_its_condition_reaches() {
    // Each table: the sizes of its contexts in the order they are loaded, and the contexts
    // asked of it. A table of 2,000 rows is asked a Context of 200 and one of 2,000, both under
    // a sixteenth of it; one of 20,000 rows a Context of 200 and one of 1,000, a sixteenth of it
    // or less, and one of 2,000, more.
    let mixed = [vec![200_i64; 25], vec![1000; 5], vec![2000; 5]].concat();
    for (sizes, asked) in [
        (vec![200_i64; 10], vec![5]),
        (vec![2000], vec![0]),
        (mixed, vec![12, 27, 32]),
    ] {
        let rows: i64 = sizes.iter().sum();
        let name = format!("exact-work-{rows}-{}", sizes.len());
        let (_, db) = vecsearch::docs(&name, DIMENSION, "");
        support::run(&db, "CREATE INDEX docs_ctx ON docs (ctx)");
        // Of each Context asked: its size, its name and the row before its first.
        let mut cases = Vec::new();
        let mut first = 0;
        for (number, &c) in sizes.iter().enumerate() {
            let ctx = format!("c{number}");
            support::load_docs(&db, first + 1..=first + c, &ctx, |n| {
                support::gathered(n, DIMENSION)
            });
            if asked.contains(&number) {
                cases.push((c, ctx, first));
            }
            first += c;
        }
        let every = support::stored(&db, "SELECT id, ctx, tag, v FROM docs");
        for (when, sealed) in [("loose", false), ("sealed", true)] {
            if sealed {
                support::seal_all(&db);
            }
            for (c, ctx, first) in &cases {
                let (c, query) = (*c, support::gathered(first + 7, DIMENSION));
                let sql = format!(
                    "SELECT id, v <=> $q AS distance FROM docs WHERE ctx = '{ctx}' ORDER BY distance USE VECTOR EXACT LIMIT 10"
                );
                let wanted = every.nearest(|row| row[1] == support::text(ctx), &query, 10);
                let found = support::answer(&db, &sql, &query);
                support::assert_same_rows(&support::found(&found, when), &wanted, when);
                // The route is chosen by counted cost (design F3): comparing the m rows the key's
                // path reaches costs m, and the walk, with no index a screen of the table, a 16th
                // of its N vectors plus 3 rows read for each of the 10 asked for in every N/m.
                let (n, m) = (rows as u64, c as u64);
                let compared = found.distance_evaluations();
                let examined = found.rows_examined();
                if m * m <= m * n / 16 + 3 * 10 * n {
                    let direct = [compared, examined] == [m, m + 10];
                    let what = format!("table {rows}, Context {c}, {when}");
                    assert!(
                        direct,
                        "{what}: compares the m rows, reads them and the 10 answer rows"
                    );
                } else {
                    // The walk screens each vector at most once and compares a row it reads once
                    // more; the doubling check keeps the rows read to test the filter below 3m.
                    assert!(
                        compared <= n + examined && examined < 3 * m + 10,
                        "table {rows}, Context {c}, {when}: {compared} compared, {examined} read"
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
