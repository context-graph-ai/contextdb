//! A column declared `WITH (quantization = 'SQ8')` keeps each vector as one scale and one
//! whole number from -127 to 127 for each of its numbers. What a SELECT returns is that kept
//! vector, every search ranks by it, and such a column is grouped and searched as any other.

mod support;
#[path = "support/vecsearch.rs"]
mod vecsearch;

use std::fs;

use contextdb::{Error, Value};

const DIMENSION: usize = 16;
const REDUCED: &str = "WITH (quantization = 'SQ8')";

/// What row `n` is written with: numbers up to three, so that the scale is not that of a
/// vector at length one.
fn written(n: i64, dimension: usize) -> Vec<f32> {
    let scatter = support::vector(n, dimension);
    scatter.iter().map(|number| 3.0 * number).collect()
}

/// The vector the store returns for row `id` of `docs`.
fn kept(db: &contextdb::Database, id: i64) -> Vec<f32> {
    let rows = support::rows(db, &format!("SELECT v FROM docs WHERE id = {id}"));
    match rows.as_slice() {
        [row] => match &row[0] {
            Value::Vector(numbers) => numbers.clone(),
            other => panic!("row {id} holds {other:?}"),
        },
        rows => panic!("{} rows with the id {id}", rows.len()),
    }
}

/// The bytes of the data file of a closed store of 2,000 rows of 384 numbers each.
fn file_of(name: &str, clauses: &str) -> u64 {
    let (folder, db) = vecsearch::docs(name, 384, clauses);
    support::load_docs(&db, 1..=2000, "a", |n| written(n, 384));
    support::close(db);
    fs::metadata(folder.join("data"))
        .expect("the store's data file")
        .len()
}

#[test]
fn a_reduced_column_keeps_the_nearest_one_byte_vector() {
    let (_, db) = vecsearch::docs("reduced-keeps", DIMENSION, REDUCED);
    support::load_docs(&db, 1..=20, "a", |n| written(n, DIMENSION));
    let largest = |numbers: &[f32]| numbers.iter().fold(0.0_f32, |most, x| most.max(x.abs()));
    for id in 1..=20 {
        let (put, got) = (written(id, DIMENSION), kept(&db, id));
        assert_eq!(got.len(), DIMENSION);
        // Every number is the scale times a whole code within 127, and the largest is 127.
        let scale = largest(&got) / 127.0;
        for (at, number) in got.iter().enumerate() {
            let code = (number / scale).round();
            assert!(code.abs() <= 127.0, "row {id}, number {at}: code {code}");
            assert_eq!(
                (scale * code).to_bits(),
                number.to_bits(),
                "row {id}, number {at} is {number:e}: the scale {scale:e} times {code}"
            );
        }
        let off = (largest(&got) - largest(&put)).abs();
        assert!(
            off <= largest(&put) / 127.0,
            "row {id}: the largest number was {:e} and is kept as {:e}",
            largest(&put),
            largest(&got)
        );
        assert!(got != put, "row {id} came back as it was written");
    }
    support::close(db);

    let (reduced, full) = (
        file_of("reduced-size", REDUCED),
        file_of("reduced-size-f32", ""),
    );
    assert!(
        2 * reduced < full,
        "2,000 reduced rows take {reduced} bytes and the same rows at full precision {full}"
    );
}

#[test]
fn a_reduced_value_that_would_expand_past_the_limit_is_refused_by_name() {
    // 250,000 numbers are kept in 250,000 bytes and expand to 1,000,000. A limit 1 MiB beyond
    // what an open sets aside: the kept bytes fit, the kept and the expanded bytes together do
    // not.
    for numbers in [250_000, 300_000] {
        let (folder, db) = vecsearch::docs(
            &format!("reduced-over-limit-{numbers}"),
            numbers,
            &format!("{REDUCED} PARTITION_KEY (ctx)"),
        );
        support::load_docs(&db, 1..=1, "a", |n| written(n, numbers));
        let query = "SELECT id FROM docs WHERE ctx = 'b' ORDER BY v <=> $v LIMIT 1";
        let q = contextdb::params! { "v" => kept(&db, 1) };
        assert_eq!(
            support::whole(&support::the_partition(&db), "pending_tuples"),
            i64::from(numbers == 250_000)
        );
        support::run(&db, "UPDATE docs SET ctx = 'b' WHERE id = 1");
        assert_eq!(
            support::whole(&support::the_partition(&db), "pending_tuples"),
            i64::from(numbers == 250_000)
        );
        assert_eq!(
            db.execute(query, &q).unwrap().rows(),
            &[vec![Value::Int64(1)]]
        );
        db.set_memory_limit(Some(support::opening::open_floor() + (1 << 20)))
            .expect("1 MiB beyond what an open sets aside");
        match support::refusal(&db, "SELECT v FROM docs WHERE id = 1") {
            Error::MemoryBudgetExceeded { .. } => {}
            other => panic!("a value that expands past the limit was refused as {other:?}"),
        }
        support::run(&db, "SET MEMORY_LIMIT 256M");
        assert_eq!(kept(&db, 1).len(), numbers, "the same read under no limit");
        support::close(db);
        let room = support::opening::reader_charge(&folder, None) + (1 << 20);
        let reader = support::open_reader(&folder, Some(room));
        assert_eq!(support::ids(&reader, "SELECT id FROM docs"), [1]);
        let error = support::refusal(&reader, "SELECT v FROM docs");
        assert!(matches!(error, Error::MemoryBudgetExceeded { .. }));
    }
}

#[test]
fn exact_on_a_reduced_column_ranks_by_the_kept_vectors() {
    let (_, db) = vecsearch::docs("reduced-exact", DIMENSION, REDUCED);
    support::load_docs(&db, 1..=500, "a", |n| written(n, DIMENSION));
    // The reference is worked out from the vectors a SELECT returns, which are the kept ones.
    let stored = support::stored(&db, support::DOCS);
    assert!(kept(&db, 1) != written(1, DIMENSION));
    let check = |state: &str| {
        for seed in [80_001, 80_002, 80_003] {
            let query = support::vector(seed, DIMENSION);
            let wanted = stored.nearest(|_| true, &query, 25);
            let sql = support::by_mode(None, "EXACT", 25);
            support::assert_nearest(&db, &sql, &query, &wanted, state);
        }
    };
    check("pending");
    assert_eq!(support::seal_all(&db), 500);
    check("sealed");
}

#[test]
fn a_reduced_column_is_grouped_and_searched_as_any_other() {
    let clauses = format!("{REDUCED} SEARCH_MODE INDEXED");
    let (_, db) = support::built("reduced-grouped", DIMENSION, &clauses, 0);
    support::load_docs(&db, 1..=2000, "a", |n| support::gathered(n, DIMENSION));
    support::maintenance(&db);
    let index = support::shown(&db, "SHOW VECTOR_INDEXES").remove(0);
    assert_eq!(support::word(&index, "quantization"), "SQ8");
    assert_eq!(
        support::whole(&index, "effective_indexing_threshold"),
        100_000
    );
    assert_eq!(
        support::field(&support::the_partition(&db), "lists"),
        &Value::Int64(45)
    );
    let stored = support::stored(&db, support::DOCS);
    for j in 0..5 {
        let query = support::gathered(81_000 + j, DIMENSION);
        let result = support::answer(&db, &support::by_mode(None, "", 10), &query);
        let state = format!("question {j} on the column's own mode");
        support::assert_sound(&result, &stored, |_| true, &query, 10, &state);
    }
}
