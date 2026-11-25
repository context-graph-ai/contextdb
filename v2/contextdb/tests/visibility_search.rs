//! What a narrowed handle is told by a search: the nearest rows it may see, under every rule
//! that hides a row, and no hidden row, by its key, its distance or a ranked join.

mod support;

use contextdb::{Error, Value, params};

use support::with_labels;
use support::{GRANTS, acl, agent, context, context_set, grant, ids, in_contexts, named};

#[test]
fn a_search_returns_the_nearest_rows_the_handle_may_see_and_no_hidden_row() {
    const DIMENSION: usize = 8;
    let db = support::open(&support::folder("visibility-search"));
    support::run(&db, GRANTS);
    grant(&db, 1, "Agent", "report-reader", acl(1));
    grant(&db, 2, "Human", "someone-else", acl(2));
    support::run(
        &db,
        &format!(
            "CREATE TABLE passages (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
             acl_id UUID ACL REFERENCES acl_grants(acl_id), v VECTOR({DIMENSION}))"
        ),
    );
    let query = support::vector(1_000, DIMENSION);
    let passage = |id: i64, context_number: u128, acl_number: u128, v: Vec<f32>| {
        db.execute(
            "INSERT INTO passages (id, context_id, acl_id, v) VALUES ($id, $context, $acl, $v)",
            &params! {
                "id" => id, "context" => context(context_number), "acl" => acl(acl_number),
                "v" => v,
            },
        )
        .unwrap_or_else(|error| panic!("passage {id} was refused: {error:?}"));
    };
    // Passage n of 1 to 40: Context 1 for odd n and 2 for even; access id 2 when n is a
    // multiple of 3 and 1 otherwise. Three more hold the very vector searched with, so that
    // each would be the first answer: 101 is hidden by its Context, 102 by its access id,
    // 103 by both.
    for n in 1..=40_i64 {
        let context_number = if n % 2 == 1 { 1 } else { 2 };
        let acl_number = if n % 3 == 0 { 2 } else { 1 };
        passage(n, context_number, acl_number, support::vector(n, DIMENSION));
    }
    passage(101, 2, 1, query.clone());
    passage(102, 1, 2, query.clone());
    passage(103, 2, 2, query.clone());

    let reader = named(&db, agent("report-reader"));
    let reader_in_one =
        db.scoped_with_constraints(Some(context_set(&[1])), None, Some(agent("report-reader")));
    let in_context_one = |row: &[Value]| row[1] == Value::Uuid(context(1));
    let granted = |row: &[Value]| row[2] == Value::Uuid(acl(1));

    // Twice: with every vector unindexed on its list, and with every vector in a block.
    for sealed in [false, true] {
        if sealed {
            assert_eq!(support::seal_all(&db), 43, "vectors sealed");
        }
        let stored = support::stored(&db, "SELECT id, context_id, acl_id, v FROM passages");
        for k in [1, 5, 43] {
            for filter in [None, Some("id > 10")] {
                let sql = support::nearest_sql("passages", "v", filter, k);
                let passes = |row: &[Value]| {
                    filter.is_none() || matches!(row[0], Value::Int64(id) if id > 10)
                };
                let what = |who: &str| format!("{who}, sealed: {sealed}");

                // The handle that declares nothing finds the planted rows first.
                let wanted = stored.nearest(passes, &query, k);
                support::assert_nearest(&db, &sql, &query, &wanted, &what("declaring nothing"));
                assert!(
                    matches!(wanted[0].key, Value::Int64(101)),
                    "the planted rows are not the nearest: {wanted:?}"
                );

                // A narrowed handle is answered with the nearest rows it may see, each at its
                // true distance; a hidden row takes no place among them.
                let wanted = stored.nearest(|row| passes(row) && granted(row), &query, k);
                support::assert_nearest(&reader, &sql, &query, &wanted, &what("the principal"));
                let wanted = stored.nearest(
                    |row| passes(row) && granted(row) && in_context_one(row),
                    &query,
                    k,
                );
                let what = what("the principal in Context 1");
                support::assert_nearest(&reader_in_one, &sql, &query, &wanted, &what);
            }
        }
    }

    let tags = "CREATE TABLE tags (id INTEGER PRIMARY KEY, c UUID NOT NULL CONTEXT_ID, label TEXT \
                SCOPE_LABEL_READ ('a', 'b') WRITE ('a', 'b'), v VECTOR(1) PARTITION_KEY (c))";
    support::run(&db, tags);
    let c = context(1);
    let row = format!("INSERT INTO tags VALUES (1, '{c}', 'b', [1.0])");
    support::run(&db, &row);
    let by_label = with_labels(&in_contexts(&db, &[1]), &["a"]);
    let sql = support::nearest_sql("tags", "v", None, 2);
    let found = support::search(&by_label, &sql, &[1.0]).expect("a search");
    assert_eq!(found.rows().len(), 0, "the nearest row is under label b");
    let said = by_label.explain(&sql).expect("explained");
    assert!(!said.contains(&c.to_string()), "no partition told");
    let keys = "CREATE TABLE keys (id INTEGER PRIMARY KEY, c UUID NOT NULL CONTEXT_ID, acl_id UUID \
                ACL REFERENCES acl_grants(acl_id), v VECTOR(1) PARTITION_KEY (c))";
    support::run(&db, keys);
    let row = format!("INSERT INTO keys VALUES (1, '{c}', '{}', [1.0])", acl(1));
    support::run(&db, &row);
    let said = reader_in_one
        .explain(&sql.replace("tags", "keys"))
        .expect("explained");
    assert!(
        !said.contains(&c.to_string()),
        "an access list may hide rows"
    );
    // With no principal that holds grants the search is refused like any read of the table.
    let sql = support::nearest_sql("passages", "v", None, 5);
    for asked in [support::nearest_sql("passages", "v", None, 0), sql.clone()] {
        match support::search(&in_contexts(&db, &[1]), &asked, &query) {
            Err(Error::PrincipalRequired { table }) => assert_eq!(table, "passages"),
            other => panic!("{asked}\nthrough a handle with no principal gave {other:?}"),
        }
    }
    // Explaining it is refused alike, and a principal that holds grants is told the filter path.
    for said in [&sql, "INSERT INTO passages (id) VALUES (1)"] {
        let r = in_contexts(&db, &[1]).explain(said);
        assert!(matches!(r, Err(Error::PrincipalRequired { .. })), "{r:?}");
    }
    let filtered = support::nearest_sql("passages", "v", Some("id > 10"), 5);
    let text = reader_in_one
        .explain(&filtered)
        .expect("a principal explains");
    assert!(text.contains("filter_path:"), "{text}", text = &*text);
}

#[test]
fn a_ranked_search_treats_a_hidden_row_joined_by_a_unique_column_as_unmatched() {
    let db = support::open(&support::folder("visibility-rank-unique"));
    for sql in [
        "CREATE TABLE results (id INTEGER PRIMARY KEY, context_id UUID CONTEXT_ID, \
         task_id INTEGER UNIQUE, weight REAL)",
        "CREATE TABLE tasks (id INTEGER PRIMARY KEY, v VECTOR(2) RANK_POLICY (JOIN results ON \
         task_id, FORMULA '{vector_score} * coalesce({weight}, 1.0)', SORT_KEY s))",
        "INSERT INTO tasks (id, v) VALUES (1, [1.0, 0.0]), (2, [1.0, 1.0])",
    ] {
        support::run(&db, sql);
    }
    for (id, task, context_number, weight) in [(10_i64, 1_i64, 2, 0.1_f64), (11, 2, 1, 1.0)] {
        let values =
            params! {"id" => id, "task" => task, "c" => context(context_number), "w" => weight};
        let sql =
            "INSERT INTO results (id, context_id, task_id, weight) VALUES ($id, $c, $task, $w)";
        db.execute(sql, &values).expect("insert result");
    }
    let sql = "SELECT id FROM tasks ORDER BY v <=> [1.0, 0.0] USE RANK s LIMIT 2";
    assert_eq!(
        ids(&in_contexts(&db, &[1]), sql),
        vec![1, 2],
        "a result hidden from the reader is no answer and no refusal: its task ranks as unmatched"
    );
}
#[test]
fn partitioning_never_changes_the_contexts_a_caller_may_read() {
    let db = support::open(&support::folder("visibility-partitioned"));
    let keyed = " PARTITION_KEY (context_id)";
    let behind = " PARTITION_KEY (context_id) INDEXING_THRESHOLD 1";
    for (table, key) in [("flat", ""), ("keyed", keyed), ("behind", behind)] {
        let declared = "(id INTEGER PRIMARY KEY, context_id UUID NOT NULL CONTEXT_ID, \
                        s INTEGER UNIQUE, v VECTOR(3)";
        support::run(&db, &format!("CREATE TABLE {table} {declared}{key})"));
        for n in 1..=30_i64 {
            let c = context(1 + n as u128 % 3);
            let row = params! { "id" => n, "c" => c, "v" => support::vector(n, 3) };
            let sql = format!("INSERT INTO {table} VALUES ($id, $c, $id, $v)");
            db.execute(&sql, &row).unwrap();
        }
    }
    for (contexts, rows) in [(&[1_u128][..], 10), (&[1, 2][..], 20)] {
        let handle = in_contexts(&db, contexts);
        for tail in ["ORDER BY id", "ORDER BY v <=> [1.0, 0.0, 0.0] LIMIT 30"] {
            let [flat, keyed] = ["flat", "keyed"]
                .map(|table| ids(&handle, &format!("SELECT id FROM {table} {tail}")));
            assert!(flat == keyed && flat.len() == rows, "{contexts:?} {tail}");
        }
    }
    // A hidden row named whole is refused where the find has Contexts of the reader's to
    // search and the whole WHERE holds for it, its distance bound too; a find left none of them,
    // as a reader given none is, finds nothing. INDEXED's own refusals come first.
    let (handle, none) = (in_contexts(&db, &[1]), in_contexts(&db, &[]));
    let d = support::run(&db, "SELECT v <=> [1.0, 0.0, 0.0] FROM keyed WHERE id = 2");
    let values = params! { "c" => context(3), "d" => d.rows()[0][0].clone() };
    let every = "AUTO EXACT INDEXED";
    let within = |op| format!("id = 2 AND v <=> [1.0, 0.0, 0.0] {op} $d");
    let (at_most, under) = (within("<="), within("<"));
    let other = "id = 2 AND k.context_id = $c";
    for (reader, table, modes, filter, wanted) in [
        (&handle, "keyed", every, "id = 2", "refused"),
        (&handle, "keyed", every, "s = 2", "refused"),
        (&handle, "keyed", every, &at_most, "refused"),
        (&handle, "keyed", every, &under, "empty"),
        (&handle, "keyed", every, other, "empty"),
        (&handle, "flat", "AUTO EXACT", other, "empty"),
        (&handle, "flat", "INDEXED", "id = 2", "narrowed"),
        (&handle, "behind", "INDEXED", "id = 2", "behind"),
        (&none, "keyed", every, "id = 2", "empty"),
        (&none, "flat", "AUTO EXACT", "id = 2", "empty"),
        (&none, "flat", "INDEXED", "id = 2", "narrowed"),
    ] {
        for mode in modes.split(' ') {
            let near = format!("ORDER BY v <=> [1.0, 0.0, 0.0] USE VECTOR {mode} LIMIT 3");
            let sql = format!("SELECT id FROM {table} AS k WHERE {filter} {near}");
            let found = reader.execute(&sql, &values);
            let got = match &found {
                Ok(answer) if answer.rows().is_empty() => "empty",
                Err(Error::VectorIndexedNarrowed { .. }) => "narrowed",
                Err(Error::VectorIndexedRouteUnavailable { .. }) => "behind",
                Err(Error::ContextScopeViolation { .. }) => "refused",
                _ => "other",
            };
            assert_eq!(got, wanted, "{sql}: {found:?}");
        }
    }
}
