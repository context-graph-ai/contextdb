//! What each declared index of the event schema costs one appended event, in
//! each of the three worlds the maintained-count policy admits.
//!
//! Two kinds of authenticated record are maintained per index. An **ordered
//! posting** is what `visit_relational_index_postings` walks to serve an
//! authenticated ordered range, so every indexed range query reads it. A
//! **prefix count** answers `COUNT(*)` over an exact equality on a leading
//! run of an index's columns in one step.
//!
//! Postings are not a choice: one per index per written row, always. Counts
//! ARE a choice, and this test is the grader of that policy, so it measures
//! all three of its values on the same schema and the same append:
//!
//! * **no declaration** — the default since the owner's 2026-09-19 ruling.
//!   22 postings and NO counts. Every `COUNT(*)` still answers exactly, from
//!   the index range.
//! * **`MAINTAIN COUNT ALL PREFIXES`** — every prefix length of every index
//!   the write touched, which is what the engine maintained unconditionally
//!   before the declaration existed: the same 26 counts, unchanged.
//! * **cg's own two declared lists** — exactly those two, and nothing else.
//!
//! The vector is depth-independent: the number of records one append
//! maintains does not change as the stream grows — only how many
//! authenticated chunks each record drags with it does. So this is measured on
//! a shallow store and read beside the per-family chunk rewrites that
//! `one_event_append_cost_by_stream_depth` measures at every depth.

use contextdb_engine::Database;
use contextdb_engine::append_work::{self, IndexPostingTouch, IndexPrefixCountTouch};

use crate::a_declared_count_is_the_only_count_an_append_writes::declare_cg_maintained_counts;
use crate::one_event_append_work_by_family::{
    append_event, declare, declare_all_prefixes_on_the_append_tables,
};

/// An index the engine derived rather than one the schema declared: the
/// uniqueness constraints, the foreign keys and the graph edges all get one.
fn is_derived(index: &str) -> bool {
    index.starts_with("__")
}

/// One event appended to the pinned cg startup schema under one
/// maintained-count declaration, measured from an append that FOLLOWS one so
/// the first append's own installation work is not counted.
fn one_append_under(
    label: &str,
    declare_counts: impl Fn(&Database),
) -> (Vec<IndexPostingTouch>, Vec<IndexPrefixCountTouch>) {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let db = Database::open(root.path().join(format!("per-index-cost-{label}.db")))
        .expect("open the fixture");
    declare(&db);
    declare_counts(&db);
    append_event(&db, 0);

    append_work::reset();
    append_event(&db, 1);
    let mut postings = append_work::take_index_postings();
    let mut counts = append_work::take_index_prefix_counts();
    postings.sort();
    counts.sort();
    (postings, counts)
}

fn report(label: &str, postings: &[IndexPostingTouch], counts: &[IndexPrefixCountTouch]) {
    let mut lines = vec![format!("=== {label} ===")];
    for IndexPostingTouch {
        table,
        index,
        postings,
        ..
    } in postings
    {
        lines.push(format!(
            "{table:<22} {index:<58} {postings:>5} posting(s){}",
            if is_derived(index) { "  (derived)" } else { "" }
        ));
    }
    for IndexPrefixCountTouch {
        table,
        prefix_columns,
        maintained_by,
        ..
    } in counts
    {
        lines.push(format!(
            "{table:<22} ({:<56}) {:>5} count   maintained_by={}",
            prefix_columns.join(", "),
            1,
            maintained_by.len(),
        ));
    }
    lines.push(format!(
        "{:<22} {:<58} {:>5} postings, {} counts, {} distinct records",
        "",
        "",
        postings.iter().map(|touch| touch.postings).sum::<u64>(),
        counts.len(),
        postings.iter().map(|touch| touch.postings).sum::<u64>() as usize + counts.len(),
    ));
    eprintln!("{}", lines.join("\n"));
}

#[test]
fn what_each_declared_index_costs_one_event_append() {
    // 1. Silence. The default writes no undeclared index.
    let (silent_postings, silent_counts) = one_append_under("silent", |_| {});
    report("no declaration", &silent_postings, &silent_counts);
    assert_eq!(
        silent_postings
            .iter()
            .map(|touch| touch.postings)
            .sum::<u64>(),
        22,
        "postings are not a policy: one per index per written row, whatever is declared"
    );
    assert_eq!(
        silent_counts.len(),
        0,
        "a table that declares no MAINTAIN COUNT maintains none, so an append writes 22 records \
         where it used to write 48"
    );

    // 2. ALL PREFIXES: what the engine maintained unconditionally before the
    //    declaration existed, now reachable by asking for it.
    let (all_postings, all_counts) =
        one_append_under("all-prefixes", declare_all_prefixes_on_the_append_tables);
    report("MAINTAIN COUNT ALL PREFIXES", &all_postings, &all_counts);
    assert_eq!(
        all_postings.iter().map(|touch| touch.postings).sum::<u64>(),
        22,
        "the declaration changes counts, never postings"
    );
    assert_eq!(
        all_counts.len(),
        26,
        "ALL PREFIXES maintains one authenticated count per distinct index-prefix key, which is \
         exactly what this schema cost on every append before the policy existed"
    );
    // Every count is keyed on a leading run of some index's columns. Nothing
    // about a READER decides whether a prefix count is maintained — the
    // declaration does, and under ALL PREFIXES the declaration admits them all.
    for count in &all_counts {
        assert!(
            !count.maintained_by.is_empty(),
            "every maintained count names the indexes that keep it: {count:?}"
        );
        assert!(
            count
                .maintained_by
                .iter()
                .any(|(_, _, prefix)| *prefix == count.prefix_columns.len()),
            "a count keys on a leading run of one of its indexes' columns: {count:?}"
        );
    }
    let derived_postings: u64 = all_postings
        .iter()
        .filter(|touch| is_derived(&touch.index))
        .map(|touch| touch.postings)
        .sum();
    assert!(
        derived_postings > 0,
        "the engine derives indexes of its own for uniqueness, foreign keys and graph edges, and \
         an append pays for those too: {all_postings:?}"
    );

    // 3. cg's own declaration: the two prefixes its reads use, and nothing
    //    else. This is the number the adoption is graded on.
    let (cg_postings, cg_counts) = one_append_under("cg", declare_cg_maintained_counts);
    report("cg's two declared lists", &cg_postings, &cg_counts);
    assert_eq!(
        cg_postings.iter().map(|touch| touch.postings).sum::<u64>(),
        22
    );
    assert_eq!(
        cg_counts
            .iter()
            .map(|touch| (touch.table.clone(), touch.prefix_columns.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("events".to_owned(), vec!["context_id".to_owned()]),
            (
                "events".to_owned(),
                vec![
                    "context_id".to_owned(),
                    "connector_id".to_owned(),
                    "source_instance_ref".to_owned(),
                ]
            ),
        ],
        "cg's append maintains exactly the two column lists its reads use: 24 distinct records \
         against the 48 every prefix of every index used to cost"
    );
}
