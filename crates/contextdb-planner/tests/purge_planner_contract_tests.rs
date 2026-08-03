use contextdb_parser::parse;
use contextdb_planner::plan;

#[test]
fn purge_plan_remains_distinct_from_ordinary_delete() {
    let statement = parse("PURGE FROM notes WHERE id = $id")
        .expect("PURGE FROM with a DELETE-shaped predicate must parse");
    let physical_plan = plan(&statement).expect("PURGE statement must plan");

    assert_eq!(physical_plan.explain(), "Purge(table=notes)");
}
