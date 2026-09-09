use contextdb_parser::parse;
use contextdb_planner::{PhysicalPlan, plan};

#[test]
fn purge_plan_remains_distinct_from_ordinary_delete() {
    let statement = parse("PURGE FROM notes WHERE id = $id")
        .expect("PURGE FROM with a DELETE-shaped predicate must parse");
    let physical_plan = plan(&statement).expect("PURGE statement must plan");

    // Statement 17: retain the exact selected table and predicate in the multi-table plan.
    assert_eq!(physical_plan.explain(), "Purge(selections=1)");
    let PhysicalPlan::Purge(purge) = physical_plan else {
        panic!("PURGE must retain its distinct physical plan");
    };
    assert_eq!(purge.selections.len(), 1);
    assert_eq!(purge.selections[0].table, "notes");
    assert!(purge.selections[0].where_clause.is_some());
}
