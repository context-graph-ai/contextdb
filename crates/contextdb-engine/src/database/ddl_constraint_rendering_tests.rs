use super::*;

/// Every machine that renders the same declared state machine must produce
/// the same constraint text: the spelling participates in schema identity
/// comparisons across sync, so a per-process ordering would make two
/// machines disagree about DDL they both authored from identical SQL.
/// `HashMap` gives every instance its own iteration order, so building the
/// same transitions in different insertion orders across many instances
/// exposes any order-dependent rendering with overwhelming probability.
#[test]
fn state_machine_constraint_rendering_is_deterministic() {
    let states = [
        "pending",
        "acknowledged",
        "resolved",
        "dismissed",
        "archived",
        "escalated",
    ];
    let mut renderings = std::collections::HashSet::new();
    for rotation in 0..states.len() {
        let mut transitions = HashMap::new();
        for offset in 0..states.len() {
            let from = states[(rotation + offset) % states.len()];
            transitions.insert(from.to_string(), vec!["resolved".to_string()]);
        }
        let meta = TableMeta {
            state_machine: Some(contextdb_core::StateMachineConstraint {
                column: "status".to_string(),
                transitions,
            }),
            ..TableMeta::default()
        };
        renderings.insert(create_table_constraints_from_meta(&meta).join("; "));
    }
    assert_eq!(
        renderings.len(),
        1,
        "identical state machines must render one constraint spelling, got {renderings:?}"
    );
    let only = renderings.into_iter().next().unwrap();
    let sorted_expectation = "STATE MACHINE (status: acknowledged -> [resolved], \
         archived -> [resolved], dismissed -> [resolved], escalated -> [resolved], \
         pending -> [resolved], resolved -> [resolved])";
    assert_eq!(
        only, sorted_expectation,
        "the one spelling is the transitions sorted by source state"
    );
}
