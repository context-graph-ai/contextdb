pub const IMMUTABLE_TABLES: &[&str] = &["observations"];

pub const ONTOLOGY_TABLES: &[&str] = &[
    "contexts",
    "intentions",
    "decisions",
    "entities",
    "entity_snapshots",
    "observations",
    "outcomes",
    "invalidations",
    "edges",
    "approvals",
    "patterns",
    "sync_state",
];

pub const INVALIDATION_TRANSITIONS: &[(&str, &[&str])] = &[
    ("pending", &["acknowledged", "dismissed"]),
    ("acknowledged", &["resolved", "dismissed"]),
    ("resolved", &[]),
    ("dismissed", &[]),
];

pub fn is_immutable(table: &str) -> bool {
    IMMUTABLE_TABLES.contains(&table)
}

pub fn is_valid_transition(from: &str, to: &str) -> bool {
    INVALIDATION_TRANSITIONS
        .iter()
        .find(|(state, _)| *state == from)
        .is_some_and(|(_, targets)| targets.contains(&to))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_immutable_tables() {
        assert!(is_immutable("observations"));
        assert!(!is_immutable("decisions"));
    }

    #[test]
    fn test_invalidation_state_machine() {
        assert!(is_valid_transition("pending", "acknowledged"));
        assert!(is_valid_transition("pending", "dismissed"));
        assert!(!is_valid_transition("resolved", "pending"));
        assert!(!is_valid_transition("dismissed", "acknowledged"));
    }
}
