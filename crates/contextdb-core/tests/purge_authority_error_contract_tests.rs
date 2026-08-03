use contextdb_core::Error;

#[test]
fn purge_requires_authoritative_hub_names_exact_destination() {
    let error = Error::PurgeRequiresAuthoritativeHub {
        hub_node_id: "hub-node-7".to_string(),
    };

    assert_eq!(
        error.to_string(),
        "PURGE must originate at authoritative hub hub-node-7; run PURGE there"
    );
    match error {
        Error::PurgeRequiresAuthoritativeHub { hub_node_id } => {
            assert_eq!(hub_node_id, "hub-node-7");
        }
        other => panic!("expected typed PURGE authority refusal, got {other:?}"),
    }
}

#[test]
fn purge_requires_standalone_execution_has_exact_typed_contract() {
    let error = Error::PurgeRequiresStandaloneExecution;

    assert_eq!(
        error.to_string(),
        "PURGE must run as a standalone authoritative statement"
    );
    match error {
        Error::PurgeRequiresStandaloneExecution => {}
        other => panic!("expected typed PURGE standalone refusal, got {other:?}"),
    }
}
