use contextdb_server::protocol::{WireChangeSet, WirePurgeChange};

#[test]
fn empty_ddl_provenance_does_not_displace_nonempty_purge_slot() {
    let original = WireChangeSet {
        ddl_provenance: Vec::new(),
        purges: vec![WirePurgeChange::default()],
        ..Default::default()
    };

    let encoded = rmp_serde::to_vec(&original).expect("encode wire changeset");
    let decoded: WireChangeSet = rmp_serde::from_slice(&encoded).expect("decode wire changeset");

    assert!(decoded.ddl_provenance.is_empty());
    assert_eq!(decoded.purges, original.purges);
}
