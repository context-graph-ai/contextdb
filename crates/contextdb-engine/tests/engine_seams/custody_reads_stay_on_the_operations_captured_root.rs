//! Snapshot isolation for the durable custody journal: every custody read of
//! one operation answers from the writer root that operation captured, across
//! later commits, ownership retirement and storage cleanup.
use crate::custody_fixture::*;
use contextdb_engine::Database;
use contextdb_server::InProcessBroker;

fn sorted(db: &Database, held: Option<&contextdb_engine::database::HeldCustodyRootForTest>, selector: &str) -> Vec<String> {
    let mut keys = db.__custody_keys_for_test(held, selector).unwrap();
    keys.sort();
    keys
}

#[tokio::test(flavor = "multi_thread")]
async fn custody_reads_stay_on_the_operations_captured_root() {
    let dir = tempfile::tempdir().unwrap();
    let broker = InProcessBroker::new();
    let hub = start_hub(&broker, std::sync::Arc::new(Database::open_memory())).await;
    let (edge, client) = bound_edge(&broker, dir.path(), None).await;
    commit_unit(&edge, 0);
    commit_unit(&edge, 1);

    let before_roots = sorted(&edge, None, "delivery_root.v1.");
    let before_manifests = sorted(&edge, None, "delivery_manifest.v1.");
    assert_eq!(before_roots.len(), 2);
    assert_eq!(before_manifests.len(), 2);
    let held = edge.__hold_custody_root_for_test().unwrap();

    // A new registration, a retirement and storage cleanup, all after capture.
    commit_unit(&edge, 2);
    update_root(&edge, 0);
    edge.run_maintenance_cycle().unwrap();
    edge.compact_currency_versions().unwrap();
    commit_unit(&edge, 3);

    // The current view moved: unit 0 retired, units 2 and 3 registered.
    let now_roots = sorted(&edge, None, "delivery_root.v1.");
    let now_manifests = sorted(&edge, None, "delivery_manifest.v1.");
    assert_eq!(now_roots.len(), 3, "{now_roots:?}");
    assert_eq!(now_manifests.len(), 4);

    // The held operation still answers exactly its captured journal, for
    // prefix and group selections alike, on every successive call.
    for _ in 0..2 {
        assert_eq!(sorted(&edge, Some(&held), "delivery_root.v1."), before_roots);
        assert_eq!(
            sorted(&edge, Some(&held), "delivery_manifest.v1."),
            before_manifests
        );
        assert!(
            sorted(&edge, Some(&held), "@policies").len()
                <= sorted(&edge, None, "@policies").len()
        );
    }
    let later_group = now_manifests
        .iter()
        .find(|key| !before_manifests.contains(key))
        .map(|key| format!("@submission:{}", uuid_of(key)))
        .unwrap();
    assert!(
        !sorted(&edge, None, &later_group).is_empty(),
        "the later unit's submission group exists now"
    );
    assert!(
        sorted(&edge, Some(&held), &later_group).is_empty(),
        "and is absent from the captured root"
    );
    drop(held);
    drop(client);
    hub.stop().await;
}

/// `delivery_manifest.v1.<simple uuid>` -> hyphenated uuid.
fn uuid_of(manifest_key: &str) -> uuid::Uuid {
    uuid::Uuid::parse_str(manifest_key.rsplit('.').next().unwrap()).unwrap()
}
