//! Independent historical row-graph guards; no production mutation hooks.
use super::*;
use crate::{Database, DatabaseOpenOptions, ReadSession, ReadSessionOptions};
use contextdb_core::{Principal, Value};

const WITNESS: TableDefinition<&str, &[u8]> = TableDefinition::new("__history_root_witness_v1");
const QUERY: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)-[:LINKS]->(b) WHERE a.id = $source COLUMNS (b.id AS t))";

const REVERSE_QUERY: &str = "SELECT t FROM GRAPH_TABLE(edges MATCH (a)<-[:LINKS]-(b) WHERE a.id = $source COLUMNS (b.id AS t))";

fn params(source: u128) -> HashMap<String, Value> {
    HashMap::from([("source".into(), Value::Uuid(uuid::Uuid::from_u128(source)))])
}

fn fixture(path: &Path) {
    let database = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    for sql in [
        "CREATE TABLE acl_grants (id UUID PRIMARY KEY, principal_kind TEXT, principal_id TEXT, acl_id UUID) SYNC OFF",
        "CREATE TABLE nodes (id UUID, acl_id UUID ACL REFERENCES acl_grants(acl_id)) SYNC OFF",
        "CREATE INDEX historical_guard_nodes_id ON nodes(id)",
        "CREATE TABLE edges (id UUID PRIMARY KEY, source_id UUID, target_id UUID, edge_type TEXT, acl_id UUID ACL REFERENCES acl_grants(acl_id)) SYNC OFF",
        "INSERT INTO acl_grants VALUES ('00000000-0000-0000-0000-000000000004', 'Agent', 'historical-guard', '00000000-0000-0000-0000-000000000004')",
    ] {
        database.execute(sql, &HashMap::new()).unwrap();
    }
    for (id, acl) in [(1, 3), (2, 4), (6, 4)] {
        database
            .execute(
                "INSERT INTO nodes VALUES ($id, $acl)",
                &HashMap::from([
                    ("id".into(), Value::Uuid(uuid::Uuid::from_u128(id))),
                    ("acl".into(), Value::Uuid(uuid::Uuid::from_u128(acl))),
                ]),
            )
            .unwrap();
    }
    database
        .execute(
            "INSERT INTO edges VALUES ($id, $source, $target, 'LINKS', $acl)",
            &HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(5))),
                ("source".into(), Value::Uuid(uuid::Uuid::from_u128(1))),
                ("target".into(), Value::Uuid(uuid::Uuid::from_u128(2))),
                ("acl".into(), Value::Uuid(uuid::Uuid::from_u128(4))),
            ]),
        )
        .unwrap();
    assert_eq!(
        database.execute(QUERY, &params(1)).unwrap().rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(2))]]
    );
    assert!(
        database
            .execute(QUERY, &params(6))
            .unwrap()
            .rows()
            .is_empty()
    );
    database.close().unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    Payload,
    MissingWitness,
    MalformedWitness,
    MissingTable,
}

fn damage(path: &Path, reverse: bool, damage: Damage) -> Vec<u8> {
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _scope =
        crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone().into())
            .unwrap();
    let store = RedbPersistence::open(path).unwrap();
    store.attach_memory_accountant(accountant.into());
    store
        .with_db(|db| {
            let write = db.begin_write().unwrap();
            let definition = if reverse {
                row_graph::REVERSE
            } else {
                row_graph::FORWARD
            };
            let encoded = {
                let mut table = write.open_table(definition).unwrap();
                let (key, mut value) = {
                    let (key, value) = table.iter().unwrap().next().unwrap().unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                };
                if matches!(damage, Damage::Payload) {
                    value.push(0);
                    table.insert(key.as_slice(), value.as_slice()).unwrap();
                }
                value
            };
            match damage {
                Damage::MissingWitness => {
                    write
                        .open_table(WITNESS)
                        .unwrap()
                        .remove(if reverse {
                            "row_graph_rev"
                        } else {
                            "row_graph_fwd"
                        })
                        .unwrap();
                }
                Damage::MalformedWitness => {
                    write
                        .open_table(WITNESS)
                        .unwrap()
                        .insert(
                            if reverse {
                                "row_graph_rev"
                            } else {
                                "row_graph_fwd"
                            },
                            &[0u8][..],
                        )
                        .unwrap();
                }
                Damage::MissingTable => {
                    assert!(write.delete_table(definition).unwrap());
                }
                Damage::Payload => {}
            }
            // Keep startup descriptor authentication current, deliberately retain the
            // producer's old row-graph witness. No sanctioned producer hook runs.
            RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
            write.commit().unwrap();
            Ok(encoded)
        })
        .unwrap()
}

fn options() -> ReadSessionOptions {
    ReadSessionOptions {
        principal: Some(Principal::Agent("historical-guard".into())),
        ..Default::default()
    }
}

fn assert_empty(session: &ReadSession, reverse: bool) {
    let query = if reverse { REVERSE_QUERY } else { QUERY };
    // These controls establish actual visibility, not a fixture label.
    assert!(
        session
            .execute("SELECT id FROM nodes WHERE id = $source", &params(1))
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        session
            .execute("SELECT id FROM nodes WHERE id = $source", &params(6))
            .unwrap()
            .rows(),
        vec![vec![Value::Uuid(uuid::Uuid::from_u128(6))]]
    );
    let answer = session
        .execute(query, &params(6))
        .expect("untouched hidden payload must not refuse or disclose its existence");
    assert!(answer.rows().is_empty());
    assert!(
        matches!(
            session.execute(query, &params(1)),
            Err(Error::AclDenied { .. })
        ),
        "a pinned hidden source retains typed ACL denial"
    );
}

fn hidden_payload_case(reverse: bool, owner: bool) {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("hidden.db");
    fixture(&path);
    damage(&path, reverse, Damage::Payload);
    if owner {
        let database = Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let reader = ReadSession::open_owner_only(&path, options()).unwrap();
        assert_eq!(
            reader.route(),
            contextdb_core::read_contract::ReadRoute::Owner
        );
        assert_empty(&reader, reverse);
        drop(reader);
        database.close().unwrap();
    } else {
        let reader = ReadSession::open_with_options(&path, options()).unwrap();
        assert_eq!(
            reader.route(),
            contextdb_core::read_contract::ReadRoute::File
        );
        assert_empty(&reader, reverse);
    }
}

#[test]
fn historical_root_a_file_forward_hidden_payload() {
    hidden_payload_case(false, false);
}
#[test]
fn historical_root_a_file_reverse_hidden_payload() {
    hidden_payload_case(true, false);
}
#[test]
fn historical_root_a_owner_forward_hidden_payload() {
    hidden_payload_case(false, true);
}
#[test]
fn historical_root_a_owner_reverse_hidden_payload() {
    hidden_payload_case(true, true);
}

// Exercise both exact private source branches without letting public ACL
// filtering hide an accessed damaged entry. The public empty-answer guard above
// independently establishes the permission boundary.
#[expect(
    clippy::result_large_err,
    reason = "The guard asserts the shared inline typed refusal vocabulary."
)]
fn cursor_outcome(path: &Path, writer: bool, reverse: bool, empty: bool) -> Result<bool> {
    use std::ops::Bound;
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::no_limit());
    let _scope =
        crate::read_image_memory::ImageMemory::enter_writer_operation(accountant.clone().into())
            .unwrap();
    let mut store = None;
    let root = if writer {
        let persistence = RedbPersistence::open(path)?;
        persistence.attach_memory_accountant(accountant.clone().into());
        let root = persistence.initialize_writer_snapshot(SnapshotId(100), Lsn(100))?;
        store = Some(persistence);
        graph_read::GraphReadRoot::Writer(root)
    } else {
        let (_image, _receipt, source) = load_read_image_with_memory(path, Some(16 * 1024 * 1024))
            .unwrap()
            .into_parts();
        graph_read::GraphReadRoot::Image(source)
    };
    let prefix = uuid::Uuid::from_u128(if empty {
        6
    } else if reverse {
        2
    } else {
        1
    });
    let mut upper = prefix.as_bytes().to_vec();
    upper.extend_from_slice(&[255; 128]);
    let result = root
        .cursor_lane(
            reverse,
            true,
            Bound::Included(prefix.as_bytes()),
            Bound::Included(&upper),
            accountant,
        )
        .and_then(|mut cursor| cursor.next_edge_with(&|| Ok(())).map(|edge| edge.is_none()));
    drop(root);
    drop(store);
    result
}

fn public_refusal(path: &Path, owner: bool, empty: bool, reverse: bool) {
    let query = if reverse { REVERSE_QUERY } else { QUERY };
    let database = owner.then(|| {
        Database::open_with_options(
            path,
            DatabaseOpenOptions {
                maintenance_policy: crate::database::MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap()
    });
    let result = if owner {
        ReadSession::open_owner_only(path, ReadSessionOptions::default())
    } else {
        ReadSession::open(path)
    };
    let outcome = result.and_then(|reader| {
        reader
            .execute(
                query,
                &params(if empty {
                    6
                } else if reverse {
                    2
                } else {
                    1
                }),
            )
            .map(|_| ())
    });
    assert!(
        matches!(outcome, Err(Error::StoreCorrupted { .. })),
        "public Legacy/owner={owner}/empty={empty}: {outcome:?}"
    );
    if let Some(database) = database {
        database.close().unwrap();
    }
}

#[test]
fn historical_root_b_native_page_on_empty_path_refuses() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for reverse in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("native.db");
        fixture(&path);
        assert!(cursor_outcome(&path, false, reverse, true).unwrap());
        let raw = redb::Database::open(&path).unwrap();
        let encoded = {
            let read = raw.begin_read().unwrap();
            let table = read
                .open_table(if reverse {
                    row_graph::REVERSE
                } else {
                    row_graph::FORWARD
                })
                .unwrap();
            assert_eq!(table.len().unwrap(), 1);
            let (_, value) = table.iter().unwrap().next().unwrap().unwrap();
            value.value().to_vec()
        };
        drop(raw);
        let mut bytes = std::fs::read(&path).unwrap();
        let offsets: Vec<_> = bytes
            .windows(encoded.len())
            .enumerate()
            .filter_map(|(offset, value)| (value == encoded.as_slice()).then_some(offset))
            .collect();
        assert!(!offsets.is_empty());
        // One-entry row-owned table: its root is the leaf visited even for the
        // empty anchor range. Flip the persisted payload without recomputing
        // native checksums or touching any root descriptor/witness.
        for offset in offsets {
            bytes[offset + encoded.len() - 1] ^= 1;
        }
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            cursor_outcome(&path, false, reverse, true),
            Err(Error::StoreCorrupted { .. })
        ));
        public_refusal(&path, false, true, reverse);
    }
}

#[test]
fn historical_root_c_missing_malformed_witness_and_missing_table_refuse() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for damage_kind in [
        Damage::MissingWitness,
        Damage::MalformedWitness,
        Damage::MissingTable,
    ] {
        for reverse in [false, true] {
            for writer in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("witness.db");
                fixture(&path);
                damage(&path, reverse, damage_kind);
                let result = cursor_outcome(&path, writer, reverse, true);
                assert!(
                    matches!(result, Err(Error::StoreCorrupted { .. })),
                    "{damage_kind:?}/writer={writer}/reverse={reverse}: {result:?}"
                );
                public_refusal(&path, writer, true, reverse);
            }
        }
    }
}

#[test]
fn historical_root_d_accessed_owned_damage_refuses() {
    let _clock = contextdb_core::Wallclock::test_clock_guard(|| 1_800_000_000_000);
    for reverse in [false, true] {
        for writer in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("accessed.db");
            fixture(&path);
            assert!(!cursor_outcome(&path, writer, reverse, false).unwrap());
            damage(&path, reverse, Damage::Payload);
            let result = cursor_outcome(&path, writer, reverse, false);
            assert!(
                matches!(result, Err(Error::StoreCorrupted { .. })),
                "writer={writer}/reverse={reverse}: {result:?}"
            );
            public_refusal(&path, writer, false, reverse);
        }
    }
}
