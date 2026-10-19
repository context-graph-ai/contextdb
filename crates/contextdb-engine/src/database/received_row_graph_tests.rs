use super::*;
use contextdb_graph::source::GraphQuery;
use contextdb_tx::OwnedAdjEntry;

fn edge_ddl(table: &str, keyed: bool) -> String {
    let identity_type = if keyed {
        "INTEGER PRIMARY KEY"
    } else {
        "INTEGER NOT NULL"
    };
    format!(
        "CREATE TABLE {table} (source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL, identity {identity_type}, UNIQUE (source_id, target_id, edge_type, identity))"
    )
}

fn insert_siblings(db: &Database, table: &str, keyed: bool) {
    db.execute(&edge_ddl(table, keyed), &HashMap::new())
        .unwrap();
    db.execute(&format!("INSERT INTO {table} VALUES ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'link', 1), ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'link', 2)"), &HashMap::new()).unwrap();
}

fn witnesses(db: &Database, direction: Direction) -> Vec<OwnedAdjEntry> {
    let memory = db.declared_reader_memory().unwrap();
    let root = db.graph_snapshot(db.snapshot_for_read(), memory).unwrap();
    let incoming = matches!(direction, Direction::Incoming);
    let mut result = Vec::new();
    db.graph_store
        .visit_edges_with_owner(
            root.as_ref(),
            GraphQuery {
                node: uuid::Uuid::from_u128(if incoming { 2 } else { 1 }),
                direction,
                identity: Some((uuid::Uuid::from_u128(if incoming { 1 } else { 2 }), "link")),
            },
            |edge, owner| {
                if let Some(owner) = owner {
                    result.push(OwnedAdjEntry {
                        owner: owner.clone(),
                        entry: edge.clone(),
                    });
                }
                Ok(true)
            },
        )
        .unwrap();
    result.sort_by(|a, b| a.owner.cmp(&b.owner));
    result
}

fn standalone_witnesses(db: &Database, direction: Direction) -> Vec<AdjEntry> {
    let memory = db.declared_reader_memory().unwrap();
    let root = db.graph_snapshot(db.snapshot_for_read(), memory).unwrap();
    let incoming = matches!(direction, Direction::Incoming);
    let mut standalone = Vec::new();
    db.graph_store
        .visit_edges_with_owner(
            root.as_ref(),
            GraphQuery {
                node: uuid::Uuid::from_u128(if incoming { 2 } else { 1 }),
                direction,
                identity: Some((uuid::Uuid::from_u128(if incoming { 1 } else { 2 }), "link")),
            },
            |edge, owner| {
                if owner.is_none() {
                    standalone.push(edge.clone());
                }
                Ok(true)
            },
        )
        .unwrap();
    standalone
}

fn assert_witnesses(db: &Database, expected: &[OwnedAdjEntry], expected_standalone: &[AdjEntry]) {
    assert_eq!(witnesses(db, Direction::Outgoing), expected);
    assert_eq!(witnesses(db, Direction::Incoming), expected);
    for direction in [Direction::Outgoing, Direction::Incoming] {
        let standalone = standalone_witnesses(db, direction);
        assert_eq!(standalone.len(), 1);
        assert_eq!(standalone[0].deleted_tx, None);
        assert_eq!(standalone[0].properties, HashMap::new());
        assert_eq!(standalone.as_slice(), expected_standalone);
    }
    for owned in expected {
        let rows = db
            .execute(
                &format!("SELECT * FROM {}", owned.owner.table),
                &HashMap::new(),
            )
            .unwrap();
        assert_eq!(
            rows.rows.len(),
            expected
                .iter()
                .filter(|e| e.owner.table == owned.owner.table)
                .count()
        );
        assert_eq!(owned.owner.created_tx, owned.entry.created_tx);
        assert_eq!(owned.owner.lsn, owned.entry.lsn);
    }
}

fn receive(db: &Database, ddl: DdlChange, generation: u64, connected: bool) -> Result<()> {
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "relay-node".into(),
        source_incarnation: Incarnation(7),
        entries: vec![crate::protocol::ReceivedDdlEntry {
            source_ddl_lsn: Lsn(41),
            ordinal: 0,
            table: Database::ddl_affected_table(&ddl).map(str::to_owned),
            table_generation: Some(generation),
            digest: vec![1],
            author: None,
        }],
    };
    let memory = db.declared_reader_memory().unwrap();
    let mut copy = crate::vector_read_memory::admission(memory.clone());
    let values = HashMap::from([
        ("source_id".into(), Value::Uuid(uuid::Uuid::from_u128(1))),
        ("target_id".into(), Value::Uuid(uuid::Uuid::from_u128(2))),
        ("edge_type".into(), Value::Text("link".into())),
        ("identity".into(), Value::Int64(3)),
    ]);
    let key = NaturalKey::single("identity".into(), Value::Int64(3));
    let rows = if connected {
        vec![RowChange {
            table: "selected_links".into(),
            natural_key: key.clone(),
            values,
            deleted: false,
            lsn: Lsn(42),
            created_at: None,
        }]
    } else {
        Vec::new()
    };
    let lineages = if connected {
        vec![(
            "selected_links".into(),
            key,
            Lsn(42),
            crate::protocol::WireRowLineage {
                author_node_id: "origin".into(),
                author_database_incarnation: Incarnation(17),
                author_local_mutation_position: Lsn(501),
                table_generation: generation,
                lineage_root: format!("author:origin:{}:501", Incarnation(17).to_hex()),
                attestation: vec![0x7f],
            },
        )]
    } else {
        Vec::new()
    };
    db.commit_received_schema_stage_with_receipt_for_test(
        &ChangeSet {
            ddl: vec![ddl],
            ddl_lsn: vec![Lsn(41)],
            rows,
            ..ChangeSet::default()
        },
        &received,
        &lineages,
        &HashMap::new(),
        None,
        false,
        &mut copy,
        memory,
    )
    .map(|result| {
        if connected {
            assert_eq!((result.applied_rows, result.skipped_rows), (1, 0));
            assert!(result.conflicts.is_empty());
        }
    })
}

fn received_identity_lifecycle(drop_selected: bool, file: bool, connected: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("witnesses.redb");
    let mut db = if file {
        Database::open(&path).unwrap()
    } else {
        Database::open_memory()
    };
    let tx = db.begin().unwrap();
    assert!(
        db.insert_edge(
            tx,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            "link".into(),
            HashMap::new()
        )
        .unwrap()
    );
    db.commit(tx).unwrap();
    insert_siblings(&db, "selected_links", connected);
    insert_siblings(&db, "other_links", false);
    let original = witnesses(&db, Direction::Outgoing);
    let standalone = standalone_witnesses(&db, Direction::Outgoing);
    assert_eq!(original.len(), 4);
    assert_witnesses(&db, &original, &standalone);
    let generation = original
        .iter()
        .find(|e| e.owner.table == "selected_links")
        .unwrap()
        .owner
        .table_generation;
    let ddl = if drop_selected {
        DdlChange::DropTable {
            name: "selected_links".into(),
        }
    } else {
        DdlChange::AlterTable {
            name: "selected_links".into(),
            columns: vec![("note".into(), "TEXT".into())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
    };
    if file {
        crate::persistence::arm_received_schema_pre_commit_fault_for_test();
        assert!(receive(&db, ddl.clone(), generation, connected).is_err());
        assert_witnesses(&db, &original, &standalone);
        db.close().unwrap();
        db = Database::open(&path).unwrap();
        assert_witnesses(&db, &original, &standalone);
    }
    receive(&db, ddl, generation, connected).unwrap();
    let mut expected: Vec<_> = original
        .into_iter()
        .filter(|e| !drop_selected || e.owner.table != "selected_links")
        .collect();
    if connected {
        let memory = db.declared_reader_memory().unwrap();
        let mut inspect = |row: &VersionedRow| {
            if row.values.get("identity") == Some(&Value::Int64(3)) {
                assert_eq!(row.deleted_tx, None);
                expected.push(OwnedAdjEntry {
                    owner: contextdb_tx::RowEdgeOwner {
                        table: "selected_links".into(),
                        table_generation: generation,
                        row_id: row.row_id,
                        created_tx: row.created_tx,
                        lsn: row.lsn,
                    },
                    entry: AdjEntry {
                        source: uuid::Uuid::from_u128(1),
                        target: uuid::Uuid::from_u128(2),
                        edge_type: "link".into(),
                        properties: HashMap::new(),
                        created_tx: row.created_tx,
                        deleted_tx: row.deleted_tx,
                        lsn: row.lsn,
                    },
                });
            }
            Ok::<_, Error>(())
        };
        if !db
            .visit_writer_rows("selected_links", &mut inspect, memory)
            .unwrap()
        {
            let tables = db.relational_store.tables.read();
            for row in tables.get("selected_links").unwrap() {
                inspect(row).unwrap();
            }
        }
        assert_eq!(expected.len(), 5);
        expected.sort_by(|a, b| a.owner.cmp(&b.owner));
    }
    assert_witnesses(&db, &expected, &standalone);
    assert_eq!(db.changes_since(Lsn(0)).unwrap().edges.len(), 1);
    if file {
        db.close().unwrap();
        let reopened = Database::open(&path).unwrap();
        assert_witnesses(&reopened, &expected, &standalone);
        let exported = temp.path().join("snapshot.redb");
        reopened.export_snapshot(&exported).unwrap();
        reopened.close().unwrap();
        let restored = Database::open(&exported).unwrap();
        assert_witnesses(&restored, &expected, &standalone);
    }
}

#[test]
fn received_alter_preserves_each_sibling_witness_and_other_table() {
    for reverse in [false, true] {
        received_missing_witness_refuses(false, reverse);
    }
    for file in [false, true] {
        for connected in [false, true] {
            received_identity_lifecycle(false, file, connected);
        }
    }
}

#[test]
fn received_drop_erases_only_selected_table_witnesses() {
    for reverse in [false, true] {
        received_missing_witness_refuses(true, reverse);
    }
    for file in [false, true] {
        received_identity_lifecycle(true, file, false);
    }
}

fn physical_graph(path: &Path) -> BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>> {
    use redb::{ReadableDatabase as _, ReadableTable as _};
    let database = redb::Database::open(path).unwrap();
    let read = database.begin_read().unwrap();
    ["row_graph_fwd", "row_graph_rev", "graph_fwd", "graph_rev"]
        .into_iter()
        .map(|name| {
            let table = read
                .open_table(redb::TableDefinition::<&[u8], &[u8]>::new(name))
                .unwrap();
            let records = table
                .iter()
                .unwrap()
                .map(|item| {
                    let (key, value) = item.unwrap();
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect();
            (name.to_owned(), records)
        })
        .collect()
}

fn received_missing_witness_refuses(drop_selected: bool, reverse: bool) {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let temp = tempfile::TempDir::new().unwrap();
    let path = temp.path().join("missing-witness.redb");
    let db = Database::open(&path).unwrap();
    let tx = db.begin().unwrap();
    assert!(
        db.insert_edge(
            tx,
            uuid::Uuid::from_u128(1),
            uuid::Uuid::from_u128(2),
            "link".into(),
            HashMap::new()
        )
        .unwrap()
    );
    db.commit(tx).unwrap();
    insert_siblings(&db, "selected_links", false);
    insert_siblings(&db, "other_links", false);
    let owners = witnesses(&db, Direction::Outgoing);
    let selected = owners
        .iter()
        .find(|e| e.owner.table == "selected_links")
        .unwrap();
    let row_id = selected.owner.row_id;
    let generation = selected.owner.table_generation;
    let before_lsn = db.current_lsn();
    let before_selected = db
        .execute("SELECT * FROM selected_links", &HashMap::new())
        .unwrap()
        .rows;
    let before_other = db
        .execute("SELECT * FROM other_links", &HashMap::new())
        .unwrap()
        .rows;
    let before_schema = format!("{:?}", db.table_meta("selected_links"));
    db.close().unwrap();
    drop(db);
    Database::remove_row_graph_witness_for_test(&path, "selected_links", row_id, reverse).unwrap();
    let damaged = physical_graph(&path);
    let db = Database::open(&path).unwrap();
    let ddl = if drop_selected {
        DdlChange::DropTable {
            name: "selected_links".into(),
        }
    } else {
        DdlChange::AlterTable {
            name: "selected_links".into(),
            columns: vec![("note".into(), "TEXT".into())],
            constraints: Vec::new(),
            foreign_keys: Vec::new(),
            composite_foreign_keys: Vec::new(),
            composite_unique: Vec::new(),
        }
    };
    assert!(receive(&db, ddl, generation, false).is_err());
    assert_eq!(db.current_lsn(), before_lsn);
    assert_eq!(
        format!("{:?}", db.table_meta("selected_links")),
        before_schema
    );
    assert_eq!(
        db.execute("SELECT * FROM selected_links", &HashMap::new())
            .unwrap()
            .rows,
        before_selected
    );
    assert_eq!(
        db.execute("SELECT * FROM other_links", &HashMap::new())
            .unwrap()
            .rows,
        before_other
    );
    db.close().unwrap();
    drop(db);
    assert_eq!(physical_graph(&path), damaged);
    let reopened = Database::open(&path).unwrap();
    assert_eq!(reopened.current_lsn(), before_lsn);
    assert_eq!(
        format!("{:?}", reopened.table_meta("selected_links")),
        before_schema
    );
    assert_eq!(
        reopened
            .execute("SELECT * FROM selected_links", &HashMap::new())
            .unwrap()
            .rows,
        before_selected
    );
    assert_eq!(
        reopened
            .execute("SELECT * FROM other_links", &HashMap::new())
            .unwrap()
            .rows,
        before_other
    );
}
