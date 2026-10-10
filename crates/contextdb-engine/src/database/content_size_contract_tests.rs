//! SQL byte totals checked by counting raw persisted values, independently of maintained totals.
use super::*;
use redb::{ReadableDatabase as _, ReadableTable, TableDefinition};
use std::collections::HashSet;

#[derive(Clone, Copy)]
struct FrozenClock;
impl contextdb_core::read_contract::DeadlineClock for FrozenClock {
    fn now_ms(&self) -> u64 {
        0
    }
    fn wait_until(&self, _: u64) -> contextdb_core::read_contract::DeadlineWait<'_> {
        Box::pin(async {})
    }
}

fn open_size_cursor(
    database: Arc<Database>,
) -> crate::executor::bounded_read_test_support::TestCursorOpen {
    use crate::executor::bounded_read_test_support as bounded;
    let request = bounded::BoundedReadRequest::new(
        "SHOW CONTENT_SIZE",
        HashMap::new(),
        contextdb_core::read_contract::ReadLimits::default(),
        Arc::new(FrozenClock),
    );
    bounded::open_cursor(database, &request).expect("open content size cursor on its captured root")
}

#[derive(Debug, Default, PartialEq, Eq)]
struct BodyBytes {
    live_rows: u64,
    live_vectors: u64,
    retained_rows: u64,
    retained_vectors: u64,
}

fn row_key(row: &VersionedRow) -> Vec<u8> {
    [
        row.row_id.0.to_be_bytes(),
        row.created_tx.0.to_be_bytes(),
        row.lsn.0.to_be_bytes(),
    ]
    .concat()
}

// Decode the authenticated placement framing solely to classify canonical keys.
// Body prices remain the complete raw stored value lengths.
fn membership_payload(encoded: &[u8]) -> &[u8] {
    let magic = b"contextdb-vector-partition";
    assert!(encoded.starts_with(magic));
    let mut cursor = magic.len();
    assert_eq!(&encoded[cursor..cursor + 2], &1u16.to_be_bytes());
    cursor += 2;
    assert_eq!(encoded[cursor], 1);
    cursor += 1;
    let len = usize::try_from(u64::from_be_bytes(
        encoded[cursor..cursor + 8].try_into().unwrap(),
    ))
    .unwrap();
    cursor += 8;
    assert_eq!(cursor + len + 32, encoded.len());
    assert_eq!(
        &encoded[cursor + len..],
        blake3::hash(&encoded[..cursor + len]).as_bytes()
    );
    &encoded[cursor..cursor + len]
}

// Never opens the maintained byte-total table or calls its pricing helper.
fn physical_oracle(database: &Database, names: &[&str]) -> BodyBytes {
    let snapshot = database.snapshot();
    let live_rows = names
        .iter()
        .map(|name| {
            let keys = database
                .scan(name, snapshot)
                .expect("read current row identities")
                .iter()
                .map(row_key)
                .collect::<HashSet<_>>();
            (*name, keys)
        })
        .collect::<Vec<_>>();
    database
        .persistence
        .as_ref()
        .expect("file fixture has persistence")
        .with_db(|file| {
            let read = file
                .begin_read()
                .expect("capture independent raw-value root");
            let mut bytes = BodyBytes::default();
            for (name, keys) in &live_rows {
                let table_name = format!("rel_{name}");
                let table = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new(&table_name))
                    .expect("open persisted row values");
                for entry in table.iter().expect("iterate persisted row values") {
                    let (key, value) = entry.expect("read raw row key and value");
                    if keys.contains(key.value()) {
                        bytes.live_rows += value.value().len() as u64;
                    } else {
                        bytes.retained_rows += value.value().len() as u64;
                    }
                }
            }
            let memberships = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(
                    "vector_partition_membership",
                ))
                .expect("open physical placement records");
            let mut live_bodies = HashSet::new();
            for entry in memberships.iter().expect("iterate placement records") {
                let (_, value) = entry.expect("read placement record");
                let (record, consumed): (
                    crate::persistence::VectorPartitionMembershipRecord,
                    usize,
                ) = bincode::serde::decode_from_slice(
                    membership_payload(value.value()),
                    bincode::config::standard(),
                )
                .expect("decode placement identity independently of total maintenance");
                assert_eq!(consumed, membership_payload(value.value()).len());
                if record.visible_from.0 <= snapshot.0
                    && record.deleted_tx.is_none_or(|tx| snapshot.0 < tx.0)
                {
                    let mut key = record.index.table.as_bytes().to_vec();
                    key.push(0);
                    key.extend_from_slice(record.index.column.as_bytes());
                    key.push(0);
                    key.extend_from_slice(&record.row_id.0.to_be_bytes());
                    key.extend_from_slice(&record.vector_created_tx.0.to_be_bytes());
                    key.extend_from_slice(&record.vector_lsn.0.to_be_bytes());
                    live_bodies.insert(key);
                }
            }
            let vectors = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new("vector_entries"))
                .expect("open canonical vector bodies");
            for entry in vectors
                .iter()
                .expect("iterate canonical vector bodies once")
            {
                let (key, value) = entry.expect("read canonical body");
                if live_bodies.contains(key.value()) {
                    bytes.live_vectors += value.value().len() as u64;
                } else {
                    bytes.retained_vectors += value.value().len() as u64;
                }
            }
            Ok(bytes)
        })
        .expect("count raw persisted value lengths")
}

fn assert_sql_totals(database: &Database, expected: &BodyBytes) {
    let answer = database
        .execute("SHOW CONTENT_SIZE", &HashMap::new())
        .expect("inspect committed body sizes");
    assert_eq!(
        answer.columns,
        [
            "live_row_bytes",
            "live_vector_bytes",
            "live_content_bytes",
            "retained_row_bytes",
            "retained_vector_bytes"
        ]
    );
    let values = [
        expected.live_rows,
        expected.live_vectors,
        expected.live_rows + expected.live_vectors,
        expected.retained_rows,
        expected.retained_vectors,
    ]
    .into_iter()
    .map(|n| Value::Int64(i64::try_from(n).expect("fixture byte count fits SQL integer")))
    .collect::<Vec<_>>();
    assert_eq!(answer.rows, vec![values]);
}

fn mutate(database: &Database, sql: &str, params: HashMap<String, Value>) {
    database
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture mutation {sql}: {error}"));
}

#[test]
fn file_content_totals_match_persisted_values_through_replacement_delete_and_prune() {
    let directory = tempfile::tempdir().expect("file fixture directory");
    let database =
        Arc::new(Database::open(directory.path().join("content.db")).expect("open file fixture"));
    database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    mutate(
        &database,
        "CREATE TABLE content_rows (id INTEGER PRIMARY KEY, payload TEXT, embedding VECTOR(3)) HISTORY ALL SYNC OFF",
        HashMap::new(),
    );
    mutate(
        &database,
        "INSERT INTO content_rows (id, payload, embedding) VALUES (1, $payload, $embedding)",
        HashMap::from([
            ("payload".into(), Value::Text("first".repeat(31))),
            ("embedding".into(), Value::Vector(vec![1.0, 2.0, 3.0])),
        ]),
    );
    let first = physical_oracle(&database, &["content_rows"]);
    assert!(first.live_rows > 0 && first.live_vectors > 0);
    assert_sql_totals(&database, &first);
    let mut held_cursor = open_size_cursor(database.clone());
    assert_eq!(
        held_cursor.first_page.rows[0][2],
        Value::Int64((first.live_rows + first.live_vectors) as i64)
    );
    assert!(!held_cursor.first_page.has_more);
    let snapshot = database.snapshot();
    let pin = database.pin_snapshot(snapshot).unwrap();
    mutate(
        &database,
        "UPDATE content_rows SET payload = $payload, embedding = $embedding WHERE id = 1",
        HashMap::from([
            ("payload".into(), Value::Text("replacement".repeat(97))),
            ("embedding".into(), Value::Vector(vec![3.0, 2.0, 1.0])),
        ]),
    );
    let replaced = physical_oracle(&database, &["content_rows"]);
    assert!(replaced.retained_rows > 0 && replaced.retained_vectors > 0);
    assert_sql_totals(&database, &replaced);
    let mut current_cursor = open_size_cursor(database.clone());
    assert_eq!(
        current_cursor.first_page.rows[0][2],
        Value::Int64((replaced.live_rows + replaced.live_vectors) as i64)
    );
    assert_eq!(
        held_cursor.first_page.rows[0][2],
        Value::Int64((first.live_rows + first.live_vectors) as i64)
    );
    current_cursor
        .cursor
        .close()
        .expect("release current cursor");
    drop(current_cursor);
    mutate(
        &database,
        "DELETE FROM content_rows WHERE id = 1",
        HashMap::new(),
    );
    let deleted = physical_oracle(&database, &["content_rows"]);
    assert_eq!(deleted.live_rows, 0);
    assert_eq!(deleted.live_vectors, 0);
    assert!(deleted.retained_rows > replaced.retained_rows);
    assert_sql_totals(&database, &deleted);
    let held = database
        .scan("content_rows", snapshot)
        .expect("held snapshot remains readable");
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0].values.get("payload"),
        Some(&Value::Text("first".repeat(31)))
    );
    mutate(
        &database,
        "ALTER TABLE content_rows SET HISTORY CURRENT ONLY",
        HashMap::new(),
    );
    database
        .compact_currency_versions()
        .expect("prune with an actual snapshot holder");
    assert_sql_totals(&database, &physical_oracle(&database, &["content_rows"]));
    assert_eq!(
        database
            .scan("content_rows", snapshot)
            .expect("pin preserves required row")
            .len(),
        1
    );
    held_cursor
        .cursor
        .close()
        .expect("release held content cursor");
    drop(held_cursor);
    drop(pin);
    database
        .compact_currency_versions()
        .expect("prune after final snapshot release");
    let pruned = physical_oracle(&database, &["content_rows"]);
    assert!(pruned.retained_rows < deleted.retained_rows);
    assert!(pruned.retained_vectors < deleted.retained_vectors);
    assert_sql_totals(&database, &pruned);
    assert_received_disjoint_file_export_restore();
    assert_ordinary_delete_move_file_export_restore();
}

#[test]
fn content_size_reads_one_total_per_table_at_fixed_table_count() {
    let mut observations = Vec::new();
    for rows in [32, 64] {
        let directory = tempfile::tempdir().expect("table-count fixture directory");
        let database = Database::open(directory.path().join("work.db")).expect("open work fixture");
        database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
        for name in ["count_rows", "empty_rows"] {
            mutate(
                &database,
                &format!(
                    "CREATE TABLE {name} (id INTEGER PRIMARY KEY, payload TEXT, embedding VECTOR(3)) HISTORY ALL"
                ),
                HashMap::new(),
            );
        }
        for id in 0..rows {
            mutate(
                &database,
                "INSERT INTO count_rows (id, payload, embedding) VALUES ($id, $payload, $embedding)",
                HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("payload".into(), Value::Text("initial".repeat(17))),
                    ("embedding".into(), Value::Vector(vec![1.0, 2.0, 3.0])),
                ]),
            );
            mutate(
                &database,
                "UPDATE count_rows SET payload = $payload, embedding = $embedding WHERE id = $id",
                HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("payload".into(), Value::Text("changed".repeat(29))),
                    ("embedding".into(), Value::Vector(vec![3.0, 2.0, 1.0])),
                ]),
            );
        }
        let expected = physical_oracle(&database, &["count_rows", "empty_rows"]);
        let table_count = database
            .persistence
            .as_ref()
            .unwrap()
            .with_db(|file| {
                let read = file.begin_read().unwrap();
                let meta = read
                    .open_table(TableDefinition::<&str, &[u8]>::new("meta"))
                    .unwrap();
                Ok(meta
                    .iter()
                    .unwrap()
                    .map(|entry| entry.unwrap().0.value().starts_with("table:"))
                    .filter(|is_table| *is_table)
                    .count() as u64)
            })
            .unwrap();
        let (_, work) =
            crate::writer_query_test_seams::observe(|| assert_sql_totals(&database, &expected));
        assert_eq!(work.content_size_table_totals_visits, table_count);
        assert_eq!(work.history.row_versions, 0);
        assert_eq!(work.history.vector_memberships, 0);
        assert_eq!(work.history.index_rebuild_inputs, 0);
        assert_eq!(work.history.change_log_entries, 0);
        assert_eq!(work.history.commit_index_entries, 0);
        observations.push((table_count, work.content_size_table_totals_visits, expected));
    }
    assert!(observations[0].0 >= 2);
    assert_eq!(observations[0].0, observations[1].0);
    assert_eq!(observations[0].1, observations[1].1);
    assert!(observations[1].2.live_rows > observations[0].2.live_rows);
    assert!(observations[1].2.retained_rows > observations[0].2.retained_rows);
    assert!(observations[1].2.live_vectors > observations[0].2.live_vectors);
    assert!(observations[1].2.retained_vectors > observations[0].2.retained_vectors);
}

fn assert_received_disjoint_file_export_restore() {
    use crate::persistence::VectorPartitionMembershipRecord;
    let directory = tempfile::tempdir().expect("file-backed disjoint placement fixture");
    let path = directory.path().join("received-placement.redb");
    let artifact = directory.path().join("received-placement.snapshot");
    let declared = Database::open(&path).expect("declare the file store");
    declared.execute(
        "CREATE TABLE placements (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, embedding VECTOR(2) PARTITION_KEY(scope) MAX_PARTITIONS 2) HISTORY ALL",
        &HashMap::new(),
    ).expect("declare both placement routes");
    let meta = declared
        .table_meta("placements")
        .expect("declared placement schema");
    declared
        .close()
        .expect("close before the physical producer fixture");
    let index = VectorIndexRef::new("placements", "embedding");
    let key = |scope: &str| VectorPartitionKey::from_values(&[Value::Text(scope.into())]).unwrap();
    let placements = [
        VectorPartitionMembershipRecord {
            index: index.clone(),
            partition_key: key("first"),
            row_id: RowId(1),
            row_created_tx: TxId(3),
            row_lsn: Lsn(3),
            vector_created_tx: TxId(2),
            vector_lsn: Lsn(2),
            visible_from: TxId(4),
            deleted_tx: Some(TxId(7)),
        },
        VectorPartitionMembershipRecord {
            index: index.clone(),
            partition_key: key("second"),
            row_id: RowId(1),
            row_created_tx: TxId(8),
            row_lsn: Lsn(8),
            vector_created_tx: TxId(2),
            vector_lsn: Lsn(2),
            visible_from: TxId(8),
            deleted_tx: None,
        },
    ];
    {
        let fixture_open_accountant_412 = Arc::new(MemoryAccountant::no_limit());
        let _fixture_decode_scope_412 = crate::read_image_memory::ImageMemory::enter_writer_operation(std::sync::Arc::clone(&fixture_open_accountant_412)).unwrap();
        let persistence = RedbPersistence::open(&path).expect("open the physical producer");
        persistence.attach_memory_accountant(std::sync::Arc::clone(&fixture_open_accountant_412));
        persistence
            .append_table_rows_batch(
                "placements",
                Some(&meta),
                &[
                    VersionedRow {
                        row_id: RowId(1),
                        values: HashMap::from([
                            ("id".into(), Value::Int64(1)),
                            ("scope".into(), Value::Text("first".into())),
                            ("embedding".into(), Value::Vector(vec![1.0, 0.0])),
                        ]),
                        created_tx: TxId(3),
                        deleted_tx: Some(TxId(7)),
                        lsn: Lsn(3),
                        created_at: None,
                    },
                    VersionedRow {
                        row_id: RowId(1),
                        values: HashMap::from([
                            ("id".into(), Value::Int64(1)),
                            ("scope".into(), Value::Text("second".into())),
                            ("embedding".into(), Value::Vector(vec![1.0, 0.0])),
                        ]),
                        created_tx: TxId(8),
                        deleted_tx: None,
                        lsn: Lsn(8),
                        created_at: None,
                    },
                ],
            )
            .expect("persist independent accepted row versions");
        persistence
            .append_vector_entries_batch(
                &[VectorEntry {
                    index: index.clone(),
                    row_id: RowId(1),
                    vector: vec![1.0, 0.0],
                    created_tx: TxId(2),
                    deleted_tx: None,
                    lsn: Lsn(2),
                }],
                &HashMap::new(),
            )
            .expect("persist exactly one canonical body with no tombstone");
        persistence
            .append_vector_partition_memberships_batch(&placements)
            .expect("the production producer accepts disjoint independent intervals");
        persistence
            .flush_commit_index_entries(&(1..=8).map(|id| (Lsn(id), TxId(id))).collect())
            .expect("publish the exact committed fixture frontier");
        persistence
            .rebuild_relational_indexes_for_current_table("placements", &meta)
            .expect("publish indexes for the accepted rows");
    }
    let body_image = |database: &Database| {
        let persistence = database
            .persistence
            .as_ref()
            .expect("file-backed publication");
        for expected in &placements {
            let actual = persistence
                .load_vector_partition_memberships_for_row(
                    &expected.index,
                    &expected.partition_key,
                    expected.row_id,
                )
                .expect("read the exact durable placement");
            assert_eq!(actual.len(), 1);
            let actual = &actual[0];
            assert_eq!(actual.index, expected.index);
            assert_eq!(actual.partition_key, expected.partition_key);
            assert_eq!(actual.row_created_tx, expected.row_created_tx);
            assert_eq!(actual.row_lsn, expected.row_lsn);
            assert_eq!(actual.vector_created_tx, expected.vector_created_tx);
            assert_eq!(actual.vector_lsn, expected.vector_lsn);
            assert_eq!(actual.visible_from, expected.visible_from);
            assert_eq!(actual.deleted_tx, expected.deleted_tx);
        }
        let raw = persistence
            .with_db(|file| {
                let read = file.begin_read().unwrap();
                let vectors = read
                    .open_table(TableDefinition::<&[u8], &[u8]>::new("vector_entries"))
                    .unwrap();
                let entries = vectors
                    .iter()
                    .unwrap()
                    .map(|entry| {
                        let (key, value) = entry.unwrap();
                        (key.value().to_vec(), value.value().to_vec())
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    entries.len(),
                    1,
                    "both placements own one physical canonical body"
                );
                Ok(entries)
            })
            .expect("capture the independent raw canonical body value");
        let totals = physical_oracle(database, &["placements"]);
        assert_eq!(totals.live_vectors, raw[0].1.len() as u64);
        assert_eq!(totals.retained_vectors, 0);
        assert_sql_totals(database, &totals);
        raw
    };
    let database = Database::open(&path).expect("open the exact disjoint source image");
    database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    let canonical_none_body = body_image(&database);
    let ddl = DdlChange::AlterTable {
        name: "placements".into(),
        columns: vec![("second_embedding".into(), "VECTOR(2)".into())],
        constraints: Vec::new(),
        foreign_keys: Vec::new(),
        composite_foreign_keys: Vec::new(),
        composite_unique: Vec::new(),
    };
    let received = crate::protocol::ReceivedDdlContext {
        tenant_id: TenantId::from("tenant-a"),
        source_node_id: "placement-author".into(),
        source_incarnation: Incarnation(7),
        entries: vec![crate::protocol::ReceivedDdlEntry {
            source_ddl_lsn: Lsn(41),
            ordinal: 0,
            table: Some("placements".into()),
            table_generation: Some(1),
            digest: vec![1],
            author: None,
        }],
    };
    let reader = database
        .declared_reader_memory()
        .expect("admit the received publication reader");
    let mut copy_owner = crate::vector_read_memory::admission(reader.clone());
    database
        .commit_received_schema_stage_for_test(
            &ChangeSet {
                ddl: vec![ddl],
                ddl_lsn: vec![Lsn(41)],
                ..ChangeSet::default()
            },
            &received,
            &[],
            &HashMap::new(),
            false,
            &mut copy_owner,
            reader,
        )
        .expect("received schema publishes the exact retained body and both placements");
    assert_eq!(
        body_image(&database),
        canonical_none_body,
        "received publication preserves the canonical None tombstone and complete persisted body bytes"
    );
    database
        .export_snapshot(&artifact)
        .expect("export the received exact image");
    database
        .close()
        .expect("close the source before restoring its artifact");
    let restored = Database::open(&artifact).expect("restore the exported file image");
    assert_eq!(
        body_image(&restored),
        canonical_none_body,
        "restore preserves both independent placements and the complete canonical None body value"
    );
    restored.close().expect("close the restored received image");
}

// Compare complete durable images, including retired canonical bodies and closed
// placement records, rather than reconstructing body identity from live rows.
fn canonical_and_membership_image(database: &Database) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    database
        .persistence
        .as_ref()
        .expect("file persistence")
        .with_db(|file| {
            let read = file
                .begin_read()
                .expect("capture the physical publication root");
            Ok(["vector_entries", "vector_partition_membership"]
                .into_iter()
                .map(|name| {
                    let table = read
                        .open_table(TableDefinition::<&[u8], &[u8]>::new(name))
                        .expect("open the actual physical vector family");
                    table
                        .iter()
                        .expect("iterate the complete durable family")
                        .map(|entry| {
                            let (key, value) = entry.expect("read the complete durable record");
                            (key.value().to_vec(), value.value().to_vec())
                        })
                        .collect()
                })
                .collect())
        })
        .expect("capture exact canonical and placement bytes")
}

fn assert_ordinary_delete_move_file_export_restore() {
    let directory = tempfile::tempdir().expect("ordinary mutation export fixture");
    let database = Database::open(directory.path().join("ordinary.redb")).expect("open file store");
    database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    mutate(
        &database,
        "CREATE TABLE canonical_moves (id INTEGER PRIMARY KEY, scope TEXT NOT NULL, embedding VECTOR(2) PARTITION_KEY(scope) MAX_PARTITIONS 2) HISTORY ALL SYNC OFF",
        HashMap::new(),
    );
    for (id, vector) in [(1, vec![1.0, 0.0]), (2, vec![1.0, 1.0])] {
        mutate(
            &database,
            "INSERT INTO canonical_moves (id, scope, embedding) VALUES ($id, 'old', $embedding)",
            HashMap::from([
                ("id".into(), Value::Int64(id)),
                ("embedding".into(), Value::Vector(vector)),
            ]),
        );
    }
    mutate(
        &database,
        "UPDATE canonical_moves SET embedding = $embedding WHERE id = 1",
        HashMap::from([("embedding".into(), Value::Vector(vec![0.0, 1.0]))]),
    );
    let row_id = |id| {
        database
            .scan("canonical_moves", database.snapshot())
            .expect("read actual prepared row identity")
            .iter()
            .find(|row| row.values.get("id") == Some(&Value::Int64(id)))
            .expect("seeded row exists")
            .row_id
    };
    let deleted_row = row_id(1);
    let moved_row = row_id(2);
    let persistence = database.persistence.as_ref().expect("file persistence");
    let before = persistence
        .load_vectors()
        .expect("read exact canonical before identities");
    assert_eq!(
        before.len(),
        3,
        "replacement retains a distinct original canonical body"
    );
    let delete_before = before
        .iter()
        .find(|body| body.row_id == deleted_row && body.deleted_tx.is_none())
        .expect("actual live DELETE source");
    let move_before = before
        .iter()
        .find(|body| body.row_id == moved_row && body.deleted_tx.is_none())
        .expect("actual live MOVE source");
    assert!(
        before
            .iter()
            .any(|body| body.row_id == deleted_row && body.deleted_tx.is_some())
    );
    let before_memberships = persistence
        .load_vector_partition_memberships()
        .expect("read complete before placements");
    mutate(
        &database,
        "DELETE FROM canonical_moves WHERE id = 1",
        HashMap::new(),
    );
    let delete_tx = TxId(database.snapshot().0);
    let after_delete = persistence
        .load_vectors()
        .expect("read canonical DELETE publication");
    assert_eq!(after_delete.len(), before.len());
    for original in &before {
        let actual = after_delete
            .iter()
            .find(|body| {
                body.index == original.index
                    && body.row_id == original.row_id
                    && body.created_tx == original.created_tx
                    && body.lsn == original.lsn
            })
            .expect("every original canonical identity survives");
        assert_eq!(actual.vector, original.vector);
        assert_eq!(
            actual.deleted_tx,
            if original.row_id == delete_before.row_id
                && original.created_tx == delete_before.created_tx
                && original.lsn == delete_before.lsn
            {
                Some(delete_tx)
            } else {
                original.deleted_tx
            },
            "DELETE stamps only its actual prepared before body"
        );
    }
    let delete_memberships = persistence
        .load_vector_partition_memberships()
        .expect("read complete DELETE placements");
    assert_eq!(delete_memberships.len(), before_memberships.len());
    for original in &before_memberships {
        let actual = delete_memberships
            .iter()
            .find(|record| {
                record.index == original.index
                    && record.partition_key == original.partition_key
                    && record.row_id == original.row_id
                    && record.row_created_tx == original.row_created_tx
                    && record.row_lsn == original.row_lsn
                    && record.vector_created_tx == original.vector_created_tx
                    && record.vector_lsn == original.vector_lsn
                    && record.visible_from == original.visible_from
            })
            .expect("every independent placement identity survives DELETE");
        assert_eq!(
            actual.deleted_tx,
            if original.row_id == deleted_row && original.deleted_tx.is_none() {
                Some(delete_tx)
            } else {
                original.deleted_tx
            }
        );
    }
    let assert_export = |label: &str| {
        let expected = canonical_and_membership_image(&database);
        let totals = physical_oracle(&database, &["canonical_moves"]);
        assert_sql_totals(&database, &totals);
        let artifact = directory.path().join(format!("{label}.snapshot"));
        database
            .export_snapshot(&artifact)
            .expect("export ordinary mutation canonical history");
        let restored = Database::open(&artifact).expect("restore ordinary mutation export");
        assert_eq!(
            canonical_and_membership_image(&restored),
            expected,
            "restore preserves every exact body key/value and every complete independent membership record"
        );
        assert_eq!(physical_oracle(&restored, &["canonical_moves"]), totals);
        assert_sql_totals(&restored, &totals);
        restored.close().expect("close restored file export");
    };
    assert_export("after-delete");
    mutate(
        &database,
        "UPDATE canonical_moves SET scope = 'new' WHERE id = 2",
        HashMap::new(),
    );
    let move_tx = TxId(database.snapshot().0);
    let after_move = persistence
        .load_vectors()
        .expect("read canonical MOVE publication");
    assert_eq!(after_move.len(), after_delete.len() + 1);
    for original in &after_delete {
        let actual = after_move
            .iter()
            .find(|body| {
                body.index == original.index
                    && body.row_id == original.row_id
                    && body.created_tx == original.created_tx
                    && body.lsn == original.lsn
            })
            .expect("every prior canonical identity survives MOVE");
        assert_eq!(actual.vector, original.vector);
        assert_eq!(
            actual.deleted_tx,
            if original.row_id == move_before.row_id
                && original.created_tx == move_before.created_tx
                && original.lsn == move_before.lsn
            {
                Some(move_tx)
            } else {
                original.deleted_tx
            },
            "MOVE stamps only its actual prepared before body"
        );
    }
    let fresh = after_move
        .iter()
        .filter(|body| {
            !after_delete.iter().any(|old| {
                old.index == body.index
                    && old.row_id == body.row_id
                    && old.created_tx == body.created_tx
                    && old.lsn == body.lsn
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(fresh.len(), 1);
    assert_eq!(fresh[0].created_tx, move_tx);
    assert_eq!(fresh[0].deleted_tx, None);
    assert_eq!(fresh[0].vector, move_before.vector);
    let move_memberships = persistence
        .load_vector_partition_memberships()
        .expect("read complete MOVE placements");
    assert_eq!(move_memberships.len(), delete_memberships.len() + 1);
    for original in &delete_memberships {
        let actual = move_memberships
            .iter()
            .find(|record| {
                record.index == original.index
                    && record.partition_key == original.partition_key
                    && record.row_id == original.row_id
                    && record.row_created_tx == original.row_created_tx
                    && record.row_lsn == original.row_lsn
                    && record.vector_created_tx == original.vector_created_tx
                    && record.vector_lsn == original.vector_lsn
                    && record.visible_from == original.visible_from
            })
            .expect("every independent placement identity survives MOVE");
        assert_eq!(
            actual.deleted_tx,
            if original.row_id == moved_row && original.deleted_tx.is_none() {
                Some(move_tx)
            } else {
                original.deleted_tx
            }
        );
    }
    let target = move_memberships
        .iter()
        .find(|record| {
            record.vector_created_tx == fresh[0].created_tx
                && record.vector_lsn == fresh[0].lsn
                && record.row_id == fresh[0].row_id
        })
        .expect("fresh exact MOVE target membership");
    assert_eq!(
        target.partition_key,
        VectorPartitionKey::from_values(&[Value::Text("new".into())]).unwrap()
    );
    assert_eq!(target.visible_from, move_tx);
    assert_eq!(target.deleted_tx, None);
    assert_export("after-move");
    database.close().expect("close ordinary mutation source");
}

// Acceptance: clearing a vector after a partition move returns NULL through reopen.
// Active cursors retain their committed vector; new statements after clear see NULL.
#[test]
fn vector_clear_after_partition_move_preserves_null_and_reader_snapshot() {
    const ARM: &str = "CONTEXTDB_VECTOR_MOVE_CLEAR_PROOF_ARM";
    if let Ok(arm) = std::env::var(ARM) {
        let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("vector-move-clear.redb");
        let open = || {
            Database::open_with_options(
                &path,
                DatabaseOpenOptions {
                    maintenance_policy: MaintenancePolicy::CallerDriven,
                    memory_limit: Some(64 * 1024 * 1024),
                    ..DatabaseOpenOptions::default()
                },
            )
            .expect("open vector clear file")
        };
        let mut db = open();
        db.execute(
            "CREATE TABLE items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(16) PARTITION_KEY (scope) MAX_PARTITIONS 2, revision INT) HISTORY ALL SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
            &HashMap::new(),
        ).unwrap();
        // Same deterministic 16-component vector as the resident history fixture.
        let mut vector = vec![0.0; 16];
        vector[0] = 1.0;
        vector[1] = (1 + 1) as f32 / 1024.0;
        vector[2] = ((1 * 17) % 251) as f32 / 1024.0;
        db.execute(
            "INSERT INTO items VALUES ($id,0,$v,1)",
            &HashMap::from([
                ("id".into(), Value::Int64(1)),
                ("v".into(), Value::Vector(vector.clone())),
            ]),
        )
        .unwrap();
        if arm == "reopened-before-move" {
            db.close().unwrap();
            drop(db);
            db = open();
        }
        // A leading NULL row suspends the real cursor before it fetches id=1.
        db.execute("INSERT INTO items VALUES (0,0,NULL,0)", &HashMap::new())
            .unwrap();
        db.execute("UPDATE items SET scope = 1 WHERE id = 1", &HashMap::new())
            .unwrap();
        assert_eq!(
            db.execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Vector(vector.clone())]],
            "non-vector partition-key update preserves the F32 body"
        );
        db.close().unwrap();
        drop(db);
        db = open();
        assert_eq!(
            db.execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
                .unwrap()
                .rows,
            vec![vec![Value::Vector(vector.clone())]],
            "non-vector update preserves the F32 body through reopen"
        );
        let old_reader = crate::ReadSession::open_with_options(
            &path,
            crate::ReadSessionOptions {
                limits: contextdb_core::read_contract::ReadLimits {
                    cursor_page_rows: 1,
                    ..contextdb_core::read_contract::ReadLimits::default()
                },
                ..crate::ReadSessionOptions::default()
            },
        )
        .expect("open reader before vector clear");
        let old_before_clear = old_reader
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .expect("read committed vector before clear")
            .rows;
        let mut active_cursor = old_reader
            .open_cursor("SELECT v FROM items ORDER BY id", &HashMap::new())
            .expect("start the vector statement before clear");
        assert_eq!(active_cursor.first_page().rows, vec![vec![Value::Null]]);
        assert!(
            active_cursor.first_page().has_more,
            "target vector must remain unfetched"
        );
        db.execute("UPDATE items SET v = NULL WHERE id = 1", &HashMap::new())
            .unwrap();
        let fresh_answer = db
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows;
        let old_after_clear = active_cursor
            .fetch(None)
            .expect("active statement keeps its vector after clear")
            .rows;
        active_cursor.close().unwrap();
        let retained_session_after_clear = old_reader
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .expect("new statement on retained session sees clear")
            .rows;
        let new_reader = crate::ReadSession::open(&path).expect("open reader after vector clear");
        let new_after_clear = new_reader
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .expect("new reader sees the committed clear")
            .rows;
        drop(new_reader);
        drop(old_reader);
        db.close().unwrap();
        drop(db);
        let db = open();
        let reopened_answer = db
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .unwrap()
            .rows;
        eprintln!(
            "VECTOR_MOVE_CLEAR_ANSWER arm={arm} phase=reopened-after-clear answer={reopened_answer:?}"
        );
        let reopened_reader = crate::ReadSession::open(&path).expect("open reader after reopen");
        let reader_after_reopen = reopened_reader
            .execute("SELECT v FROM items WHERE id = 1", &HashMap::new())
            .expect("reopened reader sees the committed clear")
            .rows;
        drop(reopened_reader);
        db.close().unwrap();
        assert_eq!(
            old_before_clear,
            vec![vec![Value::Vector(vector.clone())]],
            "reader captured the populated vector before clear"
        );
        assert_eq!(
            old_after_clear,
            vec![vec![Value::Vector(vector)]],
            "pre-clear active statement retains its committed vector"
        );
        assert_eq!(
            retained_session_after_clear,
            vec![vec![Value::Null]],
            "new statement on retained session sees NULL"
        );
        assert_eq!(
            new_after_clear,
            vec![vec![Value::Null]],
            "post-clear reader sees NULL"
        );
        assert_eq!(
            reader_after_reopen,
            vec![vec![Value::Null]],
            "reader after reopen sees NULL"
        );
        assert_eq!(
            fresh_answer,
            vec![vec![Value::Null]],
            "committed NULL answer after move then clear"
        );
        assert_eq!(
            reopened_answer,
            vec![vec![Value::Null]],
            "same committed NULL answer after reopen"
        );
        return;
    }
    let exact = format!(
        "{}::vector_clear_after_partition_move_preserves_null_and_reader_snapshot",
        module_path!().split_once("::").unwrap().1
    );
    let mut failed = Vec::new();
    for arm in ["fresh-open", "reopened-before-move"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &exact, "--nocapture", "--test-threads=1"])
            .env(ARM, arm)
            .output()
            .unwrap();
        if !output.status.success() {
            failed.push(arm);
        }
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("phase=reopened-after-clear"),
            "child must reach its reopened-after-clear answer: {arm}"
        );
    }
    assert!(
        failed.is_empty(),
        "committed NULL answer failed in fresh-process arms {failed:?}"
    );
}

// Acceptance: quantized live bodies remain readable, while a committed clear is NULL.
#[test]
fn sq8_live_vector_hydrates_but_cleared_vector_remains_null() {
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("quantized-clear.redb");
    let open = || {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                memory_limit: Some(64 * 1024 * 1024),
                ..DatabaseOpenOptions::default()
            },
        )
        .expect("open quantized clear file")
    };
    let db = open();
    db.execute(
        "CREATE TABLE compact_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(8) WITH (quantization = 'SQ8') PARTITION_KEY (scope) MAX_PARTITIONS 2) HISTORY ALL",
        &HashMap::new(),
    ).expect("declare quantized vector fixture");
    let input = vec![1.0, 0.8, 0.6, 0.4, 0.2, 0.0, -0.2, -0.4];
    let expected = vec![vec![Value::Vector(contextdb_vector::stored_vector_value(
        &input,
        contextdb_core::table_meta::VectorQuantization::SQ8,
    ))]];
    db.execute(
        "INSERT INTO compact_items VALUES (1,0,$v)",
        &HashMap::from([("v".into(), Value::Vector(input))]),
    )
    .expect("persist quantized body");
    db.execute(
        "INSERT INTO compact_items VALUES (0,0,NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);
    // Reopen removes any staged payload and requires the persisted SQ8 body.
    let db = open();
    let live_answer = db
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("hydrate live quantized vector after reopen")
        .rows;
    let old_reader = crate::ReadSession::open_with_options(
        &path,
        crate::ReadSessionOptions {
            limits: contextdb_core::read_contract::ReadLimits {
                cursor_page_rows: 1,
                ..contextdb_core::read_contract::ReadLimits::default()
            },
            ..crate::ReadSessionOptions::default()
        },
    )
    .expect("capture live SQ8 reader");
    let old_before_clear = old_reader
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("reader hydrates live quantized body")
        .rows;
    let mut active_cursor = old_reader
        .open_cursor("SELECT v FROM compact_items ORDER BY id", &HashMap::new())
        .expect("start SQ8 vector statement before clear");
    assert_eq!(active_cursor.first_page().rows, vec![vec![Value::Null]]);
    assert!(
        active_cursor.first_page().has_more,
        "SQ8 target must remain unfetched"
    );
    db.execute(
        "UPDATE compact_items SET v = NULL WHERE id = 1",
        &HashMap::new(),
    )
    .expect("commit quantized clear");
    let cleared_answer = db
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("read cleared quantized vector")
        .rows;
    let old_after_clear = active_cursor
        .fetch(None)
        .expect("active statement keeps live quantized body")
        .rows;
    active_cursor.close().unwrap();
    let retained_session_after_clear = old_reader
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("new SQ8 statement on retained session sees clear")
        .rows;
    let new_reader = crate::ReadSession::open(&path).expect("capture cleared SQ8 reader");
    let new_after_clear = new_reader
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("new reader sees quantized clear")
        .rows;
    drop(new_reader);
    drop(old_reader);
    db.close().unwrap();
    drop(db);
    let db = open();
    let reopened_answer = db
        .execute("SELECT v FROM compact_items WHERE id = 1", &HashMap::new())
        .expect("quantized clear stays NULL after reopen")
        .rows;
    db.close().unwrap();
    assert_eq!(
        live_answer, expected,
        "live SQ8 placeholder hydrates its quantized body"
    );
    assert_eq!(
        old_before_clear, expected,
        "reader receives the live SQ8 body"
    );
    assert_eq!(
        old_after_clear, expected,
        "pre-clear active statement keeps its snapshot-live SQ8 body"
    );
    assert_eq!(
        retained_session_after_clear,
        vec![vec![Value::Null]],
        "new SQ8 statement on retained session sees NULL"
    );
    assert_eq!(
        cleared_answer,
        vec![vec![Value::Null]],
        "writer receives NULL after SQ8 clear"
    );
    assert_eq!(
        new_after_clear,
        vec![vec![Value::Null]],
        "post-clear reader receives NULL for SQ8"
    );
    assert_eq!(
        reopened_answer,
        vec![vec![Value::Null]],
        "SQ8 clear remains NULL after reopen"
    );
}

// Acceptance: public execute returns a terminal value with no retained answer/copy credit.
#[test]
fn public_execute_releases_answer_and_copy_credit_before_return() {
    use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct ObservedReadBudget {
        source: Arc<dyn ReadMemoryBudget>,
        used: AtomicUsize,
        peak: AtomicUsize,
        eager_image_bytes: AtomicUsize,
    }
    impl ReadMemoryBudget for ObservedReadBudget {
        fn try_reserve(
            &self,
            bytes: usize,
            subsystem: &'static str,
            operation: &'static str,
            hint: &'static str,
        ) -> Result<()> {
            self.source.try_reserve(bytes, subsystem, operation, hint)?;
            if subsystem == "reader" && operation == "materialize_answer" {
                self.eager_image_bytes.fetch_add(bytes, Ordering::SeqCst);
            }
            let used = self.used.fetch_add(bytes, Ordering::SeqCst) + bytes;
            self.peak.fetch_max(used, Ordering::SeqCst);
            Ok(())
        }
        fn release(&self, bytes: usize) {
            let before = self.used.fetch_sub(bytes, Ordering::SeqCst);
            assert!(before >= bytes, "reader credit returned at most once");
            self.source.release(bytes);
        }
    }
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("public-answer-credit.redb");
    let open = || {
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                memory_limit: Some(64 * 1024 * 1024),
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..DatabaseOpenOptions::default()
            },
        )
        .unwrap()
    };
    let db = open();
    db.execute("CREATE TABLE answer_items (id INT PRIMARY KEY, body TEXT, v VECTOR(8) WITH (quantization = 'SQ8')) HISTORY ALL", &HashMap::new()).unwrap();
    let input = vec![1.0, 0.8, 0.6, 0.4, 0.2, 0.0, -0.2, -0.4];
    let body = "retained public answer".repeat(1024);
    let expected = vec![vec![
        Value::Text(body.clone()),
        Value::Vector(contextdb_vector::stored_vector_value(
            &input,
            contextdb_core::table_meta::VectorQuantization::SQ8,
        )),
    ]];
    db.execute(
        "INSERT INTO answer_items VALUES (1,$body,$v)",
        &HashMap::from([
            ("body".into(), Value::Text(body)),
            ("v".into(), Value::Vector(input)),
        ]),
    )
    .unwrap();
    db.close().unwrap();
    drop(db);
    let db = open();
    let sql = "SELECT body, v FROM answer_items WHERE id = 1";
    let observe = || {
        Arc::new(ObservedReadBudget {
            source: db.declared_reader_memory().unwrap(),
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            eager_image_bytes: AtomicUsize::new(0),
        })
    };
    // Ruling 96: open retains standing reader credit; operation release returns to that baseline.
    let limit = db.owner_read_config().limits.limits.memory;
    let baseline = db.declared_read_memory.retained_bytes_for_test();
    let available = limit
        .checked_sub(baseline)
        .expect("standing credit stays within the declared ceiling");
    let assert_parent_baseline = || {
        let used = db.declared_read_memory.retained_bytes_for_test();
        assert_eq!(
            used, baseline,
            "public boundary leaves exactly the standing parent credit"
        );
        assert_eq!(
            limit - used,
            available,
            "all operation availability returns before public output destruction"
        );
        let credit = ReadCredit::try_new(
            db.declared_read_memory.clone(),
            available as usize,
            "reader",
            "test_available_declared_capacity",
            "all operation credit must be released",
        )
        .expect("exact baseline available reader credit can be reserved");
        assert_eq!(
            db.declared_read_memory.retained_bytes_for_test(),
            limit,
            "probe accounts for all baseline availability"
        );
        drop(credit);
        assert_eq!(
            db.declared_read_memory.retained_bytes_for_test(),
            baseline,
            "probe releases its exact reservation"
        );
    };
    assert_parent_baseline();
    let owner = observe();
    let owned = db
        .execute_owned_with_read_memory(sql, &HashMap::new(), owner.clone())
        .unwrap();
    assert_eq!(owned.rows, expected);
    let vector_payload_bytes = owned.rows.iter().flatten().fold(0usize, |total, value| {
        let bytes = match value {
            Value::Vector(vector) => vector
                .capacity()
                .checked_mul(std::mem::size_of::<f32>())
                .unwrap(),
            _ => 0,
        };
        total.checked_add(bytes).unwrap()
    });
    let value_payload_bytes = owned.rows.iter().flatten().fold(0usize, |total, value| {
        let bytes = match value {
            Value::Text(text) => text.capacity(),
            Value::Vector(vector) => vector
                .capacity()
                .checked_mul(std::mem::size_of::<f32>())
                .unwrap(),
            _ => 0,
        };
        total.checked_add(bytes).unwrap()
    });
    let answer_credit = owned.answer_charge.held();
    let copy_credit = owned.vector_copy_charge.held();
    let eager_image_bytes = owner.eager_image_bytes.load(Ordering::SeqCst);
    assert!(
        answer_credit.checked_add(copy_credit).unwrap() > 0,
        "internal result retains actual answer/copy credit"
    );
    assert!(
        vector_payload_bytes > 0,
        "fixture returns a funded vector payload"
    );
    assert!(
        answer_credit >= vector_payload_bytes,
        "internal answer credit covers every returned vector capacity byte"
    );
    assert!(
        answer_credit >= value_payload_bytes,
        "internal answer credit covers all returned text and vector payload bytes"
    );
    assert_eq!(
        copy_credit, eager_image_bytes,
        "separate vector copy credit equals the observed eager image admission"
    );
    assert_eq!(
        eager_image_bytes, 0,
        "bounded answer route creates no eager image"
    );
    assert_eq!(
        owner.used.load(Ordering::SeqCst),
        answer_credit + copy_credit,
        "every retained operation byte follows the live internal result"
    );
    assert!(
        owner.used.load(Ordering::SeqCst) > 0,
        "internal lifetime retains executing reader credit"
    );
    let converted = owned.into_public();
    assert_eq!(converted.rows, expected);
    assert_eq!(
        owner.used.load(Ordering::SeqCst),
        0,
        "consuming conversion releases all observed credit while public rows remain alive"
    );
    assert!(owner.peak.load(Ordering::SeqCst) > 0);
    drop(owner);
    assert_parent_baseline();
    let owner = observe();
    let explicit = db
        .execute_with_read_memory(sql, &HashMap::new(), owner.clone())
        .unwrap();
    assert_eq!(explicit.rows, expected);
    assert!(
        owner.peak.load(Ordering::SeqCst) > 0,
        "public execution actually admitted memory"
    );
    assert_eq!(
        owner.used.load(Ordering::SeqCst),
        0,
        "public explicit-owner boundary releases every observed answer/copy credit before return"
    );
    drop(owner);
    assert_parent_baseline();
    let ordinary = db.execute(sql, &HashMap::new()).unwrap();
    assert_eq!(ordinary.rows, expected);
    assert_parent_baseline();
    // Keep all three public values alive through the final baseline-availability probe.
    assert_eq!(converted.rows, ordinary.rows);
    assert_eq!(explicit.rows, ordinary.rows);
    // Owned configuration follows the captured root; scalar decoding ends at return.
    let config_tx = db.begin_or_panic();
    let config_text = "owned configuration image".repeat(128);
    let config_writes = vec![
        (
            "config_parity_scalar".to_string(),
            RedbPersistence::encode_config_value(&42u64).unwrap(),
        ),
        (
            "config_parity_text/entry".to_string(),
            RedbPersistence::encode_config_value(&config_text).unwrap(),
        ),
    ];
    db.tx_mgr
        .with_write_set(config_tx, |ws| ws.config_writes.extend(config_writes))
        .unwrap();
    db.commit(config_tx).unwrap();
    let mut config_parity = Vec::new();
    for count in [2usize, 200] {
        let parent_before = db.declared_read_memory.retained_bytes_for_test();
        let config_owner = observe();
        let baseline_receipt = db.vector_memory_ownership_receipt().unwrap();
        let root = db.writer_read_scope(config_owner.clone()).unwrap();
        for _ in 0..count {
            assert_eq!(
                db.read_config_value::<String>("config_parity_text/entry", config_owner.clone())
                    .unwrap(),
                Some(config_text.clone())
            );
            assert_eq!(
                db.read_config_values_with_prefix::<String>(
                    "config_parity_text/",
                    config_owner.clone(),
                )
                .unwrap(),
                vec![("config_parity_text/entry".to_string(), config_text.clone())]
            );
        }
        let actual_owned = config_owner.used.load(Ordering::SeqCst);
        assert!(
            actual_owned > 0,
            "real owned config images retain reader credit"
        );
        let owned_receipt = db.vector_memory_ownership_receipt().unwrap();
        let owned_named_gap = owned_receipt.reader_source_metadata as i128
            - baseline_receipt.reader_source_metadata as i128
            - actual_owned as i128;
        let owned_parent_gap = owned_receipt.reader_parent_total as i128
            - baseline_receipt.reader_parent_total as i128
            - actual_owned as i128;
        drop(root);
        let owned_released = config_owner.used.load(Ordering::SeqCst);
        let scalar_baseline = db.vector_memory_ownership_receipt().unwrap();
        let root = db.writer_read_scope(config_owner.clone()).unwrap();
        for _ in 0..count {
            assert_eq!(
                db.read_config_value::<u64>("config_parity_scalar", config_owner.clone())
                    .unwrap(),
                Some(42)
            );
        }
        let scalar_held = config_owner.used.load(Ordering::SeqCst);
        let scalar_receipt = db.vector_memory_ownership_receipt().unwrap();
        let scalar_named_growth = scalar_receipt.reader_source_metadata as i128
            - scalar_baseline.reader_source_metadata as i128;
        let scalar_parent_growth = scalar_receipt.reader_parent_total as i128
            - scalar_baseline.reader_parent_total as i128;
        drop(root);
        let scalar_released = config_owner.used.load(Ordering::SeqCst);
        drop(config_owner);
        let final_parent_growth =
            db.declared_read_memory.retained_bytes_for_test() as i128 - parent_before as i128;
        config_parity.push((
            count,
            owned_named_gap,
            owned_parent_gap,
            owned_released,
            scalar_held,
            scalar_named_growth,
            scalar_parent_growth,
            scalar_released,
            final_parent_growth,
        ));
    }
    assert_eq!(
        config_parity,
        vec![(2, 0, 0, 0, 0, 0, 0, 0, 0), (200, 0, 0, 0, 0, 0, 0, 0, 0)],
        "actual root-owned config images reconcile with their named receipt and reader parent; scalar reads retain nothing at return; every root releases exactly once at N and 100N"
    );
    db.close().unwrap();
}
