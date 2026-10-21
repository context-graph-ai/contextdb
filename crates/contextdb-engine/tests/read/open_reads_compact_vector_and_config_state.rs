use contextdb_core::{Error, Lsn, RowId, TxId, Value, VectorIndexRef, VectorPartitionKey};
use contextdb_engine::direct_file_reader::test_seams::{
    DirectImageMetadataKind, DirectImageState, DirectMetadataBody,
};
use contextdb_engine::persistence::RedbPersistence;
use contextdb_engine::{Database, MaintenancePolicy, MetadataRequest, ReadSession};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[path = "../fixtures/schema_wall/mod.rs"]
mod schema_wall;

fn seed_vectors(path: &Path, dimension: usize, count: usize) {
    let db = Database::open(path).expect("create vector store");
    db.execute(
        &format!("CREATE TABLE vectors (id INTEGER PRIMARY KEY, embedding VECTOR({dimension}))"),
        &HashMap::new(),
    )
    .expect("declare vector column");
    let tx = db.begin().expect("begin vector batch");
    for id in 0..count {
        let mut embedding = vec![0.0_f32; dimension];
        embedding[id % dimension] = 1.0;
        db.execute_in_tx(
            tx,
            "INSERT INTO vectors (id, embedding) VALUES ($id, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Int64(id as i64)),
                ("embedding".into(), Value::Vector(embedding)),
            ]),
        )
        .expect("insert vector");
    }
    db.commit(tx).expect("commit vectors");
    db.close().expect("seal vector store");
}

fn open_backend_bytes(path: &Path) -> u64 {
    let bytes = Arc::new(AtomicU64::new(0));
    let counted = Arc::clone(&bytes);
    let observation = redb::observe_backend_reads(Arc::new(move |n| {
        counted.fetch_add(n as u64, Ordering::SeqCst);
    }));
    let reader = ReadSession::open(path).expect("open committed file for bounded reading");
    drop(reader);
    drop(observation);
    bytes.load(Ordering::SeqCst)
}

#[test]
fn opening_equal_vector_memberships_reads_bytes_independent_of_dimension() {
    let root = tempfile::tempdir().expect("vector fixture directory");
    let narrow = root.path().join("narrow.db");
    let wide = root.path().join("wide.db");
    const COUNT: usize = 96;
    seed_vectors(&narrow, 64, COUNT);
    seed_vectors(&wide, 768, COUNT);
    let narrow_bytes = open_backend_bytes(&narrow);
    let wide_bytes = open_backend_bytes(&wide);
    assert!(
        wide_bytes <= narrow_bytes + 16 * 1024,
        "equal membership directories should have dimension-independent open reads: narrow={narrow_bytes}, wide={wide_bytes}"
    );
}

#[test]
fn opening_reads_only_config_values_needed_for_the_reader_image() {
    let root = tempfile::tempdir().expect("config fixture directory");
    let minimal = root.path().join("minimal.db");
    let crowded = root.path().join("crowded.db");
    for path in [&minimal, &crowded] {
        let db = Database::open(path).expect("create config store");
        db.execute(
            "CREATE TABLE records (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .expect("declare equal schema");
        db.close().expect("seal config store");
    }
    let persistence = RedbPersistence::open(&crowded).expect("open config writer");
    for index in 0..32 {
        persistence
            .flush_config_value(
                &format!("unrelated_blob_{index:03}"),
                &"x".repeat(32 * 1024),
            )
            .expect("write unrelated config value with authenticated identity");
    }
    drop(persistence);
    let minimal_bytes = open_backend_bytes(&minimal);
    let crowded_bytes = open_backend_bytes(&crowded);
    assert!(
        crowded_bytes <= minimal_bytes + 32 * 1024,
        "unrelated config payload must not be scanned at open: minimal={minimal_bytes}, crowded={crowded_bytes}"
    );
}

#[test]
fn largest_schema_and_vector_image_reads_as_authored_after_open() {
    let root = tempfile::tempdir().expect("large schema fixture directory");
    let path = root.path().join("schema-wall.db");
    let db = Database::open(&path).expect("create schema wall");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    for ordinal in 0_usize..100 {
        let name = format!("schema_{ordinal:03}");
        let ddl = match ordinal.checked_sub(1) {
            Some(previous) => format!(
                "CREATE TABLE {name} (id INTEGER PRIMARY KEY, parent_id INTEGER NOT NULL REFERENCES schema_{previous:03}(id), body TEXT, FOREIGN KEY (parent_id) REFERENCES schema_{previous:03}(id)) SYNC OFF"
            ),
            None => format!("CREATE TABLE {name} (id INTEGER PRIMARY KEY, body TEXT) SYNC OFF"),
        };
        db.execute(&ddl, &HashMap::new())
            .expect("declare schema chain");
    }
    db.execute(
        "CREATE EVENT TYPE schema_099_insert WHEN INSERT ON schema_099",
        &HashMap::new(),
    )
    .expect("declare final table event");
    db.execute(
        "CREATE TRIGGER schema_099_insert_trigger ON schema_099 WHEN INSERT INCLUDING SYNC",
        &HashMap::new(),
    )
    .expect("declare final table trigger");
    schema_wall::largest::prepare_existing_rows_and_vectors(&db);
    db.execute(
        "CREATE TABLE generation_probe (id INTEGER PRIMARY KEY, embedding VECTOR(3) SEARCH_MODE INDEXED AUTO_INDEX_AT 1) SYNC OFF",
        &HashMap::new(),
    )
    .expect("declare generation control");
    db.execute(
        "INSERT INTO generation_probe VALUES (1, $embedding)",
        &HashMap::from([("embedding".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
    )
    .expect("insert generation control");
    for _ in 0..8 {
        db.run_maintenance_cycle()
            .expect("build maintained generation");
        if db
            .vector_store_for_test()
            .hnsw_maintenance_status()
            .ready_partitions
            > 0
        {
            break;
        }
    }
    let expected_catalog = db.vector_store_for_test().hnsw_maintenance_status();
    assert!(
        expected_catalog.ready_partitions > 0,
        "catalog has a ready generation"
    );
    let mut expected_tables = db.table_names();
    expected_tables.sort();
    let expected_rows = schema_wall::largest::rows_and_vectors(&db);
    db.close().expect("seal schema wall");

    let reader = ReadSession::open(&path).expect("open largest committed schema wall");
    let catalog = reader
        .file_vector_store_for_test()
        .expect("file route has a vector catalog")
        .hnsw_maintenance_status();
    assert_eq!(catalog.nonempty_indexes, expected_catalog.nonempty_indexes);
    assert_eq!(
        catalog.nonempty_partitions,
        expected_catalog.nonempty_partitions
    );
    assert_eq!(catalog.ready_partitions, expected_catalog.ready_partitions);
    for (ordinal, expected) in expected_rows.iter().enumerate() {
        let table = format!("schema_{ordinal:03}");
        let actual = reader
            .execute(
                &format!("SELECT id, body, embedding FROM {table} ORDER BY id"),
                &HashMap::new(),
            )
            .expect("read authored vector row");
        assert_eq!(&actual.rows(), expected, "{table} row and vector membership");
    }
    let schemas = reader
        .metadata(MetadataRequest::Tables, None)
        .expect("read complete schema directory");
    let DirectMetadataBody::Tables {
        mut items,
        has_more,
    } = schemas.body
    else {
        panic!("schema directory has a table inventory");
    };
    assert!(!has_more, "schema fixture table inventory fits in one page");
    items.sort();
    assert_eq!(items, expected_tables);
    let configuration = reader
        .metadata(
            MetadataRequest::ImageState {
                kind: DirectImageMetadataKind::Configuration,
            },
            None,
        )
        .expect("read committed configuration");
    let DirectMetadataBody::ImageState {
        state: DirectImageState::Configuration(configuration),
    } = configuration.body
    else {
        panic!("configuration is a typed committed image");
    };
    assert_eq!(configuration.schemas.len(), expected_tables.len());
    assert_eq!(
        reader
            .execute(
                "SELECT id FROM generation_probe ORDER BY embedding <=> $q USE VECTOR INDEXED LIMIT 1",
                &HashMap::from([("q".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
            )
            .expect("read authenticated generation")
            .rows(),
        vec![vec![Value::Int64(1)]]
    );
}

#[test]
fn a_tampered_compact_vector_directory_is_refused_at_open() {
    let root = tempfile::tempdir().expect("tamper fixture directory");
    let path = root.path().join("tampered-directory.db");
    seed_vectors(&path, 64, 8);

    let raw = redb::Database::open(&path).expect("open sealed store for test corruption");
    let read = raw.begin_read().expect("inspect physical tables");
    let directory_name = read
        .list_tables()
        .expect("list physical tables")
        .map(|table| table.name().to_owned())
        .find(|name| name.contains("vector") && name.contains("directory"))
        .expect("a compact vector membership directory is persisted apart from payloads");
    drop(read);

    let write = raw.begin_write().expect("begin test corruption");
    {
        let definition: redb::TableDefinition<&[u8], &[u8]> =
            redb::TableDefinition::new(&directory_name);
        let mut table = write
            .open_table(definition)
            .expect("open compact directory");
        let (key, mut value) = {
            let (key, value) = table
                .iter()
                .expect("iterate compact directory")
                .next()
                .expect("nonempty compact directory")
                .expect("read compact directory record");
            (key.value().to_vec(), value.value().to_vec())
        };
        let last = value.last_mut().expect("nonempty authenticated record");
        *last ^= 0x01;
        table
            .insert(key.as_slice(), value.as_slice())
            .expect("flip one persisted directory byte");
    }
    write.commit().expect("persist test corruption");
    drop(raw);

    let refusal = match ReadSession::open(&path) {
        Ok(_) => panic!("tampered directory must refuse open"),
        Err(error) => error,
    };
    assert!(
        refusal.to_string().to_ascii_lowercase().contains("corrupt"),
        "directory damage needs a corruption diagnosis: {refusal}"
    );
}

const OPEN_CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("read_open_config");
const CUSTODY_CONFIG: TableDefinition<&str, &[u8]> = TableDefinition::new("read_custody_config");
const IDENTITY_METADATA: TableDefinition<&str, &[u8]> =
    TableDefinition::new("__content_identity_metadata");
const IDENTITY_ROOTS: TableDefinition<u8, &[u8]> = TableDefinition::new("__content_identity_roots");
const OPEN_CONFIG_COMMITMENT: &str = "read_open_config_root_v1";
const CUSTODY_CONFIG_COMMITMENT: &str = "read_custody_config_root_v1";
const CONFIGURATION_FAMILY: u8 = 9;

const VECTOR_MEMBERSHIP: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_partition_membership");
const VECTOR_DIRECTORY: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("vector_open_directory");
const VECTOR_DIRECTORY_COMMITMENT: &str = "vector_open_directory_root_v1";
const VECTOR_FAMILY: u8 = 2;

// The persisted membership payload is private to the engine. Mirror only its
// on-disk shape here so the test can change a visibility fact without touching
// its identity, vector body, or any product test seam.
#[derive(serde::Serialize, serde::Deserialize)]
struct CopiedMembership {
    index: VectorIndexRef,
    partition_key: VectorPartitionKey,
    row_id: RowId,
    row_created_tx: TxId,
    row_lsn: Lsn,
    vector_created_tx: TxId,
    vector_lsn: Lsn,
    visible_from: TxId,
    deleted_tx: Option<TxId>,
}

fn change_copied_vector_visibility(path: &Path) {
    const MAGIC: &[u8] = b"contextdb-vector-partition";
    let raw = redb::Database::open(path).expect("open sealed vector fixture");
    let canonical_before = {
        let read = raw.begin_read().unwrap();
        read.open_table(IDENTITY_ROOTS)
            .unwrap()
            .get(VECTOR_FAMILY)
            .unwrap()
            .unwrap()
            .value()
            .to_vec()
    };
    let write = raw
        .begin_write()
        .expect("begin copy-only visibility mutation");
    {
        let mut memberships = write.open_table(VECTOR_MEMBERSHIP).unwrap();
        let (key, original) = {
            let (key, value) = memberships.iter().unwrap().next().unwrap().unwrap();
            (key.value().to_vec(), value.value().to_vec())
        };
        assert!(original.starts_with(MAGIC));
        let payload_start = MAGIC.len() + 2 + 1 + 8;
        let payload_end = original.len() - 32;
        let (mut member, consumed): (CopiedMembership, usize) = bincode::serde::decode_from_slice(
            &original[payload_start..payload_end],
            bincode::config::standard(),
        )
        .expect("decode copied membership");
        assert_eq!(consumed, payload_end - payload_start);
        assert!(
            member.deleted_tx.is_none(),
            "fixture membership starts live"
        );
        member.deleted_tx = Some(member.visible_from);
        let payload = bincode::serde::encode_to_vec(member, bincode::config::standard()).unwrap();
        let mut changed = original[..MAGIC.len() + 3].to_vec();
        changed.extend_from_slice(&(payload.len() as u64).to_be_bytes());
        changed.extend_from_slice(&payload);
        changed.extend_from_slice(blake3::hash(&changed).as_bytes());
        memberships
            .insert(key.as_slice(), changed.as_slice())
            .unwrap();
    }
    let mut root = [0_u8; 40];
    {
        let memberships = write.open_table(VECTOR_MEMBERSHIP).unwrap();
        let mut directory = write.open_table(VECTOR_DIRECTORY).unwrap();
        let mut count = 0_u64;
        let mut xor = [0_u8; 32];
        for item in memberships.iter().unwrap() {
            let (key, value) = item.unwrap();
            let mut hasher = blake3::Hasher::new();
            hasher.update(key.value());
            hasher.update(value.value());
            let digest = hasher.finalize();
            directory
                .insert(key.value(), digest.as_bytes().as_slice())
                .unwrap();
            count += 1;
            for (dst, src) in xor.iter_mut().zip(digest.as_bytes()) {
                *dst ^= src;
            }
        }
        root[..8].copy_from_slice(&count.to_be_bytes());
        root[8..].copy_from_slice(&xor);
        directory.insert(b"".as_slice(), root.as_slice()).unwrap();
    }
    write
        .open_table(IDENTITY_METADATA)
        .unwrap()
        .insert(
            VECTOR_DIRECTORY_COMMITMENT,
            blake3::hash(&root).as_bytes().as_slice(),
        )
        .unwrap();
    write.commit().unwrap();
    let read = raw.begin_read().unwrap();
    assert_eq!(
        read.open_table(IDENTITY_ROOTS)
            .unwrap()
            .get(VECTOR_FAMILY)
            .unwrap()
            .unwrap()
            .value(),
        canonical_before.as_slice(),
        "canonical vector identity stays unchanged"
    );
}

fn vector_match_ids(path: &Path) -> contextdb_core::Result<Vec<Vec<Value>>> {
    ReadSession::open(path)?
        .execute(
            "SELECT id FROM vectors ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 8",
            &HashMap::from([(
                "q".into(),
                Value::Vector({
                    let mut v = vec![0.0; 64];
                    v[0] = 1.0;
                    v
                }),
            )]),
        )
        .map(|answer| answer.rows().to_vec())
}

#[test]
fn a_self_consistent_visibility_copy_cannot_hide_a_committed_vector() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("visibility-copy.db");
    seed_vectors(&path, 64, 1);
    assert_eq!(
        vector_match_ids(&path).unwrap(),
        vec![vec![Value::Int64(0)]]
    );
    change_copied_vector_visibility(&path);
    let refusal = vector_match_ids(&path).expect_err("changed copied visibility must refuse");
    assert!(
        matches!(refusal, Error::StoreCorrupted { .. }),
        "expected typed damage: {refusal:?}"
    );
}

#[test]
fn a_committed_vector_delete_stays_absent_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("healthy-delete.db");
    seed_vectors(&path, 64, 1);
    let writer = Database::open(&path).unwrap();
    writer
        .execute("DELETE FROM vectors WHERE id = 0", &HashMap::new())
        .unwrap();
    writer.close().unwrap();
    assert!(vector_match_ids(&path).unwrap().is_empty());
}

fn seed_startup_setting(path: &Path) {
    let db = Database::open(path).expect("create startup-settings fixture");
    db.close().expect("seal fixture");
    let persistence = RedbPersistence::open(path).expect("open config writer");
    persistence
        .flush_config_value("disk_limit", &1_000_000_000_u64)
        .expect("commit a startup setting and its canonical identity");
    drop(persistence);
    ReadSession::open(path).expect("untampered startup settings open");
}

fn configuration_root(raw: &redb::Database) -> Vec<u8> {
    let read = raw.begin_read().expect("read canonical root");
    read.open_table(IDENTITY_ROOTS)
        .expect("identity roots")
        .get(CONFIGURATION_FAMILY)
        .expect("read Configuration root")
        .expect("Configuration root exists")
        .value()
        .to_vec()
}

fn replace_copied_setting(path: &Path, replacement: Option<Vec<u8>>) {
    let raw = redb::Database::open(path).expect("open sealed fixture for test tampering");
    let canonical_before = configuration_root(&raw);
    let write = raw.begin_write().expect("begin copy-only tampering");
    let mut root = [0_u8; 40];
    {
        let mut copy = write.open_table(OPEN_CONFIG).expect("compact startup copy");
        assert!(
            copy.get("disk_limit")
                .expect("inspect copied setting")
                .is_some(),
            "the canonical fixture must have a copied startup setting"
        );
        match replacement {
            Some(value) => {
                copy.insert("disk_limit", value.as_slice())
                    .expect("replace only the copied setting");
            }
            None => {
                copy.remove("disk_limit")
                    .expect("remove only the copied setting");
            }
        }
        let mut count = 0_u64;
        let mut xor = [0_u8; 32];
        for entry in copy.iter().expect("scan copied settings") {
            let (key, value) = entry.expect("read copied setting");
            if key.value().is_empty() {
                continue;
            }
            count += 1;
            let mut hasher = blake3::Hasher::new();
            hasher.update(key.value().as_bytes());
            hasher.update(value.value());
            for (dst, src) in xor.iter_mut().zip(hasher.finalize().as_bytes()) {
                *dst ^= src;
            }
        }
        root[..8].copy_from_slice(&count.to_be_bytes());
        root[8..].copy_from_slice(&xor);
        copy.insert("", root.as_slice())
            .expect("update local count and checksum consistently");
    }
    write
        .open_table(IDENTITY_METADATA)
        .expect("identity metadata")
        .insert(
            OPEN_CONFIG_COMMITMENT,
            blake3::hash(&root).as_bytes().as_slice(),
        )
        .expect("update copied-root metadata checksum consistently");
    write.commit().expect("persist copy-only tampering");
    assert_eq!(
        configuration_root(&raw),
        canonical_before,
        "tampering must leave the canonical Configuration content root unchanged"
    );
}

fn assert_copied_setting_refused_at_open(path: &Path) {
    let refusal = match ReadSession::open(path) {
        Ok(_) => panic!("a copy that disagrees with committed Configuration must refuse open"),
        Err(error) => error,
    };
    assert!(
        matches!(refusal, Error::StoreCorrupted { .. }),
        "a self-consistent but unauthenticated startup copy is typed damage: {refusal:?}"
    );
}

#[test]
fn a_replayed_startup_setting_with_recomputed_copy_checksums_is_refused_at_open() {
    let root = tempfile::tempdir().expect("replayed setting fixture");
    let path = root.path().join("replayed-setting.db");
    seed_startup_setting(&path);
    let replayed = bincode::serde::encode_to_vec(2_000_000_000_u64, bincode::config::standard())
        .expect("encode another valid disk limit");
    replace_copied_setting(&path, Some(replayed));
    assert_copied_setting_refused_at_open(&path);
}

#[test]
fn a_missing_copied_startup_setting_with_recomputed_copy_checksums_is_refused_at_open() {
    let root = tempfile::tempdir().expect("missing setting fixture");
    let path = root.path().join("missing-setting.db");
    seed_startup_setting(&path);
    replace_copied_setting(&path, None);
    assert_copied_setting_refused_at_open(&path);
}

fn seed_custody_policies(path: &Path) {
    let writer = Database::open(path).expect("create policy fixture");
    for table in ["first_records", "second_records"] {
        writer.execute(
            &format!("DECLARE TENANT TABLE POLICY {table} SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE"),
            &HashMap::new(),
        ).expect("commit a distinct policy record");
    }
    writer.close().expect("seal policy fixture");
}

fn omit_one_copied_custody_record(path: &Path) {
    let raw = redb::Database::open(path).expect("open sealed policy fixture");
    let canonical_before = configuration_root(&raw);
    let write = raw.begin_write().expect("begin copy-only custody omission");
    let mut root = [0_u8; 40];
    {
        let mut copy = write
            .open_table(CUSTODY_CONFIG)
            .expect("copied custody index");
        let omitted = copy
            .iter()
            .unwrap()
            .filter_map(|entry| {
                let (key, _) = entry.unwrap();
                key.value()
                    .starts_with("tenant_table_policy.v1.")
                    .then(|| key.value().to_owned())
            })
            .next()
            .expect("at least one committed policy is copied");
        copy.remove(omitted.as_str())
            .expect("omit one policy from copy");
        let mut count = 0_u64;
        let mut xor = [0_u8; 32];
        for entry in copy.iter().expect("scan remaining custody copy") {
            let (key, value) = entry.expect("read copied custody record");
            if key.value().is_empty() {
                continue;
            }
            count += 1;
            let mut hasher = blake3::Hasher::new();
            hasher.update(key.value().as_bytes());
            hasher.update(value.value());
            for (dst, src) in xor.iter_mut().zip(hasher.finalize().as_bytes()) {
                *dst ^= src;
            }
        }
        root[..8].copy_from_slice(&count.to_be_bytes());
        root[8..].copy_from_slice(&xor);
        copy.insert("", root.as_slice())
            .expect("rebuild local aggregate");
    }
    write
        .open_table(IDENTITY_METADATA)
        .unwrap()
        .insert(
            CUSTODY_CONFIG_COMMITMENT,
            blake3::hash(&root).as_bytes().as_slice(),
        )
        .expect("rebuild metadata checksum for copied index");
    write.commit().expect("persist copy-only custody omission");
    assert_eq!(
        configuration_root(&raw),
        canonical_before,
        "the committed Configuration identity is unchanged"
    );
}

#[test]
fn a_custody_copy_missing_one_policy_cannot_return_a_shortened_show_list() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("omitted-policy.db");
    seed_custody_policies(&path);
    let healthy = ReadSession::open(&path).expect("healthy read-only policy door");
    assert_eq!(
        healthy
            .execute("SHOW TENANT TABLE POLICY", &HashMap::new())
            .unwrap()
            .rows()
            .len(),
        2,
        "the committed set contains both policy records"
    );
    drop(healthy);
    omit_one_copied_custody_record(&path);
    let reader = ReadSession::open(&path).expect("inspection opens before custody selection");
    let refusal = reader
        .execute("SHOW TENANT TABLE POLICY", &HashMap::new())
        .expect_err("a shortened copied index must not answer policy inspection");
    assert!(
        matches!(refusal, Error::StoreCorrupted { .. }),
        "expected typed damage from read-only inspection: {refusal:?}"
    );
}
