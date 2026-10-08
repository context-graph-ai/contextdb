#![allow(clippy::needless_range_loop)] // Existing compatibility and test seam shape.
#![allow(clippy::type_complexity)] // Existing compatibility and test seam shape.
//! Real durable mutations checked against an independent on-demand store oracle.
//! The oracle never borrows/clones a production reader image. Its disposable
//! decoded view exists only during validation, outside measured read-open.
use contextdb_core::read_contract::ReadRoute;
use contextdb_core::{
    DirectedValue, Lsn, RowId, SnapshotId, TotalOrdAsc, TotalOrdDesc, TxId, Value, Wallclock,
};
use contextdb_engine::direct_file_reader::test_seams::{
    DirectTypedImageDigest, DirectVectorQuantization, ReadWorkCounters, ReadWorkObservationScope,
    ReadWorkPhase,
};
use contextdb_engine::read_content_test_adapter::{
    CommittedContentForTest, LogicalContentRecordForTest, committed_content_for_test,
};
use contextdb_engine::read_session::{
    ReadKernelCancellationEvent, ReadKernelSourceEvent, ReadKernelTestObserver,
    ReadSessionOperation,
};
use contextdb_engine::{
    Database, DatabaseOpenOptions, MaintenancePolicy, OwnerReadCancellation, OwnerReadConfig,
    ReadSession, ReadSessionOptions,
};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

fn secure_runtime_root(directory: &Path) -> PathBuf {
    let root = directory.join("identity-owner-runtime");
    std::fs::create_dir(&root).expect("create task-scoped owner runtime directory");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("secure task-scoped owner runtime directory");
    root
}

fn execute(database: &Database, sql: &str, params: HashMap<String, Value>) {
    database
        .execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture mutation `{sql}`: {error}"));
}

/// Populate each family the existing `DirectTypedImageDigest` advertises.
fn seed(database: &Database) {
    database.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        database,
        "CREATE TABLE identity_rows (id UUID PRIMARY KEY, payload TEXT NOT NULL, embedding VECTOR(3), sq8 VECTOR(3) WITH (quantization = 'SQ8'), sq4 VECTOR(3) WITH (quantization = 'SQ4')) HISTORY ALL SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE TABLE identity_edges (id UUID PRIMARY KEY, source_id UUID NOT NULL, target_id UUID NOT NULL, edge_type TEXT NOT NULL) DAG('IDENTITY_LINK') SYNC PULL ONLY SYNC CONFLICT KEEP FIRST",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE INDEX identity_payload ON identity_rows (payload ASC)",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE EVENT TYPE identity_insert WHEN INSERT ON identity_rows",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE SINK identity_sink TYPE callback",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE ROUTE identity_route EVENT identity_insert TO identity_sink",
        HashMap::new(),
    );
    execute(
        database,
        "CREATE SCHEDULE identity_schedule EVERY '7 HOURS' TX (identity_callback)",
        HashMap::new(),
    );
    let left = uuid::Uuid::from_u128(0x1d3a_0000_0000_0000_0000_0000_0000_0001);
    let right = uuid::Uuid::from_u128(0x1d3a_0000_0000_0000_0000_0000_0000_0002);
    execute(
        database,
        "INSERT INTO identity_rows (id, payload, embedding, sq8, sq4) VALUES ($id, $payload, $embedding, $sq8, $sq4)",
        HashMap::from([
            ("id".to_owned(), Value::Uuid(left)),
            ("payload".to_owned(), Value::Text("same-content".to_owned())),
            ("embedding".to_owned(), Value::Vector(vec![1.0, 0.0, 0.0])),
            ("sq8".to_owned(), Value::Vector(vec![0.0, 1.0, 0.0])),
            ("sq4".to_owned(), Value::Vector(vec![0.0, 0.0, 1.0])),
        ]),
    );
    execute(
        database,
        "INSERT INTO identity_edges (id, source_id, target_id, edge_type) VALUES ($id, $source, $target, $edge_type)",
        HashMap::from([
            (
                "id".to_owned(),
                Value::Uuid(uuid::Uuid::from_u128(
                    0x1d3a_0000_0000_0000_0000_0000_0000_0003,
                )),
            ),
            ("source".to_owned(), Value::Uuid(left)),
            ("target".to_owned(), Value::Uuid(right)),
            (
                "edge_type".to_owned(),
                Value::Text("IDENTITY_LINK".to_owned()),
            ),
        ]),
    );
    let tx = database
        .begin()
        .expect("begin graph property fixture transaction");
    database
        .insert_edge(
            tx,
            left,
            right,
            "IDENTITY_LINK".to_owned(),
            HashMap::from([("evidence".to_owned(), Value::Text("kept".to_owned()))]),
        )
        .expect("insert graph property fixture");
    database.commit(tx).expect("commit graph property fixture");
    database
        .set_memory_limit(Some(96 * 1024 * 1024))
        .expect("persist fixture memory limit");
    database
        .set_disk_limit(Some(512 * 1024 * 1024))
        .expect("persist fixture disk limit");
}

fn oracle_hash_parts(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    for part in parts {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    *hash.finalize().as_bytes()
}

#[derive(serde::Deserialize)]
struct OracleRelationalIndexPosition {
    key: Vec<(bool, Value)>,
    row_id: RowId,
    created_tx: TxId,
}

fn take_oracle_u32(bytes: &[u8], cursor: &mut usize) -> usize {
    let end = *cursor + 4;
    let value = u32::from_be_bytes(bytes[*cursor..end].try_into().unwrap()) as usize;
    *cursor = end;
    value
}

fn oracle_posting_parts(key: &[u8]) -> (&[u8], &[u8], u8, &[u8]) {
    assert_eq!(key.first(), Some(&1));
    let mut cursor = 1;
    let table_len = take_oracle_u32(key, &mut cursor);
    let table = &key[cursor..cursor + table_len];
    cursor += table_len;
    let index_len = take_oracle_u32(key, &mut cursor);
    let index = &key[cursor..cursor + index_len];
    cursor += index_len;
    let bound = key[cursor];
    cursor += 1;
    (table, index, bound, &key[cursor..])
}

fn oracle_tree_key_cmp(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    match (left[0], right[0]) {
        (0, 0) | (3, 3) => left.cmp(right),
        (1, 1) => {
            let (left_table, left_index, left_bound, left_order) = oracle_posting_parts(left);
            let (right_table, right_index, right_bound, right_order) = oracle_posting_parts(right);
            left_table
                .cmp(right_table)
                .then_with(|| left_index.cmp(right_index))
                .then_with(|| left_bound.cmp(&right_bound))
                .then_with(|| {
                    let decode = |bytes| {
                        bincode::serde::decode_from_slice::<OracleRelationalIndexPosition, _>(
                            bytes,
                            bincode::config::standard(),
                        )
                        .unwrap()
                        .0
                    };
                    let left = decode(left_order);
                    let right = decode(right_order);
                    let directed = |position: OracleRelationalIndexPosition| {
                        let key = position
                            .key
                            .into_iter()
                            .map(|(descending, value)| {
                                if descending {
                                    DirectedValue::Desc(TotalOrdDesc(value))
                                } else {
                                    DirectedValue::Asc(TotalOrdAsc(value))
                                }
                            })
                            .collect::<Vec<_>>();
                        (key, position.row_id, position.created_tx)
                    };
                    directed(left).cmp(&directed(right))
                })
        }
        _ => left[0].cmp(&right[0]),
    }
}

fn oracle_separator(level: u32, key: &[u8]) -> Vec<u8> {
    // A separator above 512 bytes is stored by authenticated reference. A leaf
    // keeps the actual key until it no longer fits one chunk at all: 65536
    // bytes, less the 41-byte chunk header and this entry's 36-byte frame.
    let inline = if level == 0 {
        key.len() <= 65536 - 41 - 36
    } else {
        key.len() <= 512
    };
    if inline {
        return key.to_vec();
    }
    let mut result = vec![2];
    result.extend_from_slice(&(key.len() as u32).to_be_bytes());
    result.extend_from_slice(&oracle_hash_parts(b"contextdb-prolly-key", &[key]));
    result
}

// Independent sorted rebuild: it never reads persisted proof nodes or calls
// the incremental mutation/chunker implementation.
fn oracle_chunks_root(family: u8, entries: Vec<(Vec<u8>, [u8; 32])>) -> [u8; 32] {
    if entries.is_empty() {
        return oracle_hash_parts(b"contextdb-canonical-page-empty", &[&[family]]);
    }
    let levels = oracle_chunk_levels(family, entries);
    levels.last().expect("a canonical hierarchy")[0].1
}

/// Every canonical chunk of a family, level by level, stopping at the level a
/// single chunk covers. Same rule as the root oracle, which is derived from it.
fn oracle_chunk_levels(
    family: u8,
    mut entries: Vec<(Vec<u8>, [u8; 32])>,
) -> Vec<Vec<(Vec<u8>, [u8; 32])>> {
    let mut levels = Vec::new();
    for level in 0_u32..64 {
        let mut chunks = Vec::new();
        let mut begin = 0;
        let mut size = 41_usize;
        for i in 0..entries.len() {
            let width = oracle_separator(level, &entries[i].0).len() + 36;
            if size + width > 65536 && i > begin {
                chunks.push(begin..i);
                begin = i;
                size = 41;
            }
            let end = size + width;
            let cdf = |x: usize| x.clamp(1024, 3072) - 1024;
            let probability_numerator = cdf(end) - cdf(size);
            let probability_denominator = 2048 - cdf(size);
            let random = u64::from_le_bytes(
                oracle_hash_parts(
                    b"contextdb-prolly-boundary",
                    &[
                        &level.to_be_bytes(),
                        &oracle_hash_parts(b"contextdb-prolly-key", &[&entries[i].0]),
                    ],
                )[..8]
                    .try_into()
                    .unwrap(),
            );
            let cut = (level == 0 || i - begin >= 1)
                && (end >= 3072
                    || (random as u128) * (probability_denominator as u128)
                        < (probability_numerator as u128) * (u64::MAX as u128));
            size = end;
            if cut {
                chunks.push(begin..i + 1);
                begin = i + 1;
                size = 41;
            }
        }
        if begin < entries.len() {
            chunks.push(begin..entries.len());
        }
        let next = chunks
            .into_iter()
            .map(|range| {
                let mut hash = blake3::Hasher::new();
                hash.update(b"contextdb-prolly-chunk");
                hash.update(&[family]);
                hash.update(&level.to_be_bytes());
                for (key, value) in &entries[range.clone()] {
                    let key = oracle_separator(level, key);
                    hash.update(&(key.len() as u64).to_be_bytes());
                    hash.update(&key);
                    hash.update(value);
                }
                (
                    entries[range.end - 1].0.clone(),
                    *hash.finalize().as_bytes(),
                )
            })
            .collect::<Vec<_>>();
        levels.push(next.clone());
        if next.len() == 1 {
            return levels;
        }
        entries = next;
    }
    panic!("canonical chunk hierarchy must contract");
}

fn oracle_key_hash(key: &[u8]) -> [u8; 32] {
    if key.first() == Some(&2) && key.len() == 37 {
        key[5..].try_into().unwrap()
    } else {
        oracle_hash_parts(b"contextdb-prolly-key", &[key])
    }
}

/// Everything the committed image stores in the chunk table for one family:
/// one entry per canonical chunk, plus the blob each wide key is kept in.
/// Retiring a record can only remove entries from this set.
fn oracle_stored_ids(
    family: u8,
    records: &[(Vec<u8>, Vec<u8>)],
) -> std::collections::BTreeSet<Vec<u8>> {
    let mut ids = std::collections::BTreeSet::new();
    let entries = oracle_entries(family, records);
    for (key, _) in &entries {
        if key.len() > 512 {
            let mut stored = vec![family];
            stored.extend_from_slice(&oracle_key_hash(key));
            ids.insert(stored);
        }
    }
    if entries.is_empty() {
        return ids;
    }
    for (level, chunks) in oracle_chunk_levels(family, entries).into_iter().enumerate() {
        for (last, _) in chunks {
            // A chunk is STORED under its level and then its content hash, so
            // every chunk of one level of one family occupies one contiguous
            // stretch of the table. The id locates a chunk; what authenticates
            // it is the subtree hash its parent carries, which is why this
            // layout can change without any published digest moving.
            let mut stored = vec![family, level.min(64) as u8];
            stored.extend_from_slice(
                &oracle_hash_parts(
                    b"contextdb-prolly-chunk-id",
                    &[&(level as u32).to_be_bytes(), &oracle_key_hash(&last)],
                )[..31],
            );
            ids.insert(stored);
        }
    }
    ids
}

/// What the committed store holds for the whole image, derived only from the
/// committed content.
fn canonical_stored_ids(image: &CommittedContentForTest) -> std::collections::BTreeSet<Vec<u8>> {
    let mut records = vec![Vec::new(); 11];
    for record in &image.logical_records {
        records[record.family as usize].push((record.key.clone(), record.value.clone()));
    }
    (0..11)
        .flat_map(|family| oracle_stored_ids(family as u8, &records[family]))
        .collect()
}

/// The canonical leaf chunk separators of one family: the keys a change at or
/// below them must replace.
fn oracle_leaf_separators(family: u8, records: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    let entries = oracle_entries(family, records);
    if entries.is_empty() {
        return Vec::new();
    }
    oracle_chunk_levels(family, entries)
        .into_iter()
        .next()
        .expect("a leaf level")
        .into_iter()
        .map(|(last, _)| last)
        .collect()
}

fn oracle_family_root(family: u8, records: &[(Vec<u8>, Vec<u8>)]) -> [u8; 32] {
    oracle_chunks_root(family, oracle_entries(family, records))
}

fn oracle_entries(family: u8, records: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Vec<u8>, [u8; 32])> {
    const POSTING_MAGIC: &[u8] = b"\xffcontextdb-index-posting\0";
    let mut entries = Vec::with_capacity(records.len());
    for (key, value) in records {
        const HISTORY_MAGIC: &[u8] = b"\xffcontextdb-history\0";
        let tree_key: Vec<u8> = if family == 5 && key.starts_with(POSTING_MAGIC) {
            std::iter::once(1)
                .chain(key[POSTING_MAGIC.len()..].iter().copied())
                .collect()
        } else if family == 8 && key.starts_with(HISTORY_MAGIC) {
            std::iter::once(3)
                .chain(key[HISTORY_MAGIC.len()..].iter().copied())
                .collect()
        } else {
            std::iter::once(0)
                .chain(oracle_hash_parts(
                    b"contextdb-logical-record-key-v1",
                    &[&[family], key],
                ))
                .collect()
        };
        let value_hash =
            oracle_hash_parts(b"contextdb-logical-record-value-v1", &[&[family], value]);
        entries.push((tree_key, value_hash));
    }
    entries.sort_by(|left, right| oracle_tree_key_cmp(&left.0, &right.0));
    entries.dedup_by(|left, right| oracle_tree_key_cmp(&left.0, &right.0).is_eq());
    entries
}

/// Compose one published field from the family roots it covers, with this
/// file's own hashing — never by calling the production helper.
fn oracle_compose(domain: &[u8], roots: &[[u8; 32]]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    for root in roots {
        hash.update(&(root.len() as u64).to_le_bytes());
        hash.update(root);
    }
    *hash.finalize().as_bytes()
}

pub(crate) fn independent_typed_digest(image: &CommittedContentForTest) -> DirectTypedImageDigest {
    let mut records = vec![Vec::new(); 11];
    for record in &image.logical_records {
        records[record.family as usize].push((record.key.clone(), record.value.clone()));
    }
    let roots: [[u8; 32]; 11] =
        std::array::from_fn(|family| oracle_family_root(family as u8, &records[family]));
    // Family 6 holds the one schema record per table; family 5 holds the
    // ordered index postings; family 9 the config records; family 7 the
    // per-row sync-source records; family 10 the operational records. Each
    // published field that covers more than one of those composes their roots.
    let schema = roots[6];
    let postings = roots[5];
    let configuration = roots[9];
    let sync_sources = roots[7];
    let operational = roots[10];
    let published = [
        roots[0],
        roots[1],
        roots[2],
        roots[3],
        roots[4],
        oracle_compose(
            b"contextdb-digest-schema-and-indexes-v1",
            &[schema, postings],
        ),
        oracle_compose(b"contextdb-digest-policy-v1", &[schema]),
        oracle_compose(
            b"contextdb-digest-sync-v1",
            &[schema, configuration, sync_sources],
        ),
        roots[8],
        oracle_compose(b"contextdb-digest-configuration-v1", &[configuration]),
        oracle_compose(
            b"contextdb-digest-status-v1",
            &[schema, configuration, operational],
        ),
    ];
    DirectTypedImageDigest {
        relational: published[0],
        graph: published[1],
        f32_vectors: published[2],
        sq8_vectors: published[3],
        sq4_vectors: published[4],
        schema_and_indexes: published[5],
        policy: published[6],
        sync: published[7],
        change_and_ddl: published[8],
        configuration: published[9],
        status: published[10],
        complete: oracle_compose(b"contextdb-digest-complete-v1", &published),
    }
}

#[derive(Default)]
struct Snapshots(Mutex<Vec<(ReadRoute, SnapshotId)>>);
impl ReadKernelTestObserver for Snapshots {
    fn registered_snapshot_for_test(
        &self,
        route: ReadRoute,
        _: ReadSessionOperation,
        snapshot: SnapshotId,
    ) {
        self.0.lock().unwrap().push((route, snapshot));
    }
    fn before_source_touch(&self, _: ReadKernelSourceEvent, _: OwnerReadCancellation) {}
    fn cancellation_observed(&self, _: ReadKernelCancellationEvent, _: OwnerReadCancellation) {}
}
struct Observation {
    actual_root: [u8; 32],
    content: CommittedContentForTest,
    digest: DirectTypedImageDigest,
    open_work: ReadWorkCounters,
}
fn observe(path: &Path) -> Observation {
    observe_with_root_comparison(path, true)
}

fn observe_with_root_comparison(path: &Path, compare_typed_root: bool) -> Observation {
    // The independent adapter loads the COMMITTED STORE while idle, not an
    // engine-owned image retained by either production reading session.
    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    let expected = independent_typed_digest(&content);
    let snapshots = Arc::new(Snapshots::default());
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let file = ReadSession::open_with_kernel_observer_for_test(
        path,
        ReadSessionOptions::default(),
        snapshots.clone(),
    )
    .unwrap();
    assert_eq!(file.route(), ReadRoute::File);
    let open_work = open_scope.counters();
    drop(open_scope);
    let identity = file
        .file_snapshot_for_test()
        .expect("actual immutable file identity");
    if compare_typed_root {
        assert_eq!(
            content.maintained_digest, expected,
            "the independent family oracle matches each maintained committed family"
        );
        assert_eq!(
            identity.image_id, expected.complete,
            "independently decoded committed families authenticate the actual reader root"
        );
    }
    let sql = "SELECT id, payload, embedding, sq8, sq4 FROM identity_rows ORDER BY id";
    let file_rows = file.execute(sql, &HashMap::new()).unwrap().rows;
    let file_snapshot = snapshots.0.lock().unwrap().last().copied().unwrap();
    assert_eq!(file_snapshot.0, ReadRoute::File);
    drop(file);

    let runtime = tempfile::tempdir().unwrap();
    let runtime_root = secure_runtime_root(runtime.path());
    let owner = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            owner_reads: OwnerReadConfig {
                runtime_dir: Some(runtime_root.clone()),
                ..Default::default()
            },
            test_kernel_observer: Some(snapshots.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    let owner_session =
        ReadSession::with_runtime_directory_for_test(&runtime_root, || ReadSession::open(path))
            .unwrap();
    assert_eq!(owner_session.route(), ReadRoute::Owner);
    assert_eq!(
        owner_session.execute(sql, &HashMap::new()).unwrap().rows,
        file_rows,
        "the real owner channel reads the same committed row and all vector families"
    );
    let owner_snapshot = snapshots.0.lock().unwrap().last().copied().unwrap();
    assert_eq!(owner_snapshot.0, ReadRoute::Owner);
    assert_eq!(
        owner_snapshot.1, file_snapshot.1,
        "both real kernels registered the same committed MVCC identity"
    );
    // This is the existing MVCC identity, not a substitute content root. The
    // owner protocol currently has no content-root field; do not invent one.
    drop(owner_session);
    owner.close().unwrap();
    let after_owner = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    assert_eq!(
        independent_typed_digest(&after_owner),
        expected,
        "owner read/open/close does not silently alter covered committed content"
    );
    Observation {
        actual_root: identity.image_id,
        content,
        digest: expected,
        open_work,
    }
}
fn caller_driven(path: &Path) -> Database {
    Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap()
}

fn fixture() -> (tempfile::TempDir, PathBuf) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("identity.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    seed(&db);
    db.close().unwrap();
    (root, path)
}
fn mutate(path: &Path, action: impl FnOnce(&Database)) -> Observation {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    action(&db);
    db.close().unwrap();
    observe(path)
}
fn delta(
    before: &Observation,
    after: &Observation,
    name: &str,
    family: fn(&DirectTypedImageDigest) -> [u8; 32],
) {
    assert_ne!(
        family(&before.digest),
        family(&after.digest),
        "the real {name} mutation changed its covered committed family"
    );
    assert_ne!(
        before.actual_root, after.actual_root,
        "a real {name} mutation changes the authenticated file identity"
    );
}
/// What a reader open may hash: the proof path of the last committed
/// commit-index entry and of the last committed schema statement, which the
/// frontiers it exposes are read from. A path is at most one chunk per tree
/// level, and a chunk is at most 64 KiB; eight levels is past any store this
/// suite builds. Nothing else -- no row, vector or full-image hash.
const OPEN_FRONTIER_PROOF_BYTES: u64 = 2 * 8 * 64 * 1024;

fn unchanged_open_hash(observation: &Observation) {
    assert!(
        observation.open_work.hashed_bytes <= OPEN_FRONTIER_PROOF_BYTES
            && observation.open_work.hashed_rows == 0
            && observation.open_work.hashed_vectors == 0,
        "the actual reader open authenticates its root without a full-image hash, proving only \
         its two history frontiers: {:?}",
        observation.open_work
    );
}
fn left() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x1d3a_0000_0000_0000_0000_0000_0000_0001)
}
fn right() -> uuid::Uuid {
    uuid::Uuid::from_u128(0x1d3a_0000_0000_0000_0000_0000_0000_0002)
}

#[test]
fn independent_oracle_matches_the_maintained_typed_digest_for_every_current_family() {
    let (_root, path) = fixture();
    let baseline = observe(&path);
    let repeated = observe(&path);
    assert_eq!(
        baseline.digest, repeated.digest,
        "equal FULL committed content has equal identity"
    );
    assert!(
        !baseline.content.relational_rows.is_empty() && !baseline.content.graph_edges.is_empty()
    );
    for mode in [
        DirectVectorQuantization::F32,
        DirectVectorQuantization::Sq8,
        DirectVectorQuantization::Sq4,
    ] {
        assert!(
            baseline
                .content
                .vectors
                .iter()
                .any(|v| v.quantization == mode)
        );
    }
    assert!(!baseline.content.configuration.schemas.is_empty());
    assert!(!baseline.content.changes.rows.is_empty());
    assert!(!baseline.content.sync.tables.is_empty());
    assert!(!baseline.content.events.event_types.is_empty());
    assert!(!baseline.content.events.sinks.is_empty());
    assert!(!baseline.content.events.routes.is_empty());
    assert!(!baseline.content.events.schedules.is_empty());
    let mut shuffled = baseline.content.clone();
    shuffled.changes.rows.reverse();
    shuffled.changes.edges.reverse();
    shuffled.changes.vectors.reverse();
    shuffled.changes.ddl.reverse();
    shuffled.changes.ddl_lsn.reverse();
    shuffled.logical_records.reverse();
    assert_eq!(
        independent_typed_digest(&shuffled),
        baseline.digest,
        "unordered durable change traversals have one canonical independent identity"
    );
    unchanged_open_hash(&repeated);
}

#[test]
fn nullable_vector_placeholders_live_only_in_vector_identity_families() {
    let (_root, path) = fixture();
    let before = observe(&path);
    let after = mutate(&path, |db| {
        execute(
            db,
            "INSERT INTO identity_rows (id, payload) VALUES ($id, 'null-vector-placeholders')",
            HashMap::from([("id".to_owned(), Value::Uuid(right()))]),
        );
    });
    assert!(after.content.relational_rows.iter().any(|row| {
        row.table == "identity_rows"
            && row.values.get("payload") == Some(&Value::Text("null-vector-placeholders".into()))
    }));
    assert_eq!(
        (
            before.digest.f32_vectors,
            before.digest.sq8_vectors,
            before.digest.sq4_vectors,
        ),
        (
            after.digest.f32_vectors,
            after.digest.sq8_vectors,
            after.digest.sq4_vectors,
        ),
        "nullable vector placeholders do not manufacture vector leaves"
    );
    unchanged_open_hash(&after);
}

#[test]
fn one_row_change_updates_only_bounded_logical_pages() {
    const ROWS: i64 = 512;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("incremental-work.db");
    let db = caller_driven(&path);
    execute(
        &db,
        "CREATE TABLE incremental_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL) HISTORY ALL",
        HashMap::new(),
    );
    let tx = db.begin().unwrap();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO incremental_rows (id, payload) VALUES ($id, 'original')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.close().unwrap();
    // The committed store the update starts from, read independently.
    let before = canonical_stored_ids(
        &{
            let declared_read_memory =
                contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    contextdb_engine::OwnerReadConfig::default()
                        .limits
                        .limits
                        .memory,
                );
            let operation_read_memory = declared_read_memory
                .child()
                .expect("admit the committed-content observation owner");
            committed_content_for_test(&path, operation_read_memory, declared_read_memory)
        }
        .unwrap(),
    );

    let db = caller_driven(&path);
    db.reset_content_identity_work_for_test().unwrap();
    execute(
        &db,
        "UPDATE incremental_rows SET payload = 'changed' WHERE id = 0",
        HashMap::new(),
    );
    let (page_reads, page_writes, page_removes) = db.take_content_identity_work_for_test().unwrap();
    let (page_read_bytes, page_written_bytes) =
        db.take_content_identity_work_bytes_for_test().unwrap();
    let page_operations = page_reads + page_writes + page_removes;
    eprintln!(
        "incremental logical identity point update: rows={ROWS} page_reads={page_reads} page_writes={page_writes} page_removes={page_removes} read_bytes={page_read_bytes} written_bytes={page_written_bytes}"
    );
    assert!(
        page_operations > 0,
        "the real update must maintain its logical pages"
    );
    assert!(
        page_operations < ROWS as u64,
        "one changed row and its commit/history records must not enumerate all {ROWS} row leaves: reads={page_reads} writes={page_writes} removes={page_removes}"
    );
    assert!(
        page_reads <= 32,
        "one transaction reads only the logical pages touched by its coalesced mutations: reads={page_reads}"
    );
    assert!(
        page_writes <= 32,
        "one transaction flushes each changed logical page once: writes={page_writes}"
    );
    assert!(page_read_bytes <= 2 * 1024 * 1024);
    assert!(page_written_bytes <= 2 * 1024 * 1024);
    db.close().unwrap();
    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    // Derived, not guessed: the transaction must remove exactly the stored
    // chunks and key blobs that the committed content no longer has - no
    // fewer, which would orphan them, and no more, which would drop a chunk
    // the image still needs. Both sides come from the independent rebuild of
    // the committed content, never from the implementation's own bookkeeping.
    let retired = before.difference(&canonical_stored_ids(&content)).count();
    assert_eq!(
        page_removes as usize, retired,
        "the update retires exactly the chunks its committed content no longer contains: \
         removes={page_removes} retired={retired}"
    );
    let expected = independent_typed_digest(&content);
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = ReadSession::open(&path).unwrap();
    let open_work = open_scope.counters();
    assert!(
        open_work.hashed_bytes <= OPEN_FRONTIER_PROOF_BYTES,
        "open proves only its two history frontiers: {open_work:?}"
    );
    assert_eq!(
        reader.file_snapshot_for_test().unwrap().image_id,
        expected.complete
    );
    assert_eq!(
        reader
            .execute(
                "SELECT payload FROM incremental_rows WHERE id = 0",
                &HashMap::new(),
            )
            .unwrap()
            .rows,
        vec![vec![Value::Text("changed".into())]]
    );
}

#[test]
fn authoritative_purge_commits_its_complete_logical_root_and_reopens_cleanly() {
    let (_root, path) = fixture();
    let before = observe(&path);
    let after = mutate(&path, |db| {
        execute(
            db,
            "PURGE FROM identity_rows WHERE payload = 'same-content'",
            HashMap::new(),
        );
    });
    assert!(
        after.content.relational_rows.iter().all(|row| {
            row.table != "identity_rows"
                || row.values.get("payload") != Some(&Value::Text("same-content".into()))
        }),
        "PURGE removes the selected current row"
    );
    assert!(
        after
            .content
            .vectors
            .iter()
            .all(|vector| vector.index.table != "identity_rows"),
        "PURGE removes every selected vector family"
    );
    delta(&before, &after, "PURGE row", |digest| digest.relational);
    delta(&before, &after, "PURGE F32 vector", |digest| {
        digest.f32_vectors
    });
    delta(&before, &after, "PURGE SQ8 vector", |digest| {
        digest.sq8_vectors
    });
    delta(&before, &after, "PURGE SQ4 vector", |digest| {
        digest.sq4_vectors
    });
    delta(&before, &after, "PURGE history", |digest| {
        digest.change_and_ddl
    });
    unchanged_open_hash(&after);
}

#[test]
fn committed_image_identity_records_history_and_rollback_never_publishes() {
    let (_root, path) = fixture();
    let baseline = observe(&path);
    let changed = mutate(&path, |db| {
        execute(
            db,
            "UPDATE identity_rows SET payload = 'changed-content'",
            HashMap::new(),
        );
    });
    delta(&baseline, &changed, "ordinary UPDATE", |d| d.relational);
    let rolled_back = mutate(&path, |db| {
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "UPDATE identity_rows SET payload = 'rolled-back'",
            &HashMap::new(),
        )
        .unwrap();
        db.execute_in_tx(
            tx,
            "UPDATE identity_rows SET embedding = $v",
            &HashMap::from([("v".into(), Value::Vector(vec![0., 0., 1.]))]),
        )
        .unwrap();
        db.rollback(tx).unwrap();
    });
    assert_eq!(
        rolled_back.digest, changed.digest,
        "rollback publishes no partial row/vector/history root"
    );
    let equal_rows = mutate(&path, |db| {
        execute(
            db,
            "UPDATE identity_rows SET payload = 'same-content'",
            HashMap::new(),
        )
    });
    assert_eq!(equal_rows.digest.relational, baseline.digest.relational);
    assert_ne!(
        equal_rows.digest.change_and_ddl,
        baseline.digest.change_and_ddl
    );
    assert_ne!(
        equal_rows.digest.complete, baseline.digest.complete,
        "row-only equality cannot erase committed history"
    );
    let no_op_update = mutate(&path, |db| {
        execute(
            db,
            "UPDATE identity_rows SET payload = 'same-content'",
            HashMap::new(),
        )
    });
    assert_eq!(no_op_update.digest.relational, equal_rows.digest.relational);
    delta(
        &equal_rows,
        &no_op_update,
        "equal-value UPDATE history",
        |d| d.change_and_ddl,
    );
    unchanged_open_hash(&no_op_update);
}

#[test]
fn real_vector_and_graph_mutations_change_their_committed_identity_families() {
    let (_root, path) = fixture();
    let mut before = observe(&path);
    let cases: [(&str, fn(&DirectTypedImageDigest) -> [u8; 32]); 3] = [
        ("embedding", |d| d.f32_vectors),
        ("sq8", |d| d.sq8_vectors),
        ("sq4", |d| d.sq4_vectors),
    ];
    for (column, family) in cases {
        let after = mutate(&path, |db| {
            execute(
                db,
                &format!("UPDATE identity_rows SET {column} = $v"),
                HashMap::from([("v".into(), Value::Vector(vec![0.5, 0.5, -0.5]))]),
            )
        });
        delta(&before, &after, column, family);
        before = after;
    }
    let graph = mutate(&path, |db| {
        let tx = db.begin().unwrap();
        db.insert_edge(
            tx,
            left(),
            right(),
            "IDENTITY_LINK".into(),
            HashMap::from([("evidence".into(), Value::Text("new-property".into()))]),
        )
        .unwrap();
        db.commit(tx).unwrap();
    });
    delta(&before, &graph, "graph property insertion", |d| d.graph);
    let deletion = mutate(&path, |db| {
        let tx = db.begin().unwrap();
        db.delete_edge(tx, left(), right(), "IDENTITY_LINK")
            .unwrap();
        db.commit(tx).unwrap();
        execute(db, "DELETE FROM identity_edges", HashMap::new());
    });
    delta(&graph, &deletion, "graph deletion", |d| d.graph);
    let rows_deleted = mutate(&path, |db| {
        execute(db, "DELETE FROM identity_rows", HashMap::new())
    });
    delta(&deletion, &rows_deleted, "row deletion", |d| d.relational);
    for (_, family) in cases {
        assert_ne!(
            family(&deletion.digest),
            family(&rows_deleted.digest),
            "deletion removes each vector family"
        );
    }
    unchanged_open_hash(&rows_deleted);
}

#[test]
fn schema_policy_and_configuration_mutations_are_content_identity_changes() {
    let (_root, path) = fixture();
    let initial = observe(&path);
    let schema = mutate(&path, |db| {
        execute(
            db,
            "ALTER TABLE identity_rows ADD COLUMN revision INTEGER",
            HashMap::new(),
        );
        execute(
            db,
            "CREATE INDEX identity_revision ON identity_rows (revision)",
            HashMap::new(),
        );
    });
    delta(&initial, &schema, "schema/index", |d| d.schema_and_indexes);
    let policy = mutate(&path, |db| {
        execute(
            db,
            "ALTER TABLE identity_rows SET SYNC CONFLICT KEEP LATEST",
            HashMap::new(),
        );
        execute(
            db,
            "ALTER TABLE identity_rows SET HISTORY CURRENT ONLY",
            HashMap::new(),
        );
    });
    delta(&schema, &policy, "declared history/conflict policy", |d| {
        d.policy
    });
    let config = mutate(&path, |db| {
        db.set_memory_limit(Some(95 * 1024 * 1024)).unwrap();
        db.set_disk_limit(Some(511 * 1024 * 1024)).unwrap();
    });
    delta(&policy, &config, "durable memory/disk configuration", |d| {
        d.configuration
    });
    assert_ne!(
        policy.content.durable_auxiliary["configuration"],
        config.content.durable_auxiliary["configuration"]
    );
    unchanged_open_hash(&config);
}

#[test]
fn durable_sink_queue_ack_changes_identity_without_a_row_commit() {
    let (_root, path) = fixture();
    let before = observe(&path);
    assert!(
        before.content.events.sinks[0].queued > 0,
        "fixture queued a real INSERT event"
    );
    let (delivered_tx, delivered_rx) = std::sync::mpsc::channel();
    let after = mutate(&path, |db| {
        let lsn = db.current_lsn();
        db.register_sink("identity_sink", None, move |_| {
            delivered_tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
        delivered_rx
            .recv_timeout(std::time::Duration::from_secs(30))
            .expect("real sink callback ran");
        assert_eq!(
            db.current_lsn(),
            lsn,
            "dispatch is not an application-row transaction"
        );
        // `mutate` closes and joins the dispatcher. Its post-callback durable
        // acknowledgment/audit completes before the on-demand adapter opens.
    });
    assert_eq!(before.digest.relational, after.digest.relational);
    assert_eq!(before.digest.change_and_ddl, after.digest.change_and_ddl);
    assert_eq!(after.content.events.sinks[0].queued, 0);
    assert_ne!(
        before.content.durable_auxiliary["sink_queues"],
        after.content.durable_auxiliary["sink_queues"]
    );
    assert_eq!(
        before.content.durable_auxiliary["sink_audit"],
        after.content.durable_auxiliary["sink_audit"],
        "successful delivery does not manufacture the overflow-only sink audit"
    );
    delta(&before, &after, "sink acknowledgment", |d| d.status);
    unchanged_open_hash(&after);
}

#[test]
fn trigger_audit_and_schedule_firing_are_real_durable_identity_mutations() {
    const BASE: u64 = 2_000_000_000_000;
    let wall = Arc::new(std::sync::atomic::AtomicU64::new(BASE));
    let owned_wall = wall.clone();
    let _clock =
        Wallclock::test_clock_guard(move || owned_wall.load(std::sync::atomic::Ordering::SeqCst));
    let (_root, path) = fixture();
    let declared = mutate(&path, |db| {
        execute(
            db,
            "CREATE TRIGGER identity_trigger ON identity_rows WHEN UPDATE",
            HashMap::new(),
        );
    });
    let triggered = mutate(&path, |db| {
        db.register_trigger_callback("identity_trigger", |_, _| Ok(()))
            .unwrap();
        execute(
            db,
            "UPDATE identity_rows SET payload = 'triggered'",
            HashMap::new(),
        );
        assert!(
            !db.trigger_audit_log().is_empty(),
            "ordinary UPDATE ran its declared trigger"
        );
    });
    assert_ne!(
        declared.content.durable_auxiliary["trigger_audit"],
        triggered.content.durable_auxiliary["trigger_audit"]
    );
    assert_ne!(declared.digest.complete, triggered.digest.complete);
    let scheduled = mutate(&path, |db| {
        let pause = db.pause_cron_tickler_for_test();
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observation = fired.clone();
        db.register_cron_callback("identity_callback", move |_| {
            observation.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        wall.store(
            BASE + 7 * 60 * 60 * 1000 + 1,
            std::sync::atomic::Ordering::SeqCst,
        );
        assert_eq!(
            {
                let seam_read_memory = db
                    .declared_reader_memory()
                    .expect("admit the fixture operation reader");
                let mut seam_copy_owner =
                    contextdb_engine::vector_read_memory::admission(seam_read_memory.clone());
                db.cron_run_due_now_for_test(&mut seam_copy_owner, seam_read_memory)
            }
            .unwrap(),
            1
        );
        assert_eq!(fired.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(db.cron_status()[0].fire_count, 1);
        drop(pause);
    });
    assert_ne!(
        triggered.content.durable_auxiliary["configuration"],
        scheduled.content.durable_auxiliary["configuration"]
    );
    delta(&triggered, &scheduled, "durable schedule firing", |d| {
        d.status
    });
    unchanged_open_hash(&scheduled);
}

#[test]
fn maintenance_pruning_and_retention_declarations_change_the_committed_families() {
    const BASE: u64 = 2_000_000_000_000;
    let wall = Arc::new(std::sync::atomic::AtomicU64::new(BASE));
    let owned_wall = wall.clone();
    let _clock =
        Wallclock::test_clock_guard(move || owned_wall.load(std::sync::atomic::Ordering::SeqCst));
    let (_root, path) = fixture();
    let baseline = observe(&path);
    let retained = mutate(&path, |db| {
        execute(
            db,
            "CREATE TABLE identity_retained (id INTEGER PRIMARY KEY, expires_at TIMESTAMP EXPIRES) RETAIN 1 HOURS",
            HashMap::new(),
        );
        execute(
            db,
            "INSERT INTO identity_retained VALUES (1, $expires)",
            HashMap::from([("expires".into(), Value::Timestamp((BASE + 1000) as i64))]),
        );
    });
    delta(&baseline, &retained, "retention declaration", |d| d.status);
    let pruned = mutate(&path, |db| {
        wall.store(
            BASE + 2 * 60 * 60 * 1000,
            std::sync::atomic::Ordering::SeqCst,
        );
        let report = db.run_pruning_cycle_checked().unwrap();
        assert_eq!(
            report.pruned_rows, 1,
            "the declared retention actually reclaimed its expired row"
        );
        db.run_maintenance_cycle().unwrap();
    });
    delta(&retained, &pruned, "ordinary retention maintenance", |d| {
        d.relational
    });
    unchanged_open_hash(&pruned);
}

#[test]
fn currency_pruning_preserves_surviving_row_and_vector_identity() {
    let (_root, path) = fixture();
    let before_prune = mutate(&path, |db| {
        execute(
            db,
            "CREATE TABLE identity_currency (id INTEGER PRIMARY KEY, payload TEXT NOT NULL, embedding VECTOR(3)) HISTORY CURRENT ONLY SYNC OFF",
            HashMap::new(),
        );
        execute(
            db,
            "INSERT INTO identity_currency VALUES (1, 'first', $v)",
            HashMap::from([("v".into(), Value::Vector(vec![1.0, 0.0, 0.0]))]),
        );
        execute(
            db,
            "UPDATE identity_currency SET payload = 'second', embedding = $v WHERE id = 1",
            HashMap::from([("v".into(), Value::Vector(vec![0.0, 1.0, 0.0]))]),
        );
        execute(
            db,
            "UPDATE identity_currency SET payload = 'current', embedding = $v WHERE id = 1",
            HashMap::from([("v".into(), Value::Vector(vec![0.0, 0.0, 1.0]))]),
        );
    });
    let after_prune = mutate(&path, |db| {
        assert!(
            db.compact_currency_versions().unwrap().pruned_versions > 0,
            "the fixture must execute a real old-version removal"
        );
    });
    assert_eq!(
        before_prune.digest.relational, after_prune.digest.relational,
        "removing superseded row versions keeps the current logical row"
    );
    assert_eq!(
        before_prune.digest.f32_vectors, after_prune.digest.f32_vectors,
        "removing superseded vector versions keeps the current logical vector"
    );
    assert!(after_prune.content.relational_rows.iter().any(|row| {
        row.table == "identity_currency"
            && row.values.get("payload") == Some(&Value::Text("current".into()))
    }));
    unchanged_open_hash(&after_prune);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_push_and_pull_preserve_content_identity_and_sync_provenance() {
    use contextdb_core::TenantId;
    use contextdb_engine::sync_types::{ConflictPolicies, ConflictPolicy};
    use contextdb_server::subjects::push_subject;
    use contextdb_server::{FabricIdentity, InProcessBroker, SyncClient, SyncServer};
    use std::sync::atomic::{AtomicBool, Ordering};
    let (root, path) = fixture();
    let before = observe(&path);
    let hub = Arc::new(
        Database::open_with_options(
            &path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let source_path = root.path().join("source.db");
    let source = Arc::new(
        Database::open_with_options(
            &source_path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    // Real schema-only sync initialization, followed by an authored row on the
    // edge. The ordinary authenticated push supplies its real source identity.
    let mut schema = hub
        .changes_since(Lsn(0))
        .expect("read the declared changes");
    schema.rows.clear();
    schema.edges.clear();
    schema.vectors.clear();
    source
        .apply_changes(
            schema,
            &ConflictPolicies::uniform(ConflictPolicy::LatestWins),
        )
        .unwrap();
    execute(
        &source,
        "INSERT INTO identity_rows (id, payload, embedding, sq8, sq4) VALUES ($id, 'from-edge', $v, $v, $v)",
        HashMap::from([
            ("id".into(), Value::Uuid(uuid::Uuid::from_u128(0x7777))),
            ("v".into(), Value::Vector(vec![0.2, -0.3, 0.8])),
        ]),
    );
    let broker = InProcessBroker::new();
    let hub_identity = Arc::new(FabricIdentity::generate());
    let node = hub_identity.node_id();
    let tenant = TenantId::from("identity-real-sync");
    let server = Arc::new(
        SyncServer::with_authenticated_transport_and_identity_for_test(
            hub.clone(),
            broker.server_as(&node),
            tenant.clone(),
            node,
            hub_identity,
        )
        .expect("construct the authenticated sync endpoint"),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let task = tokio::spawn({
        let server = server.clone();
        let stop = stop.clone();
        async move { server.run_until(stop).await }
    });
    broker
        .wait_for_registered_route_for_test(&push_subject("identity-real-sync"))
        .await;
    let source_identity = Arc::new(FabricIdentity::generate());
    let source_node = source_identity.node_id();
    let client = SyncClient::with_authenticated_transport_and_identity_for_test(
        source.clone(),
        broker.client_as(&source_node),
        tenant,
        source_identity,
    )
    .expect("construct the authenticated sync endpoint");
    let pushed = client.push().await;
    let pulled = if pushed.is_ok() {
        Some(client.pull_default().await)
    } else {
        None
    };
    stop.store(true, Ordering::SeqCst);
    task.await.unwrap();
    pushed.expect("authenticated source mutation reaches the hub");
    pulled
        .unwrap()
        .expect("authenticated pull reads the committed hub state");
    drop(client);
    drop(server);
    Arc::try_unwrap(hub)
        .unwrap_or_else(|_| panic!("server released receiver"))
        .close()
        .unwrap();
    Arc::try_unwrap(source)
        .unwrap_or_else(|_| panic!("client released source"))
        .close()
        .unwrap();
    let after = observe(&path);
    delta(&before, &after, "authenticated sync provenance", |d| d.sync);
    delta(&before, &after, "authenticated synced row", |d| {
        d.relational
    });
    let pulled_state = observe(&source_path);
    assert!(
        pulled_state
            .content
            .relational_rows
            .iter()
            .any(|row| row.values.get("payload") == Some(&Value::Text("from-edge".into())))
    );
    // Hub and edge histories/provenance are different covered content. Do not
    // manufacture complete-root equality from their equal current row values.
    unchanged_open_hash(&after);
    unchanged_open_hash(&pulled_state);
}

#[test]
fn checkpoint_export_and_restore_preserve_the_actual_full_committed_image() {
    let (root, path) = fixture();
    mutate(&path, |db| {
        execute(
            db,
            "UPDATE identity_rows SET payload = 'after-history'",
            HashMap::new(),
        )
    });
    let before = observe(&path);
    let export = root.path().join("checkpoint.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.export_snapshot(&export).unwrap();
    db.close().unwrap();
    let exported = observe(&export);
    assert_eq!(
        exported.digest, before.digest,
        "checkpoint preserves every stable full-image family, including history and queues"
    );
    let restored_writer = Database::open(&export).unwrap();
    restored_writer.close().unwrap();
    let restored = observe(&export);
    assert_eq!(
        restored.digest, exported.digest,
        "restoring/restarting a checkpoint preserves full content identity"
    );
    unchanged_open_hash(&restored);
}

/// The published Status root is reproducible from the DECLARATION alone.
///
/// The oracle reads `TableMeta.maintained_counts` and its own copy of the
/// default — silence maintains none — and never calls the production
/// membership helper, so it disagrees the moment production maintains a count
/// for a list the declaration omits, or drops one the declaration names. This
/// test walks all three readings on a populated table and, for each, checks
/// the independently recomputed typed digest against the published one.
#[test]
fn the_content_identity_root_is_independently_recomputable_from_the_declaration() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("declared-counts.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE placements (id UUID PRIMARY KEY, owner TEXT NOT NULL, slot TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX placements_by_owner_slot ON placements (owner, slot)",
        HashMap::new(),
    );
    for n in 0..24u128 {
        execute(
            &db,
            "INSERT INTO placements (id, owner, slot) VALUES ($id, $owner, $slot)",
            HashMap::from([
                ("id".into(), Value::Uuid(uuid::Uuid::from_u128(n))),
                (
                    "owner".into(),
                    Value::Text(if n % 3 == 0 {
                        "ada".into()
                    } else {
                        "bob".into()
                    }),
                ),
                (
                    "slot".into(),
                    Value::Text(if n % 2 == 0 {
                        "left".into()
                    } else {
                        "right".into()
                    }),
                ),
            ]),
        );
    }
    db.close().unwrap();

    // Silence: the store holds NO count record at all, and the oracle agrees.
    let silent = reconstructed(&path);
    assert_eq!(
        count_records(&silent),
        0,
        "an undeclared table maintains no index-prefix count"
    );

    let declared = declare(&path, "ALTER TABLE placements SET MAINTAIN COUNT (owner)");
    assert_eq!(
        count_records(&declared),
        2,
        "one count per distinct value of the one declared list: ada and bob"
    );

    let all_prefixes = declare(
        &path,
        "ALTER TABLE placements SET MAINTAIN COUNT ALL PREFIXES",
    );
    assert_eq!(
        count_records(&all_prefixes),
        6,
        "two owners plus their four owner/slot pairs"
    );

    let none = declare(&path, "ALTER TABLE placements SET MAINTAIN COUNT NONE");
    assert_eq!(
        count_records(&none),
        0,
        "dropping every list removes exactly its records"
    );

    // The disagreement the oracle exists to produce: recompute the same image
    // while claiming one count the declaration does not name, and the
    // recomputed root must NOT match the published one.
    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    let mut claimed = content.clone();
    claimed.logical_records.push(LogicalContentRecordForTest {
        family: 10,
        key: [
            count_namespace_prefix().as_slice(),
            &(8u64).to_le_bytes(),
            b"invented",
        ]
        .concat(),
        value: 1u64.to_le_bytes().to_vec(),
    });
    assert_ne!(
        independent_typed_digest(&claimed),
        content.maintained_digest,
        "a count the declaration does not name changes the recomputed root, so the oracle \
         cannot agree with a wrong member set"
    );
}

/// The logical-key prefix every `rel-index-count-v2` record opens with: the
/// independent enumeration length-prefixes each key part.
fn count_namespace_prefix() -> Vec<u8> {
    const NAMESPACE: &[u8] = b"rel-index-count-v2";
    let mut prefix = (NAMESPACE.len() as u64).to_le_bytes().to_vec();
    prefix.extend_from_slice(NAMESPACE);
    prefix
}

/// Run one declaration against the store and read the committed image back
/// through the independent adapter, checking the recomputed typed digest
/// against the published one -- the oracle claim itself, without the shared
/// fixture's own schema.
fn declare(path: &Path, ddl: &str) -> CommittedContentForTest {
    let db = Database::open_with_options(
        path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    execute(&db, ddl, HashMap::new());
    db.close().unwrap();
    reconstructed(path)
}

fn reconstructed(path: &Path) -> CommittedContentForTest {
    let content = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    assert_eq!(
        independent_typed_digest(&content),
        content.maintained_digest,
        "the independently recomputed typed digest matches the published one"
    );
    content
}

/// Every `rel-index-count-v2` record the independently enumerated image holds.
fn count_records(content: &CommittedContentForTest) -> usize {
    let prefix = count_namespace_prefix();
    content
        .logical_records
        .iter()
        .filter(|record| record.key.starts_with(&prefix))
        .count()
}

fn stage_atomic_mutation(db: &Database) -> contextdb_core::TxId {
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "UPDATE identity_rows SET payload = 'atomic', embedding = $v, sq8 = $v, sq4 = $v",
        &HashMap::from([("v".into(), Value::Vector(vec![-0.5, 0.5, 0.25]))]),
    )
    .unwrap();
    db.insert_edge(
        tx,
        left(),
        right(),
        "IDENTITY_LINK".into(),
        HashMap::from([("evidence".into(), Value::Text("atomic-property".into()))]),
    )
    .unwrap();
    db.execute_in_tx(tx, "INSERT INTO identity_rows (id, payload, embedding, sq8, sq4) VALUES ($id, 'atomic-new-row', $v, $v, $v)",
        &HashMap::from([("id".into(), Value::Uuid(uuid::Uuid::from_u128(0xa701))),
            ("v".into(), Value::Vector(vec![0., 0., -1.]))])).unwrap();
    tx
}
#[test]
fn rollback_keeps_rows_all_vectors_edges_and_prepared_events_out_of_the_root() {
    let (_root, path) = fixture();
    let before = observe(&path);
    let rollback = mutate(&path, |db| {
        let tx = stage_atomic_mutation(db);
        db.rollback(tx).unwrap();
    });
    assert_eq!(rollback.digest, before.digest);
    assert_eq!(
        rollback.content.durable_auxiliary["sink_queues"],
        before.content.durable_auxiliary["sink_queues"]
    );
    let schema_rollback = mutate(&path, |db| {
        let tx = db.begin().unwrap();
        db.execute_in_tx(
            tx,
            "CREATE TABLE identity_rollback_schema (id INTEGER PRIMARY KEY)",
            &HashMap::new(),
        )
        .unwrap();
        db.rollback(tx).unwrap();
    });
    assert_eq!(
        schema_rollback.digest, before.digest,
        "rolled back schema publishes no content root"
    );
    let committed = mutate(&path, |db| {
        let tx = stage_atomic_mutation(db);
        db.commit(tx).unwrap();
    });
    for family in [
        (
            "row",
            (|d: &DirectTypedImageDigest| d.relational) as fn(&DirectTypedImageDigest) -> [u8; 32],
        ),
        ("F32", |d| d.f32_vectors),
        ("SQ8", |d| d.sq8_vectors),
        ("SQ4", |d| d.sq4_vectors),
        ("edge", |d| d.graph),
        ("event queue", |d| d.status),
        ("history", |d| d.change_and_ddl),
    ] {
        delta(&before, &committed, family.0, family.1);
    }
    unchanged_open_hash(&committed);
}

#[test]
fn crashes_before_commit_and_after_durable_commit_publish_only_complete_roots() {
    use std::io::{BufRead, Write};
    use std::process::{Command, Stdio};
    const ROLE: &str = "CONTEXTDB_IDENTITY_CRASH_BOUNDARY";
    const STORE: &str = "CONTEXTDB_IDENTITY_CRASH_STORE";
    const STAMP: u64 = 2_000_000_000_000;
    let _clock = Wallclock::test_clock_guard(|| STAMP);
    if let Ok(boundary) = std::env::var(ROLE) {
        let path = PathBuf::from(std::env::var(STORE).unwrap());
        let db = Database::open_with_options(
            path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        let tx = stage_atomic_mutation(&db);
        let notify = || {
            println!("IDENTITY_BOUNDARY_REACHED");
            std::io::stdout().flush().unwrap();
        };
        let wait_for_parent = || {
            let mut command = String::new();
            std::io::stdin().read_line(&mut command).unwrap();
            assert_eq!(command.trim(), "release");
        };
        match boundary.as_str() {
            "before-commit" => {
                notify();
                wait_for_parent();
                db.rollback(tx).unwrap();
            }
            "after-durable-before-graph-publication" => {
                // PersistentCompositeStore flushes the WHOLE write set before
                // CompositeStore pauses after relational memory publication.
                // This cut therefore owes the new complete durable root.
                let pause = db.pause_after_relational_apply_for_test();
                std::thread::scope(|scope| {
                    let commit = scope.spawn(|| {
                        let _clock = Wallclock::test_clock_guard(|| STAMP);
                        db.commit(tx)
                    });
                    assert!(pause.wait_until_reached(std::time::Duration::from_secs(30)));
                    notify();
                    wait_for_parent();
                    pause.release();
                    commit.join().unwrap().unwrap();
                });
            }
            "after-commit" => {
                db.commit(tx).unwrap();
                notify();
                wait_for_parent();
            }
            _ => panic!("unknown ordinary transaction boundary"),
        }
        db.close().unwrap();
        return;
    }
    struct Disposable(Option<std::process::Child>);
    impl Drop for Disposable {
        fn drop(&mut self) {
            if let Some(child) = &mut self.0 {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
    let (root, path) = fixture();
    let baseline = observe(&path);
    let expected_path = root.path().join("expected.db");
    std::fs::copy(&path, &expected_path).unwrap();
    let committed = mutate(&expected_path, |db| {
        let tx = stage_atomic_mutation(db);
        db.commit(tx).unwrap();
    });
    assert_ne!(baseline.digest.complete, committed.digest.complete);
    for boundary in [
        "before-commit",
        "after-durable-before-graph-publication",
        "after-commit",
    ] {
        let cut_path = root.path().join(format!("{boundary}.db"));
        std::fs::copy(&path, &cut_path).unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                &crate::child_test_filter(
                    module_path!(),
                    "crashes_before_commit_and_after_durable_commit_publish_only_complete_roots",
                ),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(ROLE, boundary)
            .env(STORE, &cut_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = Disposable(Some(child));
        let stdout = child.0.as_mut().unwrap().stdout.take().unwrap();
        let (ready, observed) = std::sync::mpsc::channel();
        let output = std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if line.unwrap().contains("IDENTITY_BOUNDARY_REACHED") {
                    let _ = ready.send(());
                }
            }
        });
        observed
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("actual transaction reaches the named cut");
        child.0.as_mut().unwrap().kill().unwrap();
        child.0.as_mut().unwrap().wait().unwrap();
        drop(child);
        output.join().unwrap();
        // Ordinary writable recovery runs before a direct read, as required by
        // the existing restart contract. No migration/reset helper is involved.
        let recovery = Database::open_with_options(
            &cut_path,
            DatabaseOpenOptions {
                maintenance_policy: MaintenancePolicy::CallerDriven,
                ..Default::default()
            },
        )
        .unwrap();
        recovery.close().unwrap();
        let recovered = observe(&cut_path);
        let expected = if boundary == "before-commit" {
            &baseline
        } else {
            &committed
        };
        assert_eq!(
            recovered.digest, expected.digest,
            "{boundary}: never expose a proper subset of the complete committed root"
        );
        assert_eq!(
            recovered.content.durable_auxiliary["queue_contents"],
            expected.content.durable_auxiliary["queue_contents"],
            "{boundary}: queued effects share the content transaction"
        );
    }
    unchanged_open_hash(&committed);
}

#[test]
fn generation_publication_keeps_the_root_equal_to_the_independent_committed_content() {
    let (_root, path) = fixture();
    let baseline = observe(&path);
    let published = mutate(&path, |db| {
        let durable_generation = |column: &str| {
            let state = db
                .vector_store_for_test()
                .partition_graph_generation_status(&contextdb_vector::VectorPartitionRef::new(
                    contextdb_core::VectorIndexRef::new("identity_rows", column),
                    contextdb_core::VectorPartitionKey::unpartitioned(),
                ))?;
            state
                .base
                .or(state.dormant_base)
                .filter(|g| g.durable_bytes > 0)
        };
        for _ in 0..64 {
            db.run_maintenance_cycle().unwrap();
            if ["embedding", "sq8", "sq4"]
                .iter()
                .all(|c| durable_generation(c).is_some())
            {
                break;
            }
        }
        for column in ["embedding", "sq8", "sq4"] {
            assert!(
                durable_generation(column).is_some(),
                "maintenance really published {column}'s durable saved generation"
            );
        }
    });
    // A derived graph publication need not change the logical content covered
    // by the typed digest. It must still authenticate EXACTLY that content,
    // including history; raw current-row equality would be an inadequate check.
    assert_eq!(
        published.digest,
        independent_typed_digest(&published.content)
    );
    if independent_typed_digest(&baseline.content) == independent_typed_digest(&published.content) {
        assert_eq!(published.digest.complete, baseline.digest.complete);
    }
    let updated = mutate(&path, |db| {
        execute(
            db,
            "UPDATE identity_rows SET embedding = $v",
            HashMap::from([("v".into(), Value::Vector(vec![0., -1., 0.]))]),
        );
        db.run_maintenance_cycle().unwrap();
    });
    delta(
        &published,
        &updated,
        "write followed by generation publication",
        |d| d.f32_vectors,
    );
    unchanged_open_hash(&updated);
}

#[test]
fn pruning_only_durable_trigger_audit_cannot_leave_a_stale_content_root() {
    const BASE: u64 = 2_000_000_000_000;
    let wall = Arc::new(std::sync::atomic::AtomicU64::new(BASE));
    let supplied = wall.clone();
    let _clock =
        Wallclock::test_clock_guard(move || supplied.load(std::sync::atomic::Ordering::SeqCst));
    let (_root, path) = fixture();
    mutate(&path, |db| {
        execute(
            db,
            "CREATE TRIGGER identity_audit ON identity_rows WHEN UPDATE",
            HashMap::new(),
        );
        db.register_trigger_callback("identity_audit", |_, _| Ok(()))
            .unwrap();
        execute(
            db,
            "UPDATE identity_rows SET payload = 'audited'",
            HashMap::new(),
        );
        assert!(!db.trigger_audit_log().is_empty());
        // Build existing vector work before the audit-only mutation below.
        for _ in 0..8 {
            db.run_maintenance_cycle().unwrap();
        }
    });
    let before = observe_with_root_comparison(&path, false);
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            maintenance_policy: MaintenancePolicy::CallerDriven,
            ..Default::default()
        },
    )
    .unwrap();
    {
        db.set_trigger_audit_retention(std::time::Duration::from_secs(1))
            .unwrap();
        wall.store(BASE + 2000, std::sync::atomic::Ordering::SeqCst);
        let lsn = db.current_lsn();
        db.run_maintenance_cycle().unwrap();
        assert_eq!(
            db.current_lsn(),
            lsn,
            "audit retention is not a row/DDL commit"
        );
    }
    db.close().unwrap();
    let after = observe_with_root_comparison(&path, false);
    assert_ne!(
        before.content.durable_auxiliary["trigger_audit"],
        after.content.durable_auxiliary["trigger_audit"],
        "the actual committed audit bytes were pruned"
    );
    assert_eq!(before.digest.relational, after.digest.relational);
    assert_eq!(before.digest.change_and_ddl, after.digest.change_and_ddl);
    assert_ne!(
        before.actual_root, after.actual_root,
        "covered durable audit content changed without an LSN or row change; its root must change"
    );
    unchanged_open_hash(&after);
}

fn relational_tree_key(table: &str, row_id: contextdb_core::RowId) -> Vec<u8> {
    let mut record = Vec::new();
    for part in [table.as_bytes(), &row_id.0.to_le_bytes()] {
        record.extend_from_slice(&(part.len() as u64).to_le_bytes());
        record.extend_from_slice(part);
    }
    std::iter::once(0)
        .chain(oracle_hash_parts(
            b"contextdb-logical-record-key-v1",
            &[&[0], &record],
        ))
        .collect()
}

/// The two cases the old `page_removes <= 32` bound was covering for: a change
/// that really does retire a canonical separator, and a transaction that stages
/// the same row several times. Both are held to what the independently rebuilt
/// committed content says must be gone, not to a number.
#[test]
fn a_retired_separator_and_a_coalesced_update_remove_exactly_the_canonical_chunks() {
    const ROWS: i64 = 512;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("separator.db");
    let db = caller_driven(&path);
    execute(
        &db,
        "CREATE TABLE separator_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
        HashMap::new(),
    );
    let tx = db.begin().unwrap();
    for id in 0..ROWS {
        db.execute_in_tx(
            tx,
            "INSERT INTO separator_rows (id, payload) VALUES ($id, 'original')",
            &HashMap::from([("id".to_owned(), Value::Int64(id))]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.close().unwrap();

    let before_image = {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        committed_content_for_test(&path, operation_read_memory, declared_read_memory)
    }
    .unwrap();
    let before = canonical_stored_ids(&before_image);
    let relational = before_image
        .logical_records
        .iter()
        .filter(|record| record.family == 0)
        .map(|record| (record.key.clone(), record.value.clone()))
        .collect::<Vec<_>>();
    let separators = oracle_leaf_separators(0, &relational)
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    assert!(
        separators.len() > 1,
        "the fixture spans several canonical leaf chunks"
    );
    let terminator = before_image
        .relational_rows
        .iter()
        .find(|row| {
            row.table == "separator_rows"
                && separators.contains(&relational_tree_key(&row.table, row.row_id))
        })
        .expect("a stored row terminates a canonical leaf chunk");
    let terminator_id = terminator
        .values
        .get("id")
        .cloned()
        .expect("the terminating row's declared key");

    let db = caller_driven(&path);
    db.reset_content_identity_work_for_test().unwrap();
    execute(
        &db,
        "DELETE FROM separator_rows WHERE id = $id",
        HashMap::from([("id".to_owned(), terminator_id)]),
    );
    let (_, _, page_removes) = db.take_content_identity_work_for_test().unwrap();
    db.close().unwrap();
    let after = canonical_stored_ids(
        &{
            let declared_read_memory =
                contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    contextdb_engine::OwnerReadConfig::default()
                        .limits
                        .limits
                        .memory,
                );
            let operation_read_memory = declared_read_memory
                .child()
                .expect("admit the committed-content observation owner");
            committed_content_for_test(&path, operation_read_memory, declared_read_memory)
        }
        .unwrap(),
    );
    let retired = before.difference(&after).count();
    eprintln!("separator replacement: removes={page_removes} retired={retired}");
    assert!(
        retired > 0,
        "removing a chunk's own separator really does retire canonical chunks"
    );
    assert_eq!(
        page_removes as usize, retired,
        "a replaced separator removes exactly the chunks the committed content no longer has"
    );

    // One transaction that stages the same row several times is the same
    // committed content, and therefore exactly the same proof work, as one
    // change to its final value.
    let staged = |values: &[&str]| {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("coalesced.db");
        let db = caller_driven(&path);
        execute(
            &db,
            "CREATE TABLE coalesced_rows (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)",
            HashMap::new(),
        );
        let tx = db.begin().unwrap();
        for id in 0..ROWS {
            db.execute_in_tx(
                tx,
                "INSERT INTO coalesced_rows (id, payload) VALUES ($id, 'original')",
                &HashMap::from([("id".to_owned(), Value::Int64(id))]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        db.close().unwrap();
        let before = canonical_stored_ids(
            &{
                let declared_read_memory =
                    contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                        contextdb_engine::OwnerReadConfig::default()
                            .limits
                            .limits
                            .memory,
                    );
                let operation_read_memory = declared_read_memory
                    .child()
                    .expect("admit the committed-content observation owner");
                committed_content_for_test(&path, operation_read_memory, declared_read_memory)
            }
            .unwrap(),
        );

        let db = caller_driven(&path);
        db.reset_content_identity_work_for_test().unwrap();
        let tx = db.begin().unwrap();
        for value in values {
            db.execute_in_tx(
                tx,
                "UPDATE coalesced_rows SET payload = $payload WHERE id = 7",
                &HashMap::from([("payload".to_owned(), Value::Text((*value).to_owned()))]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
        let work = db.take_content_identity_work_for_test().unwrap();
        db.close().unwrap();
        let image = {
            let declared_read_memory =
                contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    contextdb_engine::OwnerReadConfig::default()
                        .limits
                        .limits
                        .memory,
                );
            let operation_read_memory = declared_read_memory
                .child()
                .expect("admit the committed-content observation owner");
            committed_content_for_test(&path, operation_read_memory, declared_read_memory)
        }
        .unwrap();
        let retired = before.difference(&canonical_stored_ids(&image)).count();
        (work, retired, independent_typed_digest(&image))
    };
    let (once_work, once_retired, once_digest) = staged(&["final"]);
    let (staged_work, staged_retired, staged_digest) =
        staged(&["first", "second", "third", "final"]);
    eprintln!(
        "coalesced update: once={once_work:?} retired={once_retired}; \
         staged={staged_work:?} retired={staged_retired}"
    );
    assert_eq!(
        once_digest, staged_digest,
        "four staged versions of one row commit the content of the last one"
    );
    assert_eq!(
        once_work, staged_work,
        "and therefore cost exactly the proof work of one change"
    );
    assert_eq!(staged_work.2 as usize, staged_retired);
    assert_eq!(once_work.2 as usize, once_retired);
}

fn oversized_label(marker: char) -> String {
    let mut label = String::with_capacity(70 * 1024);
    label.push(marker);
    label.extend(std::iter::repeat_n('o', 70 * 1024 - 1));
    label
}

/// Keys above one authenticated chunk go through the whole life of a record —
/// insert, update, delete, reopen and a rolled-back transaction — and the
/// independently rebuilt image agrees at every step, including exactly which
/// stored chunks and key blobs each step retires.
#[test]
fn oversized_index_keys_match_the_independent_rebuild_through_their_whole_life() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("oversized-lifecycle.db");
    let db = caller_driven(&path);
    execute(
        &db,
        "CREATE TABLE oversized_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX oversized_rows_label ON oversized_rows (label ASC)",
        HashMap::new(),
    );
    let tx = db.begin().unwrap();
    for (row, marker) in ['a', 'b', 'c', 'd'].into_iter().enumerate() {
        db.execute_in_tx(
            tx,
            "INSERT INTO oversized_rows (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(row as i64)),
                ("label".to_owned(), Value::Text(oversized_label(marker))),
            ]),
        )
        .unwrap();
    }
    // Ordinary keys beside them: one canonical image has to hold both forms.
    for row in 4..24i64 {
        db.execute_in_tx(
            tx,
            "INSERT INTO oversized_rows (id, label) VALUES ($id, $label)",
            &HashMap::from([
                ("id".to_owned(), Value::Int64(row)),
                (
                    "label".to_owned(),
                    Value::Text(format!("ordinary-{row:03}")),
                ),
            ]),
        )
        .unwrap();
    }
    db.commit(tx).unwrap();
    db.close().unwrap();

    let agrees = |what: &str| {
        let content = {
            let declared_read_memory =
                contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                    contextdb_engine::OwnerReadConfig::default()
                        .limits
                        .limits
                        .memory,
                );
            let operation_read_memory = declared_read_memory
                .child()
                .expect("admit the committed-content observation owner");
            committed_content_for_test(&path, operation_read_memory, declared_read_memory)
        }
        .unwrap();
        let expected = independent_typed_digest(&content);
        assert_eq!(
            content.maintained_digest, expected,
            "the independent rebuild matches the maintained image after {what}"
        );
        // A reader still authenticates its root against that image.
        let reader = ReadSession::open(&path).unwrap();
        assert_eq!(
            reader.file_snapshot_for_test().unwrap().image_id,
            expected.complete,
            "the reader root matches the independent rebuild after {what}"
        );
        drop(reader);
        (canonical_stored_ids(&content), expected)
    };
    let (mut ids, inserted) = agrees("insert");

    // Each step retires exactly the stored entries its committed content no
    // longer contains, oversized keys and their blobs included.
    let mut step = |what: &str, sql: &str, params: HashMap<String, Value>| {
        let db = caller_driven(&path);
        db.reset_content_identity_work_for_test().unwrap();
        execute(&db, sql, params);
        let (_, _, removes) = db.take_content_identity_work_for_test().unwrap();
        db.close().unwrap();
        let (after, digest) = agrees(what);
        let retired = ids.difference(&after).count();
        eprintln!("{what}: removes={removes} retired={retired}");
        assert_eq!(
            removes as usize, retired,
            "{what} retires exactly what the rebuild says is gone"
        );
        ids = after;
        digest
    };
    let updated = step(
        "update of an oversized key",
        "UPDATE oversized_rows SET label = $label WHERE id = 1",
        HashMap::from([("label".to_owned(), Value::Text(oversized_label('z')))]),
    );
    assert_ne!(
        inserted.schema_and_indexes, updated.schema_and_indexes,
        "replacing an oversized index key changes its committed family"
    );
    let deleted = step(
        "delete of an oversized row",
        "DELETE FROM oversized_rows WHERE id = 2",
        HashMap::new(),
    );
    assert_ne!(updated.relational, deleted.relational);

    // Reopen: the oversized keys are read back through their stored
    // references, and nothing about the committed image moves.
    let db = caller_driven(&path);
    assert_eq!(
        db.execute(
            "SELECT id FROM oversized_rows ORDER BY label",
            &HashMap::new()
        )
        .unwrap()
        .rows
        .len(),
        23,
        "every surviving row reads back after reopen"
    );
    db.close().unwrap();
    let (after_reopen, reopened) = agrees("reopen");
    assert_eq!(reopened, deleted, "reopening changes no committed family");
    assert_eq!(after_reopen, ids, "reopening retires nothing");

    // A rolled-back transaction over an oversized key publishes nothing.
    let db = caller_driven(&path);
    let tx = db.begin().unwrap();
    db.execute_in_tx(
        tx,
        "INSERT INTO oversized_rows (id, label) VALUES ($id, $label)",
        &HashMap::from([
            ("id".to_owned(), Value::Int64(99)),
            ("label".to_owned(), Value::Text(oversized_label('r'))),
        ]),
    )
    .unwrap();
    db.execute_in_tx(
        tx,
        "DELETE FROM oversized_rows WHERE id = 3",
        &HashMap::new(),
    )
    .unwrap();
    db.rollback(tx).unwrap();
    db.close().unwrap();
    let (after_abort, aborted) = agrees("a rolled-back transaction");
    assert_eq!(
        aborted, reopened,
        "a rolled-back oversized insert and delete publish no family"
    );
    assert_eq!(
        after_abort, ids,
        "a rolled-back transaction leaves every stored chunk and key blob in place"
    );
}
