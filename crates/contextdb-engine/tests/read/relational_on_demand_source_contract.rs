#![allow(clippy::assertions_on_constants)] // Existing compatibility and test seam shape.
use super::reader_proof_support as fixture;
use contextdb_core::Value;
use contextdb_engine::Database;
use contextdb_engine::direct_file_reader::test_seams::{ReadWorkObservationScope, ReadWorkPhase};
use redb::{ReadableDatabase, ReadableTable, TableDefinition, TableHandle};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

const ROWS: usize = 6_400;
const BUCKETS: usize = 8;
const PAYLOAD_BYTES: usize = 4_096;

fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(PAYLOAD_BYTES / 8)
}

fn params(values: impl IntoIterator<Item = (&'static str, Value)>) -> HashMap<String, Value> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

fn seed(path: &std::path::Path) {
    assert!(ROWS * PAYLOAD_BYTES > fixture::SESSION_BYTES);
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE relational_work (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, sequence INTEGER NOT NULL, payload TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX relational_bucket_sequence ON relational_work (bucket, sequence)",
        &HashMap::new(),
    )
    .unwrap();
    // This file's claims are ABOUT the maintained index-prefix counts, so the
    // fixtures declare them. Silence maintains none, and there would be no
    // maintained count here to be exact about.
    db.execute(
        "ALTER TABLE relational_work SET MAINTAIN COUNT ALL PREFIXES",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..ROWS).step_by(400) {
        let tx = db.begin().unwrap();
        for row in start..(start + 400).min(ROWS) {
            db.execute_in_tx(
                tx,
                "INSERT INTO relational_work VALUES ($id, $bucket, $sequence, $payload)",
                &params([
                    ("id", Value::Int64(row as i64)),
                    ("bucket", Value::Int64((row % BUCKETS) as i64)),
                    ("sequence", Value::Int64((row / BUCKETS) as i64)),
                    ("payload", Value::Text(payload(row))),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    db.close().unwrap();
}

#[test]
fn public_relational_reads_fault_only_the_selected_rows_and_survive_writer_recycle() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("relational-on-demand.db");
    seed(&path);

    let routes = Arc::new(fixture::Routes::default());
    let open_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Open);
    let reader = fixture::open_file(&path, fixture::options(), routes.clone());
    fixture::assert_open_proves_history_and_startup_digest(open_scope.counters(), 6, 5_750);
    drop(open_scope);
    let snapshot = reader.file_snapshot_for_test().unwrap();

    let point_scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let point = reader
        .execute(
            "SELECT payload FROM relational_work WHERE id = $id",
            &params([("id", Value::Int64(17))]),
        )
        .unwrap();
    assert_eq!(point.rows(), vec![vec![Value::Text(payload(17))]]);
    assert!(point.trace().index_used.is_some());
    let point_work = point_scope.counters();
    assert!(point_work.decoded_rows > 0 && point_work.decoded_rows <= 2);
    drop(point_scope);

    let range = reader
        .execute(
            "SELECT id FROM relational_work WHERE bucket = $bucket AND sequence >= $low AND sequence < $high ORDER BY sequence LIMIT 16",
            &params([
                ("bucket", Value::Int64(3)),
                ("low", Value::Int64(100)),
                ("high", Value::Int64(116)),
            ]),
        )
        .unwrap();
    assert_eq!(
        range.rows(),
        (100..116)
            .map(|sequence| vec![Value::Int64(sequence * BUCKETS as i64 + 3)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        range.trace().index_used.as_deref(),
        Some("relational_bucket_sequence")
    );
    assert!(range.trace().sort_elided);
    let reverse = reader
        .execute(
            "SELECT id FROM relational_work WHERE bucket = $bucket AND sequence >= $low AND sequence < $high ORDER BY sequence DESC LIMIT 16",
            &params([
                ("bucket", Value::Int64(3)),
                ("low", Value::Int64(100)),
                ("high", Value::Int64(116)),
            ]),
        )
        .unwrap();
    assert_eq!(
        reverse.rows(),
        (100..116)
            .rev()
            .map(|sequence| vec![Value::Int64(sequence * BUCKETS as i64 + 3)])
            .collect::<Vec<_>>()
    );
    assert!(reverse.trace().sort_elided);

    let physical = reader
        .execute(
            "SELECT id FROM relational_work WHERE payload = $payload LIMIT 1",
            &params([("payload", Value::Text(payload(0)))]),
        )
        .unwrap();
    assert_eq!(physical.rows(), vec![vec![Value::Int64(0)]]);
    assert!(physical.trace().index_used.is_none());

    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "UPDATE relational_work SET bucket = 1, sequence = 900, payload = 'new-snapshot-value' WHERE id = $id",
            &params([("id", Value::Int64(5_000))]),
        )
        .unwrap();
    let compacted = writer.compact_now().unwrap();
    assert!(compacted.ran && compacted.handle_recycled);
    writer.close().unwrap();

    let backend_bytes = Arc::new(AtomicU64::new(0));
    let observed = backend_bytes.clone();
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_add(bytes as u64, Ordering::SeqCst);
    }));
    let old = reader
        .execute(
            "SELECT payload FROM relational_work WHERE id = $id",
            &params([("id", Value::Int64(5_000))]),
        )
        .unwrap();
    drop(observation);
    assert_eq!(old.rows(), vec![vec![Value::Text(payload(5_000))]]);
    assert!(backend_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    assert_eq!(
        reader
            .execute(
                "SELECT id FROM relational_work WHERE bucket = 0 AND sequence = 625",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(5_000)]]
    );

    for bucket in 0..BUCKETS {
        let rows = reader
            .execute(
                "SELECT payload FROM relational_work WHERE bucket = $bucket ORDER BY sequence",
                &params([("bucket", Value::Int64(bucket as i64))]),
            )
            .unwrap();
        assert_eq!(rows.rows().len(), ROWS / BUCKETS);
        assert_eq!(rows.rows()[0], vec![Value::Text(payload(bucket))]);
        assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    }
    let reloaded_bytes = Arc::new(AtomicU64::new(0));
    let observed = reloaded_bytes.clone();
    let observation = redb::observe_backend_reads(Arc::new(move |bytes| {
        observed.fetch_add(bytes as u64, Ordering::SeqCst);
    }));
    let reloaded = reader
        .execute(
            "SELECT payload FROM relational_work WHERE bucket = 0 ORDER BY sequence",
            &HashMap::new(),
        )
        .unwrap();
    drop(observation);
    assert_eq!(reloaded.rows().len(), ROWS / BUCKETS);
    assert!(reloaded_bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(reader.file_snapshot_for_test(), Some(snapshot));
    routes.file_only();
    drop(reader);

    let newer = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_ne!(newer.file_snapshot_for_test(), Some(snapshot));
    assert_eq!(
        newer
            .execute(
                "SELECT id, payload FROM relational_work WHERE bucket = 1 AND sequence = 900",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![
            Value::Int64(5_000),
            Value::Text("new-snapshot-value".to_owned())
        ]]
    );
    assert!(
        newer
            .execute(
                "SELECT id FROM relational_work WHERE bucket = 0 AND sequence = 625",
                &HashMap::new(),
            )
            .unwrap()
            .rows()
            .is_empty()
    );
}

#[test]
fn authenticated_index_count_stays_snapshot_exact_without_loading_matching_row_bodies() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("relational-count-on-demand.db");
    seed(&path);

    let old = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    let count = |reader: &contextdb_engine::ReadSession, bucket: i64| {
        let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let result = reader
            .execute(
                "SELECT COUNT(*) AS n FROM relational_work WHERE bucket = $bucket",
                &params([("bucket", Value::Int64(bucket))]),
            )
            .unwrap();
        let work = scope.counters();
        drop(scope);
        (result, work)
    };

    let (before, before_work) = count(&old, 3);
    assert_eq!(
        before.rows(),
        vec![vec![Value::Int64((ROWS / BUCKETS) as i64)]]
    );
    assert_eq!(
        before_work.decoded_rows, 0,
        "an exact maintained index count must not decode each matching row body: {before_work:?}"
    );
    assert_eq!(
        before_work.hashed_rows, 0,
        "the authenticated count record replaces per-row hashing: {before_work:?}"
    );

    let writer = Database::open(&path).unwrap();
    writer
        .execute(
            "INSERT INTO relational_work VALUES ($id, $bucket, $sequence, $payload)",
            &params([
                ("id", Value::Int64(ROWS as i64)),
                ("bucket", Value::Int64(3)),
                ("sequence", Value::Int64((ROWS / BUCKETS) as i64)),
                ("payload", Value::Text(payload(ROWS))),
            ]),
        )
        .unwrap();
    writer
        .execute(
            "INSERT INTO relational_work VALUES ($id, $bucket, $sequence, $payload)",
            &params([
                ("id", Value::Int64((ROWS + 1) as i64)),
                ("bucket", Value::Int64(7)),
                ("sequence", Value::Int64((ROWS / BUCKETS) as i64)),
                ("payload", Value::Text(payload(ROWS + 1))),
            ]),
        )
        .unwrap();
    writer.close().unwrap();

    let (old_after_commit, old_after_work) = count(&old, 3);
    assert_eq!(old_after_commit.rows(), before.rows());
    assert_eq!(old_after_work.decoded_rows, 0);
    drop(old);

    let fresh = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    let (matching, matching_work) = count(&fresh, 3);
    assert_eq!(
        matching.rows(),
        vec![vec![Value::Int64((ROWS / BUCKETS + 1) as i64)]]
    );
    assert_eq!(matching_work.decoded_rows, 0);
    assert_eq!(matching_work.hashed_rows, 0);
    let (unrelated, unrelated_work) = count(&fresh, 0);
    assert_eq!(unrelated.rows(), before.rows());
    assert_eq!(unrelated_work.decoded_rows, 0);
    assert_eq!(unrelated_work.hashed_rows, 0);
    let (absent, absent_work) = count(&fresh, i64::MAX);
    assert_eq!(absent.rows(), vec![vec![Value::Int64(0)]]);
    assert_eq!(
        absent_work.decoded_rows, 0,
        "authenticated count absence is an exact zero without row fallback: {absent_work:?}"
    );
    assert_eq!(absent_work.hashed_rows, 0);

    drop(fresh);
    let raw = redb::Database::open(&path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<&[u8], u64> = TableDefinition::new("__rel_index_counts");
        let mut counts = write.open_table(definition).unwrap();
        let altered = counts
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, count) = entry.unwrap();
                (key.value().to_vec(), count.value().saturating_add(1))
            })
            .collect::<Vec<_>>();
        assert!(!altered.is_empty());
        for (key, count) in altered {
            counts.insert(key.as_slice(), count).unwrap();
        }
    }
    write.commit().unwrap();
    drop(raw);

    let corrupted = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    let error = corrupted
        .execute(
            "SELECT COUNT(*) AS n FROM relational_work WHERE bucket = $bucket",
            &params([("bucket", Value::Int64(3))]),
        )
        .expect_err("a physically changed count must fail content authentication");
    assert!(
        error.to_string().contains("does not authenticate"),
        "the count fails through the content-identity boundary: {error}"
    );
}

/// Indexes that share leading columns share one stored count, and a prefix a
/// primary key or unique constraint already bounds at 0 or 1 stores none at all
/// — the postings hold at most one entry for such a key, so `COUNT(*)` reads it
/// from there and answers exactly the same number.
#[test]
fn shared_index_prefixes_have_one_exact_count_per_row_and_prefix() {
    fn count_records(path: &std::path::Path) -> usize {
        let raw = redb::Database::open(path).unwrap();
        let read = raw.begin_read().unwrap();
        let counts = read
            .open_table(TableDefinition::<&[u8], u64>::new("__rel_index_counts"))
            .unwrap();
        counts.iter().unwrap().count()
    }

    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("shared-index-prefix-counts.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE shared_counts (id INTEGER PRIMARY KEY, bucket INTEGER NOT NULL, sequence INTEGER NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX shared_counts_bucket ON shared_counts (bucket)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "ALTER TABLE shared_counts SET MAINTAIN COUNT ALL PREFIXES",
        &HashMap::new(),
    )
    .unwrap();
    for (id, bucket, sequence) in [(0, 0, 0), (1, 0, 1), (2, 1, 0), (3, 1, 1)] {
        db.execute(
            "INSERT INTO shared_counts VALUES ($id, $bucket, $sequence)",
            &params([
                ("id", Value::Int64(id)),
                ("bucket", Value::Int64(bucket)),
                ("sequence", Value::Int64(sequence)),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();
    let records_before_composite = count_records(&path);

    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE INDEX shared_counts_bucket_sequence ON shared_counts (bucket, sequence)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    let records_after_composite = count_records(&path);
    assert_eq!(
        records_after_composite - records_before_composite,
        4,
        "the composite index adds only its four distinct second-level prefixes"
    );

    let reader = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(
        reader
            .execute(
                "SELECT COUNT(*) AS n FROM shared_counts WHERE bucket = 0",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(2)]]
    );
    assert_eq!(
        reader
            .execute(
                "SELECT COUNT(*) AS n FROM shared_counts WHERE bucket = 0 AND sequence = 1",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]]
    );
    drop(reader);

    let db = Database::open(&path).unwrap();
    db.execute(
        "INSERT INTO shared_counts VALUES (4, 0, 2)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    assert_eq!(
        count_records(&path) - records_after_composite,
        1,
        "one new row adds one new composite suffix, and nothing for its primary key: a count over \
         the whole of a primary key can only be 0 or 1, so it is not stored"
    );
    let reader = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(
        reader
            .execute(
                "SELECT COUNT(*) AS n FROM shared_counts WHERE bucket = 0",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(3)]]
    );
    // The bounded case, asserted rather than assumed: storing no count for a
    // primary key does not cost the answer. A present key counts one, an absent
    // key counts zero, read through the same authenticated index.
    assert_eq!(
        reader
            .execute(
                "SELECT COUNT(*) AS n FROM shared_counts WHERE id = 4",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(1)]],
        "a primary key that holds a row counts one although no count is stored for it"
    );
    assert_eq!(
        reader
            .execute(
                "SELECT COUNT(*) AS n FROM shared_counts WHERE id = 99",
                &HashMap::new(),
            )
            .unwrap()
            .rows(),
        vec![vec![Value::Int64(0)]],
        "and a primary key that holds none counts zero"
    );
}

#[test]
fn authenticated_exact_counts_encode_the_planners_variant_exact_probe_values() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("coerced-count-keys.db");
    let id = Uuid::from_u128(0xC0A1_0000_0000_0000_0000_0000_0000_0001);
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE coerced_counts (id UUID PRIMARY KEY, whole INTEGER NOT NULL, enabled BOOL NOT NULL, token UUID NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    for index in [
        "CREATE INDEX coerced_counts_whole ON coerced_counts (whole)",
        "CREATE INDEX coerced_counts_enabled ON coerced_counts (enabled)",
        "CREATE INDEX coerced_counts_token ON coerced_counts (token)",
        "ALTER TABLE coerced_counts SET MAINTAIN COUNT ALL PREFIXES",
    ] {
        db.execute(index, &HashMap::new()).unwrap();
    }
    db.execute(
        "INSERT INTO coerced_counts VALUES ($id, 7, true, $token)",
        &params([("id", Value::Uuid(id)), ("token", Value::Uuid(id))]),
    )
    .unwrap();
    db.close().unwrap();

    let reader = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    for (column, probe) in [
        ("whole", Value::Int64(7)),
        ("enabled", Value::Bool(true)),
        ("token", Value::Uuid(id)),
    ] {
        let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
        let result = reader
            .execute(
                &format!("SELECT COUNT(*) AS n FROM coerced_counts WHERE {column} = $probe"),
                &params([("probe", probe)]),
            )
            .unwrap();
        let work = scope.counters();
        drop(scope);
        assert_eq!(result.rows(), vec![vec![Value::Int64(1)]], "{column} count");
        assert_eq!(
            work.decoded_rows, 0,
            "the variant-exact {column} probe reaches the matching authenticated count key: {work:?}"
        );
        assert_eq!(
            work.hashed_rows, 0,
            "the count key replaces row authentication"
        );
    }

    let absent = reader
        .execute(
            "SELECT COUNT(*) AS n FROM coerced_counts WHERE whole = $probe",
            &params([("probe", Value::Int64(999))]),
        )
        .unwrap();
    assert_eq!(absent.rows(), vec![vec![Value::Int64(0)]]);
}

const NEAR_LIMIT_INDEX_TEXT_BYTES: usize = 60 * 1024;
const WIDE_INDEX_TEXT_BYTES: usize = 70 * 1024;

fn wide_label(marker: char, bytes: usize) -> String {
    let mut label = String::with_capacity(bytes);
    label.push(marker);
    label.extend(std::iter::repeat_n('x', bytes - 1));
    label
}

/// A 70 KiB indexed TEXT value is ordinary user data: it commits, reads back
/// through its index and authenticates against the committed root. The 60 KiB
/// value beside it keeps the existing inline coverage honest, and damaging the
/// stored wide key must surface as content-authentication failure rather than
/// a silently different answer.
#[test]
fn a_seventy_kibibyte_indexed_text_value_commits_reads_and_authenticates() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("wide-index-key.db");
    let labels = [
        ('a', 32),
        ('b', NEAR_LIMIT_INDEX_TEXT_BYTES),
        ('c', WIDE_INDEX_TEXT_BYTES),
    ];

    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE wide_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX wide_rows_label ON wide_rows (label ASC)",
        &HashMap::new(),
    )
    .unwrap();
    for (row, (marker, bytes)) in labels.iter().enumerate() {
        db.execute(
            "INSERT INTO wide_rows VALUES ($id, $label)",
            &params([
                ("id", Value::Int64(row as i64)),
                ("label", Value::Text(wide_label(*marker, *bytes))),
            ]),
        )
        .expect("a valid indexed TEXT value commits whatever its size");
    }
    db.close().unwrap();

    let reader = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    let ordered = {
        let fixture_answer_23 = reader
            .execute("SELECT id FROM wide_rows ORDER BY label", &HashMap::new())
            .unwrap();
        fixture_answer_23.rows().to_vec()
    };
    assert_eq!(
        ordered,
        vec![
            vec![Value::Int64(0)],
            vec![Value::Int64(1)],
            vec![Value::Int64(2)]
        ],
        "the authenticated index answers in declared order across every key size"
    );
    let selected = {
        let fixture_answer_24 = reader
            .execute(
                "SELECT id FROM wide_rows WHERE label = $label",
                &params([("label", Value::Text(wide_label('c', WIDE_INDEX_TEXT_BYTES)))]),
            )
            .unwrap();
        fixture_answer_24.rows().to_vec()
    };
    assert_eq!(selected, vec![vec![Value::Int64(2)]]);
    drop(reader);

    let raw = redb::Database::open(&path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new("__content_identity_nodes");
        let mut nodes = write.open_table(definition).unwrap();
        // The authenticated wide-key blob: a zero marker, then the key itself.
        let (key, mut value) = nodes
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .filter(|(_, value)| value.first() == Some(&0))
            .max_by_key(|(_, value)| value.len())
            .expect("the widest index key is stored by authenticated reference");
        assert!(
            value.len() > NEAR_LIMIT_INDEX_TEXT_BYTES,
            "the stored blob is the 70 KiB key: {}",
            value.len()
        );
        let last = value.last_mut().unwrap();
        *last ^= 0x01;
        nodes.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    write.commit().unwrap();
    drop(raw);

    // The next writer must consult that key to maintain the index proof, and
    // a changed stored key can never be accepted as the committed one.
    let writer = Database::open(&path).unwrap();
    let error = writer
        .execute(
            "INSERT INTO wide_rows VALUES ($id, $label)",
            &params([
                ("id", Value::Int64(3)),
                (
                    "label",
                    Value::Text(wide_label('c', WIDE_INDEX_TEXT_BYTES + 1)),
                ),
            ]),
        )
        .expect_err("a changed stored index key must fail the maintained proof");
    assert!(
        error
            .to_string()
            .contains("authenticated separator key is damaged"),
        "the wide key fails through the content-identity boundary: {error}"
    );
    writer.close().unwrap();
}

/// The physical posting layout, re-encoded by hand so this test can write a
/// stored key the engine itself would never produce.
#[derive(serde::Serialize, serde::Deserialize)]
struct PhysicalIndexPosition {
    key: Vec<(bool, Value)>,
    row_id: contextdb_core::RowId,
    created_tx: contextdb_core::TxId,
}

/// An independent mirror of the durable posting key: the same redb type name,
/// and the same total order the engine now navigates its index with. Written
/// by hand, from the order's stated rules rather than from the engine's code,
/// so the fixture does not borrow the implementation it is holding to account.
///
/// The rules: a key that decodes **exactly** comes before one that does not;
/// decoding keys are compared component by component (direction flag first,
/// then the declared order for a like pair, then a stable shape rank with NULL
/// last), then component count, row id, transaction id, and finally raw bytes;
/// keys that do not decode are ordered by raw bytes among themselves. This is a
/// lexicographic composition, so it is total and transitive over every byte
/// string - the pair-dependent "typed if both decode, raw otherwise" fallback
/// this fixture used before is neither, and it disagreed with the engine about
/// where a trailing byte belongs.
#[derive(Debug)]
struct PhysicalIndexKey(Vec<u8>);

/// Exact decode: trailing bytes mean this is not a posting position at all.
fn mirrored_position(bytes: &[u8]) -> Option<PhysicalIndexPosition> {
    let (position, consumed) = bincode::serde::decode_from_slice::<PhysicalIndexPosition, _>(
        bytes,
        bincode::config::standard(),
    )
    .ok()?;
    (consumed == bytes.len()).then_some(position)
}

fn mirrored_value_rank(value: &Value) -> u8 {
    match value {
        Value::Bool(_) => 0,
        Value::Int64(_) => 1,
        Value::Float64(_) => 2,
        Value::Timestamp(_) => 3,
        Value::TxId(_) => 4,
        Value::Uuid(_) => 5,
        Value::Text(_) => 6,
        Value::Json(_) => 7,
        Value::Vector(_) => 8,
        Value::Null => 9,
    }
}

fn mirrored_component_order(left: &(bool, Value), right: &(bool, Value)) -> std::cmp::Ordering {
    if left.0 != right.0 {
        return left.0.cmp(&right.0);
    }
    let ascending = if mirrored_value_rank(&left.1) == mirrored_value_rank(&right.1)
        && !matches!(left.1, Value::Json(_) | Value::Vector(_))
    {
        contextdb_core::TotalOrdAsc(left.1.clone())
            .cmp(&contextdb_core::TotalOrdAsc(right.1.clone()))
    } else {
        mirrored_value_rank(&left.1)
            .cmp(&mirrored_value_rank(&right.1))
            .then_with(|| match (&left.1, &right.1) {
                (Value::Json(left), Value::Json(right)) => left.to_string().cmp(&right.to_string()),
                (Value::Vector(left), Value::Vector(right)) => {
                    left.len().cmp(&right.len()).then_with(|| {
                        left.iter()
                            .map(|component| component.to_bits())
                            .cmp(right.iter().map(|component| component.to_bits()))
                    })
                }
                _ => std::cmp::Ordering::Equal,
            })
    };
    if left.0 {
        ascending.reverse()
    } else {
        ascending
    }
}

fn mirrored_total_order(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    match (mirrored_position(left), mirrored_position(right)) {
        (Some(left_position), Some(right_position)) => {
            for (left_part, right_part) in left_position.key.iter().zip(right_position.key.iter()) {
                let ordering = mirrored_component_order(left_part, right_part);
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            left_position
                .key
                .len()
                .cmp(&right_position.key.len())
                .then_with(|| left_position.row_id.cmp(&right_position.row_id))
                .then_with(|| left_position.created_tx.0.cmp(&right_position.created_tx.0))
                .then_with(|| left.cmp(right))
        }
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => left.cmp(right),
    }
}

impl redb::Value for PhysicalIndexKey {
    type SelfType<'a> = PhysicalIndexKey;
    type AsBytes<'a> = &'a [u8];
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        PhysicalIndexKey(data.to_vec())
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> {
        &value.0
    }
    fn type_name() -> redb::TypeName {
        redb::TypeName::new("contextdb.RelationalIndexPosition.v1")
    }
}

impl redb::Key for PhysicalIndexKey {
    fn compare(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
        mirrored_total_order(left, right)
    }
}

/// The physical index table whose first stored posting orders a TEXT value:
/// the declared index over `label`, told apart from the engine's own key index
/// by what it stores rather than by a name this test would have to compute.
fn text_physical_index_table(path: &std::path::Path) -> String {
    let raw = redb::Database::open(path).unwrap();
    let read = raw.begin_read().unwrap();
    let names = read
        .list_tables()
        .unwrap()
        .map(|handle| handle.name().to_owned())
        .filter(|name| {
            name.strip_prefix("__rel_index_")
                .is_some_and(|rest| rest.len() == 64 && rest.chars().all(|c| c.is_ascii_hexdigit()))
        })
        .collect::<Vec<_>>();
    let mut text = names
        .into_iter()
        .filter(|name| {
            let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(name);
            let table = read.open_table(definition).unwrap();
            let Some(entry) = table.iter().unwrap().next() else {
                return false;
            };
            let key = entry.unwrap().0.value().0;
            mirrored_position(&key)
                .is_some_and(|position| matches!(position.key.first(), Some((_, Value::Text(_)))))
        })
        .collect::<Vec<_>>();
    assert_eq!(text.len(), 1, "one index orders a TEXT column: {text:?}");
    text.pop().expect("the declared TEXT index")
}

/// Replace one stored posting key with a decodable key whose declared type and
/// direction disagree with the index.
fn damage_physical_index_key(
    path: &std::path::Path,
    table: &str,
    replaced: &str,
    replacement: PhysicalIndexPosition,
) {
    let raw = redb::Database::open(path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(table);
        let mut index = write.open_table(definition).unwrap();
        let target = index
            .iter()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.0.value().0, entry.1.value().to_vec())
            })
            .find(|(key, _)| {
                mirrored_position(key).is_some_and(|position| {
                    matches!(position.key.first(), Some((_, Value::Text(text))) if text == replaced)
                })
            })
            .expect("the posting this test replaces");
        index.remove(PhysicalIndexKey(target.0)).unwrap();
        let damaged =
            bincode::serde::encode_to_vec(&replacement, bincode::config::standard()).unwrap();
        index
            .insert(PhysicalIndexKey(damaged), target.1.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

/// A stored index key that disagrees with its declaration is corruption, and it
/// is reported as corruption on the point read and on both range directions —
/// never as a panic, and never as an answer.
#[test]
fn a_damaged_physical_index_key_is_typed_corruption_on_point_and_both_range_directions() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("physical-index-damage.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE damaged_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
        &HashMap::new(),
    )
    .unwrap();
    db.execute(
        "CREATE INDEX damaged_rows_label ON damaged_rows (label ASC)",
        &HashMap::new(),
    )
    .unwrap();
    for row in 0..8i64 {
        db.execute(
            "INSERT INTO damaged_rows VALUES ($id, $label)",
            &params([
                ("id", Value::Int64(row)),
                ("label", Value::Text(format!("label-{row:03}"))),
            ]),
        )
        .unwrap();
    }
    db.close().unwrap();

    // The declared order is preserved for valid data before anything is damaged.
    let healthy = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(
        healthy
            .execute(
                "SELECT id FROM damaged_rows ORDER BY label",
                &HashMap::new()
            )
            .unwrap()
            .rows(),
        (0..8)
            .map(|row| vec![Value::Int64(row)])
            .collect::<Vec<_>>()
    );
    assert_eq!(
        healthy
            .execute(
                "SELECT id FROM damaged_rows ORDER BY label DESC",
                &HashMap::new()
            )
            .unwrap()
            .rows(),
        (0..8)
            .rev()
            .map(|row| vec![Value::Int64(row)])
            .collect::<Vec<_>>()
    );
    drop(healthy);

    let damaged_table = text_physical_index_table(&path);
    // An INTEGER value, stored descending, in an index declared over a TEXT
    // column ascending: decodable, and unorderable against its siblings. The
    // derived ordering this key reaches panics on the first comparison and ties
    // on the second.
    // Two stored keys the declaration could never have produced, and the two
    // pairs the derived ordering cannot survive: an INTEGER where the column
    // declares TEXT (that comparison panics), and a descending key in an
    // ascending index (that comparison answers Equal for unequal keys). Their
    // encodings place one below and one above every valid posting, so a
    // forward range, a reverse range and a point probe each meet one.
    damage_physical_index_key(
        &path,
        &damaged_table,
        "label-000",
        PhysicalIndexPosition {
            key: vec![(false, Value::Int64(7))],
            row_id: contextdb_core::RowId(0),
            created_tx: contextdb_core::TxId(1),
        },
    );
    damage_physical_index_key(
        &path,
        &damaged_table,
        "label-007",
        PhysicalIndexPosition {
            key: vec![(true, Value::Text("label-007".to_owned()))],
            row_id: contextdb_core::RowId(7),
            created_tx: contextdb_core::TxId(8),
        },
    );

    for (what, sql) in [
        (
            "forward range",
            "SELECT id FROM damaged_rows ORDER BY label",
        ),
        (
            "reverse range",
            "SELECT id FROM damaged_rows WHERE label < 'label-900' ORDER BY label DESC",
        ),
        (
            "point probe",
            "SELECT id FROM damaged_rows WHERE label = 'zzzz-past-every-valid-label'",
        ),
    ] {
        let reader = fixture::open_file(
            &path,
            fixture::options(),
            Arc::new(fixture::Routes::default()),
        );
        let error = reader
            .execute(sql, &HashMap::new())
            .expect_err("a damaged stored index key is never an answer");
        assert!(
            error.to_string().contains("does not authenticate")
                || error.to_string().contains("durable index"),
            "{what} reports the typed corruption error: {error}"
        );
        drop(reader);
    }
}

/// Replace one stored posting's *key bytes* with anything at all, keeping its
/// value. This is how a trailing, truncated or non-canonically encoded key gets
/// into the physical index: the engine never writes one.
fn replace_physical_index_key_bytes(
    path: &std::path::Path,
    table: &str,
    replaced: &str,
    rewrite: impl FnOnce(&[u8]) -> Vec<u8>,
) {
    let raw = redb::Database::open(path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(table);
        let mut index = write.open_table(definition).unwrap();
        let target = index
            .iter()
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.0.value().0, entry.1.value().to_vec())
            })
            .find(|(key, _)| {
                mirrored_position(key).is_some_and(|position| {
                    matches!(position.key.first(), Some((_, Value::Text(text))) if text == replaced)
                })
            })
            .expect("the posting this test rewrites");
        let rewritten = rewrite(&target.0);
        assert_ne!(rewritten, target.0, "the rewrite changes the stored key");
        index.remove(PhysicalIndexKey(target.0)).unwrap();
        index
            .insert(PhysicalIndexKey(rewritten), target.1.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

/// A bincode `standard()` varint accepts a non-minimal encoding: 251 followed
/// by a little-endian u16 encodes the same small integer as a single byte. Find
/// a trailing integer field of a canonical posting key and re-encode it that
/// way, so the bytes decode exactly to the same position but are not the bytes
/// the engine would have written.
fn non_canonically_encoded(canonical: &[u8]) -> Vec<u8> {
    let expected = mirrored_position(canonical).expect("the canonical key decodes");
    for at in (0..canonical.len()).rev() {
        let byte = canonical[at];
        if byte >= 251 {
            continue;
        }
        let mut rewritten = canonical[..at].to_vec();
        rewritten.extend_from_slice(&[251, byte, 0]);
        rewritten.extend_from_slice(&canonical[at + 1..]);
        let Some(decoded) = mirrored_position(&rewritten) else {
            continue;
        };
        let canonical_again =
            bincode::serde::encode_to_vec(&decoded, bincode::config::standard()).unwrap();
        if canonical_again == canonical
            && decoded.row_id == expected.row_id
            && decoded.created_tx == expected.created_tx
        {
            return rewritten;
        }
    }
    panic!("no non-minimal varint rewrite of this key was found");
}

/// Physical posting keys are decoded exactly and canonically. A key carrying a
/// trailing byte, a truncated key, and a key whose integers are encoded
/// non-minimally are each stored corruption, reported before the posting can
/// advance a cursor or reach an answer, on the point probe and on both range
/// directions.
#[test]
fn trailing_truncated_and_noncanonical_posting_keys_are_typed_corruption_everywhere() {
    let corrupted = |what: &str, label: &str, probe: &str, rewrite: fn(&[u8]) -> Vec<u8>| {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join(format!("noncanonical-{what}.db"));
        let db = Database::open(&path).unwrap();
        db.execute(
            "CREATE TABLE odd_rows (id INTEGER PRIMARY KEY, label TEXT NOT NULL)",
            &HashMap::new(),
        )
        .unwrap();
        db.execute(
            "CREATE INDEX odd_rows_label ON odd_rows (label ASC)",
            &HashMap::new(),
        )
        .unwrap();
        for row in 0..8i64 {
            db.execute(
                "INSERT INTO odd_rows VALUES ($id, $label)",
                &params([
                    ("id", Value::Int64(row)),
                    ("label", Value::Text(format!("label-{row:03}"))),
                ]),
            )
            .unwrap();
        }
        db.close().unwrap();

        let table = text_physical_index_table(&path);
        replace_physical_index_key_bytes(&path, &table, label, rewrite);

        let point = format!("SELECT id FROM odd_rows WHERE label = '{probe}'");
        for (route, sql) in [
            ("forward range", "SELECT id FROM odd_rows ORDER BY label"),
            // A lower-bounded reverse walk: its seek edge is the TOP of the
            // index, so it starts at the last stored key. A reverse walk with
            // an upper bound cannot reach a key that does not decode, because
            // the durable order puts every such key above every decoding one -
            // that is the order's property, not a gap in this check, and the
            // forward range and the point probe cover those keys.
            (
                "reverse range",
                "SELECT id FROM odd_rows WHERE label > 'label-000' ORDER BY label DESC",
            ),
            ("point probe", point.as_str()),
        ] {
            let reader = fixture::open_file(
                &path,
                fixture::options(),
                Arc::new(fixture::Routes::default()),
            );
            let answer = reader.execute(sql, &HashMap::new());
            let error = match answer {
                Ok(result) => panic!(
                    "{what} on the {route} answered {:?} instead of reporting stored corruption",
                    result.rows()
                ),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("does not authenticate")
                    || error.to_string().contains("durable index")
                    || error.to_string().contains("index position"),
                "{what} on the {route} reports the typed corruption error: {error}"
            );
            drop(reader);
        }
    };

    // A valid key with one extra byte. The declaration would order it exactly
    // where the valid key was; the durable order puts it past every decoding
    // key, and either way it must never be delivered.
    // The point probe for the two non-decoding cases seeks past every valid
    // label, because the durable order puts a key that does not decode after
    // every key that does; the non-canonical key decodes, so it stays where the
    // declaration puts it and the probe seeks it by name.
    corrupted(
        "trailing",
        "label-003",
        "zzzz-past-every-valid-label",
        |canonical| {
            let mut rewritten = canonical.to_vec();
            rewritten.push(0);
            rewritten
        },
    );
    // A key cut short: it does not decode at all.
    corrupted(
        "truncated",
        "label-004",
        "zzzz-past-every-valid-label",
        |canonical| canonical[..canonical.len() - 1].to_vec(),
    );
    // A key that decodes to exactly the right position from bytes the engine
    // would never have produced.
    corrupted(
        "noncanonical",
        "label-005",
        "label-005",
        non_canonically_encoded,
    );
}

// ---------------------------------------------------------------------------
// The identities-only candidate route over a single-column AUTOMATIC index.
//
// A vector search whose relational filter is an equality or a finite `IN` on a
// PK/UNIQUE column takes its candidate identities from that automatic index
// alone -- no row body is read, so the posting IS the answer. Those identities
// must be authenticated against the committed content root exactly like every
// other candidate a bounded file read delivers: a posting deleted behind the
// proof's back must not silently remove an allowed candidate, and a posting
// added behind the proof's back must not enter the candidate set.
// ---------------------------------------------------------------------------

const EXACT_LABELS: usize = 8;

fn exact_label(row: usize) -> String {
    format!("exact-{row:03}")
}

/// Rows keyed by an automatic single-column index (`label TEXT UNIQUE`), seeded
/// across several transactions, with one row deleted and re-inserted under a
/// freed label so the physical index also carries tombstoned postings the
/// reconciliation has to skip rather than call extra.
fn seed_exact_candidates(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE exact_candidates (id INTEGER PRIMARY KEY, label TEXT UNIQUE NOT NULL, embedding VECTOR(2) SEARCH_MODE EXACT)",
        &HashMap::new(),
    )
    .unwrap();
    for start in (0..EXACT_LABELS).step_by(4) {
        let tx = db.begin().unwrap();
        for row in start..start + 4 {
            db.execute_in_tx(
                tx,
                "INSERT INTO exact_candidates VALUES ($id, $label, $embedding)",
                &params([
                    ("id", Value::Int64(row as i64)),
                    ("label", Value::Text(exact_label(row))),
                    (
                        "embedding",
                        Value::Vector(vec![1.0 - row as f32 / 16.0, row as f32 / 16.0]),
                    ),
                ]),
            )
            .unwrap();
        }
        db.commit(tx).unwrap();
    }
    // A committed delete leaves a tombstoned posting in the physical index and
    // removes the logical one from the proof; a later row re-uses the freed
    // label under a new row id and a later transaction.
    db.execute("DELETE FROM exact_candidates WHERE id = 7", &HashMap::new())
        .unwrap();
    db.execute(
        "INSERT INTO exact_candidates VALUES ($id, $label, $embedding)",
        &params([
            ("id", Value::Int64(107)),
            ("label", Value::Text(exact_label(7))),
            ("embedding", Value::Vector(vec![0.5, 0.5])),
        ]),
    )
    .unwrap();
    db.close().unwrap();
}

fn exact_in_list_params() -> HashMap<String, Value> {
    let mut values = HashMap::from([("q".to_owned(), Value::Vector(vec![1.0, 0.0]))]);
    for row in 0..6 {
        values.insert(format!("l{row}"), Value::Text(exact_label(row)));
    }
    values
}

const EXACT_IN_LIST: &str = "SELECT id FROM exact_candidates \
     WHERE label IN ($l0, $l1, $l2, $l3, $l4, $l5) \
     ORDER BY embedding <=> $q LIMIT 8";

fn exact_ids(result: &contextdb_engine::QueryResult) -> Vec<i64> {
    let mut ids = result
        .rows()
        .iter()
        .map(|row| match row.as_slice() {
            [Value::Int64(id)] => *id,
            other => panic!("one integer id per candidate row: {other:?}"),
        })
        .collect::<Vec<_>>();
    ids.sort_unstable();
    ids
}

/// The live posting of one label, as the engine stored it.
fn live_exact_posting(
    path: &std::path::Path,
    table: &str,
    label: &str,
) -> (Vec<u8>, PhysicalIndexPosition, Vec<u8>) {
    let raw = redb::Database::open(path).unwrap();
    let read = raw.begin_read().unwrap();
    let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(table);
    let index = read.open_table(definition).unwrap();
    index
        .iter()
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.unwrap();
            let key = entry.0.value().0;
            let value = entry.1.value().to_vec();
            // A live posting encodes `None` for its deleted transaction.
            let deleted: Option<contextdb_core::TxId> =
                bincode::serde::decode_from_slice(&value, bincode::config::standard())
                    .map(|(deleted, _)| deleted)
                    .unwrap_or(None);
            let position = mirrored_position(&key)?;
            (deleted.is_none()
                && matches!(position.key.first(), Some((_, Value::Text(text))) if text == label))
            .then_some((key, position, value))
        })
        .next()
        .unwrap_or_else(|| panic!("a live posting for {label}"))
}

/// Delete one live posting from the physical index, behind the committed proof.
fn remove_live_exact_posting(path: &std::path::Path, table: &str, label: &str) {
    let (key, _, _) = live_exact_posting(path, table, label);
    let raw = redb::Database::open(path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(table);
        let mut index = write.open_table(definition).unwrap();
        index.remove(PhysicalIndexKey(key)).unwrap();
    }
    write.commit().unwrap();
}

/// Add a live posting the committed proof never recorded: `label`'s key
/// pointing at the row that really belongs to `borrowed_from`. The candidate
/// set must not gain that row.
fn add_unproven_exact_posting(
    path: &std::path::Path,
    table: &str,
    label: &str,
    borrowed_from: &str,
) {
    let (_, donor, value) = live_exact_posting(path, table, borrowed_from);
    let raw = redb::Database::open(path).unwrap();
    let write = raw.begin_write().unwrap();
    {
        let definition: TableDefinition<PhysicalIndexKey, &[u8]> = TableDefinition::new(table);
        let mut index = write.open_table(definition).unwrap();
        let forged = PhysicalIndexPosition {
            // `false` is the ascending direction flag this index declares, so
            // the forged posting really does land inside the probed run rather
            // than in the malformed region a mis-declared key sorts into.
            key: vec![(false, Value::Text(label.to_owned()))],
            row_id: donor.row_id,
            created_tx: donor.created_tx,
        };
        let encoded = bincode::serde::encode_to_vec(&forged, bincode::config::standard()).unwrap();
        index
            .insert(PhysicalIndexKey(encoded), value.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

/// A healthy store answers the automatic-index candidate route completely: every
/// matching row once, tombstoned postings skipped, across several transactions,
/// on the single-key probe, the multi-key `IN` probe and the reverse ordered
/// walk of the same index.
#[test]
fn exact_automatic_index_candidates_stay_complete_across_keys_transactions_and_directions() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("exact-candidates-complete.db");
    seed_exact_candidates(&path);

    let reader = fixture::open_file(
        &path,
        fixture::options(),
        Arc::new(fixture::Routes::default()),
    );
    assert_eq!(
        exact_ids(
            &reader
                .execute(EXACT_IN_LIST, &exact_in_list_params())
                .unwrap()
        ),
        vec![0, 1, 2, 3, 4, 5],
        "the finite IN probe delivers exactly the rows its keys name"
    );
    assert_eq!(
        exact_ids(
            &reader
                .execute(
                    "SELECT id FROM exact_candidates WHERE label = $l0 ORDER BY embedding <=> $q LIMIT 8",
                    &exact_in_list_params(),
                )
                .unwrap()
        ),
        vec![0],
        "the single-key equality probe delivers its one row"
    );
    // The re-used label resolves to the row that owns it now, not the deleted
    // one whose tombstoned posting still sits in the physical index.
    assert_eq!(
        exact_ids(
            &reader
                .execute(
                    "SELECT id FROM exact_candidates WHERE label = $l ORDER BY embedding <=> $q LIMIT 8",
                    &params([
                        ("l", Value::Text(exact_label(7))),
                        ("q", Value::Vector(vec![1.0, 0.0])),
                    ]),
                )
                .unwrap()
        ),
        vec![107],
        "a tombstoned posting is skipped, not delivered and not called extra"
    );
    // The same automatic index, walked the other way.
    assert_eq!(
        exact_ids(
            &reader
                .execute(
                    "SELECT id FROM exact_candidates WHERE label IN ($l0, $l1, $l2, $l3, $l4, $l5) ORDER BY label DESC",
                    &exact_in_list_params(),
                )
                .unwrap()
        ),
        vec![0, 1, 2, 3, 4, 5],
        "the reverse walk of the same automatic index answers the same set"
    );
}

/// Every identity the automatic-index candidate route delivers is authenticated
/// against the committed content root. A live posting removed from, or added to,
/// the physical index behind that root is typed store corruption before any
/// identity is delivered -- never a quietly smaller or quietly larger candidate
/// set. Covered at the first probed key, a middle one, the run boundary the
/// cursor crosses, and the tail, for the multi-key `IN` probe and the
/// single-key equality probe alike.
#[test]
fn an_unproven_automatic_index_candidate_is_typed_corruption_before_any_identity() {
    for (what, damage) in [
        ("missing first", (exact_label(0), None::<String>)),
        ("missing run boundary", (exact_label(1), None)),
        ("missing middle", (exact_label(3), None)),
        ("missing tail", (exact_label(5), None)),
        ("extra first", (exact_label(0), Some(exact_label(6)))),
        ("extra run boundary", (exact_label(1), Some(exact_label(6)))),
        ("extra middle", (exact_label(3), Some(exact_label(6)))),
        ("extra tail", (exact_label(5), Some(exact_label(6)))),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("exact-candidates-damaged.db");
        seed_exact_candidates(&path);
        let table = text_physical_index_table(&path);
        let (label, borrowed) = damage;
        match borrowed.as_deref() {
            None => remove_live_exact_posting(&path, &table, &label),
            Some(donor) => add_unproven_exact_posting(&path, &table, &label, donor),
        }

        let reader = fixture::open_file(
            &path,
            fixture::options(),
            Arc::new(fixture::Routes::default()),
        );
        let outcome = reader.execute(EXACT_IN_LIST, &exact_in_list_params());
        let error = outcome.as_ref().err().map(ToString::to_string);
        assert!(
            error.as_deref().is_some_and(|error| {
                error.contains("does not authenticate")
                    || error.contains("physical index")
                    || error.contains("durable relational index")
            }),
            "{what}: the IN probe reports typed corruption instead of answering \
             from the physical index: {:?}",
            outcome.as_ref().map(exact_ids),
        );

        let single = reader.execute(
            "SELECT id FROM exact_candidates WHERE label = $l ORDER BY embedding <=> $q LIMIT 8",
            &params([
                ("l", Value::Text(label.clone())),
                ("q", Value::Vector(vec![1.0, 0.0])),
            ]),
        );
        let single_error = single.as_ref().err().map(ToString::to_string);
        assert!(
            single_error.as_deref().is_some_and(|error| {
                error.contains("does not authenticate")
                    || error.contains("physical index")
                    || error.contains("durable relational index")
            }),
            "{what}: the single-key equality probe reports typed corruption: {:?}",
            single.as_ref().map(exact_ids),
        );
        drop(reader);
    }
}
