//! Stored chunks are authenticated before a typed comparator touches their
//! keys, and the comparator itself refuses a malformed typed key instead of
//! panicking or tying.

use super::*;

const TABLE: &str = "items";
const INDEX: &str = "by_value";

fn posting(row: u64, descending: bool) -> ContentIdentityMutation {
    let direction = if descending {
        contextdb_core::DirectedValue::Desc(contextdb_core::TotalOrdDesc(Value::Int64(row as i64)))
    } else {
        contextdb_core::DirectedValue::Asc(contextdb_core::TotalOrdAsc(Value::Int64(row as i64)))
    };
    let key =
        crate::persistence::DurableRelationalIndexKey::new(vec![direction], RowId(row), TxId(0))
            .expect("encode ordered posting key");
    relational_index_posting_mutation(TABLE, INDEX, &key.0)
}

/// Byte offset of the encoded index position inside a posting tree key.
fn order_key_offset() -> usize {
    1 + 4 + TABLE.len() + 4 + INDEX.len() + 1
}

fn fixture(
    path: &std::path::Path,
    rows: u64,
) -> (ProofFixtureDatabase, Vec<ContentIdentityMutation>) {
    let database = ProofFixtureDatabase::create(path).expect("create proof fixture");
    let records = (0..rows).map(|row| posting(row, false)).collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();
    (database, records)
}

/// Rewrite one stored leaf chunk, damaging a byte inside a key that is not the
/// chunk's separator. `reauthenticate` decides whether the chunk is left
/// self-consistent: a self-consistent chunk is exactly the case that reaches
/// the typed comparator.
fn damage_leaf_key(
    database: &redb::Database,
    offset_in_key: usize,
    patch: u8,
    reauthenticate: bool,
) {
    let family = LogicalFamily::SchemaAndIndexes;
    let write = database.begin_write().unwrap();
    {
        let mut nodes = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        let mut target = None;
        for row in nodes.iter().unwrap() {
            let (key, value) = row.unwrap();
            if key.value().first() != Some(&(family as u8))
                || value.value().first() != Some(&NODE_VERSION)
            {
                continue;
            }
            let node = decode_node(family, value.value()).expect("stored chunk decodes");
            if node.level == 0 && node.entries.len() >= 2 {
                target = Some((key.value().to_vec(), node));
                break;
            }
        }
        let (storage_key, mut node) = target.expect("a leaf chunk with at least two postings");
        let position = order_key_offset() + offset_in_key;
        node.entries[0].key[position] = patch;
        if reauthenticate {
            node.subtree_hash = node_hash(family, node.level, &node.entries);
        }
        let encoded = encode_node(&node);
        nodes
            .insert(storage_key.as_slice(), encoded.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
}

fn read_failure(database: &redb::Database, records: &[ContentIdentityMutation]) -> Error {
    let read = database.begin_read().unwrap();
    let verifier =
        ContentIdentityReadVerifier::open(&read, std::path::Path::new("proofs.db"), None).unwrap();
    let mut delivered = 0;
    let scan = verifier.visit_relational_index_postings::<Error>(
        TABLE,
        INDEX,
        std::ops::Bound::Unbounded,
        false,
        &ProofMemory::unlimited(),
        &mut || Ok(()),
        &mut |_| {
            delivered += 1;
            Ok(true)
        },
    );
    let scan_error = scan.expect_err("a damaged posting key cannot deliver a scan");
    assert_eq!(delivered, 0, "no posting is delivered from a damaged chunk");
    let point = records
        .iter()
        .find_map(|record| verifier.verify(record).err())
        .expect("a damaged posting key cannot authenticate a record");
    assert!(
        matches!(point, Error::StoreCorrupted { .. }),
        "point authentication reports the typed corruption error: {point:?}"
    );
    assert!(
        matches!(scan_error, Error::StoreCorrupted { .. }),
        "the range path reports the typed corruption error: {scan_error:?}"
    );
    scan_error
}

#[test]
fn a_changed_posting_key_is_refused_before_any_typed_comparison() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = fixture(&root.path().join("proofs.db"), 8);
    // The variant byte of the first key's `Value`, with the chunk hash left
    // stale: authentication must decide this, not the comparator.
    damage_leaf_key(&database, 2, 6, false);
    let error = read_failure(&database, &records);
    assert!(
        error
            .to_string()
            .contains("does not authenticate its contents"),
        "the chunk is refused by its own hash before a key is compared: {error}"
    );
}

#[test]
fn a_self_consistent_chunk_with_a_changed_value_discriminant_is_typed_corruption() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = fixture(&root.path().join("proofs.db"), 8);
    // `Value::Int64` (bincode variant 2) becomes `Value::Timestamp` (6): the
    // key stays the same length and still decodes, so the chunk authenticates
    // and the comparator is the code under test. Core's total order panics on
    // this pair; the proof boundary must not.
    damage_leaf_key(&database, 2, 6, true);
    let error = read_failure(&database, &records);
    assert!(
        error.to_string().contains("unlike value types"),
        "the validated comparator names the mismatch: {error}"
    );
}

#[test]
fn a_self_consistent_chunk_with_a_changed_direction_is_typed_corruption() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = fixture(&root.path().join("proofs.db"), 8);
    // The ascending flag becomes descending. Core's `DirectedValue::cmp`
    // answers `Equal` for that pair, which would silently tie two distinct
    // keys inside the invariant the tree rests on.
    damage_leaf_key(&database, 1, 1, true);
    let error = read_failure(&database, &records);
    assert!(
        error.to_string().contains("sort direction"),
        "the validated comparator names the direction mismatch: {error}"
    );
}

#[test]
fn the_proof_comparator_never_ties_or_panics_on_a_mixed_key() {
    let ascending = mutation_tree_key(&posting(7, false)).unwrap();
    let descending = mutation_tree_key(&posting(7, true)).unwrap();
    let direction = identity_key_cmp(&ascending, &descending)
        .expect_err("an Asc/Desc pair is never an ordering");
    assert!(
        direction.to_string().contains("sort direction"),
        "{direction}"
    );

    let mut retyped = ascending.clone();
    retyped[order_key_offset() + 2] = 6;
    assert!(
        matches!(
            decode_posting_position(&retyped[order_key_offset()..])
                .expect("the retyped key still decodes")
                .key
                .first(),
            Some((false, Value::Timestamp(_)))
        ),
        "the damage really does produce a different value type"
    );
    let variants = identity_key_cmp(&ascending, &retyped)
        .expect_err("unlike value types are never an ordering");
    assert!(
        variants.to_string().contains("unlike value types"),
        "{variants}"
    );

    // Valid keys keep exactly the ordering the index declares.
    assert_eq!(
        identity_key_cmp(
            &mutation_tree_key(&posting(1, false)).unwrap(),
            &mutation_tree_key(&posting(2, false)).unwrap()
        )
        .unwrap(),
        Ordering::Less
    );
    assert_eq!(
        identity_key_cmp(
            &mutation_tree_key(&posting(2, true)).unwrap(),
            &mutation_tree_key(&posting(1, true)).unwrap()
        )
        .unwrap(),
        Ordering::Less,
        "a descending index orders the larger value first"
    );
}
