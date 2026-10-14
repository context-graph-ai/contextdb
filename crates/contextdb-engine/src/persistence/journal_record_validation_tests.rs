//! The journal record check T2 runs without decoding: every field of both
//! journal variants is parsed heap-free with exact consumption, so a record
//! that carries a valid checksum but a malformed body is refused exactly as
//! the full decoder refuses it.
use super::*;

fn route() -> (VectorIndexRef, VectorPartitionKey) {
    (
        VectorIndexRef::new("points", "embedding"),
        VectorPartitionKey::from_values(&[contextdb_core::Value::Int64(1)]).unwrap(),
    )
}

fn membership(index: &VectorIndexRef, key: &VectorPartitionKey) -> VectorPartitionMembershipRecord {
    VectorPartitionMembershipRecord {
        index: index.clone(),
        partition_key: key.clone(),
        row_id: RowId(7),
        row_created_tx: TxId(3),
        row_lsn: Lsn(4),
        vector_created_tx: TxId(3),
        vector_lsn: Lsn(4),
        visible_from: TxId(3),
        deleted_tx: None,
    }
}

fn upsert(lsn: u64, ordinal: u64) -> VectorPartitionJournalRecord {
    let (index, key) = route();
    VectorPartitionJournalRecord {
        change: VectorPartitionJournalChange::Upsert(membership(&index, &key)),
        index,
        partition_key: key,
        lsn: Lsn(lsn),
        ordinal,
    }
}

fn tombstone(lsn: u64, ordinal: u64) -> VectorPartitionJournalRecord {
    let (index, key) = route();
    VectorPartitionJournalRecord {
        change: VectorPartitionJournalChange::Tombstone(VectorPartitionTombstoneRecord {
            membership: membership(&index, &key),
            deleted_tx: TxId(9),
            lsn: Lsn(lsn),
            cause: VectorPartitionTombstoneCause::Move,
        }),
        index,
        partition_key: key,
        lsn: Lsn(lsn),
        ordinal,
    }
}

/// The record envelope around an arbitrary payload, with a valid checksum.
fn envelope(payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    encoded.extend_from_slice(VECTOR_PARTITION_RECORD_MAGIC);
    encoded.extend_from_slice(&VECTOR_PARTITION_RECORD_VERSION.to_be_bytes());
    encoded.push(VectorPartitionRecordKind::Journal as u8);
    encoded.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    encoded.extend_from_slice(payload);
    let checksum = blake3::hash(&encoded);
    encoded.extend_from_slice(checksum.as_bytes());
    encoded
}

fn check(
    record: &VectorPartitionJournalRecord,
    encoded: &[u8],
) -> std::result::Result<u64, String> {
    let (index, key) = route();
    let prefix = RedbPersistence::vector_partition_prefix(&index, &key);
    let physical = RedbPersistence::vector_partition_journal_key(record);
    validate_vector_partition_journal_record(
        &prefix,
        &physical,
        encoded,
        &index,
        &key,
        vector_partition_journal_record_width_bound(&index, &key),
    )
}

fn payload(record: &VectorPartitionJournalRecord) -> Vec<u8> {
    bincode::serde::encode_to_vec(record, bincode::config::standard()).unwrap()
}

#[test]
fn both_valid_variants_pass_and_agree_with_the_full_decoder() {
    for record in [upsert(41, 2), tombstone(300, 70_000)] {
        let encoded = RedbPersistence::encode_vector_partition_record(
            VectorPartitionRecordKind::Journal,
            &record,
        )
        .unwrap();
        assert_eq!(check(&record, &encoded), Ok(record.lsn.0));
        let body =
            vector_partition_record_payload(VectorPartitionRecordKind::Journal, &encoded).unwrap();
        let (decoded, used): (VectorPartitionJournalRecord, usize) =
            bincode::serde::decode_from_slice(body, bincode::config::standard()).unwrap();
        assert_eq!((decoded, used), (record, body.len()));
    }
}

#[test]
fn a_checksummed_record_with_an_invalid_option_tag_is_refused() {
    let record = upsert(41, 2);
    let mut body = payload(&record);
    // An upsert ends with its membership's `deleted_tx: None` tag.
    *body.last_mut().unwrap() = 2;
    let refused = check(&record, &envelope(&body)).unwrap_err();
    assert!(refused.contains("membership"), "{refused}");
}

#[test]
fn a_checksummed_record_with_an_invalid_tombstone_cause_is_refused() {
    let record = tombstone(300, 5);
    let mut body = payload(&record);
    // A tombstone ends with its cause variant.
    *body.last_mut().unwrap() = 7;
    let refused = check(&record, &envelope(&body)).unwrap_err();
    assert!(refused.contains("tombstone"), "{refused}");
}

#[test]
fn a_checksummed_record_with_a_trailing_byte_is_refused() {
    for record in [upsert(41, 2), tombstone(300, 5)] {
        let mut body = payload(&record);
        body.push(0);
        let refused = check(&record, &envelope(&body)).unwrap_err();
        assert!(refused.contains("trailing"), "{refused}");
    }
}

#[test]
fn a_record_under_another_physical_key_is_refused() {
    let record = upsert(41, 2);
    let encoded = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::Journal,
        &record,
    )
    .unwrap();
    let elsewhere = upsert(41, 3);
    let refused = check(&elsewhere, &encoded).unwrap_err();
    assert!(refused.contains("physical key"), "{refused}");
}

#[test]
fn a_damaged_checksum_is_refused() {
    let record = upsert(41, 2);
    let mut encoded = RedbPersistence::encode_vector_partition_record(
        VectorPartitionRecordKind::Journal,
        &record,
    )
    .unwrap();
    *encoded.last_mut().unwrap() ^= 0xff;
    assert_eq!(
        check(&record, &encoded),
        Err("checksum mismatch".to_owned())
    );
}

#[test]
fn a_record_wider_than_its_route_allows_is_refused_before_it_is_hashed() {
    let record = upsert(41, 2);
    let mut body = payload(&record);
    body.resize(64 * 1024, 0);
    let refused = check(&record, &envelope(&body)).unwrap_err();
    assert!(refused.contains("wider"), "{refused}");
}
