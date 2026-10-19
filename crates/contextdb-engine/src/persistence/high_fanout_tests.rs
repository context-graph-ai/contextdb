#![allow(clippy::needless_range_loop)] // Existing compatibility and test seam shape.
use super::*;

fn oracle_key(level: u32, key: &[u8]) -> Vec<u8> {
    // A separator is referenced above 512 bytes; a leaf keeps the actual key
    // until it no longer fits one chunk (65536 - 41 header - 36 entry frame).
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
    result.extend_from_slice(&hash_parts(b"contextdb-prolly-key", &[key]));
    result
}

// A complete sorted rebuild, deliberately independent of edit_level,
// LevelCursor, ChunkBuilder and chunk_cut. Compare the whole reachable image,
// not only a root made by the same incremental updater in another order.
fn rebuilt(
    family: LogicalFamily,
    mut entries: Vec<PageEntry>,
) -> (FamilyRoot, BTreeMap<[u8; 33], Node>) {
    entries.sort_by(|a, b| identity_key_cmp(&a.key, &b.key).unwrap());
    let mut nodes = BTreeMap::new();
    if entries.is_empty() {
        return (empty_root(family), nodes);
    }
    for level in 0u32..64 {
        let mut ranges = Vec::new();
        let mut from = 0;
        let mut bytes = 41usize;
        for i in 0..entries.len() {
            let len = oracle_key(level, &entries[i].key).len() + 36;
            if bytes + len > 65536 && i > from {
                ranges.push(from..i);
                from = i;
                bytes = 41;
            }
            let cdf = |n: usize| n.clamp(1024, 3072) - 1024;
            let sample = u64::from_le_bytes(
                hash_parts(
                    b"contextdb-prolly-boundary",
                    &[
                        &level.to_be_bytes(),
                        &hash_parts(b"contextdb-prolly-key", &[&entries[i].key]),
                    ],
                )[..8]
                    .try_into()
                    .unwrap(),
            );
            let cut = (level == 0 || i > from)
                && (bytes + len >= 3072
                    || (sample as u128) * ((2048 - cdf(bytes)) as u128)
                        < ((cdf(bytes + len) - cdf(bytes)) as u128) * (u64::MAX as u128));
            bytes += len;
            if cut {
                ranges.push(from..i + 1);
                from = i + 1;
                bytes = 41;
            }
        }
        if from < entries.len() {
            ranges.push(from..entries.len());
        }
        let mut next = Vec::new();
        for range in ranges {
            let group = entries[range].to_vec();
            let mut hash = blake3::Hasher::new();
            hash.update(b"contextdb-prolly-chunk");
            hash.update(&[family as u8]);
            hash.update(&level.to_be_bytes());
            for e in &group {
                let key = oracle_key(level, &e.key);
                hash.update(&(key.len() as u64).to_be_bytes());
                hash.update(&key);
                hash.update(&e.value_hash);
            }
            let subtree_hash = *hash.finalize().as_bytes();
            let key = group.last().unwrap().key.clone();
            nodes.insert(
                node_storage_key(family, &chunk_id(level, &key)),
                Node {
                    level,
                    entries: group,
                    subtree_hash,
                },
            );
            next.push(PageEntry {
                key,
                value_hash: subtree_hash,
            });
        }
        if next.len() == 1 {
            return (
                FamilyRoot {
                    root: Some(chunk_id(level, &next[0].key)),
                    hash: next[0].value_hash,
                    open_config_root: empty_root(family).open_config_root,
                },
                nodes,
            );
        }
        entries = next;
    }
    panic!("hierarchy did not contract")
}

fn assert_image(db: &redb::Database, family: LogicalFamily, records: &[ContentIdentityMutation]) {
    let entries = records
        .iter()
        .map(|m| PageEntry {
            key: mutation_tree_key(m).unwrap(),
            value_hash: value_hash(family, m.logical_value.as_deref().unwrap()),
        })
        .collect();
    let (expected, nodes) = rebuilt(family, entries);
    let read = db.begin_read().unwrap();
    let roots = read.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
    let actual = decode_root(family, roots.get(family as u8).unwrap().unwrap().value()).unwrap();
    assert!(
        actual == expected,
        "incremental root differs from independent sorted rebuild"
    );
    let stored = read.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    let mut count = 0;
    for row in stored.iter().unwrap() {
        let (key, bytes) = row.unwrap();
        if key.value()[0] != family as u8 {
            continue;
        }
        if bytes.value()[0] == 0 {
            assert!(
                records
                    .iter()
                    .any(|m| mutation_tree_key(m).unwrap() == bytes.value()[1..])
            );
            continue;
        }
        let expected = nodes
            .get(key.value())
            .expect("no orphan chunks after replacement");
        assert_eq!(bytes.value(), encode_node(expected));
        assert!(bytes.value().len() <= MAX_NODE_BYTES);
        count += 1;
    }
    assert_eq!(count, nodes.len(), "all canonical chunks are present");
}

fn apply(db: &ProofFixtureDatabase, mutations: &[ContentIdentityMutation]) {
    let write = db.begin_write().unwrap();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        mutations,
        &mut db.admission(None),
    )
    .unwrap();
    write.commit().unwrap();
}

#[test]
fn canonical_incremental_insert_delete_cascades_match_sorted_rebuild_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = ProofFixtureDatabase::create(dir.path().join("canonical.db")).unwrap();
    let write = db.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    write.commit().unwrap();
    let schema_fixture = crate::Database::open_memory();
    schema_fixture
        .execute(
            "CREATE TABLE fixture (id INTEGER PRIMARY KEY)",
            &std::collections::HashMap::new(),
        )
        .unwrap();
    let schema_meta = schema_fixture.table_meta("fixture").unwrap();
    for family in LogicalFamily::ALL {
        let records = (0..4096u64)
            .map(|i| {
                if family == SCHEMA_RECORD_FAMILY {
                    let name = format!("fixture_{i:016x}");
                    schema_mutations(&name, &schema_meta).unwrap().remove(0)
                } else if family == LogicalFamily::Configuration {
                    ContentIdentityMutation::put(
                        family,
                        format!("fixture_{i:016x}").into_bytes(),
                        (i * 7).to_be_bytes().to_vec(),
                    )
                } else {
                    ContentIdentityMutation::put(
                        family,
                        i.to_be_bytes().to_vec(),
                        (i * 7).to_be_bytes().to_vec(),
                    )
                }
            })
            .collect::<Vec<_>>();
        let mut live = Vec::new();
        // Scrambled one-transaction batches create different boundary histories.
        for phase in 0..8 {
            let batch = (0..512)
                .map(|j| records[((phase * 512 + j) * 2053) % 4096].clone())
                .collect::<Vec<_>>();
            apply(&db, &batch);
            live.extend(batch);
            assert_image(&db, family, &live);
        }
        for phase in 0..8 {
            let removed = live.drain(..live.len().min(513)).collect::<Vec<_>>();
            apply(
                &db,
                &removed
                    .iter()
                    .map(|m| ContentIdentityMutation::remove(family, m.logical_key.clone()))
                    .collect::<Vec<_>>(),
            );
            assert_image(&db, family, &live);
            if phase == 3 {
                db.compact().unwrap();
                assert_image(&db, family, &live);
            }
        }
        assert!(live.is_empty());
    }
}

#[test]
fn large_ordered_keys_split_by_bytes_and_rejoin_after_boundary_deletions() {
    let dir = tempfile::tempdir().unwrap();
    let db = ProofFixtureDatabase::create(dir.path().join("large.db")).unwrap();
    let write = db.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    write.commit().unwrap();
    let mut records = (0..160u64)
        .map(|i| {
            let key = crate::persistence::DurableRelationalIndexKey::new(
                vec![contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(Value::Text(format!(
                        "{i:05}{}",
                        "x".repeat(500 + (i as usize % 7) * 10000)
                    ))),
                )],
                RowId(i),
                TxId(0),
            )
            .unwrap();
            relational_index_posting_mutation("large", "by_key", &key.0)
        })
        .collect::<Vec<_>>();
    for batch in records.chunks(7).rev() {
        apply(&db, batch);
    }
    assert_image(&db, LogicalFamily::SchemaAndIndexes, &records);
    {
        let read = db.begin_read().unwrap();
        let verifier = ContentIdentityReadVerifier::open(&read, dir.path(), None).unwrap();
        for record in &records {
            verifier.verify(record).unwrap();
        }
        for reverse in [false, true] {
            let mut keys = Vec::new();
            verifier
                .visit_relational_index_postings::<Error>(
                    "large",
                    "by_key",
                    std::ops::Bound::Unbounded,
                    reverse,
                    &ProofMemory::unlimited(),
                    &mut || Ok(()),
                    &mut |key| {
                        keys.push(key.to_vec());
                        Ok(true)
                    },
                )
                .unwrap();
            let mut expected = records
                .iter()
                .map(|m| {
                    let key = mutation_tree_key(m).unwrap();
                    parse_posting_tree_key(&key).unwrap().3.to_vec()
                })
                .collect::<Vec<_>>();
            if reverse {
                expected.reverse();
            }
            assert_eq!(keys, expected);
        }
    }
    for _ in 0..4 {
        let removed = records.drain(..records.len().min(39)).collect::<Vec<_>>();
        apply(
            &db,
            &removed
                .iter()
                .map(|m| ContentIdentityMutation::remove(m.family, m.logical_key.clone()))
                .collect::<Vec<_>>(),
        );
        assert_image(&db, LogicalFamily::SchemaAndIndexes, &records);
    }
}

#[test]
fn wide_separator_reference_corruption_is_rejected_before_a_posting_is_used() {
    let dir = tempfile::tempdir().unwrap();
    let db = ProofFixtureDatabase::create(dir.path().join("damaged-key.db")).unwrap();
    let write = db.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    write.commit().unwrap();
    let records = (0..4u64)
        .map(|i| {
            let key = crate::persistence::DurableRelationalIndexKey::new(
                vec![contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(Value::Text(format!("{i}{}", "q".repeat(60000)))),
                )],
                RowId(i),
                TxId(0),
            )
            .unwrap();
            relational_index_posting_mutation("wide", "by_key", &key.0)
        })
        .collect::<Vec<_>>();
    apply(&db, &records);
    let target = mutation_tree_key(&records[0]).unwrap();
    let write = db.begin_write().unwrap();
    {
        let mut nodes = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        let storage_key = node_storage_key(LogicalFamily::SchemaAndIndexes, &key_hash(&target));
        let mut damaged = nodes
            .get(storage_key.as_slice())
            .unwrap()
            .unwrap()
            .value()
            .to_vec();
        *damaged.last_mut().unwrap() ^= 1;
        nodes
            .insert(storage_key.as_slice(), damaged.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let verifier = ContentIdentityReadVerifier::open(&read, dir.path(), None).unwrap();
    assert!(verifier.verify(&records[0]).is_err());
    let mut delivered = 0;
    assert!(
        verifier
            .visit_relational_index_postings::<Error>(
                "wide",
                "by_key",
                std::ops::Bound::Unbounded,
                false,
                &ProofMemory::unlimited(),
                &mut || Ok(()),
                &mut |_| {
                    delivered += 1;
                    Ok(true)
                }
            )
            .is_err()
    );
    assert_eq!(delivered, 0);
}

#[test]
fn remove_then_put_coalesces_to_the_same_physical_work_as_a_value_update() {
    let dir = tempfile::tempdir().unwrap();
    let mut results = Vec::new();
    for with_remove in [false, true] {
        let db =
            ProofFixtureDatabase::create(dir.path().join(format!("coalesce-{with_remove}.db")))
                .unwrap();
        let write = db.begin_write().unwrap();
        RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
        write.commit().unwrap();
        let records = (0..1000u64)
            .map(|id| {
                ContentIdentityMutation::put(
                    LogicalFamily::Relational,
                    id.to_le_bytes().to_vec(),
                    b"before".to_vec(),
                )
            })
            .collect::<Vec<_>>();
        apply(&db, &records);
        let mut changes = Vec::new();
        if with_remove {
            changes.push(ContentIdentityMutation::remove(
                LogicalFamily::Relational,
                records[0].logical_key.clone(),
            ));
        }
        changes.push(ContentIdentityMutation::put(
            LogicalFamily::Relational,
            records[0].logical_key.clone(),
            b"after".to_vec(),
        ));
        reset_work_for_test();
        apply(&db, &changes);
        results.push((take_work_for_test(), take_work_bytes_for_test()));
    }
    assert_eq!(
        results[0], results[1],
        "remove/put must coalesce before any proof traversal or write"
    );
    assert_eq!(results[0].0.2, 0, "fixed-key value updates remove no chunk");
}

#[test]
fn keys_too_large_for_a_chunk_are_authenticated_by_reference_and_rebuild_identically() {
    let dir = tempfile::tempdir().unwrap();
    let db = ProofFixtureDatabase::create(dir.path().join("oversized.db")).unwrap();
    let write = db.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    write.commit().unwrap();
    // A mix the leaf rule must keep apart: ordinary keys, keys that still fit
    // one chunk inline, and keys no chunk can hold at all.
    let sizes = [32usize, 600, 60 * 1024, 70 * 1024, 70 * 1024 + 7];
    let mut records = (0..40u64)
        .map(|i| {
            let size = sizes[i as usize % sizes.len()];
            let key = crate::persistence::DurableRelationalIndexKey::new(
                vec![contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(Value::Text(format!("{i:05}{}", "z".repeat(size)))),
                )],
                RowId(i),
                TxId(0),
            )
            .unwrap();
            relational_index_posting_mutation("oversized", "by_key", &key.0)
        })
        .collect::<Vec<_>>();
    for batch in records.chunks(3).rev() {
        apply(&db, batch);
    }
    assert_image(&db, LogicalFamily::SchemaAndIndexes, &records);

    let expected_keys = |records: &[ContentIdentityMutation], reverse: bool| {
        let mut keys = records
            .iter()
            .map(|m| {
                let key = mutation_tree_key(m).unwrap();
                parse_posting_tree_key(&key).unwrap().3.to_vec()
            })
            .collect::<Vec<_>>();
        keys.sort_by(|left, right| {
            RedbPersistence::compare_relational_index_order_keys(left, right)
        });
        if reverse {
            keys.reverse();
        }
        keys
    };
    {
        let read = db.begin_read().unwrap();
        let verifier = ContentIdentityReadVerifier::open(&read, dir.path(), None).unwrap();
        for record in &records {
            verifier.verify(record).unwrap();
        }
        for reverse in [false, true] {
            let mut keys = Vec::new();
            verifier
                .visit_relational_index_postings::<Error>(
                    "oversized",
                    "by_key",
                    std::ops::Bound::Unbounded,
                    reverse,
                    &ProofMemory::unlimited(),
                    &mut || Ok(()),
                    &mut |key| {
                        keys.push(key.to_vec());
                        Ok(true)
                    },
                )
                .unwrap();
            assert_eq!(keys, expected_keys(&records, reverse));
        }
    }

    // Deleting the oversized keys rejoins the surviving chunks.
    let removed = records
        .iter()
        .filter(|m| m.logical_key.len() > 64 * 1024)
        .map(|m| ContentIdentityMutation::remove(m.family, m.logical_key.clone()))
        .collect::<Vec<_>>();
    assert!(!removed.is_empty());
    records.retain(|m| m.logical_key.len() <= 64 * 1024);
    apply(&db, &removed);
    assert_image(&db, LogicalFamily::SchemaAndIndexes, &records);
}

#[test]
fn a_damaged_oversized_leaf_key_is_typed_corruption_before_any_posting() {
    let dir = tempfile::tempdir().unwrap();
    let db = ProofFixtureDatabase::create(dir.path().join("oversized-damage.db")).unwrap();
    let write = db.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    write.commit().unwrap();
    let records = (0..4u64)
        .map(|i| {
            let key = crate::persistence::DurableRelationalIndexKey::new(
                vec![contextdb_core::DirectedValue::Asc(
                    contextdb_core::TotalOrdAsc(Value::Text(format!(
                        "{i}{}",
                        "q".repeat(70 * 1024)
                    ))),
                )],
                RowId(i),
                TxId(0),
            )
            .unwrap();
            relational_index_posting_mutation("oversized", "by_key", &key.0)
        })
        .collect::<Vec<_>>();
    apply(&db, &records);
    let target = mutation_tree_key(&records[0]).unwrap();
    assert!(
        target.len() > MAX_INLINE_LEAF_KEY_BYTES,
        "the fixture key really is too large for one chunk: {}",
        target.len()
    );
    let write = db.begin_write().unwrap();
    {
        let mut nodes = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        let storage_key = node_storage_key(LogicalFamily::SchemaAndIndexes, &key_hash(&target));
        let mut damaged = nodes
            .get(storage_key.as_slice())
            .unwrap()
            .expect("the oversized leaf key is stored by reference")
            .value()
            .to_vec();
        *damaged.last_mut().unwrap() ^= 1;
        nodes
            .insert(storage_key.as_slice(), damaged.as_slice())
            .unwrap();
    }
    write.commit().unwrap();
    let read = db.begin_read().unwrap();
    let verifier = ContentIdentityReadVerifier::open(&read, dir.path(), None).unwrap();
    // The chunk names this key by its authenticated hash, so proving the
    // record does not depend on the stored key bytes at all - and a changed
    // record is still refused.
    verifier.verify(&records[0]).unwrap();
    let changed = ContentIdentityMutation::put(
        records[0].family,
        records[0].logical_key.clone(),
        b"changed".to_vec(),
    );
    assert!(matches!(
        verifier
            .verify(&changed)
            .expect_err("a changed record is refused"),
        Error::StoreCorrupted { .. }
    ));
    let mut delivered = 0;
    let scan = verifier
        .visit_relational_index_postings::<Error>(
            "oversized",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &ProofMemory::unlimited(),
            &mut || Ok(()),
            &mut |_| {
                delivered += 1;
                Ok(true)
            },
        )
        .expect_err("a damaged stored key cannot deliver a posting");
    assert!(matches!(scan, Error::StoreCorrupted { .. }), "{scan:?}");
    assert_eq!(delivered, 0);
}
