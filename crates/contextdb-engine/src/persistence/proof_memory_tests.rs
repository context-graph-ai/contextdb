//! What a proof walk actually holds is charged and bounded: the mutation
//! transaction by the tree it edits rather than by how much it writes, and the
//! range read by the memory its caller had admitted rather than by a fixed
//! count of pages.

use super::*;

/// Keys wide enough that every internal separator is held by reference, which
/// is the shape that used to materialize a megabyte of keys per cached chunk.
fn wide_posting(row: u64, key_bytes: usize) -> ContentIdentityMutation {
    let key = crate::persistence::DurableRelationalIndexKey::new(
        vec![contextdb_core::DirectedValue::Asc(
            contextdb_core::TotalOrdAsc(Value::Text(format!("{row:012}{}", "w".repeat(key_bytes)))),
        )],
        RowId(row),
        TxId(0),
    )
    .expect("encode ordered posting key");
    relational_index_posting_mutation("wide", "by_key", &key.0)
}

fn committed(
    path: &std::path::Path,
    rows: u64,
    key_bytes: usize,
) -> (ProofFixtureDatabase, Vec<ContentIdentityMutation>) {
    let database = ProofFixtureDatabase::create(path).expect("create proof fixture");
    let records = (0..rows)
        .map(|row| wide_posting(row, key_bytes))
        .collect::<Vec<_>>();
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

fn tree_height(database: &redb::Database) -> u32 {
    let read = database.begin_read().unwrap();
    let roots = read.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
    let family = LogicalFamily::SchemaAndIndexes;
    let root = decode_root(family, roots.get(family as u8).unwrap().unwrap().value())
        .unwrap()
        .root
        .expect("a committed family root");
    let nodes = read.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    read_node(&nodes, family, root).unwrap().level
}

fn scan(
    database: &redb::Database,
    proof: &ProofMemory,
) -> std::result::Result<Vec<Vec<u8>>, Error> {
    let read = database.begin_read().unwrap();
    let verifier =
        ContentIdentityReadVerifier::open(&read, std::path::Path::new("proofs"), None).unwrap();
    let mut keys = Vec::new();
    verifier.visit_relational_index_postings::<Error>(
        "wide",
        "by_key",
        std::ops::Bound::Unbounded,
        false,
        proof,
        &mut || Ok(()),
        &mut |key| {
            keys.push(key.to_vec());
            Ok(true)
        },
    )?;
    Ok(keys)
}

/// The old reservation was four fixed pages. A descent holds one chunk per
/// level plus the key it is comparing, so the charge has to follow the tree.
#[test]
fn a_deep_wide_key_read_charges_its_actual_depth_and_never_exceeds_its_pool() {
    let root = tempfile::tempdir().unwrap();
    let (shallow, shallow_records) = committed(&root.path().join("shallow.db"), 8, 1024);
    let (deep, deep_records) = committed(&root.path().join("deep.db"), 6_000, 1024);
    let deep_height = tree_height(&deep);
    let shallow_height = tree_height(&shallow);
    assert!(
        deep_height >= 2 && shallow_height < deep_height,
        "the fixtures really are a deep tree and a shallow one: \
         deep={deep_height} shallow={shallow_height}"
    );

    let pool = 8 * 1024 * 1024;
    let shallow_proof = ProofMemory::with_limit(pool);
    assert_eq!(scan(&shallow, &shallow_proof).unwrap().len(), 8);
    let deep_proof = ProofMemory::with_limit(pool);
    assert_eq!(scan(&deep, &deep_proof).unwrap().len(), 6_000);
    assert_eq!(deep_proof.live_bytes(), 0, "every live charge is returned");
    eprintln!(
        "proof charge: shallow live={} peak={}; deep(level {deep_height}) live={} peak={}",
        shallow_proof.live_peak(),
        shallow_proof.peak(),
        deep_proof.live_peak(),
        deep_proof.peak()
    );
    assert!(
        deep_proof.live_peak() > shallow_proof.live_peak(),
        "a deeper descent over wider keys charges more than a single chunk: deep={} shallow={}",
        deep_proof.live_peak(),
        shallow_proof.live_peak()
    );
    for proof in [&shallow_proof, &deep_proof] {
        assert!(proof.peak() <= pool, "charged {} of {pool}", proof.peak());
        assert!(!proof.exhausted());
    }

    // A pool that cannot hold the descent refuses to retain the walk rather
    // than quietly exceeding what it was admitted.
    let tight = ProofMemory::with_limit(2048);
    let delivered = scan(&deep, &tight).unwrap().len();
    assert!(tight.exhausted(), "the walk reports it ran out of its pool");
    assert!(
        tight.peak() <= 2048,
        "a refused charge is never taken: {}",
        tight.peak()
    );
    assert!(
        delivered < deep_records.len(),
        "an exhausted pool stops the walk instead of completing it"
    );
    assert_eq!(shallow_records.len(), 8);
}

/// The mutation path kept every chunk it touched, with every wide separator of
/// every chunk resolved. It now keeps the proof path and reads a chunk again
/// if it needs one it dropped, so its charge follows the tree and not the size
/// of the transaction.
#[test]
fn a_wide_key_writer_transaction_holds_its_proof_path_not_its_postings() {
    let root = tempfile::tempdir().unwrap();
    let measure = |name: &str, rows: u64| {
        let (database, records) = committed(&root.path().join(name), rows, 1024);
        let _ = take_proof_memory_peak_for_test();
        // One transaction, every posting replaced: the widest proof walk this
        // shape can ask for.
        let replacements = records
            .iter()
            .map(|record| {
                ContentIdentityMutation::put(
                    record.family,
                    record.logical_key.clone(),
                    b"replaced".to_vec(),
                )
            })
            .collect::<Vec<_>>();
        let write = database.begin_write().unwrap();
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &replacements,
            &mut database.admission(None),
        )
        .unwrap();
        write.commit().unwrap();
        let (bodies, kept, _) = take_proof_memory_peak_for_test();
        (bodies, kept, tree_height(&database))
    };

    let (small_bodies, small_kept, _) = measure("small.db", 1_500);
    let (large_bodies, large_kept, large_height) = measure("large.db", 6_000);
    eprintln!(
        "writer proof charge: 1500 postings bodies={small_bodies} record={small_kept}; \
         6000 postings bodies={large_bodies} record={large_kept} height={large_height}"
    );
    assert!(large_height >= 2);
    let bound = proof_path_bytes(large_height);
    for bodies in [small_bodies, large_bodies] {
        assert!(
            bodies <= bound,
            "the retained chunk bodies stay inside the proof path of the tree being \
             edited: {bodies} of {bound}"
        );
    }
    assert!(
        large_bodies <= small_bodies.saturating_mul(2),
        "four times the postings must not cost proportionally more held proof: \
         small={small_bodies} large={large_bodies}"
    );
    // The only part that grows is the record of which chunks were touched, and
    // it is chunk ids and hashes - never a copy of a key.
    assert!(
        large_kept < 6_000 * 1024 / 8,
        "no whole key is retained outside the charged bodies: {large_kept}"
    );
}

/// Every byte the committed proof holds, read back exactly as stored: the
/// chunk table and the family roots. A failed transaction has to leave both
/// byte-identical, which a root digest alone cannot show.
type StoredProof = (BTreeMap<Vec<u8>, Vec<u8>>, BTreeMap<u8, Vec<u8>>);

fn stored_proof(database: &redb::Database) -> StoredProof {
    let read = database.begin_read().unwrap();
    let nodes = read.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    let roots = read.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
    (
        nodes
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .collect(),
        roots
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value(), value.value().to_vec())
            })
            .collect(),
    )
}

fn accountant(limit: usize) -> Arc<crate::memory_accounting::MemoryAccountant> {
    Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        limit,
    ))
}

fn replacements(records: &[ContentIdentityMutation]) -> Vec<ContentIdentityMutation> {
    records
        .iter()
        .map(|record| {
            ContentIdentityMutation::put(
                record.family,
                record.logical_key.clone(),
                b"replaced".to_vec(),
            )
        })
        .collect()
}

/// A proof update that cannot be admitted is the existing typed refusal, it
/// gives back every byte it took, and it leaves the stored proof exactly as it
/// was - compared chunk by chunk and root by root, not by digest.
#[test]
fn a_refused_wide_key_transaction_returns_its_charge_and_changes_no_stored_byte() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = committed(&root.path().join("refused.db"), 2_000, 1024);
    let before = stored_proof(&database);

    // Enough to start the walk and not enough to finish it: the refusal has to
    // arrive with work already staged, which is what makes the rollback worth
    // asserting. The configured budget is never raised to make this pass.
    let budget = accountant(256 * 1024);
    let baseline = budget.usage().used;
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&budget).into()));
    let failure = RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &replacements(&records),
        &mut admission,
    )
    .expect_err("a proof this budget cannot admit is refused");
    assert!(
        matches!(failure, Error::MemoryBudgetExceeded { .. }),
        "the existing typed budget refusal, not a new error: {failure:?}"
    );
    assert!(
        admission.peak() > 0,
        "the refusal came after real admitted work"
    );
    drop(admission);
    drop(write);

    assert_eq!(
        budget.usage().used,
        baseline,
        "every admitted byte is given back when the transaction ends"
    );
    let after = stored_proof(&database);
    assert_eq!(
        after.0, before.0,
        "not one stored chunk or key blob changed"
    );
    assert_eq!(after.1, before.1, "not one family root changed");

    // The same transaction succeeds once it is admitted, so the refusal was
    // the budget and not the work.
    let generous = accountant(64 * 1024 * 1024);
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&generous).into()));
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &replacements(&records),
        &mut admission,
    )
    .expect("an admitted proof update commits");
    drop(admission);
    write.commit().unwrap();
    assert_eq!(
        generous.usage().used,
        0,
        "a committed transaction also gives back everything it admitted"
    );
    assert_ne!(
        stored_proof(&database).1,
        before.1,
        "the admitted transaction really did change the committed proof"
    );
}

/// A transaction that meets damaged storage mid-walk aborts the same way:
/// typed error, charge returned, stored proof untouched.
#[test]
fn a_damaged_chunk_aborts_the_walk_and_changes_no_stored_byte() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = committed(&root.path().join("damaged.db"), 2_000, 1024);
    let family = LogicalFamily::SchemaAndIndexes;
    // Damage an internal chunk the next walk has to read.
    let write = database.begin_write().unwrap();
    {
        let mut nodes = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        let (key, mut value) = nodes
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .find(|(key, value)| {
                key.first() == Some(&(family as u8))
                    && value.first() == Some(&NODE_VERSION)
                    && decode_node(family, value).is_ok_and(|node| node.level > 0)
            })
            .expect("an internal chunk");
        *value.last_mut().unwrap() ^= 1;
        nodes.insert(key.as_slice(), value.as_slice()).unwrap();
    }
    write.commit().unwrap();

    let before = stored_proof(&database);
    let budget = accountant(64 * 1024 * 1024);
    let baseline = budget.usage().used;
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&budget).into()));
    let failure = RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &replacements(&records),
        &mut admission,
    )
    .expect_err("a damaged chunk cannot be accepted as the committed one");
    assert!(
        failure.to_string().contains("authenticate")
            || failure.to_string().contains("disagrees")
            || failure.to_string().contains("damaged"),
        "the writer names the damage: {failure}"
    );
    drop(admission);
    drop(write);
    assert_eq!(budget.usage().used, baseline, "the charge is returned");
    let after = stored_proof(&database);
    assert_eq!(after.0, before.0, "not one stored chunk changed");
    assert_eq!(after.1, before.1, "not one family root changed");
}

/// The production reader's own walks, under a real accountant: every transient
/// copy is admitted before it is made and given back afterwards, and a budget
/// that cannot hold them refuses instead of allocating.
#[test]
fn a_tight_reader_admits_every_transient_and_returns_all_of_it() {
    let root = tempfile::tempdir().unwrap();
    // Keys far above one chunk, so every page walk resolves stored key blobs
    // and every comparison copies one.
    let (database, records) = committed(&root.path().join("tight-reader.db"), 24, 70 * 1024);
    let read = database.begin_read().unwrap();

    let budget = accountant(64 * 1024 * 1024);
    let baseline = budget.usage().used;
    let verifier = ContentIdentityReadVerifier::open(
        &read,
        std::path::Path::new("tight-reader.db"),
        Some(Arc::clone(&budget).into()),
    )
    .unwrap();
    for record in &records {
        verifier.verify(record).unwrap();
    }
    let mut delivered = 0;
    let proof = ProofMemory::admitted(Some(Arc::clone(&budget).into()), 8 * 1024 * 1024);
    verifier
        .visit_relational_index_postings::<Error>(
            "wide",
            "by_key",
            std::ops::Bound::Unbounded,
            false,
            &proof,
            &mut || Ok(()),
            &mut |_| {
                delivered += 1;
                Ok(true)
            },
        )
        .unwrap();
    assert_eq!(delivered, records.len());
    let peak = budget.high_water_for_test();
    eprintln!(
        "reader admission: peak={peak} baseline={baseline} held_after={} walk_peak={}",
        budget.usage().used,
        proof.peak()
    );
    assert!(
        peak > 70 * 1024,
        "the transient wide-key copies really were admitted: {peak}"
    );
    drop(proof);
    // The bounded record witness set is admitted once, for the source handle's
    // lifetime, through READ_VERIFIER_MEMORY_BYTES - so the walk itself holds
    // nothing once it ends, and the witnesses are never charged twice.
    let retained = budget.usage().used;
    assert!(
        retained <= VERIFIED_RECORD_LIMIT * RECORD_WITNESS_BYTES,
        "the walk holds no more than the reserved witness set: {retained}"
    );
    drop(verifier);
    assert_eq!(
        budget.usage().used,
        baseline,
        "the reader gives back every byte it admitted"
    );

    // A budget below one page and one wide key refuses, and still returns
    // whatever it took on the way.
    let tight = accountant(32 * 1024);
    let tight_baseline = tight.usage().used;
    let verifier = ContentIdentityReadVerifier::open(
        &read,
        std::path::Path::new("tight-reader.db"),
        Some(Arc::clone(&tight).into()),
    )
    .unwrap();
    let refusals = records
        .iter()
        .filter_map(|record| verifier.verify(record).err())
        .count();
    assert!(
        refusals > 0,
        "a reader that cannot admit its scratch refuses instead of allocating"
    );
    let held = tight.usage().used;
    assert!(
        held <= VERIFIED_RECORD_LIMIT * RECORD_WITNESS_BYTES,
        "a refused read holds nothing but the witnesses it did admit: {held}"
    );
    drop(verifier);
    assert_eq!(
        tight.usage().used,
        tight_baseline,
        "and gives those back when it ends"
    );

    // A referenced key larger than the pool currently holds. The copy is
    // admitted through the pool's owner before it exists, so sufficient
    // remaining memory grows the pool and insufficient remaining memory is
    // the typed refusal with no copy.
    struct PoolGrowth {
        accountant: Arc<crate::memory_accounting::MemoryAccountant>,
        ceiling: usize,
        held: std::sync::atomic::AtomicUsize,
    }
    impl ProofPoolGrowth for PoolGrowth {
        fn grow(&self, bytes: usize) -> bool {
            use std::sync::atomic::Ordering;
            match self.held.load(Ordering::SeqCst).checked_add(bytes) {
                Some(target) if target <= self.ceiling => {}
                _ => return false,
            }
            if self.accountant.try_allocate(bytes).is_err() {
                return false;
            }
            self.held.fetch_add(bytes, Ordering::SeqCst);
            true
        }
        fn ceiling(&self) -> usize {
            self.ceiling
        }
    }
    impl Drop for PoolGrowth {
        fn drop(&mut self) {
            self.accountant
                .release(self.held.load(std::sync::atomic::Ordering::SeqCst));
        }
    }

    let key_bytes = 70 * 1024;
    let (wide, wide_records) = committed(&root.path().join("wide-key-pool.db"), 8, key_bytes);

    let unwatched = take_resolved_key_copy_observations_for_test();
    assert!(
        unwatched.is_empty(),
        "ordinary unobserved walks record no key-copy events"
    );
    assert_eq!(
        unwatched.capacity(),
        0,
        "the default-disabled recorder owns no backing buffer"
    );
    drop(unwatched);

    // (a) The current pool is smaller than one referenced key; remaining
    // session memory is enough. The walk succeeds and the accountant has
    // already admitted the key by the time the copy is made.
    {
        let budget = accountant(8 * 1024 * 1024);
        let _resolved_key_watch = watch_resolved_key_copies_for_test();
        let growth = Arc::new(PoolGrowth {
            accountant: Arc::clone(&budget),
            ceiling: 8 * 1024 * 1024,
            held: std::sync::atomic::AtomicUsize::new(1024),
        });
        budget.try_allocate(1024).expect("hold the initial pool");
        let proof = ProofMemory::growable(1024, growth);
        assert_eq!(scan(&wide, &proof).unwrap().len(), wide_records.len());
        let events = take_resolved_key_copy_observations_for_test();
        let copies = events
            .iter()
            .enumerate()
            .filter(|(_, (kind, len))| *kind == "copy" && *len >= key_bytes)
            .map(|(index, (_, len))| (index, *len))
            .collect::<Vec<_>>();
        assert!(
            !copies.is_empty(),
            "the walk copied at least one referenced key: {events:?}"
        );
        for (index, len) in &copies {
            let charged_first = events[..*index]
                .iter()
                .any(|(kind, charged)| *kind == "charge" && *charged >= *len);
            assert!(
                charged_first,
                "referenced key of {len} bytes was copied before it was charged: {events:?}"
            );
        }
        drop(proof);
        assert_eq!(
            budget.usage().used,
            0,
            "the grown pool is returned with the walk"
        );
    }

    let released = take_resolved_key_copy_observations_for_test();
    assert!(
        released.is_empty(),
        "ending the successful watch disables and clears events"
    );
    assert_eq!(
        released.capacity(),
        0,
        "ending the watch releases every backing-buffer byte"
    );
    drop(released);

    // (b) Remaining session memory cannot hold the referenced key. The typed
    // refusal arrives before any copy, and the accountant never observes one.
    {
        let budget = accountant(8 * 1024);
        let baseline = budget.admitted_bytes_for_test();
        let _resolved_key_watch = watch_resolved_key_copies_for_test();
        let proof = ProofMemory::admitted(Some(Arc::clone(&budget).into()), 8 * 1024 * 1024);
        let refused = scan(&wide, &proof);
        assert!(
            matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
            "a referenced key this budget cannot admit is the existing typed \
             refusal, not a copy outside it: {refused:?}"
        );
        let events = take_resolved_key_copy_observations_for_test();
        assert!(
            events.iter().all(|(kind, _)| *kind != "copy"),
            "no referenced key is copied after the accountant has refused it: {events:?}"
        );
        let admitted = budget.admitted_bytes_for_test().saturating_sub(baseline);
        assert!(
            admitted < key_bytes,
            "the accountant never admitted a copy of the referenced key: \
             admitted={admitted} key_bytes={key_bytes}"
        );
        drop(proof);
        assert_eq!(budget.usage().used, 0, "the refused walk holds nothing");
    }
    let released = take_resolved_key_copy_observations_for_test();
    assert!(
        released.is_empty(),
        "ending the refusal watch disables and clears events"
    );
    assert_eq!(
        released.capacity(),
        0,
        "refused walks release the watch backing buffer too"
    );
}

/// Every writer record lives in the reusable admitted window. Refusing
/// that window must leave the stored chunk records and bodies untouched.
#[test]
fn a_refused_chunk_record_is_never_inserted_before_it_is_admitted() {
    use redb::ReadableTableMetadata;
    let directory = tempfile::tempdir().unwrap();
    let database =
        ProofFixtureDatabase::create(directory.path().join("refused-window.db")).unwrap();
    let write = database.begin_write().unwrap();
    let mut table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(1));
    accountant
        .try_allocate(1)
        .expect("consume the whole budget so nothing more can be admitted");
    let mut admission = database.admission(Some(Arc::clone(&accountant).into()));
    let family = LogicalFamily::SchemaAndIndexes;
    let root = empty_root(family);

    let refused = update_family(&mut table, &mut admission, family, &root, Vec::new(), 0);

    assert!(
        matches!(refused, Err(Error::MemoryBudgetExceeded { .. })),
        "an unadmittable chunk record window is refused: {}",
        refused
            .err()
            .map_or_else(|| "accepted".to_owned(), |error| error.to_string())
    );
    assert_eq!(
        table.len().unwrap(),
        0,
        "no chunk record or body is published before its working storage is admitted"
    );
    assert_eq!(admission.held(), 0, "no proof storage survives refusal");
    assert_eq!(
        accountant.usage().used,
        1,
        "the refusal admits nothing and returns everything"
    );

    drop(table);
    drop(write);
    drop(admission);

    // Disposable record storage now lives in the real scratch backend. Its
    // exact factory payload is admitted before creation; no heap record array
    // grows as records are staged.
    let (existing, records) = committed(&directory.path().join("scratch-control.db"), 8, 1024);
    let before = stored_proof(&existing);
    assert!(
        !before.0.is_empty(),
        "existing records exercise preservation"
    );
    let bytes = redb::scratch_backend_allocation_size();
    assert!(bytes > 0, "the concrete factory has an owned payload");
    let scratch_accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        bytes + 1,
    ));
    scratch_accountant.try_allocate(1).unwrap();
    let mut scratch_admission = existing.admission(Some(Arc::clone(&scratch_accountant).into()));
    let (backend, charge) = scratch_admission
        .open_scratch()
        .expect("the exact backend payload is admitted by the real factory");
    assert_eq!(charge.bytes, bytes);
    assert_eq!(scratch_admission.peak(), bytes);
    assert_eq!(scratch_accountant.usage().used, bytes + 1);
    drop(backend);
    drop(charge);
    assert_eq!(scratch_admission.held(), 0);
    assert_eq!(
        scratch_accountant.usage().used,
        1,
        "scratch drop returns its owner charge"
    );

    // Removing one byte of that exact headroom refuses the same real backend
    // before a new proof record or body can be inserted.
    scratch_accountant.try_allocate(1).unwrap();
    let new_record = wide_posting(999, 1024);
    let edits = vec![(
        mutation_tree_key(&new_record).unwrap(),
        Some(value_hash(
            family,
            new_record.logical_value.as_deref().unwrap(),
        )),
    )];
    let stored_root = decode_root(family, before.1.get(&(family as u8)).unwrap()).unwrap();
    let write = existing.begin_write().unwrap();
    let mut table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    let refused = update_family(
        &mut table,
        &mut scratch_admission,
        family,
        &stored_root,
        edits,
        0,
    );
    assert!(matches!(refused, Err(Error::MemoryBudgetExceeded { .. })));
    let after = table
        .iter()
        .unwrap()
        .map(|entry| {
            let (key, value) = entry.unwrap();
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        after, before.0,
        "the refused record and body are never inserted"
    );
    assert_eq!(scratch_admission.held(), 0);
    assert_eq!(
        scratch_accountant.usage().used,
        2,
        "the refusal retains no scratch charge"
    );
    drop(table);
    drop(write);
    drop(scratch_admission);
    assert_eq!(
        stored_proof(&existing),
        before,
        "committed chunks and roots are byte-identical"
    );
    let read = existing.begin_read().unwrap();
    let verifier = ContentIdentityReadVerifier::open(&read, directory.path(), None).unwrap();
    for record in &records {
        verifier
            .verify(record)
            .expect("all committed records remain authenticated");
    }
    assert!(
        verifier.verify(&new_record).is_err(),
        "the refused record is absent"
    );
    scratch_accountant.release(2);
    assert_eq!(
        scratch_accountant.usage().used,
        0,
        "all owners release their bytes"
    );
}

/// Coalescing keeps one entry per logical key. Staging the same key many times
/// must therefore cost the admitted memory of one entry, not one per mutation:
/// `BTreeMap::insert` discards the new key when the entry is already occupied,
/// so the charge that key carried has to be given back at that moment.
#[test]
fn repeated_writes_to_one_key_admit_one_entry_not_one_per_mutation() {
    let root = tempfile::tempdir().unwrap();
    let (database, records) = committed(&root.path().join("coalesced.db"), 4, 70 * 1024);
    let subject = records[1].clone();
    let key_len = subject.logical_key.len();

    let peak_for = |mutations: &[ContentIdentityMutation]| -> usize {
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            256 * 1024 * 1024,
        ));
        let write = database.begin_write().unwrap();
        let mut admission = database.admission(Some(Arc::clone(&accountant).into()));
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            mutations,
            &mut admission,
        )
        .expect("an admitted transaction commits");
        let peak = admission.peak();
        drop(admission);
        drop(write);
        assert_eq!(accountant.usage().used, 0, "the admission is returned");
        peak
    };

    // The control: one update of one key.
    let once = peak_for(&[ContentIdentityMutation::put(
        subject.family,
        subject.logical_key.clone(),
        b"once".to_vec(),
    )]);
    // The same key staged 64 times in one transaction. The caller's own slice
    // of 64 mutations is the caller's memory and is deliberately not charged
    // here; what is measured is what the proof maintenance itself retains.
    let staged = (0..64u8)
        .map(|round| {
            ContentIdentityMutation::put(
                subject.family,
                subject.logical_key.clone(),
                vec![round; 8],
            )
        })
        .collect::<Vec<_>>();
    let repeated = peak_for(&staged);

    eprintln!(
        "coalescing admission: once={once} repeated={repeated} \
         one_entry={} key_len={key_len}",
        mutation_entry_bytes(key_len + 1)
    );
    assert!(
        repeated <= once.saturating_add(mutation_entry_bytes(key_len + 1)),
        "coalescing one key 64 times must not retain 64 entry charges: \
         once={once} repeated={repeated} one_entry={}",
        mutation_entry_bytes(key_len + 1)
    );
}
