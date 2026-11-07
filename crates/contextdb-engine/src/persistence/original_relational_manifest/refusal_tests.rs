//! The legacy (original-frame) relational manifest callers refuse every
//! inconsistent physical row and rooted witness they are handed (five named
//! damage refusals). A real legacy store is seeded through the
//! engine, then reopened as a bare persistence with a real writer Original;
//! each test drives one caller with a test-private inconsistent input (a
//! wrong predecessor body, an extra pending witness, a pending removal, or a
//! physical row written into the uncommitted write) and checks the exact
//! refusal, beside a control on the same store that passes. Nothing here is
//! committed, and no production corruption seam is used.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]

use super::*;
use crate::memory_accounting::MemoryAccountant;

const TABLE: &str = "refusals";

/// One physical version: its key in the relation and its stored body.
#[derive(Clone)]
struct Version {
    key: Vec<u8>,
    body: Vec<u8>,
}

struct Fixture {
    _root: tempfile::TempDir,
    persistence: RedbPersistence,
    // Row 1 has one version; row 2's newest is the last of its two versions.
    one: Version,
    newest: Version,
}

fn fixture() -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("refusals.db");
    {
        let mut options = crate::read_session::DatabaseOpenOptions::default();
        options.owner_reads.enabled = false;
        options.maintenance_policy = crate::MaintenancePolicy::CallerDriven;
        // open_with_options always assembles the legacy (production) host;
        // only open_owned_for_test builds the owned one.
        let db = crate::Database::open_with_options(&path, options).unwrap();
        let sql = |text: &str| {
            db.execute(text, &HashMap::new()).unwrap();
        };
        sql(&format!(
            "CREATE TABLE {TABLE} (id INTEGER PRIMARY KEY, body TEXT) HISTORY ALL"
        ));
        sql(&format!("INSERT INTO {TABLE} VALUES (1, 'one')"));
        sql(&format!("INSERT INTO {TABLE} VALUES (2, 'old')"));
        sql(&format!("UPDATE {TABLE} SET body = 'newest' WHERE id = 2"));
        db.close().unwrap();
    }
    let persistence = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let versions = persistence
        .with_db(|raw| {
            let read = raw.begin_read().map_err(RedbPersistence::storage_error)?;
            let name = RedbPersistence::rel_table_name(TABLE);
            let relation = read
                .open_table(TableDefinition::<&[u8], &[u8]>::new(name.as_str()))
                .map_err(RedbPersistence::storage_error)?;
            relation
                .iter()
                .map_err(RedbPersistence::storage_error)?
                .map(|entry| {
                    let (key, body) = entry.map_err(RedbPersistence::storage_error)?;
                    Ok(Version {
                        key: key.value().to_vec(),
                        body: body.value().to_vec(),
                    })
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap();
    assert_eq!(versions.len(), 3, "row 1 once, row 2 old and newest");
    let row = |v: &Version| u64::from_be_bytes(v.key[..8].try_into().unwrap());
    assert_eq!(
        row(&versions[1]),
        row(&versions[2]),
        "row 2 holds two versions"
    );
    assert_ne!(row(&versions[0]), row(&versions[1]));
    Fixture {
        _root: root,
        persistence,
        one: versions[0].clone(),
        newest: versions[2].clone(),
    }
}

fn row_id(version: &Version) -> RowId {
    RowId(u64::from_be_bytes(version.key[..8].try_into().unwrap()))
}

/// `version` with one field changed: its body re-encoded and its physical
/// key re-derived, so the pair is self-consistent (the manifest coordinate
/// of a body is computed only against its own physical identity).
fn variant(version: &Version, edit: impl FnOnce(&mut VersionedRow)) -> Version {
    let mut row = RedbPersistence::decode_versioned_row(&version.body, None).unwrap();
    edit(&mut row);
    Version {
        key: RedbPersistence::rel_row_key(&row),
        body: RedbPersistence::encode_versioned_row(&row, None).unwrap(),
    }
}

/// A version of the same row whose physical key sorts just after `version`.
fn later(version: &Version) -> Version {
    let later = variant(version, |row| row.lsn = Lsn(row.lsn.0 + 1));
    assert!(later.key > version.key && later.key[..8] == version.key[..8]);
    later
}

/// Run `check` inside one uncommitted write of the fixture store, under a
/// real preparation Original of a writer budget.
fn in_write<T>(
    fixture: &Fixture,
    check: impl FnOnce(
        &RedbPersistence,
        &crate::memory_accounting::OwnedMemoryReservation,
        &redb::WriteTransaction,
    ) -> T,
) -> T {
    let _decode = crate::read_image_memory::ImageMemory::enter_writer_operation(
        crate::memory_accounting::AccountantHandle::standalone(None, true).unwrap(),
    )
    .unwrap();
    let writer = MemoryAccountant::writer_no_limit().unwrap();
    let funder = writer.prepare_source_funder().unwrap();
    let original = writer.reserve_assembly_original(&funder).unwrap();
    let persistence = &fixture.persistence;
    persistence
        .with_db(|raw| {
            let write = raw.begin_write().map_err(RedbPersistence::storage_error)?;
            let outcome = check(persistence, &original, &write);
            drop(write);
            Ok(outcome)
        })
        .unwrap()
}

fn coordinate(
    persistence: &RedbPersistence,
    original: &crate::memory_accounting::OwnedMemoryReservation,
    physical: &[u8],
    body: &[u8],
) -> Vec<u8> {
    let mut admission = content_identity::original_writer::ProofAdmission::new(
        original,
        persistence.proof_scratch_owner(),
    )
    .unwrap();
    manifest::manifest_for_body(TABLE, physical, body, &mut admission).unwrap()
}

#[track_caller]
fn assert_refused(outcome: Result<()>, reason: &str) {
    match &outcome {
        Err(Error::StoreCorrupted { reason: found, .. })
            if found.contains(&format!("relational version manifest: {reason}")) => {}
        other => panic!("expected the refusal {reason:?}, got {other:?}"),
    }
}

fn relation<'w>(
    write: &'w redb::WriteTransaction,
) -> redb::Table<'w, &'static [u8], &'static [u8]> {
    let name = RedbPersistence::rel_table_name(TABLE);
    write
        .open_table(TableDefinition::<&[u8], &[u8]>::new(name.as_str()))
        .unwrap()
}

#[test]
fn a_replaced_version_whose_persisted_predecessor_disagrees_with_its_rooted_member_is_refused() {
    let fixture = fixture();
    let newest = fixture.newest.clone();
    in_write(&fixture, |persistence, original, write| {
        // The same physical identity with a different value: a persisted
        // predecessor the rooted member does not witness.
        let forged = variant(&newest, |row| {
            row.values
                .insert("body".into(), Value::Text("forged".into()));
        });
        assert_eq!(forged.key, newest.key);
        persistence
            .replace_relational_manifest_pending_in_write_original(
                original,
                write,
                TABLE,
                &newest.key,
                Some(&newest.body),
                Some(&newest.body),
                None,
            )
            .expect("control: the true predecessor agrees with its rooted member");
        assert_refused(
            persistence.replace_relational_manifest_pending_in_write_original(
                original,
                write,
                TABLE,
                &newest.key,
                Some(&forged.body),
                Some(&newest.body),
                None,
            ),
            "persisted predecessor and rooted version member disagree",
        );
    });
}

#[test]
fn a_second_witness_of_the_same_physical_version_is_refused() {
    let fixture = fixture();
    let newest = fixture.newest.clone();
    in_write(&fixture, |persistence, original, write| {
        // The newest version closed: the same physical identity, a second body.
        let closed = variant(&newest, |row| {
            row.deleted_tx = Some(TxId(row.created_tx.0 + 1));
        });
        assert_eq!(closed.key, newest.key);
        let rooted = coordinate(persistence, original, &newest.key, &newest.body);
        let second = coordinate(persistence, original, &newest.key, &closed.body);
        assert!(
            second > rooted,
            "the extra witness sorts after the rooted one, so the predecessor still agrees"
        );
        let publication = || {
            OriginalPendingManifestPublication::new(
                content_identity::original_writer::ProofAdmission::new(
                    original,
                    persistence.proof_scratch_owner(),
                )
                .unwrap(),
            )
        };
        let mut clean = publication();
        persistence
            .replace_relational_manifest_pending_in_write_original(
                original,
                write,
                TABLE,
                &newest.key,
                Some(&newest.body),
                Some(&closed.body),
                Some(&mut clean),
            )
            .expect("control: one witness per physical version replaces cleanly");
        let mut doubled = publication();
        doubled.push(&second, true).unwrap();
        assert_refused(
            persistence.replace_relational_manifest_pending_in_write_original(
                original,
                write,
                TABLE,
                &newest.key,
                Some(&newest.body),
                Some(&closed.body),
                Some(&mut doubled),
            ),
            "duplicate physical version member",
        );
    });
}

#[test]
fn a_physical_table_row_past_the_last_witness_is_refused_when_the_table_retires() {
    let fixture = fixture();
    let newest = fixture.newest.clone();
    in_write(&fixture, |persistence, original, write| {
        let mut batch = content_identity::original_writer::OriginalIdentityBatch::new(
            persistence,
            original,
            write,
            "__received_version_manifest_edits",
        )
        .unwrap();
        persistence
            .retire_relational_manifests_batch_in_write_original(original, write, TABLE, &mut batch)
            .expect("control: every physical version is witnessed");
    });
    in_write(&fixture, |persistence, original, write| {
        let mut extra = newest.key.clone();
        extra[..8].copy_from_slice(&u64::MAX.to_be_bytes());
        relation(write)
            .insert(extra.as_slice(), newest.body.as_slice())
            .unwrap();
        let mut batch = content_identity::original_writer::OriginalIdentityBatch::new(
            persistence,
            original,
            write,
            "__received_version_manifest_edits",
        )
        .unwrap();
        assert_refused(
            persistence.retire_relational_manifests_batch_in_write_original(
                original, write, TABLE, &mut batch,
            ),
            "unwitnessed physical table suffix",
        );
    });
}

#[test]
fn a_witnessed_row_with_no_physical_version_is_refused() {
    let fixture = fixture();
    let one = fixture.one.clone();
    in_write(&fixture, |persistence, original, write| {
        let absent = RowId(row_id(&one).0 + 1_000);
        let ghost = variant(&one, |row| row.row_id = absent);
        let witness = manifest::manifest_mutation(
            &coordinate(persistence, original, &ghost.key, &ghost.body),
            true,
        )
        .unwrap();
        let relation = relation(write);
        persistence
            .verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                absent,
                &relation,
                &[],
            )
            .expect("control: an absent row with no witness is consistent");
        assert_refused(
            persistence.verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                absent,
                &relation,
                std::slice::from_ref(&witness),
            ),
            "writer group version is physically missing",
        );
    });
}

#[test]
fn a_newest_witness_with_no_physical_version_is_refused() {
    let fixture = fixture();
    let one = fixture.one.clone();
    in_write(&fixture, |persistence, original, write| {
        let rooted = coordinate(persistence, original, &one.key, &one.body);
        let newer = later(&one);
        let newer = coordinate(persistence, original, &newer.key, &newer.body);
        let relation = relation(write);
        persistence
            .verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                row_id(&one),
                &relation,
                &[],
            )
            .expect("control: the row's newest physical version is its newest witness");
        // The newest physical version's own witness removed, a newer witness
        // with no physical version in its place.
        let pending = [
            manifest::manifest_mutation(&rooted, false).unwrap(),
            manifest::manifest_mutation(&newer, true).unwrap(),
        ];
        assert_refused(
            persistence.verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                row_id(&one),
                &relation,
                &pending,
            ),
            "writer group version is physically missing",
        );
    });
}

#[test]
fn a_witness_after_the_newest_physical_version_is_refused() {
    let fixture = fixture();
    let newest = fixture.newest.clone();
    in_write(&fixture, |persistence, original, write| {
        let after = later(&newest);
        let after = coordinate(persistence, original, &after.key, &after.body);
        let relation = relation(write);
        persistence
            .verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                row_id(&newest),
                &relation,
                &[],
            )
            .expect("control: nothing is witnessed after the newest physical version");
        assert_refused(
            persistence.verify_relational_group_pending_in_write_original(
                original,
                write,
                TABLE,
                row_id(&newest),
                &relation,
                &[manifest::manifest_mutation(&after, true).unwrap()],
            ),
            "duplicate or physically missing writer group version",
        );
    });
}
