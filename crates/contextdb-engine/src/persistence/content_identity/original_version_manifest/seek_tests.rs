//! Rooted seeks of the legacy (original-frame) writer manifest with this
//! commit's pending deltas overlaid, bounded by the caller's scope:
//! every stored member the seek passes inside its scope, including one a
//! pending delta removed, and the first stored successor past the scope are
//! authenticated against the committed root before either is discarded; a
//! seek that starts outside its own scope is refused; and the seek's scratch
//! comes back.
//!
//! Mirrors `content_identity/version_manifest_seek_tests.rs`, the ordinary
//! overlay's guards, through the publication-ordered slice overlay.

use super::*;
use crate::memory_accounting::{MemoryAccountant, OwnedMemoryReservation};

const TABLE: &str = "seek";

/// The scope of one physical row: the table's manifest prefix and the row's
/// physical identity.
fn row_scope(n: u64) -> Vec<u8> {
    let mut key = manifest_prefix(TABLE).expect("a manifest prefix");
    let mut identity = [0u8; 24];
    identity[16..].copy_from_slice(&n.to_be_bytes());
    key.extend_from_slice(&identity);
    key
}

/// A well-formed manifest coordinate of row `n`: its open version for
/// `version` 0, else the version closed at transaction `version`.
fn coordinate(n: u64, version: u64) -> Vec<u8> {
    let mut key = row_scope(n);
    key.push(u8::from(version != 0));
    key.extend_from_slice(&version.to_be_bytes());
    key.extend_from_slice(&[0u8; 32]);
    parse_manifest(&key).expect("a well-formed coordinate");
    key
}

fn order_of(coordinate: &[u8]) -> (u64, u64) {
    let manifest = parse_manifest(coordinate).expect("a well-formed coordinate");
    let row = u64::from_be_bytes(manifest.identity[16..].try_into().unwrap());
    let at = row_scope(row).len() + 1;
    let version = u64::from_be_bytes(coordinate[at..at + 8].try_into().unwrap());
    (row, version)
}

/// A committed manifest holding exactly `members`.
fn store(members: &[(u64, u64)]) -> (tempfile::TempDir, super::super::ProofFixtureDatabase) {
    let root = tempfile::tempdir().expect("fixture directory");
    let database = super::super::ProofFixtureDatabase::create(root.path().join("seek.db"))
        .expect("create the proof fixture");
    let write = database.begin_write().unwrap();
    crate::persistence::erasure_frontiers::initialize(&write).unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    let records = members
        .iter()
        .map(|(n, version)| manifest_mutation(&coordinate(*n, *version), true).unwrap())
        .collect::<Vec<_>>();
    RedbPersistence::apply_content_identity_mutations_in_write(
        &write,
        &records,
        &mut database.admission(None),
    )
    .unwrap();
    write.commit().unwrap();
    (root, database)
}

/// Seek `start` within `scope` with `pending` overlaid, under a real
/// accountant. Returns the answer; checks the admission holds only the
/// answer's copy and gives everything back.
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn seek(
    database: &super::super::ProofFixtureDatabase,
    scope: &[u8],
    pending: &[((u64, u64), bool)],
    start: &[u8],
    exclusive: bool,
) -> Result<Option<(u64, u64)>> {
    let writer = MemoryAccountant::writer_budget(64 * 1024 * 1024).unwrap();
    let funding = writer.accountant();
    let overlay = pending
        .iter()
        .map(|((n, version), present)| {
            manifest_mutation(&coordinate(*n, *version), *present).unwrap()
        })
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    let original = OwnedMemoryReservation::try_new_for(
        funding.clone(),
        0,
        "persistence",
        "prepare_commit",
        "test writer",
    )
    .unwrap();
    let mut admission = ProofAdmission::new(&original, Some(Arc::downgrade(&database.owner)))
        .expect("a proof admission");
    let before = funding.usage().used;
    let found = writer_manifest_next_pending(
        &write,
        scope,
        start,
        exclusive,
        &mut admission,
        overlay.as_slice(),
    );
    let held = funding.usage().used - before;
    let answer = found?.map(|key| {
        assert_eq!(held, key.len(), "the seek holds only its answer's copy");
        order_of(&key)
    });
    if answer.is_none() {
        assert_eq!(held, 0, "a seek with no answer holds nothing");
    }
    drop(admission);
    assert_eq!(
        funding.usage().used,
        before,
        "the admission returns everything"
    );
    drop(original);
    Ok(answer)
}

/// Flip the last byte of every stored chunk of the manifest's family.
fn damage_every_chunk(database: &super::super::ProofFixtureDatabase) {
    let family = LogicalFamily::ChangeAndDdl;
    let write = database.begin_write().unwrap();
    {
        let mut nodes = crate::persistence::content_identity::payloads::open(&write).unwrap();
        let damaged = nodes
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().to_vec(), value.value().to_vec())
            })
            .filter(|(key, value)| {
                key.first() == Some(&(family as u8)) && value.first() == Some(&NODE_VERSION)
            })
            .collect::<Vec<_>>();
        assert!(!damaged.is_empty(), "the manifest has stored chunks");
        for (key, mut value) in damaged {
            *value.last_mut().unwrap() ^= 1;
            nodes.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    write.commit().unwrap();
}

#[test]
fn a_damaged_removed_predecessor_in_scope_is_still_authenticated() {
    // The table scope, as the whole-table witness walk uses it.
    let (_root, database) = store(&[(10, 0), (20, 0), (30, 0), (40, 0)]);
    let table = manifest_prefix(TABLE).unwrap();
    let pending = [((20, 0), false), ((15, 0), true)];
    let start = coordinate(11, 0);
    assert_eq!(
        seek(&database, &table, &pending, &start, false).expect("undamaged"),
        Some((15, 0)),
        "control: the undamaged manifest answers with the pending member"
    );
    damage_every_chunk(&database);
    assert!(
        seek(&database, &table, &pending, &start, false).is_err(),
        "a damaged stored member the seek passes is refused, even one this commit removes"
    );
}

#[test]
fn a_damaged_removed_version_of_the_row_itself_is_still_authenticated() {
    // A row scope, as the per-row predecessor and duplicate checks use it.
    let (_root, database) = store(&[(10, 0), (10, 1), (20, 0)]);
    let scope = row_scope(10);
    let pending = [((10, 0), false)];
    assert_eq!(
        seek(&database, &scope, &pending, &scope, false).expect("undamaged"),
        Some((10, 1)),
        "control: the row's remaining version answers"
    );
    damage_every_chunk(&database);
    assert!(
        seek(&database, &scope, &pending, &scope, false).is_err(),
        "the removed version of this row is authenticated before it is skipped"
    );
}

#[test]
fn the_first_stored_successor_past_the_scope_is_authenticated_before_it_is_discarded() {
    // As in the ordinary overlay, the seek's first stored successor is
    // authenticated whether or not it answers; here it lies past the row.
    let (_root, database) = store(&[(20, 0)]);
    let scope = row_scope(10);
    let answer = seek(&database, &scope, &[], &scope, false).expect("undamaged");
    assert!(
        answer.is_none_or(|(row, _)| row != 10),
        "control: the row has no member, so the caller reads no answer within its scope"
    );
    damage_every_chunk(&database);
    assert!(
        seek(&database, &scope, &[], &scope, false).is_err(),
        "a damaged successor past the scope is refused, not silently discarded"
    );
}

#[test]
fn a_seek_that_starts_outside_its_scope_is_refused() {
    let (_root, database) = store(&[(10, 0), (20, 0)]);
    let scope = row_scope(10);
    let outside = coordinate(20, 0);
    assert!(
        seek(&database, &scope, &[], &outside, false).is_err(),
        "a caller whose start lies outside its own scope fails loudly"
    );
    assert!(
        seek(&database, &scope, &[((20, 0), true)], &outside, true).is_err(),
        "with a pending member too"
    );
}
