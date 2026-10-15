//! Rooted seeks of a writer's version manifest with this commit's pending
//! deltas overlaid: which member answers, that every stored member the seek
//! passes (including one a pending delta removed) is authenticated against
//! the committed root, and that the seek's scratch comes back.
//!
//! The overlay here is the publication-ordered list; the indexed overlay
//! answers every seek exactly as this list does
//! (`the_indexed_pending_overlay_answers_every_seek_as_the_list_rescan_does`).

use super::*;
use crate::memory_accounting::MemoryAccountant;

const TABLE: &str = "seek";

/// A well-formed manifest coordinate whose order is `n`'s.
fn coordinate(n: u64) -> Vec<u8> {
    let mut key = manifest_prefix(TABLE).expect("a manifest prefix");
    let mut identity = [0u8; 24];
    identity[16..].copy_from_slice(&n.to_be_bytes());
    key.extend_from_slice(&identity);
    key.push(0);
    key.extend_from_slice(&0u64.to_be_bytes());
    key.extend_from_slice(&[0u8; 32]);
    parse_manifest(&key).expect("a well-formed coordinate");
    key
}

fn order_of(coordinate: &[u8]) -> u64 {
    let manifest = parse_manifest(coordinate).expect("a well-formed coordinate");
    u64::from_be_bytes(manifest.identity[16..].try_into().unwrap())
}

/// A committed manifest holding exactly `members`.
fn store(members: &[u64]) -> (tempfile::TempDir, super::super::ProofFixtureDatabase) {
    let root = tempfile::tempdir().expect("fixture directory");
    let database = super::super::ProofFixtureDatabase::create(root.path().join("seek.db"))
        .expect("create the proof fixture");
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    let records = members
        .iter()
        .map(|n| manifest_mutation(&coordinate(*n), true).unwrap())
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

/// Seek from `start` with `pending` (coordinate order, present) overlaid,
/// under a real accountant. Returns the answer and the bytes the admission
/// still holds after the seek (exactly the answer's own copy).
fn seek(
    database: &super::super::ProofFixtureDatabase,
    pending: &[(u64, bool)],
    start: u64,
    exclusive: bool,
) -> Result<(Option<u64>, usize)> {
    let budget = Arc::new(MemoryAccountant::with_budget(64 * 1024 * 1024));
    let overlay = pending
        .iter()
        .map(|(n, present)| manifest_mutation(&coordinate(*n), *present).unwrap())
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    let mut admission = database.admission(Some(Arc::clone(&budget).into()));
    let found = writer_manifest_next_pending(
        &write,
        &coordinate(start),
        exclusive,
        &mut admission,
        overlay.as_slice(),
    );
    let held = budget.usage().used;
    let answer = found?.map(|key| {
        assert_eq!(held, key.len(), "the seek holds only its answer's copy");
        order_of(&key)
    });
    if answer.is_none() {
        assert_eq!(held, 0, "a seek with no answer holds nothing");
    }
    drop(admission);
    assert_eq!(budget.usage().used, 0, "the admission returns everything");
    Ok((answer, held))
}

fn answer(
    database: &super::super::ProofFixtureDatabase,
    pending: &[(u64, bool)],
    start: u64,
    exclusive: bool,
) -> Option<u64> {
    seek(database, pending, start, exclusive)
        .expect("an undamaged manifest answers")
        .0
}

#[test]
fn a_pending_member_before_at_or_after_a_stored_one_answers_by_order() {
    let (_root, database) = store(&[10, 20, 30, 40]);
    assert_eq!(answer(&database, &[], 11, false), Some(20), "the stored member");
    assert_eq!(
        answer(&database, &[(15, true)], 11, false),
        Some(15),
        "a pending member before the stored one answers"
    );
    assert_eq!(
        answer(&database, &[(20, true)], 11, false),
        Some(20),
        "a pending member equal to a stored one answers once, as the stored one"
    );
    assert_eq!(
        answer(&database, &[(25, true)], 11, false),
        Some(20),
        "a pending member after the stored one does not"
    );
    assert_eq!(answer(&database, &[(50, true)], 41, false), Some(50));
    assert_eq!(answer(&database, &[], 41, false), None);
}

#[test]
fn the_latest_pending_delta_decides_whether_a_member_exists() {
    let (_root, database) = store(&[10, 20, 30, 40]);
    assert_eq!(
        answer(&database, &[(20, false)], 11, false),
        Some(30),
        "a removed stored member is skipped"
    );
    assert_eq!(
        answer(&database, &[(20, false), (20, true)], 11, false),
        Some(20),
        "removed then added again: the latest delta adds it"
    );
    assert_eq!(
        answer(&database, &[(20, true), (20, false)], 11, false),
        Some(30),
        "added then removed: the latest delta removes it"
    );
    assert_eq!(
        answer(&database, &[(20, false), (30, false)], 11, false),
        Some(40),
        "consecutive removed stored members are all skipped"
    );
    assert_eq!(
        answer(&database, &[(20, false), (30, false), (35, true)], 11, false),
        Some(35),
        "a pending member past removed stored members answers"
    );
    assert_eq!(
        answer(&database, &[(15, true), (15, false)], 11, false),
        Some(20),
        "a pending member removed again never answers"
    );
    assert_eq!(
        answer(
            &database,
            &[(10, false), (20, false), (30, false), (40, false)],
            0,
            false
        ),
        None,
        "every stored member removed: nothing answers"
    );
}

#[test]
fn inclusive_and_exclusive_bounds_hold_for_stored_and_pending_members() {
    let (_root, database) = store(&[10, 20, 30, 40]);
    assert_eq!(answer(&database, &[], 20, false), Some(20), "inclusive keeps the bound");
    assert_eq!(answer(&database, &[], 20, true), Some(30), "exclusive passes the bound");
    assert_eq!(answer(&database, &[(25, true)], 20, true), Some(25));
    assert_eq!(
        answer(&database, &[(20, true)], 20, true),
        Some(30),
        "exclusive passes a pending member at the bound"
    );
    assert_eq!(answer(&database, &[(22, true)], 22, false), Some(22));
    assert_eq!(answer(&database, &[(22, true)], 22, true), Some(30));
    assert_eq!(
        answer(&database, &[(30, false)], 20, true),
        Some(40),
        "exclusive and a removed successor"
    );
}

#[test]
fn a_damaged_removed_predecessor_is_still_authenticated() {
    let (_root, database) = store(&[10, 20, 30, 40]);
    let pending = [(20, false), (15, true)];
    assert_eq!(
        answer(&database, &pending, 11, false),
        Some(15),
        "control: the undamaged manifest answers with the pending member"
    );
    // Damage every chunk of the manifest's family. The seek reaches the stored
    // member 20 first, which this commit removes: it must still authenticate
    // it rather than trust the overlay to make it disappear.
    let family = LogicalFamily::ChangeAndDdl;
    let write = database.begin_write().unwrap();
    {
        let mut nodes = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
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
    assert!(
        seek(&database, &pending, 11, false).is_err(),
        "a damaged stored member the seek passes is refused, even one this commit removes"
    );
}
