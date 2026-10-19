//! Admission is keyed on the physical store file, not on the name a caller
//! used to reach it. A writer queued through one name closes admission to
//! every reader and cleaner that reaches the same file another way: through
//! a hard link, through a renamed path, with no companion file at all.
//!
//! Promise (contextdb-intent, Policies 21-23; the shipped read contract's
//! hard-link and rename continuity, `reader_snapshot_registration_lifecycle`):
//! a writer's bounded wait cannot be starved by arrivals that happen to use a
//! different name, and a second writer through any name is told the store is
//! held by a writer, never that it is held by readers.
//!
//! Test-estate reason, shared by every function here (2026-10-15, reader/writer
//! handoff): before this work the gate lived on the companion, which belongs
//! to a pathname; no earlier test reaches one store through two names while a
//! writer is queued. The cleaner, rename, second-writer and companionless
//! cases are rows of the same helper with the arrival changed, because the
//! schedule is identical and only who arrives, and how, differs.

use crate::writer_acquisition_support::{
    Phase, assert_no_entrant_reached_the_protected_section, counts, next_event, open_writer,
    park_readers, seeded_store, watch,
};
use contextdb_core::Error;
use contextdb_core::read_contract::ReadFailureKind;
use contextdb_core::store_companion_path;
use contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionEvent;
use contextdb_engine::{Database, DatabaseOpenOptions, ReadSession};
use std::path::{Path, PathBuf};

/// How the second name's companion looks when its arrivals come.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SecondName {
    /// The second name has no companion file at all.
    NoCompanion,
    /// The second name has a trusted, idle companion of its own.
    IdleCompanion,
}

fn is_held_by_writer(error: &Error) -> bool {
    matches!(error, Error::ReadFailure(failure) if failure.kind() == ReadFailureKind::HeldByWriter)
}

/// A store `p`, closed; a hard link `q` to it; `q`'s companion as asked.
fn two_names(directory: &Path, second: SecondName) -> (PathBuf, PathBuf) {
    let p = seeded_store(directory, "first-name.db");
    let q = directory.join("second-name.db");
    std::fs::hard_link(&p, &q).expect("a second name for the same physical store");
    if second == SecondName::IdleCompanion {
        // A writer through the second name creates its companion, then goes.
        Database::open(&q)
            .expect("claim the store through its second name")
            .close()
            .expect("release it");
    }
    (p, q)
}

fn queue_through(
    path: &Path,
) -> (
    crate::writer_acquisition_support::ParkedReader,
    crate::writer_acquisition_support::WriterOpen,
    contextdb_engine::persistence::read_persistence_test_scaffold::WriterAdmissionWatch,
) {
    let mut readers = park_readers(path, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let watch = watch(path);
    let writer = open_writer(path, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "behind the reader inside the section"),
        WriterAdmissionEvent::Waiting,
        "the writer must be queued before the second name's arrivals"
    );
    (reader, writer, watch)
}

/// with the writer queued through `p`, readers that arrive through `q`
/// take no gate request, no hold and no data lock, are answered with the typed
/// held-by-writer refusal, and the writer opens after the reader inside.
fn assert_arrivals_through_the_second_name_are_closed_out(second: SecondName) {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let (p, q) = two_names(directory.path(), second);
    let (reader, writer, watch) = queue_through(&p);
    let before = counts(&p);

    for _ in 0..3 {
        match ReadSession::open(&q) {
            Err(error) if is_held_by_writer(&error) => {}
            Err(other) => panic!("an arrival through the second name is held_by_writer: {other:?}"),
            Ok(_) => panic!(
                "a reader through the second name was admitted past the queued writer: \
                 the writer's claim does not reach the physical store"
            ),
        }
    }
    assert_no_entrant_reached_the_protected_section(before, counts(&p));

    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader inside left"),
        WriterAdmissionEvent::Acquired
    );
    writer
        .finish()
        .expect("the writer opens after the reader")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): admission is keyed on the physical store file: readers that
/// reach it through a hard link with no companion take no gate request, hold or data lock past a
/// queued writer. Before this work the gate lived on the companion, which belongs to a pathname.
#[test]
fn readers_through_a_hard_link_without_a_companion_cannot_pass_a_queued_writer() {
    assert_arrivals_through_the_second_name_are_closed_out(SecondName::NoCompanion);
}

/// Test-estate reason (2026-10-15): the same through a second name that has a trusted, idle
/// companion of its own.
#[test]
fn readers_through_a_hard_link_with_an_idle_companion_cannot_pass_a_queued_writer() {
    assert_arrivals_through_the_second_name_are_closed_out(SecondName::IdleCompanion);
}

/// Test-estate reason (2026-10-15): last-reader cleanup through a second name enters no data-lock
/// section past a queued writer. It becomes decisive once cleanup takes the shared admission path.
#[test]
fn a_cleaner_through_a_hard_link_cannot_pass_a_queued_writer() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let (p, q) = two_names(directory.path(), SecondName::NoCompanion);
    let earlier =
        ReadSession::open(&q).expect("a reader through the second name before the writer");
    let (reader, writer, watch) = queue_through(&p);
    let before = counts(&p);

    drop(earlier);
    assert_no_entrant_reached_the_protected_section(before, counts(&p));

    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader inside left"),
        WriterAdmissionEvent::Acquired
    );
    writer
        .finish()
        .expect("the writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): renaming the reader's original name away while the writer waits
/// does not open admission to arrivals through another name for the same file.
#[test]
fn a_rename_while_the_writer_waits_does_not_open_admission() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let original = seeded_store(directory.path(), "original.db");
    let mut readers = park_readers(&original, Phase::SourceStart, 1);
    let reader = readers.remove(0);
    let moved = directory.path().join("moved.db");
    std::fs::rename(&original, &moved).expect("move the reader's name away");
    let third = directory.path().join("third.db");
    std::fs::hard_link(&moved, &third).expect("a third name for the same file");

    let watch = watch(&moved);
    let writer = open_writer(&moved, DatabaseOpenOptions::default());
    assert_eq!(
        next_event(&watch, "behind the reader whose name moved"),
        WriterAdmissionEvent::Waiting,
        "the writer queues behind a reader that holds the same physical file"
    );
    let before = counts(&moved);
    match ReadSession::open(&third) {
        Err(error) if is_held_by_writer(&error) => {}
        other => panic!(
            "an arrival through a third name must be held_by_writer: {:?}",
            other.as_ref().err()
        ),
    }
    assert_no_entrant_reached_the_protected_section(before, counts(&moved));

    reader.let_go();
    drop(reader.session());
    assert_eq!(
        next_event(&watch, "after the reader inside left"),
        WriterAdmissionEvent::Acquired
    );
    writer
        .finish()
        .expect("the writer opens")
        .close()
        .expect("close");
}

/// Test-estate reason (2026-10-15): a second writer through another name for the store is told a
/// writer holds it, never that readers do. Before the physical claim the data lock's refusal was
/// reported as held by readers.
#[test]
fn a_second_writer_through_a_hard_link_is_held_by_writer_not_by_readers() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let (p, q) = two_names(directory.path(), SecondName::NoCompanion);
    let first = Database::open(&p).expect("the first writer");
    match Database::open(&q) {
        Err(error) if is_held_by_writer(&error) => {}
        Err(other) => panic!(
            "a second writer through another name is held_by_writer, never held_by_readers: \
             {other:?}"
        ),
        Ok(_) => panic!("two writers hold one physical store"),
    }
    first.close().expect("close the first writer");
}

/// Test-estate reason (2026-10-15): a reader whose companion has been removed is still stopped by
/// the physical claim and enters no data-lock section past a queued writer.
#[test]
fn a_companionless_reader_cannot_pass_a_queued_writer() {
    let directory = tempfile::TempDir::new().expect("task-scoped directory");
    let p = seeded_store(directory.path(), "companionless.db");
    let (reader, writer, watch) = queue_through(&p);
    std::fs::remove_file(store_companion_path(&p)).expect("remove the companion the writer made");
    let before = counts(&p);
    match ReadSession::open(&p) {
        Err(error) if is_held_by_writer(&error) => {}
        other => panic!(
            "a companionless reader behind a queued writer is held_by_writer: {:?}",
            other.as_ref().err()
        ),
    }
    assert_no_entrant_reached_the_protected_section(before, counts(&p));

    reader.let_go();
    drop(reader.session());
    // The proof removed the companion the queued writer owns, so the
    // writer's own open may now refuse (its companion is gone); that is the
    // fixture's doing and not the contract under test. Only the entrant
    // checks above carry the promise.
    let _ = watch;
    if let Ok(database) = writer.finish() {
        database.close().expect("close");
    }
}
