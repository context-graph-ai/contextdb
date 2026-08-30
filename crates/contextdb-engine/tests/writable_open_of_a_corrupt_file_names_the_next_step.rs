//! A file that is not a store leaves the operator with the same next step,
//! whichever door they knocked at.
//!
//! An operator who points either door at a file that cannot be a store is
//! stuck until they are told what to go and do about it. The reading door
//! already tells them: it names the file and ends with the one recovery
//! sentence this build publishes. The writable door is the door the operator
//! reaches for when they mean to FIX the store, so it is the door that most
//! needs to say it -- and a writable open that ends at a bare storage
//! complaint has handed the person trying to repair their data less than the
//! person who only wanted to read it.
//!
//! So: one unreadable file, one closing instruction, whoever opened it.

#![cfg(all(unix, feature = "test-seams"))]

use contextdb_engine::{Database, ReadSession};
use std::path::Path;

/// A file that was never a store: ordinary prose where the format marker
/// should be. This is what a mistyped path, a restored text backup, or a
/// half-written export leaves at the store's name.
fn write_prose_file(path: &Path) {
    std::fs::write(
        path,
        b"this file holds notes a person typed, not a store any engine wrote\n",
    )
    .expect("place a non-store file at the store path");
}

/// A file with nothing in it at all. It shares the prose file's verdict --
/// no committed image can be decoded from it -- while sharing none of its
/// bytes, so the closing instruction is proven on two different faults
/// rather than on one fixture's byte pattern.
fn write_empty_file(path: &Path) {
    std::fs::write(path, b"").expect("place an empty file at the store path");
}

/// The sentence a rendered refusal closes with: everything after the last
/// em-dash separator. A refusal that offers no next step has none, and the
/// caller says so rather than comparing two absences and calling them equal.
fn next_step(rendered: &str) -> String {
    rendered
        .rsplit_once(" \u{2014} ")
        .map(|(_, step)| step.to_owned())
        .unwrap_or_else(|| panic!("the refusal offers no next step: {rendered}"))
}

/// Both doors refuse the file, and both close with the same instruction.
fn assert_both_doors_close_with_the_same_next_step(path: &Path, shape: &str) {
    let writable = Database::open(path).err().unwrap_or_else(|| {
        panic!("a writable open refuses a {shape} file rather than adopting it as a store")
    });
    let reading = ReadSession::open(path)
        .err()
        .unwrap_or_else(|| panic!("a reading open refuses a {shape} file"));

    let writable = writable.to_string();
    let reading = reading.to_string();

    // Both doors must name the recovery commands, so an empty tail can never
    // satisfy the comparison below, and each failure names the door that
    // came up short rather than leaving the reader to guess.
    for (door, rendered) in [("reading", &reading), ("writable", &writable)] {
        assert!(
            rendered.contains("contextdb diagnose"),
            "the {door} refusal for a {shape} file says what to run to see what is \
             salvageable: {rendered}"
        );
        assert!(
            rendered.contains("contextdb reset"),
            "the {door} refusal for a {shape} file names the command that recreates the \
             store: {rendered}"
        );
    }
    assert_eq!(
        next_step(&writable),
        next_step(&reading),
        "one unreadable {shape} file, one next step, whoever opened it"
    );
}

#[test]
fn a_writable_open_of_a_prose_file_closes_with_the_readers_next_step() {
    let directory = tempfile::TempDir::new().expect("task-scoped non-store-file directory");
    let path = directory.path().join("prose-at-the-store-path.db");
    write_prose_file(&path);

    assert_both_doors_close_with_the_same_next_step(&path, "prose");
}

#[test]
fn a_writable_open_of_an_empty_file_closes_with_the_readers_next_step() {
    let directory = tempfile::TempDir::new().expect("task-scoped non-store-file directory");
    let path = directory.path().join("empty-at-the-store-path.db");
    write_empty_file(&path);

    assert_both_doors_close_with_the_same_next_step(&path, "empty");
}

#[test]
fn a_store_the_engine_wrote_still_opens_through_both_doors() {
    let directory = tempfile::TempDir::new().expect("task-scoped non-store-file directory");
    let path = directory.path().join("intact.db");
    let database = Database::open(&path).expect("create a valid store");
    database
        .execute(
            "CREATE TABLE kept (id INTEGER PRIMARY KEY, payload TEXT)",
            &std::collections::HashMap::new(),
        )
        .expect("give the store a table");
    database.close().expect("the writer closes cleanly");

    // The refusals above are about files that cannot be stores, not about
    // caution at the doors: a real store still opens on both routes.
    let reopened = Database::open(&path).expect("a store the engine wrote opens for writing");
    reopened.close().expect("the second writer closes cleanly");
    ReadSession::open(&path).expect("a store the engine wrote opens for reading");
}
