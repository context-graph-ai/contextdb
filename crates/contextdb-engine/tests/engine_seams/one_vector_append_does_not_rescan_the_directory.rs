//! One vector append's directory-binding work must not scale with store size.
//!
//! Design promise: a user appending one event pays a bounded, incremental
//! append cost as the store grows: fingerprints are maintained incrementally,
//! and a full-image scan on every commit would violate that bound.
//! `append_work::last_directory_binding_work()` reports how many copied
//! records the most recent commit hashed to bind each compact copy to its
//! committed family root.
//!
//! Three operations exercise the bound: appending one vector, deleting one
//! vector, and writing one custody policy record. Each measures a
//! SMALL store against a LARGE, unrelated store built in its own setup phase,
//! and asserts the one-record commit's binding work is bounded by what that
//! commit actually changed, not by the size of the store. Each also proves
//! the store still authenticates the change on reopen, so a bounded proof
//! cannot pass by silently skipping the binding.

use contextdb_core::{TenantId, Value};
use contextdb_engine::Database;
use contextdb_engine::append_work::{self, DirectoryBindingWork};
use contextdb_engine::custody_types::ApplicationTablePolicyExpectation;
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

/// Width of the vector column used throughout. Small enough that the test's
/// own cost is dominated by the directory walk under test, not by vector
/// arithmetic.
const DIM: usize = 4;

/// A "few memberships" store.
const SMALL_ROWS: usize = 5;
/// A "thousands of memberships across several partitions" store — large
/// enough that an O(n) full-table rehash on every commit is visibly and
/// increasingly expensive, never a rounding error.
const LARGE_ROWS: usize = 2_000;

/// A bounded directory-binding walk may touch the changed record plus a
/// handful of bookkeeping keys (an empty-key aggregate, a root marker); it
/// must never touch a number of records that tracks the size of the store.
/// The constant bound allows bookkeeping work while excluding a directory
/// scan proportional to the number of stored records.
const BOUND_CONST: u64 = 8;

fn empty() -> HashMap<String, Value> {
    HashMap::new()
}

fn params(pairs: Vec<(&str, Value)>) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// Distinct, deterministic pre-existing row ids, one namespace per store so
/// the small and large stores below never collide even though each is its
/// own file.
fn row_id(namespace: u128, ordinal: usize) -> Uuid {
    Uuid::from_u128(namespace + ordinal as u128)
}

fn scope_id(namespace: u128, ordinal: usize, scopes: usize) -> Uuid {
    Uuid::from_u128(namespace + (ordinal % scopes) as u128)
}

/// A distinct, deterministic vector per ordinal. Never all-zero (a zero
/// vector is a legitimate degenerate case some quantizers special-case).
fn vector_for(ordinal: usize) -> Vec<f32> {
    (0..DIM)
        .map(|dimension| ((ordinal * 31 + dimension * 7 + 1) % 97) as f32 / 97.0)
        .collect()
}

fn insert_vector(db: &Database, id: Uuid, scope: Uuid, vector: Vec<f32>) {
    db.execute(
        "INSERT INTO growth (id, scope_id, embedding) VALUES ($id, $scope, $embedding)",
        &params(vec![
            ("id", Value::Uuid(id)),
            ("scope", Value::Uuid(scope)),
            ("embedding", Value::Vector(vector)),
        ]),
    )
    .expect("insert one vector into the partitioned growth table");
}

fn delete_vector_row(db: &Database, id: Uuid) {
    db.execute(
        "DELETE FROM growth WHERE id = $id",
        &params(vec![("id", Value::Uuid(id))]),
    )
    .expect("delete one row carrying a vector");
}

fn search_scope(db: &Database, scope: Uuid, query: &[f32], limit: usize) -> Vec<Uuid> {
    db.execute(
        &format!(
            "SELECT id FROM growth WHERE scope_id = $scope ORDER BY embedding <=> $query LIMIT {limit}"
        ),
        &params(vec![
            ("scope", Value::Uuid(scope)),
            ("query", Value::Vector(query.to_vec())),
        ]),
    )
    .expect("a scoped vector search is served")
    .rows()
    .iter()
    .map(|row| match row.first() {
        Some(Value::Uuid(id)) => *id,
        other => panic!("a vector search answers with an id-leading row, got {other:?}"),
    })
    .collect()
}

/// A partitioned vector store pre-populated with `rows` memberships spread
/// over `scopes` partitions, built in its own setup phase (its commits are
/// not part of any measurement below).
fn build_vector_store(
    directory: &Path,
    file_name: &str,
    namespace: u128,
    rows: usize,
    scopes: usize,
) -> Database {
    let db = Database::open(directory.join(file_name)).expect("open a writable vector store");
    db.execute(
        &format!(
            "CREATE TABLE growth (id UUID PRIMARY KEY, scope_id UUID NOT NULL, \
             embedding VECTOR({DIM}) PARTITION_KEY (scope_id) MAX_PARTITIONS 64 SEARCH_MODE AUTO)"
        ),
        &empty(),
    )
    .expect("declare a partitioned vector column");
    for ordinal in 0..rows {
        insert_vector(
            &db,
            row_id(namespace, ordinal),
            scope_id(namespace, ordinal, scopes),
            vector_for(ordinal),
        );
    }
    db
}

fn append_one_vector(
    db: &Database,
    id: Uuid,
    scope: Uuid,
    vector: Vec<f32>,
) -> DirectoryBindingWork {
    append_work::reset();
    insert_vector(db, id, scope, vector);
    append_work::last_directory_binding_work()
}

fn delete_one_vector(db: &Database, id: Uuid) -> DirectoryBindingWork {
    append_work::reset();
    delete_vector_row(db, id);
    append_work::last_directory_binding_work()
}

fn assert_bounded_by_changed_count(
    label: &str,
    small: u64,
    large: u64,
    small_rows: usize,
    large_rows: usize,
) {
    // One INSERT (or one DELETE) of one vector stages exactly one changed
    // vector-directory membership record. A bounded directory-binding walk
    // visits exactly that record — the strongest form of a "bounded by the
    // records that commit changed" bar — never the rest of
    // the table.
    assert_eq!(
        small, 1,
        "{label}: one vector commit in a small store ({small_rows} pre-existing memberships) \
         must hash only the one membership record it changed, not {small}"
    );
    assert_eq!(
        large, 1,
        "{label}: one vector commit in a large store ({large_rows} pre-existing memberships) \
         must hash only the one membership record it changed, not {large}"
    );
    assert!(
        large <= small + BOUND_CONST,
        "{label}: the directory-binding work of one vector commit must not grow with the size \
         of the store: {small} at {small_rows} memberships vs {large} at {large_rows} memberships"
    );
}

#[test]
fn appending_one_vector_hashes_only_what_that_commit_changed() {
    let directory = tempfile::tempdir().expect("task-scoped fixture root");

    let small = build_vector_store(
        directory.path(),
        "small-insert.db",
        0xA000_0000,
        SMALL_ROWS,
        2,
    );
    let large = build_vector_store(
        directory.path(),
        "large-insert.db",
        0xB000_0000,
        LARGE_ROWS,
        8,
    );

    let appended_vector = vector_for(999_999);
    let small_scope = scope_id(0xA000_0000, 0, 2);
    let large_scope = scope_id(0xB000_0000, 0, 8);
    let appended_id_small = Uuid::from_u128(0xA000_0000_0000_0000_0000_0000_ABCD_0001);
    let appended_id_large = Uuid::from_u128(0xB000_0000_0000_0000_0000_0000_ABCD_0001);

    let small_work = append_one_vector(
        &small,
        appended_id_small,
        small_scope,
        appended_vector.clone(),
    );
    let large_work = append_one_vector(
        &large,
        appended_id_large,
        large_scope,
        appended_vector.clone(),
    );

    eprintln!(
        "one vector append: small store ({SMALL_ROWS} memberships) = {small_work:?}, large \
         store ({LARGE_ROWS} memberships) = {large_work:?}"
    );

    assert_bounded_by_changed_count(
        "vector append",
        small_work.vector_memberships,
        large_work.vector_memberships,
        SMALL_ROWS,
        LARGE_ROWS,
    );

    // Authenticate: the store still finds the appended vector after reopen,
    // so the bound cannot be met by silently skipping the binding.
    large
        .close()
        .expect("close the large writer before reopening it");
    drop(large);
    let reopened = Database::open(directory.path().join("large-insert.db"))
        .expect("reopen the large store after the bounded append");
    let found = search_scope(&reopened, large_scope, &appended_vector, LARGE_ROWS + 10);
    assert!(
        found.contains(&appended_id_large),
        "the vector appended under a bounded directory-binding walk must still be found by \
         search after reopen: {found:?}"
    );
}

#[test]
fn deleting_one_vector_hashes_only_what_that_commit_changed() {
    let directory = tempfile::tempdir().expect("task-scoped fixture root");

    let small = build_vector_store(
        directory.path(),
        "small-delete.db",
        0xC000_0000,
        SMALL_ROWS,
        2,
    );
    let large = build_vector_store(
        directory.path(),
        "large-delete.db",
        0xD000_0000,
        LARGE_ROWS,
        8,
    );

    // Delete the very first row each store was seeded with.
    let doomed_small = row_id(0xC000_0000, 0);
    let doomed_large = row_id(0xD000_0000, 0);
    let doomed_large_scope = scope_id(0xD000_0000, 0, 8);
    let doomed_large_vector = vector_for(0);

    let small_work = delete_one_vector(&small, doomed_small);
    let large_work = delete_one_vector(&large, doomed_large);

    eprintln!(
        "one vector delete: small store ({SMALL_ROWS} memberships) = {small_work:?}, large \
         store ({LARGE_ROWS} memberships) = {large_work:?}"
    );

    assert_bounded_by_changed_count(
        "vector delete/visibility change",
        small_work.vector_memberships,
        large_work.vector_memberships,
        SMALL_ROWS,
        LARGE_ROWS,
    );

    // Authenticate: the store no longer finds the deleted vector after
    // reopen, so the bound cannot be met by skipping the tombstone itself
    // (as opposed to merely skipping its proof).
    large
        .close()
        .expect("close the large writer before reopening it");
    drop(large);
    let reopened = Database::open(directory.path().join("large-delete.db"))
        .expect("reopen the large store after the bounded delete");
    let found = search_scope(
        &reopened,
        doomed_large_scope,
        &doomed_large_vector,
        LARGE_ROWS + 10,
    );
    assert!(
        !found.contains(&doomed_large),
        "the vector deleted under a bounded directory-binding walk must stay gone after \
         reopen: {found:?}"
    );
}

/// A distinct, deterministic tenant-table-policy name, one namespace per
/// store so the small and large custody stores below never collide.
fn policy_table_name(namespace: &str, ordinal: usize) -> String {
    format!("{namespace}_policy_table_{ordinal:06}")
}

/// Seed `count` tenant table policy records, starting at `start`, in ONE
/// commit — the public test seam `Database::__seed_tenant_table_policies_for_test`
/// (`custody/fixtures.rs:174-209`), which commits `Record::Policy` entries
/// keyed `tenant_table_policy.v1.*` (`records.rs:750`, matched by
/// `custody::records::owned_key`, `records.rs:843-846`) without requiring an
/// actual sync exchange. None of the named tables need to exist; the policy
/// digest is over the table name and the declared clauses alone.
fn seed_policies(db: &Database, namespace: &str, tenant: &TenantId, start: usize, count: usize) {
    let mut expectation = ApplicationTablePolicyExpectation::new();
    for ordinal in start..start + count {
        expectation = expectation
            .expect_table(
                policy_table_name(namespace, ordinal),
                "SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST IMMUTABLE",
            )
            .expect("a valid tenant table policy expectation clause");
    }
    db.__seed_tenant_table_policies_for_test(tenant.clone(), expectation)
        .expect("seed tenant table policy records in one commit");
}

/// "A few" custody records.
const SMALL_CUSTODY_RECORDS: usize = 5;
/// "Thousands" of custody records.
const LARGE_CUSTODY_RECORDS: usize = 2_000;

#[test]
fn one_custody_policy_write_hashes_only_what_that_commit_changed() {
    let directory = tempfile::tempdir().expect("task-scoped fixture root");
    let small = Database::open(directory.path().join("small-custody.db"))
        .expect("open a writable small custody store");
    let large = Database::open(directory.path().join("large-custody.db"))
        .expect("open a writable large custody store");
    let tenant = TenantId::from("one-custody-write-append-work");

    seed_policies(&small, "small", &tenant, 0, SMALL_CUSTODY_RECORDS);
    seed_policies(&large, "large", &tenant, 0, LARGE_CUSTODY_RECORDS);

    append_work::reset();
    seed_policies(&small, "small", &tenant, SMALL_CUSTODY_RECORDS, 1);
    let small_work = append_work::last_directory_binding_work();

    append_work::reset();
    seed_policies(&large, "large", &tenant, LARGE_CUSTODY_RECORDS, 1);
    let large_work = append_work::last_directory_binding_work();

    eprintln!(
        "one custody policy write: small store ({SMALL_CUSTODY_RECORDS} custody records) = \
         {small_work:?}, large store ({LARGE_CUSTODY_RECORDS} custody records) = {large_work:?}"
    );

    // The custody count reports records hashed when a commit binds the
    // custody index to its committed root.
    assert_eq!(
        small_work.custody_records, 1,
        "one custody policy write in a small store ({SMALL_CUSTODY_RECORDS} pre-existing \
         records) must hash only the one record it changed, not {small_work:?}"
    );
    assert_eq!(
        large_work.custody_records, 1,
        "one custody policy write in a large store ({LARGE_CUSTODY_RECORDS} pre-existing \
         records) must hash only the one record it changed, not {large_work:?}"
    );
    assert!(
        large_work.custody_records <= small_work.custody_records + BOUND_CONST,
        "the directory-binding work of one custody policy write must not grow with the size of \
         the custody index: {} at {SMALL_CUSTODY_RECORDS} records vs {} at \
         {LARGE_CUSTODY_RECORDS}",
        small_work.custody_records,
        large_work.custody_records,
    );

    // Authenticate: the newly written policy record survives reopen, so the
    // bound cannot be met by skipping the write it is supposed to prove.
    large
        .close()
        .expect("close the large custody writer before reopening it");
    drop(large);
    let reopened = Database::open(directory.path().join("large-custody.db"))
        .expect("reopen the large custody store after the bounded write");
    let extra_table = policy_table_name("large", LARGE_CUSTODY_RECORDS);
    let records = reopened
        .__delivery_prerequisites_for_test()
        .expect("read custody records back through the public test seam")
        .to_string();
    assert!(
        records.contains(&extra_table),
        "the custody policy written under a bounded directory-binding walk must survive reopen: \
         missing {extra_table:?} in {records}"
    );
}
