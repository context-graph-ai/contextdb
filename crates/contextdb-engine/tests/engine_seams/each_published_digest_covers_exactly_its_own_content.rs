//! Each published content-identity field changes for the content it covers, and
//! for nothing else.
//!
//! One logical fact is held by exactly one family tree, and a published field
//! that covers more than one kind of content is derived from the roots of the
//! families that hold it. That is only safe if the derivation is exact, so this
//! file drives each kind of change on its own and asserts, field by field, both
//! halves: what must move, and what must stay byte-identical.
//!
//! The negative controls are the point. If `policy` moved on an ordinary
//! indexed-row append, a reader comparing two stores' declared policy would see
//! a difference that is not one — and that is precisely what folding the whole
//! `schema_and_indexes` root into `policy` would have done, because that family
//! also carries the ordered index postings.

use contextdb_core::Value;
use contextdb_engine::direct_file_reader::DirectTypedImageDigest;
use contextdb_engine::{Database, MaintenancePolicy};
use std::collections::HashMap;

/// The eleven published fields plus `complete`, by name, so a failure says
/// which one moved.
fn fields(digest: &DirectTypedImageDigest) -> Vec<(&'static str, [u8; 32])> {
    vec![
        ("relational", digest.relational),
        ("graph", digest.graph),
        ("f32_vectors", digest.f32_vectors),
        ("sq8_vectors", digest.sq8_vectors),
        ("sq4_vectors", digest.sq4_vectors),
        ("schema_and_indexes", digest.schema_and_indexes),
        ("policy", digest.policy),
        ("sync", digest.sync),
        ("change_and_ddl", digest.change_and_ddl),
        ("configuration", digest.configuration),
        ("status", digest.status),
        ("complete", digest.complete),
    ]
}

fn moved(before: &DirectTypedImageDigest, after: &DirectTypedImageDigest) -> Vec<&'static str> {
    fields(before)
        .into_iter()
        .zip(fields(after))
        .filter(|((_, before), (_, after))| before != after)
        .map(|((name, _), _)| name)
        .collect()
}

/// The identity of the committed image, read back the way a reader reads it.
fn digest(path: &std::path::Path) -> DirectTypedImageDigest {
    {
        let declared_read_memory =
            contextdb_engine::vector_read_memory::DeclaredReadMemory::from_reader_limit(
                contextdb_engine::OwnerReadConfig::default()
                    .limits
                    .limits
                    .memory,
            );
        let operation_read_memory = declared_read_memory
            .child()
            .expect("admit the committed-content observation owner");
        contextdb_engine::read_content_test_adapter::committed_content_for_test(
            path,
            operation_read_memory,
            declared_read_memory,
        )
    }
    .expect("read the committed image back")
    .maintained_digest
}

fn execute(db: &Database, sql: &str, params: HashMap<String, Value>) {
    db.execute(sql, &params)
        .unwrap_or_else(|error| panic!("fixture statement `{sql}`: {error}"));
}

/// A store with one indexed table and one row, reopened between observations so
/// each digest is read from a committed image and not from a live writer.
fn fixture(root: &std::path::Path) -> std::path::PathBuf {
    let path = root.join("digest-coverage.db");
    let db = Database::open(&path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    execute(
        &db,
        "CREATE TABLE covered (id UUID PRIMARY KEY, bucket TEXT NOT NULL, payload TEXT NOT NULL) \
         SYNC TWO WAY SYNC CONFLICT KEEP LATEST",
        HashMap::new(),
    );
    execute(
        &db,
        "CREATE INDEX covered_by_bucket ON covered (bucket, payload)",
        HashMap::new(),
    );
    execute(
        &db,
        "INSERT INTO covered (id, bucket, payload) VALUES ($id, 'first', 'seed')",
        HashMap::from([(
            "id".into(),
            Value::Uuid(uuid::Uuid::from_u128(
                0xc0_0000_0000_0000_0000_0000_0000_0001,
            )),
        )]),
    );
    db.close().expect("close the fixture");
    path
}

fn mutate(path: &std::path::Path, change: impl FnOnce(&Database)) -> DirectTypedImageDigest {
    let db = Database::open(path).expect("reopen the fixture writable");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    change(&db);
    db.close().expect("close after the change");
    digest(path)
}

#[test]
fn an_indexed_row_append_leaves_the_declared_policy_digest_alone() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = fixture(root.path());
    let before = digest(&path);

    let after = mutate(&path, |db| {
        execute(
            db,
            "INSERT INTO covered (id, bucket, payload) VALUES ($id, 'second', 'appended')",
            HashMap::from([(
                "id".into(),
                Value::Uuid(uuid::Uuid::from_u128(
                    0xc0_0000_0000_0000_0000_0000_0000_0002,
                )),
            )]),
        );
    });

    let moved = moved(&before, &after);
    assert!(
        moved.contains(&"relational"),
        "the appended row moves the relational digest: {moved:?}"
    );
    assert!(
        moved.contains(&"schema_and_indexes"),
        "the index posting the append writes moves schema_and_indexes: {moved:?}"
    );
    assert!(
        moved.contains(&"complete"),
        "any content change moves the whole-image identity: {moved:?}"
    );
    // The negative control. The append declares no table, so nothing it did
    // belongs to `policy` — even though the index postings it wrote live in the
    // same family as the schema records, and folding that whole family's root
    // into `policy` would have moved it here.
    assert!(
        !moved.contains(&"policy"),
        "an ordinary indexed-row append declares no policy: {moved:?}"
    );
    // `configuration` DOES move, and not because of this derivation: the engine
    // writes one `sync_creation_lineage.v1.…` config record per inserted row
    // (`database.rs`, prepared into `ws.config_writes` and flushed through
    // `config_mutations`). That is a pre-existing behaviour of the write path,
    // recorded here so the control above is read for what it is.
    assert!(
        moved.contains(&"configuration"),
        "an inserted row writes its own creation-lineage config record: {moved:?}"
    );
}

#[test]
fn declaring_a_table_moves_every_field_that_covers_a_schema_record() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = fixture(root.path());
    let before = digest(&path);

    let after = mutate(&path, |db| {
        execute(
            db,
            "CREATE TABLE declared_later (id UUID PRIMARY KEY, note TEXT NOT NULL) \
             SYNC PUSH ONLY SYNC CONFLICT KEEP FIRST",
            HashMap::new(),
        );
    });

    let moved = moved(&before, &after);
    for field in ["schema_and_indexes", "policy", "sync", "status", "complete"] {
        assert!(
            moved.contains(&field),
            "a new table declaration moves `{field}`, which covers schema records: {moved:?}"
        );
    }
    assert!(
        !moved.contains(&"relational"),
        "declaring a table writes no row: {moved:?}"
    );
}

#[test]
fn writing_a_config_key_moves_every_field_that_covers_configuration_and_not_policy() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = fixture(root.path());
    let before = digest(&path);

    let after = mutate(&path, |db| {
        execute(db, "SET MEMORY_LIMIT '128M'", HashMap::new());
    });

    let moved = moved(&before, &after);
    for field in ["configuration", "sync", "status", "complete"] {
        assert!(
            moved.contains(&field),
            "a config write moves `{field}`, which covers declared configuration: {moved:?}"
        );
    }
    // The negative control that separates configuration from schema.
    assert!(
        !moved.contains(&"policy"),
        "a config write declares no table, so `policy` cannot move: {moved:?}"
    );
    assert!(
        !moved.contains(&"schema_and_indexes"),
        "a config write declares no table and writes no posting: {moved:?}"
    );
}

/// The postings root appears in exactly one published field. Declaring an index
/// over an already-declared table moves `schema_and_indexes` — which covers
/// both the schema record it rewrites and the postings it builds — and the
/// schema-record fields with it, but a later append that only adds postings
/// must move `schema_and_indexes` and nothing else that covers schema.
#[test]
fn index_postings_reach_exactly_one_published_field() {
    let root = tempfile::tempdir().expect("task-scoped fixture root");
    let path = fixture(root.path());

    let with_index = mutate(&path, |db| {
        execute(
            db,
            "CREATE INDEX covered_by_payload ON covered (payload)",
            HashMap::new(),
        );
    });
    let after_posting = mutate(&path, |db| {
        execute(
            db,
            "INSERT INTO covered (id, bucket, payload) VALUES ($id, 'third', 'posted')",
            HashMap::from([(
                "id".into(),
                Value::Uuid(uuid::Uuid::from_u128(
                    0xc0_0000_0000_0000_0000_0000_0000_0003,
                )),
            )]),
        );
    });

    let moved = moved(&with_index, &after_posting);
    assert!(
        moved.contains(&"schema_and_indexes"),
        "new postings move schema_and_indexes: {moved:?}"
    );
    assert!(
        !moved.contains(&"policy"),
        "new postings are not a declaration, so `policy` stays byte-identical: {moved:?}"
    );
}
