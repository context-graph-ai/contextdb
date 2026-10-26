//! A bounded cursor over an exact vector search answers what the same
//! statement answers when executed, however many rows the scan admits.
//!
//! The statement is the shape an application's default read uses: one
//! Context and generation partition of a 768-dimension vector column,
//! filtered on two flags and ranked by distance, read through a read session
//! at a 1 GiB memory limit. Every page the cursor builds is charged to the
//! request's reader credit and must release only what that credit holds.
//! A 16-passage scan is the small control; the larger rows make the scan
//! admit more of the partition before it ranks.

use contextdb_core::Value;
use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use std::collections::HashMap;
use std::path::Path;
use uuid::Uuid;

const GIB: usize = 1 << 30;
const CONTEXT: u128 = 17;
const GENERATION: u128 = 18;
const SEARCH: &str = "SELECT id, context_id, generation_id, rendered_text, score FROM passages \
     WHERE context_id = $context AND generation_id = $generation \
     AND is_current = $is_current AND is_searchable = $is_searchable \
     ORDER BY vector_text <=> $query LIMIT 10";

/// A deterministic 768-dimension vector for `seed`.
fn vector(seed: u64) -> Vec<f32> {
    let mut state = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    (0..768)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % 2001) as f32 / 1000.0 - 1.0
        })
        .collect()
}

/// A store with `rows` current passages in one Context and generation.
fn store(path: &Path, rows: u64, search_mode: &str) -> Database {
    let db = Database::open(path).expect("open the store");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        &format!(
            "CREATE TABLE passages (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
             generation_id UUID NOT NULL, rendered_text TEXT NOT NULL, \
             vector_text VECTOR(768) PARTITION_KEY (context_id, generation_id) MAX_PARTITIONS 4096 \
             SEARCH_MODE {search_mode}, is_current BOOLEAN NOT NULL, \
             is_searchable BOOLEAN NOT NULL DEFAULT true) HISTORY CURRENT ONLY SYNC OFF"
        ),
        &HashMap::new(),
    )
    .expect("declare the passages");
    for row in 0..rows {
        db.execute(
            "INSERT INTO passages (id, context_id, generation_id, rendered_text, vector_text, is_current) \
             VALUES ($id, $context, $generation, $text, $vector, TRUE)",
            &HashMap::from([
                ("id".into(), Value::Uuid(Uuid::from_u128(1_000 + row as u128))),
                ("context".into(), Value::Uuid(Uuid::from_u128(CONTEXT))),
                ("generation".into(), Value::Uuid(Uuid::from_u128(GENERATION))),
                ("text".into(), Value::Text(format!("passage {row}"))),
                ("vector".into(), Value::Vector(vector(row))),
            ]),
        )
        .expect("write one passage");
    }
    db
}

fn session(path: &Path) -> ReadSession {
    ReadSession::open_with_options(
        path,
        ReadSessionOptions {
            limits: ReadLimits {
                result_rows: 500,
                result_bytes: ReadLimits::SHIPPED_RESULT_BYTES,
                work: 5_000_000,
                active_ms: 600_000,
                memory: GIB as u64,
                cursor_page_rows: 100,
                cursor_page_bytes: ReadLimits::SHIPPED_CURSOR_PAGE_BYTES,
                cursor_idle_ms: 300_000,
                cursor_lifetime_ms: 1_800_000,
            },
            memory_limit: Some(GIB),
            ..ReadSessionOptions::default()
        },
    )
    .expect("open a read session at a 1 GiB memory limit")
}

fn params() -> HashMap<String, Value> {
    HashMap::from([
        ("context".into(), Value::Uuid(Uuid::from_u128(CONTEXT))),
        (
            "generation".into(),
            Value::Uuid(Uuid::from_u128(GENERATION)),
        ),
        ("is_current".into(), Value::Bool(true)),
        ("is_searchable".into(), Value::Bool(true)),
        ("query".into(), Value::Vector(vector(999_999))),
    ])
}

/// The cursor opens, pages to its end, and yields exactly the rows the
/// executed statement answers.
fn assert_cursor_matches_execute(session: &ReadSession) {
    let executed = session
        .execute(SEARCH, &params())
        .expect("the executed search answers")
        .rows()
        .to_vec();
    assert_eq!(executed.len(), 10);
    let mut cursor = session
        .open_cursor(SEARCH, &params())
        .unwrap_or_else(|error| {
            panic!(
                "a cursor over the exact search must answer; every page it builds releases \
             only the reader credit it charged: {error:?}"
            )
        });
    let mut paged = cursor.first_page().rows().to_vec();
    let mut more = cursor.first_page().has_more();
    while more {
        let page = cursor
            .fetch(None)
            .unwrap_or_else(|error| panic!("a later page answers: {error:?}"));
        paged.extend(page.rows().iter().cloned());
        more = page.has_more();
    }
    drop(cursor);
    assert_eq!(paged, executed, "the cursor yields the executed answer");
}

fn file_route(rows: u64, search_mode: &str) {
    let directory = tempfile::tempdir().expect("store directory");
    let path = directory.path().join("passages.db");
    let db = store(&path, rows, search_mode);
    db.close().expect("close the writer");
    drop(db);
    assert_cursor_matches_execute(&session(&path));
}

#[test]
fn an_exact_search_cursor_over_64_passages_answers_on_the_file_route() {
    file_route(64, "EXACT");
}

#[test]
fn an_exact_search_cursor_over_512_passages_answers_on_the_file_route() {
    file_route(512, "EXACT");
}

#[test]
fn an_automatic_search_cursor_over_512_passages_answers_on_the_file_route() {
    file_route(512, "AUTO");
}

#[test]
fn an_automatic_search_cursor_over_512_passages_answers_through_the_owner() {
    let directory = tempfile::tempdir().expect("store directory");
    let path = directory.path().join("passages.db");
    let db = store(&path, 512, "AUTO");
    assert_cursor_matches_execute(&session(&path));
    db.close().expect("close the writer");
}

#[test]
fn an_exact_search_cursor_over_16_passages_answers_on_the_file_route() {
    file_route(16, "EXACT");
}
