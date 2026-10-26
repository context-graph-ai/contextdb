//! A session or store setting declared outside its allowed range is refused
//! with one typed refusal that names the setting, the refused value and the
//! range it allows, and the previous value stays in force.
//!
//! Promise: `SET READ_CACHE_SHARE = 101`, on SQL or through
//! `ReadSession::set_read_cache_share`, and `SET MEMORY_LIMIT` above the
//! store's startup ceiling are both refused with
//! `Error::InvalidSetting { name, value, allowed }`, the same variant for the
//! same family of settings, so a caller can tell a bad setting from any other
//! failure without reading its message. The memory-limit refusal still tells
//! the operator the ceiling they must stay under, and removing the limit under
//! a ceiling keeps its own wording.

#![cfg(unix)]

#[path = "read/reader_proof_support.rs"]
#[allow(dead_code)]
mod reader_proof_support;

use contextdb_core::read_contract::ReadLimits;
use contextdb_core::{Error, Value};
use contextdb_engine::{Database, DatabaseOpenOptions, ReadSessionOptions};
use reader_proof_support as fixture;
use std::collections::HashMap;
use std::sync::Arc;

const MIB: usize = 1024 * 1024;
/// The allowed range a READ_CACHE_SHARE refusal names.
const READ_CACHE_SHARE_RANGE: &str = "0..=100, an integer percent";

/// The refused setting's name, value and allowed range, or a panic naming
/// what came back instead.
fn invalid_setting(error: Error) -> (&'static str, String, String) {
    match error {
        Error::InvalidSetting {
            name,
            value,
            allowed,
        } => (name, value, allowed.to_string()),
        other => {
            panic!("an out-of-range setting is refused with Error::InvalidSetting, got {other:?}")
        }
    }
}

/// READ_CACHE_SHARE = 101 is refused with `InvalidSetting` naming the setting,
/// the value 101 and the range 0..=100, through SQL and through the session
/// API, and the session keeps the share it had declared before.
#[test]
fn an_out_of_range_read_cache_share_is_refused_as_an_invalid_setting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("setting.db");
    let db = Database::open(&path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
    let reader = fixture::open_file(
        &path,
        ReadSessionOptions {
            memory_limit: Some(64 * MIB),
            limits: ReadLimits {
                memory: (64 * MIB) as u64,
                ..fixture::limits()
            },
            read_cache_share: Some(40),
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    );

    let sql = reader
        .execute("SET READ_CACHE_SHARE = 101", &HashMap::new())
        .map(drop)
        .expect_err("a 101% read cache share is refused");
    assert_eq!(
        invalid_setting(sql),
        (
            "READ_CACHE_SHARE",
            "101".to_owned(),
            READ_CACHE_SHARE_RANGE.to_owned()
        ),
        "SQL names the setting, the refused value and the allowed range"
    );
    let api = reader
        .set_read_cache_share(Some(101))
        .expect_err("a 101% read cache share is refused");
    assert_eq!(
        invalid_setting(api),
        (
            "READ_CACHE_SHARE",
            "101".to_owned(),
            READ_CACHE_SHARE_RANGE.to_owned()
        ),
        "the session API refuses it with the same typed refusal"
    );
    assert_eq!(
        reader.options().read_cache_share,
        Some(40),
        "the refused declarations leave the previous share in place"
    );
}

/// SET MEMORY_LIMIT above the store's startup ceiling is refused with the
/// same `InvalidSetting` variant naming MEMORY_LIMIT, and the store keeps the
/// limit it had.
#[test]
fn a_memory_limit_above_the_startup_ceiling_is_refused_as_an_invalid_setting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ceiling.db");
    let db = Database::open_with_options(
        &path,
        DatabaseOpenOptions {
            memory_limit: Some(64 * MIB),
            ..DatabaseOpenOptions::default()
        },
    )
    .expect("a store opens under a 64 MiB startup ceiling");
    let refusal = db
        .execute("SET MEMORY_LIMIT '128M'", &HashMap::new())
        .map(drop)
        .expect_err("a memory limit above the startup ceiling is refused");
    let (name, value, allowed) = invalid_setting(refusal);
    assert_eq!(name, "MEMORY_LIMIT");
    assert_eq!(
        value,
        (128 * MIB).to_string(),
        "the refusal names the refused limit in bytes"
    );
    assert!(
        allowed.contains(&(64 * MIB).to_string()),
        "the refusal tells the operator the startup ceiling they must stay under \
         ({} bytes); it said {allowed:?}",
        64 * MIB
    );
    let removal = db
        .execute("SET MEMORY_LIMIT none", &HashMap::new())
        .map(drop)
        .expect_err("removing the memory limit under a startup ceiling is refused");
    let removal_text = removal.to_string();
    let (name, _, allowed) = invalid_setting(removal);
    assert_eq!(name, "MEMORY_LIMIT");
    assert!(
        removal_text.contains("cannot remove") && allowed.contains(&(64 * MIB).to_string()),
        "removing the limit keeps its own wording, that the limit cannot be removed while a \
         startup ceiling is set, and names the ceiling: {removal_text}"
    );
    let shown = db
        .execute("SHOW MEMORY_LIMIT", &HashMap::new())
        .expect("the store reports its memory limit");
    let limit = shown
        .columns()
        .iter()
        .position(|column| column == "limit")
        .expect("SHOW MEMORY_LIMIT has a limit column");
    assert_eq!(
        shown.rows()[0][limit],
        Value::Int64((64 * MIB) as i64),
        "the refused declarations leave the startup limit in force"
    );
    db.close().unwrap();
}

fn reader_at_40(path: &std::path::Path) -> contextdb_engine::ReadSession {
    fixture::open_file(
        path,
        ReadSessionOptions {
            memory_limit: Some(64 * MIB),
            limits: ReadLimits {
                memory: (64 * MIB) as u64,
                ..fixture::limits()
            },
            read_cache_share: Some(40),
            ..Default::default()
        },
        Arc::new(fixture::Routes::default()),
    )
}

fn small_store(path: &std::path::Path) {
    let db = Database::open(path).unwrap();
    db.execute(
        "CREATE TABLE small (id INTEGER PRIMARY KEY)",
        &HashMap::new(),
    )
    .unwrap();
    db.close().unwrap();
}

/// A 100% share asked for through the session API is refused by total
/// admission with the typed memory-budget refusal, as it is in SQL, and the
/// session keeps its previous share.
#[test]
fn a_read_cache_share_of_100_on_the_session_api_is_refused_by_admission() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("api-100.db");
    small_store(&path);
    let reader = reader_at_40(&path);
    let refusal = reader
        .set_read_cache_share(Some(100))
        .expect_err("a 100% share leaves nothing for the reader's own index and state");
    assert!(
        matches!(refusal, Error::MemoryBudgetExceeded { .. }),
        "the session API refuses 100% with the typed memory-budget refusal: {refusal:?}"
    );
    assert_eq!(
        reader.options().read_cache_share,
        Some(40),
        "the refused declaration leaves the previous share in place"
    );
}

/// `SET READ_CACHE_SHARE 40` without `=` declares the share, as MEMORY_LIMIT
/// is written without one, and `SET MEMORY_LIMIT = 64M` with `=` declares the
/// limit; both spellings are accepted for both settings. A percent sign or a
/// fraction is a value the statement understands and refuses as an invalid
/// setting, never a parse error.
#[test]
fn both_set_spellings_parse_and_a_malformed_percent_is_an_invalid_setting() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("spellings.db");
    small_store(&path);
    let reader = reader_at_40(&path);
    reader
        .execute("SET READ_CACHE_SHARE 30", &HashMap::new())
        .expect("SET READ_CACHE_SHARE without = declares the share");
    assert_eq!(reader.options().read_cache_share, Some(30));
    for malformed in ["40%", "4.5"] {
        let refusal = reader
            .execute(
                &format!("SET READ_CACHE_SHARE = {malformed}"),
                &HashMap::new(),
            )
            .map(drop)
            .expect_err("a malformed percent is refused");
        assert_eq!(
            invalid_setting(refusal),
            (
                "READ_CACHE_SHARE",
                malformed.to_owned(),
                READ_CACHE_SHARE_RANGE.to_owned()
            ),
            "{malformed} is refused as an invalid READ_CACHE_SHARE, not a parse error"
        );
        assert_eq!(
            reader.options().read_cache_share,
            Some(30),
            "the refused {malformed} leaves the previous share in place"
        );
    }
    drop(reader);

    let writer = Database::open(&path).unwrap();
    writer
        .execute("SET MEMORY_LIMIT = 64M", &HashMap::new())
        .expect("SET MEMORY_LIMIT with = declares the limit");
    let shown = writer
        .execute("SHOW MEMORY_LIMIT", &HashMap::new())
        .expect("the store reports its memory limit");
    let limit = shown
        .columns()
        .iter()
        .position(|column| column == "limit")
        .expect("SHOW MEMORY_LIMIT has a limit column");
    assert_eq!(shown.rows()[0][limit], Value::Int64((64 * MIB) as i64));
    writer.close().unwrap();
}
