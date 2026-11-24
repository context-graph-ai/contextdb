//! Design-pinned opening charges shared by library and shell fixtures.
use contextdb::{Error, ReadSession, ReadSessionOptions, Value, params};
use std::path::Path;

/// A writer's floor (M1): the account of the smallest caches and a file under 4 GiB.
pub fn open_floor() -> usize {
    set_aside(None)
}

/// A writer's account (M1): 1 MiB, the caches (bookkeeping inside) and 768 KiB of maps.
pub fn set_aside(limit: Option<usize>) -> usize {
    let payload = limit.map_or(8 << 20, |n| (n / 4).max(8 << 20));
    let plans = limit.map_or(1 << 20, |n| (n / 32).clamp(1 << 20, 8 << 20));
    (1 << 20) + payload + plans + (768 << 10)
}

/// Pinned caches/maps plus retained catalog heap, measured independently of those terms.
pub fn opening_charge(folder: &Path, limit: Option<usize>) -> usize {
    let bytes = std::fs::metadata(folder.join("data")).unwrap().len();
    let mut options = ReadSessionOptions::default();
    options.limits.memory = 32 << 20;
    let reader = ReadSession::open_with_options(folder, options).unwrap();
    // A text as long as the limit is refused at its parse (M5), before the catalog is loaded.
    let before = match reader.execute(&" ".repeat(32 << 20), &params! {}) {
        Err(Error::MemoryBudgetExceeded {
            budget_limit_bytes,
            available_bytes,
            ..
        }) => budget_limit_bytes - available_bytes,
        other => panic!("pre-catalog parse admission: {other:?}"),
    };
    drop(reader.table_names().unwrap());
    let report = reader.execute("SHOW MEMORY_LIMIT", &params! {}).unwrap();
    let at = report
        .columns()
        .iter()
        .position(|name| name == "used")
        .unwrap();
    let Value::Int64(used) = report.rows()[0][at] else {
        panic!("used is an integer")
    };
    let catalog = (used as u64).checked_sub(before).unwrap() as usize;
    set_aside(limit) + (bytes / (4 << 30)) as usize * (768 << 10) + catalog
}

/// A reader's open (M1): a writer's, with the room of one check (M2) in place of the maps.
pub fn reader_charge(folder: &Path, limit: Option<usize>) -> usize {
    let bytes = std::fs::metadata(folder.join("data")).unwrap().len() as usize;
    opening_charge(folder, limit) - (bytes / (4 << 30) + 1) * (768 << 10) + 65_536 * 8 + 65_536 / 8
}

/// The smallest observed cap that admits this reader's catalog, including its temporary
/// decode workspace. Each retry adds only the shortfall reported by the public refusal.
pub fn reader_ready_limit(folder: &Path, spare: usize) -> usize {
    let mut limit = reader_charge(folder, None) + spare;
    loop {
        let mut options = ReadSessionOptions::default();
        options.limits.memory = limit as u64;
        let ready = ReadSession::open_with_options(folder, options).and_then(|reader| {
            drop(reader.table_names()?);
            Ok(reader)
        });
        match ready {
            Ok(_) => return limit,
            Err(contextdb::Error::MemoryBudgetExceeded {
                requested_bytes,
                available_bytes,
                ..
            }) if requested_bytes > available_bytes => {
                limit += usize::try_from(requested_bytes - available_bytes)
                    .expect("the reported shortfall");
            }
            Err(error) => panic!("reader catalog admission: {error:?}"),
        }
    }
}
