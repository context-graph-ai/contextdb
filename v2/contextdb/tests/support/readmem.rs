//! Setup shared by the reader tests.
use crate::support::{self, Millis};
use contextdb::{Database, DatabaseOpenOptions};
use std::path::Path;
use std::sync::{Arc, atomic::AtomicU64};

/// Opens the store at `folder` on a clock of milliseconds the test moves by hand, and declares
/// it with rows `1..=rows` written together. Gives the store and the clock.
pub fn clocked(folder: &Path, rows: i64) -> (Database, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(0));
    let mut options = DatabaseOpenOptions::default();
    options.clock = Some(Arc::new(Millis(now.clone())));
    let db = support::open_with(folder, options).unwrap();
    support::declare(&db);
    support::insert_together(&db, 1..=rows);
    (db, now)
}
