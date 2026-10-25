//! `ReadSession` routes. Each read route is driven on both backings a
//! consumer can get: the owner route (the writer is open and serves the
//! read) and the file route (no writer is open).

use super::fixture::{Store, params};
use super::{Row, call};
use contextdb_core::Value;
use contextdb_engine::{
    MetadataRequest, OwnerReadCancellation, ReadProgress, ReadProgressObserver, ReadSession,
    ReadSessionOptions,
};
use std::sync::Arc;

struct Quiet;

impl ReadProgressObserver for Quiet {
    fn progress(&self, _progress: ReadProgress) {}
}

const QUERY: &str = "SELECT id, body FROM notes WHERE bucket = $bucket ORDER BY id";

fn bucket() -> std::collections::HashMap<String, Value> {
    params(&[("bucket", Value::Int64(1))])
}

/// Open a session over a store whose writer is still open, and over the
/// same population with no writer, and hand both to `read`.
fn on_both_backings(
    read: fn(&ReadSession) -> contextdb_core::Result<()>,
) -> super::Outcome {
    let live = Store::open();
    let (_dir, closed) = Store::closed();
    call(|| {
        let owner = ReadSession::open(&live.path)?;
        read(&owner)?;
        let file = ReadSession::open(&closed)?;
        read(&file)
    })
}

pub fn rows() -> Vec<Row> {
    vec![
        Row {
            route: "contextdb_engine::ReadSession::open",
            drive: || on_both_backings(|_| Ok(())),
        },
        Row {
            route: "contextdb_engine::ReadSession::open_with_options",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| ReadSession::open_with_options(&path, ReadSessionOptions::default()))
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_in_runtime_dir",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| ReadSession::open_in_runtime_dir(&path, ReadSessionOptions::default(), None))
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_with_progress",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| {
                    ReadSession::open_with_progress(
                        &path,
                        ReadSessionOptions::default(),
                        Arc::new(Quiet),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_with_progress_in_runtime_dir",
            drive: || {
                let (_dir, path) = Store::closed();
                call(|| {
                    ReadSession::open_with_progress_in_runtime_dir(
                        &path,
                        ReadSessionOptions::default(),
                        None,
                        Arc::new(Quiet),
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_owner_only",
            drive: || {
                let live = Store::open();
                call(|| ReadSession::open_owner_only(&live.path, ReadSessionOptions::default()))
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_owner_only_in_runtime_dir",
            drive: || {
                let live = Store::open();
                call(|| {
                    ReadSession::open_owner_only_in_runtime_dir(
                        &live.path,
                        ReadSessionOptions::default(),
                        None,
                    )
                })
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::execute",
            drive: || on_both_backings(|session| session.execute(QUERY, &bucket()).map(drop)),
        },
        Row {
            route: "contextdb_engine::ReadSession::execute_with_cancellation",
            drive: || {
                on_both_backings(|session| {
                    session
                        .execute_with_cancellation(QUERY, &bucket(), &OwnerReadCancellation::new())
                        .map(drop)
                })
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::open_cursor",
            drive: || on_both_backings(|session| session.open_cursor(QUERY, &bucket()).map(drop)),
        },
        Row {
            route: "contextdb_engine::ReadSession::open_cursor_with_cancellation",
            drive: || {
                on_both_backings(|session| {
                    session
                        .open_cursor_with_cancellation(
                            QUERY,
                            &bucket(),
                            &OwnerReadCancellation::new(),
                        )
                        .map(drop)
                })
            },
        },
        Row {
            route: "contextdb_engine::ReadSession::metadata",
            drive: || {
                on_both_backings(|session| {
                    session.metadata(MetadataRequest::Tables, None)?;
                    session
                        .metadata(
                            MetadataRequest::Schema {
                                table: "breadth_rows".to_string(),
                            },
                            None,
                        )
                        .map(drop)
                })
            },
        },
    ]
}
