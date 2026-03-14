use std::sync::Arc;

mod formatter;
mod repl;

fn main() {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ":memory:".to_string());

    let db = if path == ":memory:" {
        contextdb_engine::Database::open_memory()
    } else {
        eprintln!("Note: disk-backed storage not yet implemented, using in-memory");
        contextdb_engine::Database::open_memory()
    };

    crate::repl::run(Arc::new(db), None, None);
}
