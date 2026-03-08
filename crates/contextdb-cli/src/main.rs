use std::env;

mod formatter;
mod repl;

fn main() {
    let path = env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: contextdb <path.contextdb | :memory:>");
        std::process::exit(1);
    });

    let db = if path == ":memory:" {
        contextdb_engine::Database::open_memory()
    } else {
        eprintln!("Note: disk-backed storage not yet implemented, using in-memory");
        contextdb_engine::Database::open_memory()
    };

    repl::run(db);
}
