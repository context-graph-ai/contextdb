use std::env;
use std::error::Error;
use std::path::PathBuf;

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let _db_path = env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("missing database path argument")?;

    println!("WORKLOAD_START");
    Err("peak_rss_harness is not implemented yet".into())
}
