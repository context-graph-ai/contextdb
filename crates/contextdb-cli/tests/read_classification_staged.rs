//! Runs the composed command-classification proof against the real binary.
//!
//! `tests/staged/read_classification_cli.sh` is a production-binary integration
//! proof: it drives a supplied `contextdb` end to end and checks its
//! command-line behavior — which spellings exist, what each one classifies as,
//! what a refusal leaves behind on disk. It lives outside Cargo because it
//! invokes a binary rather than linking a crate, and until now nothing invoked
//! it, so it proved nothing.
//!
//! This is how it executes: one cargo test that hands it the binary Cargo just
//! built, through the same compile-time `CARGO_BIN_EXE_contextdb` every other
//! journey in this estate resolves. That choice keeps the script the single
//! source of the proof — it is not reimplemented here — while putting it in the
//! one gate everything else already runs through, so it cannot rot unnoticed.
//!
//! A missing `jq` or `rg` FAILS rather than skipping. A proof that quietly does
//! not run is worse than no proof: it reports green while checking nothing.

use std::process::{Command, Stdio};

fn require_tool(tool: &str) {
    let found = Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {tool}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    assert!(
        found,
        "the staged classification proof needs `{tool}` to verify exact results; \
         install it rather than letting the proof silently not run"
    );
}

#[test]
fn the_staged_classification_proof_passes_against_the_built_binary() {
    require_tool("jq");
    require_tool("rg");

    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/staged/read_classification_cli.sh"
    );

    let output = Command::new("bash")
        .arg(script)
        .env("CONTEXTDB_CLI", env!("CARGO_BIN_EXE_contextdb"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run the staged classification proof");

    assert!(
        output.status.success(),
        "the staged command-classification proof failed (exit {:?}).\n--- stdout ---\n{}\n--- stderr ---\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
