//! The supported product surface has one authenticated Iroh transport.

use std::path::PathBuf;
use std::process::Command;

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .to_path_buf()
}

fn contains_ascii_case_insensitive(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(unix)]
fn checked_path(root: &std::path::Path, raw: &[u8]) -> Option<PathBuf> {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;
    Some(root.join(std::path::Path::new(OsStr::from_bytes(raw))))
}

#[cfg(not(unix))]
fn checked_path(root: &std::path::Path, raw: &[u8]) -> Option<PathBuf> {
    std::str::from_utf8(raw).ok().map(|path| root.join(path))
}

#[test]
fn tracked_product_surface_contains_no_broker_transport() {
    let root = workspace_root();
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z"])
        .output()
        .expect("list tracked files");
    assert!(output.status.success(), "git ls-files failed: {output:?}");

    let broker_token = [b"na".as_slice(), b"ts".as_slice()].concat();
    let client_token = [
        b"async".as_slice(),
        b"-".as_slice(),
        b"na".as_slice(),
        b"ts".as_slice(),
    ]
    .concat();
    let self_path: &[u8] = b"crates/contextdb-server/tests/no_broker_surface.rs";
    let enterprise: &[u8] = b"contextdb-enterprise/";
    let mut hits = Vec::new();

    for raw in output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
    {
        if raw.starts_with(enterprise) || raw == self_path {
            continue;
        }
        let display = String::from_utf8_lossy(raw).into_owned();
        if contains_ascii_case_insensitive(raw, &broker_token)
            || contains_ascii_case_insensitive(raw, &client_token)
        {
            hits.push(format!("path:{display}"));
            continue;
        }
        let Some(path) = checked_path(&root, raw) else {
            // A non-UTF-8 tracked path may not evade the audit on platforms
            // that cannot reconstruct it from Git's byte stream.
            hits.push(format!("unreadable-path:{display}"));
            continue;
        };
        if !path.is_file() {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) => {
                hits.push(format!("unreadable-content:{display}:{error}"));
                continue;
            }
        };
        if contains_ascii_case_insensitive(&bytes, &broker_token)
            || contains_ascii_case_insensitive(&bytes, &client_token)
        {
            hits.push(format!("content:{display}"));
        }
    }
    assert!(
        hits.is_empty(),
        "tracked ContextDB manifests, lockfiles, workflows, docs, skills, examples, benches, tests, config, environment paths, and gate commands must contain no broker transport residue: {hits:?}"
    );
}
