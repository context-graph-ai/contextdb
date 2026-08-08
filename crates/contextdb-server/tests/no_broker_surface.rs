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

/// `git ls-files -z` scoped to the whole workspace root (no pathspec), walked
/// directly off the filesystem. Duplicated from the sibling fallback in
/// `crates/contextdb-engine/tests/timeless_source_vocabulary_tests.rs`
/// (cross-crate test-support sharing isn't available); used only when the
/// source tree is not a git repository (e.g. a `git archive` export) --
/// git stays the primary source because it honors `.gitignore`, so this
/// fallback is only reached when git itself cannot answer.
fn walk_tracked_like_paths(root: &std::path::Path) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    walk_dir_into(root, root, &mut out);
    out.sort();
    out
}

fn walk_dir_into(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<Vec<u8>>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name();
        // Only the VCS metadata directory and build output are excluded --
        // tracked dot-paths (`.github/workflows/*.yml`, `.gitignore`,
        // `.dockerignore`, `.cursor/rules`) must still be walked, or a
        // `git archive` export silently stops scanning whole categories
        // (workflows) that this test's own failure message names.
        if name == ".git" || name == "target" {
            continue;
        }
        if path.is_dir() {
            walk_dir_into(&path, root, out);
        } else if path.is_file()
            && let Ok(rel) = path.strip_prefix(root)
        {
            out.push(raw_relative_path_bytes(rel));
        }
    }
}

#[cfg(unix)]
fn raw_relative_path_bytes(rel: &std::path::Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    rel.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn raw_relative_path_bytes(rel: &std::path::Path) -> Vec<u8> {
    rel.to_string_lossy().replace('\\', "/").into_bytes()
}

#[test]
fn tracked_product_surface_contains_no_broker_transport() {
    let root = workspace_root();
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z"])
        // The not-a-repo detection matches git's English message; pin the
        // locale so a translated git cannot change the failure mode.
        .env("LC_ALL", "C")
        .output()
        .expect("list tracked files");
    let raw_paths: Vec<Vec<u8>> = if output.status.success() {
        output
            .stdout
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .map(|path| path.to_vec())
            .collect()
    } else {
        // Not every source tree that must pass this gate is a git checkout --
        // a `git archive` export or source tarball has no `.git` directory.
        // Fall back to a plain filesystem walk over the same scope; any
        // OTHER git failure (corrupt repo, permissions, ...) still panics.
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("not a git repository"),
            "git ls-files failed: {output:?}"
        );
        walk_tracked_like_paths(&root)
    };
    // An empty candidate set means the scan is vacuous -- e.g. the source
    // tree was unpacked inside an unrelated git repository, where ls-files
    // matches nothing yet exits 0. A green audit must mean files were
    // actually read.
    assert!(
        !raw_paths.is_empty(),
        "audit found no files to scan under {}",
        root.display()
    );

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

    for raw in &raw_paths {
        let raw = raw.as_slice();
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

/// The filesystem-walk fallback only runs when git is unavailable (e.g. a
/// `git archive` export). It must still cover every path git would have
/// scanned, or the export silently drops whole categories -- workflows,
/// dotfiles -- from the audit. Compared as raw bytes, not UTF-8 strings, so a
/// mangled non-UTF-8 path cannot masquerade as a match.
#[test]
fn filesystem_walk_fallback_is_a_superset_of_git_tracked_paths() {
    let root = workspace_root();
    let output = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z"])
        .env("LC_ALL", "C")
        .output()
        .expect("list tracked files");
    if !output.status.success() {
        // No git repository here either -- nothing to compare against.
        return;
    }

    let git_paths: std::collections::BTreeSet<Vec<u8>> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| path.to_vec())
        .collect();
    let walk_paths: std::collections::BTreeSet<Vec<u8>> =
        walk_tracked_like_paths(&root).into_iter().collect();

    let missing: Vec<String> = git_paths
        .difference(&walk_paths)
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect();
    assert!(
        missing.is_empty(),
        "filesystem walk fallback missed git-tracked paths that a git-free export must still scan: {missing:?}"
    );
}
