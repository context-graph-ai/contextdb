//! Operator-guidance contract audit: the copy-paste recipes an operator or a
//! coding agent follows must match the shipped CLI contract in `docs/cli.md`.
//!
//! Scope: root `AGENTS.md`, every `skills/*/SKILL.md`, and every `docs/*.md`
//! except `docs/cli.md` itself (the contract page, which quotes the old shapes
//! nowhere but does name `--all` when saying it does not exist).
//!
//! The forms rejected here all shipped in the guidance at once and all send a
//! reader somewhere the binary refuses to go:
//!
//! * `--all` — never existed on the CLI; ordinary results are complete or
//!   refused (`docs/cli.md`, "Ordinary results: complete or refused").
//! * a bare-array ordinary result (`[{...}]` on its own) and its `jq '.[]...'`
//!   consumer — a successful `SELECT` under `--json` is
//!   `{"result":{"columns":[...],"rows":[...]}}`.
//! * `{"tables":[` — `.tables` is a bounded page,
//!   `{"tables":{"items":[...],"has_more":...,"continuation":...}}`.
//! * `{"events":{` — the `.events status` document is keyed `events_status`.
//!
//! And two positive requirements: the root throwaway-store recipe and every
//! file-backed mutation skill must show `--write`, because a bare file open is
//! a read session that refuses mutation with `write_requires_flag`.

use std::path::{Path, PathBuf};

#[path = "audit_support/mod.rs"]
mod audit_support;
use audit_support::workspace_root;

/// Guidance files audited for the stale forms: the root agent rules, every
/// skill, and every reference doc except the CLI contract page itself.
fn guidance_files() -> Vec<PathBuf> {
    let root = workspace_root();
    let mut files = vec![root.join("AGENTS.md"), root.join("README.md")];

    let mut skills: Vec<PathBuf> = std::fs::read_dir(root.join("skills"))
        .expect("skills/ must exist")
        .map(|entry| {
            entry
                .expect("readable skills/ entry")
                .path()
                .join("SKILL.md")
        })
        .filter(|path| path.exists())
        .collect();
    skills.sort();
    files.extend(skills);

    let mut docs: Vec<PathBuf> = std::fs::read_dir(root.join("docs"))
        .expect("docs/ must exist")
        .map(|entry| entry.expect("readable docs/ entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .filter(|path| path.file_name().is_some_and(|name| name != "cli.md"))
        .collect();
    docs.sort();
    files.extend(docs);

    files
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

fn relative(path: &Path) -> String {
    path.strip_prefix(workspace_root())
        .unwrap_or(path)
        .display()
        .to_string()
}

/// Every line of a fenced code block, with its 1-based line number. Prose may
/// legitimately name a stale form while explaining that it is gone; a runnable
/// block may not.
fn code_block_lines(contents: &str) -> Vec<(usize, &str)> {
    let mut inside = false;
    let mut lines = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim_start().starts_with("```") {
            inside = !inside;
            continue;
        }
        if inside {
            lines.push((index + 1, line));
        }
    }
    lines
}

#[test]
fn operator_guidance_never_offers_the_all_flag() {
    let mut offenders = Vec::new();
    for path in guidance_files() {
        let contents = read(&path);
        for (line_number, line) in contents.lines().enumerate() {
            // `cargo fmt --all` and friends are a different tool's flag; this
            // audit is about the `contextdb` CLI only.
            if line.contains("cargo ") {
                continue;
            }
            // `--all` as a word: the CLI has no such flag on any command.
            let stale = line.split_whitespace().any(|token| {
                token.trim_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-') == "--all"
            });
            if stale {
                offenders.push(format!(
                    "{}:{}: {}",
                    relative(&path),
                    line_number + 1,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "`--all` does not exist on the CLI; an ordinary result is complete or refused with \
         `owner_limit_exceeded`, and a large result pages with `.cursor open`. Remove these:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn operator_guidance_shows_ordinary_results_in_the_namespaced_document() {
    let mut offenders = Vec::new();
    for path in guidance_files() {
        let contents = read(&path);
        for (line_number, line) in code_block_lines(&contents) {
            let trimmed = line.trim();
            // A JSON line that opens with a row array is the pre-freeze
            // ordinary-result shape.
            let bare_array_result = trimmed.starts_with("[{\"") && trimmed.ends_with(']');
            // `jq` reaching into the row array at the document root.
            let bare_array_consumer = trimmed.contains("jq") && trimmed.contains("'.[]");
            if bare_array_result || bare_array_consumer {
                offenders.push(format!("{}:{}: {}", relative(&path), line_number, trimmed));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "a successful ordinary SELECT under --json is \
         {{\"result\":{{\"columns\":[…],\"rows\":[…]}}}}, so a consumer reads `.result.rows`. \
         Rewrite these:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn operator_guidance_shows_metadata_in_its_bounded_namespaced_document() {
    let mut offenders = Vec::new();
    for path in guidance_files() {
        let contents = read(&path);
        for (line_number, line) in contents.lines().enumerate() {
            let trimmed = line.trim();
            // `.tables` is a bounded page object, never a bare item array.
            if trimmed.contains("{\"tables\":[") {
                offenders.push(format!(
                    "{}:{}: {trimmed}\n    -> .tables is \
                     {{\"tables\":{{\"items\":[…],\"has_more\":…,\"continuation\":…}}}}",
                    relative(&path),
                    line_number + 1
                ));
            }
            // The `.events status` document is keyed `events_status`.
            if trimmed.contains("{\"events\":{") {
                offenders.push(format!(
                    "{}:{}: {trimmed}\n    -> .events status is \
                     {{\"events_status\":{{\"items\":[…],\"has_more\":…,\"continuation\":…}}}}",
                    relative(&path),
                    line_number + 1
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "metadata documents are namespaced and bounded (docs/cli.md, \"Metadata: bounded and \
         resumable\"). Rewrite these:\n{}",
        offenders.join("\n")
    );
}

#[test]
fn the_root_throwaway_store_recipe_demonstrates_the_write_flag() {
    let root = workspace_root();
    let contents = read(&root.join("AGENTS.md"));
    let recipe = code_block_lines(&contents)
        .into_iter()
        .filter(|(_, line)| line.contains("contextdb \"$db\""))
        .collect::<Vec<_>>();
    assert!(
        !recipe.is_empty(),
        "AGENTS.md must keep the throwaway-store recipe that opens `\"$db\"`"
    );
    for (line_number, line) in recipe {
        assert!(
            line.contains("--write"),
            "AGENTS.md:{line_number}: `{}` creates a store and inserts, so it needs --write — a \
             bare file open is a read session that refuses to create the store \
             (`store_not_found`) and refuses the INSERT (`write_requires_flag`)",
            line.trim()
        );
    }
}

/// Every skill whose recipes mutate a file-backed store. A reader copying any
/// of these without `--write` is refused before the advertised task.
const FILE_BACKED_MUTATION_SKILLS: &[&str] = &[
    "using-contextdb",
    "querying-the-graph",
    "vector-search",
    "running-triggers-and-schedules",
    "sync",
    "operating-a-store",
];

#[test]
fn every_file_backed_mutation_skill_demonstrates_the_write_flag() {
    let root = workspace_root();
    for skill in FILE_BACKED_MUTATION_SKILLS {
        let path = root.join("skills").join(skill).join("SKILL.md");
        let contents = read(&path);
        let demonstrates = code_block_lines(&contents)
            .into_iter()
            .any(|(_, line)| line.contains("contextdb") && line.contains("--write"));
        assert!(
            demonstrates,
            "skills/{skill}/SKILL.md mutates a file-backed store but no runnable line shows \
             `--write`; a reader copying it is refused with `write_requires_flag`"
        );
    }
}

/// A file-backed CLI invocation inside a runnable block that carries a
/// writer-only flag must also carry `--write`, or argument validation rejects
/// it (exit `2`) before any statement runs.
#[test]
fn a_sync_enrollment_recipe_carries_the_write_flag() {
    let mut offenders = Vec::new();
    for path in guidance_files() {
        let contents = read(&path);
        for (line_number, line) in code_block_lines(&contents) {
            let is_invocation = line.contains("contextdb ") && !line.contains("contextdb-server");
            let writer_only = line.contains("--sync-endpoint") || line.contains("--tenant-id");
            if is_invocation && writer_only && !line.contains("--write") {
                offenders.push(format!(
                    "{}:{}: {}",
                    relative(&path),
                    line_number,
                    line.trim()
                ));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "--sync-endpoint and --tenant-id configure a --write session's edge enrollment; without \
         --write the CLI refuses at argument validation with exit 2 and nothing runs. Fix \
         these:\n{}",
        offenders.join("\n")
    );
}
