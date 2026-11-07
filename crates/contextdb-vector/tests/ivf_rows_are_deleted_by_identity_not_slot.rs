//! A clustered index deletes a row by its identity, the row and the commit
//! LSN of the version it removes, so a caller never needs to know which
//! generation holds the row's live copy. Outside the crate, a delete by row
//! identity compiles and a delete by a slot of the serving generation does
//! not exist. The probe is a small crate written to a temporary directory and
//! checked offline by the same Cargo that runs this test, through a copy of
//! the workspace lockfile; `CONTEXTDB_IVF_POLICY_PROBE_TARGET` names a
//! different build directory for it, by default `ivf-policy-probe` under the
//! test's target, which it shares with the policy probe.
use std::path::PathBuf;
use std::process::Command;

const MAIN: &str = r#"
use contextdb_vector::ivf::{ClusteredIndex, RowIdentity};
use contextdb_vector::ivf::storage::PageStore;
#[allow(dead_code)]
fn delete<S: PageStore>(index: &mut ClusteredIndex<S>) {
    #[cfg(not(feature = "slot"))]
    let outcome = index.delete_row(0, RowIdentity { row: 7, commit_lsn: 3 });
    #[cfg(feature = "slot")]
    let outcome = index.delete_slot(0, 7);
    std::hint::black_box(outcome.is_ok());
}
fn main() {}
"#;

fn check(slot: bool) -> std::process::Output {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let base = option_env!("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let probe = base.join(format!("ivf-delete-probe-{}", std::process::id()));
    std::fs::create_dir_all(probe.join("src")).expect("create the probe crate");
    std::fs::write(
        probe.join("Cargo.toml"),
        format!(
            "[package]\nname = \"ivf-delete-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[workspace]\n\n[features]\nslot = []\n\n[dependencies]\ncontextdb-vector = {{ path = {:?} }}\n",
            crate_dir
        ),
    )
    .expect("write the probe manifest");
    std::fs::write(probe.join("src/main.rs"), MAIN).expect("write the probe source");
    std::fs::copy(crate_dir.join("../../Cargo.lock"), probe.join("Cargo.lock"))
        .expect("copy the workspace lockfile into the probe");
    let target = std::env::var_os("CONTEXTDB_IVF_POLICY_PROBE_TARGET")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("CARGO_TARGET_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| crate_dir.join("../../target"))
                .join("ivf-policy-probe")
        });
    let mut cargo = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    cargo
        .args(["check", "--offline", "--quiet", "--target-dir"])
        .arg(target)
        .arg("--manifest-path")
        .arg(probe.join("Cargo.toml"));
    if slot {
        cargo.args(["--features", "slot"]);
    }
    let output = cargo.output().expect("run cargo check on the probe");
    std::fs::remove_dir_all(&probe).expect("remove the probe crate");
    output
}

#[test]
fn a_row_is_deleted_by_its_identity_and_no_slot_delete_exists() {
    let by_identity = check(false);
    assert!(
        by_identity.status.success(),
        "a delete by row identity compiles from outside the crate: {}",
        String::from_utf8_lossy(&by_identity.stderr)
    );
    let by_slot = check(true);
    let stderr = String::from_utf8_lossy(&by_slot.stderr);
    assert!(
        !by_slot.status.success() && stderr.contains("delete_slot"),
        "a delete by a serving-generation slot does not exist: {stderr}"
    );
}
