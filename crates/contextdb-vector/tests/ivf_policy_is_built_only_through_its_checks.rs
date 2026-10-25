//! A clustered `Policy` exists only through `Policy::checked`: outside the
//! crate, a struct literal that would skip its checks (here a dimension no
//! list page can hold) does not compile, while the checked constructor does.
//! The probe is a small crate written to a temporary directory and checked
//! by the same Cargo that runs this test, offline.
use std::path::PathBuf;
use std::process::Command;

const MAIN: &str = r#"
use contextdb_vector::ivf::Policy;
fn main() {
    #[cfg(not(feature = "literal"))]
    let policy = Policy::checked(4, 8, 2, 16, 1 << 20, true).expect("a checked policy");
    #[cfg(feature = "literal")]
    let policy = Policy {
        dimensions: 4_000,
        lists: 8,
        probes: 2,
        rerank: 16,
        buffer_cap_bytes: 1 << 20,
        extend_to_k: true,
    };
    std::hint::black_box(policy);
}
"#;

fn check(literal: bool) -> std::process::Output {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let probe = tempfile_dir();
    std::fs::create_dir_all(probe.join("src")).expect("create the probe crate");
    std::fs::write(
        probe.join("Cargo.toml"),
        format!(
            "[package]\nname = \"ivf-policy-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n\n[workspace]\n\n[features]\nliteral = []\n\n[dependencies]\ncontextdb-vector = {{ path = {:?} }}\n",
            crate_dir
        ),
    )
    .expect("write the probe manifest");
    std::fs::write(probe.join("src/main.rs"), MAIN).expect("write the probe source");
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| crate_dir.join("../../target"))
        .join("ivf-policy-probe");
    let mut cargo = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()));
    cargo
        .args(["check", "--offline", "--quiet", "--target-dir"])
        .arg(target)
        .arg("--manifest-path")
        .arg(probe.join("Cargo.toml"));
    if literal {
        cargo.args(["--features", "literal"]);
    }
    let output = cargo.output().expect("run cargo check on the probe");
    std::fs::remove_dir_all(&probe).expect("remove the probe crate");
    output
}

fn tempfile_dir() -> PathBuf {
    let base = option_env!("CARGO_TARGET_TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    base.join(format!("ivf-policy-probe-{}", std::process::id()))
}

#[test]
fn a_policy_cannot_be_built_outside_its_checks() {
    let checked = check(false);
    assert!(
        checked.status.success(),
        "positive control: the checked constructor compiles from outside the crate: {}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let literal = check(true);
    let stderr = String::from_utf8_lossy(&literal.stderr);
    assert!(
        !literal.status.success() && stderr.contains("private") && stderr.contains("Policy"),
        "a Policy struct literal is refused because its fields are private: {stderr}"
    );
}
