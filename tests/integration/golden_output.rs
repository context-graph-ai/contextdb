#[test]
fn golden_fixtures_exist() {
    let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures");
    for name in [
        "ann-input.yaml",
        "ann-output.yaml",
        "bfs-input.yaml",
        "bfs-output.yaml",
        "impact-analysis-input.yaml",
        "impact-analysis-output.yaml",
    ] {
        let p = base.join(name);
        assert!(p.exists(), "missing fixture {}", p.display());
    }
}
