#[test]
fn read_contract_module_types_compile_for_a_cross_crate_consumer() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/fixtures/read_contract/public_api.rs");
}

#[test]
fn core_manifest_makes_bincode_a_production_dependency() {
    let manifest: toml::Value = include_str!("../Cargo.toml")
        .parse()
        .expect("parse the contextdb-core manifest");
    let dependencies = manifest["dependencies"]
        .as_table()
        .expect("contextdb-core has a dependencies table");
    let dev_dependencies = manifest["dev-dependencies"]
        .as_table()
        .expect("contextdb-core has a dev-dependencies table");

    assert!(
        dependencies.contains_key("bincode"),
        "the canonical read codec requires bincode in [dependencies]"
    );
    assert!(
        !dev_dependencies.contains_key("bincode"),
        "bincode must not be available only through [dev-dependencies]"
    );
}
