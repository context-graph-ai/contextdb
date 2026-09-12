use contextdb_core::read_contract::ChannelAddress;
use contextdb_engine::local_transport::derive_channel_address;
use std::fs;
use std::path::{Path, PathBuf};

fn expected_address(path: &Path) -> ChannelAddress {
    let resolved = fs::canonicalize(path).expect("resolve test store path");
    ChannelAddress(*blake3::hash(resolved.as_os_str().as_encoded_bytes()).as_bytes())
}

fn store_in_working_directory(name: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(name)
        .tempdir_in(std::env::current_dir().expect("working directory"))
        .expect("temporary store directory")
}

#[test]
#[cfg(unix)]
fn equivalent_store_spellings_share_the_same_fixed_address() {
    let root = store_in_working_directory("local-address-spellings-");
    let store = root.path().join("same-store.db");
    fs::write(&store, b"store").expect("write store marker");
    let relative = store
        .strip_prefix(std::env::current_dir().expect("working directory"))
        .expect("store is below working directory")
        .to_path_buf();
    let alias = root.path().join("store-alias.db");
    std::os::unix::fs::symlink(&store, &alias).expect("store symlink");

    let expected = expected_address(&store);
    let absolute = derive_channel_address(&store).expect("absolute spelling address");
    let relative_address = derive_channel_address(&relative).expect("relative spelling address");
    let alias_address = derive_channel_address(&alias).expect("symlink spelling address");

    assert_eq!(absolute, expected);
    assert_eq!(relative_address, expected);
    assert_eq!(alias_address, expected);
    assert_eq!(absolute.0.len(), 32);
}

#[test]
fn long_store_paths_stay_fixed_length_and_distinct_stores_do_not_collapse() {
    let root = tempfile::tempdir().expect("temporary address root");
    let mut long = root.path().to_path_buf();
    for part in 0..16 {
        long.push(format!("segment-{part:02}-{}", "a".repeat(56)));
    }
    fs::create_dir_all(&long).expect("long store directory");
    let first = long.join("first.db");
    let second = long.join("second.db");
    fs::write(&first, b"first").expect("first marker");
    fs::write(&second, b"second").expect("second marker");

    let first_address = derive_channel_address(&first).expect("long first address");
    let second_address = derive_channel_address(&second).expect("long second address");

    assert_eq!(first_address, expected_address(&first));
    assert_eq!(second_address, expected_address(&second));
    assert_eq!(first_address.0.len(), 32);
    assert_eq!(second_address.0.len(), 32);
    assert_ne!(first_address, second_address);
}

#[test]
fn addresses_depend_on_the_resolved_store_not_the_fixture_name() {
    let root = tempfile::tempdir().expect("temporary address root");
    let stores: Vec<PathBuf> = ["alpha.db", "beta.redb", "gamma.store"]
        .into_iter()
        .map(|name| root.path().join(name))
        .collect();
    for (index, store) in stores.iter().enumerate() {
        fs::write(store, format!("store-{index}")).expect("store marker");
    }

    let actual: Vec<_> = stores
        .iter()
        .map(|store| derive_channel_address(store).expect("derived address"))
        .collect();
    let expected: Vec<_> = stores.iter().map(|store| expected_address(store)).collect();
    assert_eq!(actual, expected);
    assert_ne!(actual[0], actual[1]);
    assert_ne!(actual[1], actual[2]);
    assert_ne!(actual[0], actual[2]);
}
