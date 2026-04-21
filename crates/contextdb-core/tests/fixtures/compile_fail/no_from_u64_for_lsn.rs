fn takes_from<T: From<u64>>(_: T) {}

fn main() {
    takes_from::<contextdb_core::Lsn>(42u64);
}
