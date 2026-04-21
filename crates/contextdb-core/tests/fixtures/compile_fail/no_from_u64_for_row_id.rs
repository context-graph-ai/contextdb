fn takes_from<T: From<u64>>(_: T) {}

fn main() {
    takes_from::<contextdb_core::RowId>(42u64);
}
