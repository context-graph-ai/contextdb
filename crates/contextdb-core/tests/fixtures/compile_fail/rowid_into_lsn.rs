use contextdb_core::{Lsn, RowId};

fn main() {
    let rid = RowId(1);
    let _lsn: Lsn = rid;
}
