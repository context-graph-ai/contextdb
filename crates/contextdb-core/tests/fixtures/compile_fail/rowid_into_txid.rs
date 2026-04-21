use contextdb_core::{RowId, TxId};

fn main() {
    let rid = RowId(1);
    let _tx: TxId = rid;
}
