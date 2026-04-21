use contextdb_core::{Lsn, TxId};

fn main() {
    let tx = TxId(1);
    let _lsn: Lsn = tx;
}
