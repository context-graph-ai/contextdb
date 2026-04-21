use contextdb_core::{Lsn, TxId};

fn main() {
    let lsn = Lsn(1);
    let _tx: TxId = lsn;
}
