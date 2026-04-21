use contextdb_core::{SnapshotId, TxId};

fn main() {
    let tx = TxId(1);
    // Direct binding must fail. The public semantic bridge is SnapshotId::from_tx(tx),
    // not a blanket From/Into impl — so bare assignment remains blocked.
    let _s: SnapshotId = tx;
}
