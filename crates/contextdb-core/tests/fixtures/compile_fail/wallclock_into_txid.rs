use contextdb_core::{TxId, Wallclock};

fn main() {
    let w = Wallclock(1);
    let _tx: TxId = w;
}
