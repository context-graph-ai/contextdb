use contextdb_core::{Lsn, Wallclock};

fn main() {
    let w = Wallclock(1);
    let _lsn: Lsn = w;
}
