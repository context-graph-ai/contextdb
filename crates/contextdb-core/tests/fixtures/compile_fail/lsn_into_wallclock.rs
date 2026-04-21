use contextdb_core::{Lsn, Wallclock};

fn main() {
    let lsn = Lsn(1);
    let _w: Wallclock = lsn;
}
