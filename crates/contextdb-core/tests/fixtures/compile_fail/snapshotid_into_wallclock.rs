use contextdb_core::{SnapshotId, Wallclock};

fn main() {
    let s = SnapshotId(1);
    let _w: Wallclock = s;
}
