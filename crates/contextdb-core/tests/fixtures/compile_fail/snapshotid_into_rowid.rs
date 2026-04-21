use contextdb_core::{RowId, SnapshotId};

fn main() {
    let s = SnapshotId(1);
    let _rid: RowId = s;
}
