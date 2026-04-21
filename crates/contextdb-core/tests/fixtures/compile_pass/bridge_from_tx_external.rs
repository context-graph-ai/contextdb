// Lives at tests/fixtures/compile_pass/bridge_from_tx_external.rs and is exercised by
// `trybuild::TestCases::new().pass(...)`. Trybuild compiles this file as its own crate
// against contextdb-core's public surface, proving the bridges are externally reachable
// and const-callable. If either method stops being `pub` or `const fn`, this fixture fails.

use contextdb_core::{SnapshotId, TxId};

// Const context forces `const fn`-ness of both bridges.
const SNAP: SnapshotId = SnapshotId::from_tx(TxId(42));
const TX: TxId = TxId::from_snapshot(SnapshotId(100));

fn main() {
    assert_eq!(SNAP.0, 42);
    assert_eq!(TX.0, 100);

    // Also confirm runtime callability from an external crate.
    let s = SnapshotId::from_tx(TxId(7));
    assert_eq!(s.0, 7);
    let t = TxId::from_snapshot(SnapshotId(9));
    assert_eq!(t.0, 9);
}
