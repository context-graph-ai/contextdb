//! Test support for observing a cursor continued into the file after its
//! owner exits. Bound to the engine once the engine offers the observation it
//! names; until then it reports what is missing.

use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::ReadCursor;

/// The memory ceiling and limits the file read behind `cursor` uses, once the
/// cursor has continued into the file after its owner exited.
pub fn continued_cursor_file_limits(cursor: &ReadCursor) -> (Option<usize>, ReadLimits) {
    let _ = cursor;
    unimplemented!("the limits of a cursor continued into the file cannot be observed yet")
}
