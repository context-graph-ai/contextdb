//! Observe the admitted limits of the file reader behind a continued cursor.

use contextdb_core::read_contract::ReadLimits;
use contextdb_engine::ReadCursor;

/// The memory ceiling and limits the file read behind `cursor` uses, once the
/// cursor has continued into the file after its owner exited.
pub fn continued_cursor_file_limits(cursor: &ReadCursor) -> (Option<usize>, ReadLimits) {
    cursor
        .continued_file_limits_for_test()
        .expect("the cursor has opened its continued file reader")
}
