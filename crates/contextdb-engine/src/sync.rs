use crate::database::Database;
use crate::sync_types::ChangeSet;
use contextdb_core::{Lsn, Result};

pub trait ChangeTracking {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn changes_since(&self, since_lsn: Lsn) -> Result<ChangeSet>;
    fn current_lsn(&self) -> Lsn;
}
impl ChangeTracking for Database {
    fn changes_since(&self, since_lsn: Lsn) -> Result<ChangeSet> {
        Database::changes_since(self, since_lsn)
    }
    fn current_lsn(&self) -> Lsn {
        Database::current_lsn(self)
    }
}
