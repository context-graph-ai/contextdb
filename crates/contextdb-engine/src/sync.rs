use crate::database::Database;
use crate::sync_types::ChangeSet;
use contextdb_core::Lsn;

pub trait ChangeTracking {
    fn changes_since(&self, since_lsn: Lsn) -> ChangeSet;
    fn current_lsn(&self) -> Lsn;
}

impl ChangeTracking for Database {
    fn changes_since(&self, since_lsn: Lsn) -> ChangeSet {
        Database::changes_since(self, since_lsn)
    }

    fn current_lsn(&self) -> Lsn {
        Database::current_lsn(self)
    }
}
