use crate::database::Database;
use crate::sync_types::{ApplyResult, ChangeSet, ConflictPolicies};
use contextdb_core::{Lsn, Result};

pub trait ChangeTracking {
    fn changes_since(&self, since_lsn: Lsn) -> ChangeSet;
    fn current_lsn(&self) -> Lsn;
}

pub trait ChangeApplication {
    fn apply_changes(&self, changes: ChangeSet, policies: &ConflictPolicies)
    -> Result<ApplyResult>;
}

impl ChangeTracking for Database {
    fn changes_since(&self, since_lsn: Lsn) -> ChangeSet {
        Database::changes_since(self, since_lsn)
    }

    fn current_lsn(&self) -> Lsn {
        Database::current_lsn(self)
    }
}

impl ChangeApplication for Database {
    fn apply_changes(
        &self,
        changes: ChangeSet,
        policies: &ConflictPolicies,
    ) -> Result<ApplyResult> {
        Database::apply_changes(self, changes, policies)
    }
}
