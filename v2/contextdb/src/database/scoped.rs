//! Handles narrowed to the rows one reader may see and write.

use std::collections::BTreeSet;
use std::sync::Mutex;

use super::{Database, locked};
use crate::query::Reader;
use crate::types::{ContextId, Principal, ScopeLabel};

impl Database {
    /// A handle over the same open store that is shown, and may write, only the rows these
    /// Contexts, scope labels and principal may. `None` declares nothing for that rule.
    ///
    /// The new handle never has more than this one: of Contexts and of scope labels it keeps
    /// those that both name, and a principal this handle has is kept whatever is asked. It
    /// holds the store open like the handle it was made from, so the store is let go when each
    /// of them is closed or dropped. Made from a closed handle, it is closed.
    pub fn scoped_with_constraints(
        &self,
        contexts: Option<BTreeSet<ContextId>>,
        scope_labels: Option<BTreeSet<ScopeLabel>>,
        principal: Option<Principal>,
    ) -> Database {
        let held = &self.reader;
        Database {
            open: Mutex::new(locked(&self.open).clone()),
            reader: Reader {
                contexts: both(&held.contexts, contexts),
                scope_labels: both(&held.scope_labels, scope_labels),
                principal: held.principal.clone().or(principal),
            },
        }
    }

    /// [`Database::scoped_with_constraints`] with these Contexts and nothing else declared.
    pub fn scoped_with_contexts(&self, contexts: BTreeSet<ContextId>) -> Database {
        self.scoped_with_constraints(Some(contexts), None, None)
    }
}

/// What a handle that holds `held` is narrowed to when `asked` is asked of it.
fn both<T: Ord + Clone>(
    held: &Option<BTreeSet<T>>,
    asked: Option<BTreeSet<T>>,
) -> Option<BTreeSet<T>> {
    match (held, asked) {
        (Some(held), Some(asked)) => Some(held.intersection(&asked).cloned().collect()),
        (held, asked) => asked.or_else(|| held.clone()),
    }
}
