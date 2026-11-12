//! ALTER TABLE: one action, read into what it changes.

use super::ddl::{column_def, consolidation, table_option};
use super::{Pair, Rule, child, first_ident, ident, search_mode, signed, two_idents};
use crate::sql::ast::*;
use crate::types::Result;

pub(super) fn alter_table(pair: Pair<'_>) -> Result<AlterTable> {
    let mut parts = pair.into_inner();
    let table = ident(&parts.next().expect("a table"));
    let part = parts.next().expect("an action");
    let action = match part.as_rule() {
        Rule::add_column => {
            let column = child(part, Rule::column_def).expect("a column");
            AlterAction::AddColumn(Box::new(column_def(column)?))
        }
        Rule::drop_column => AlterAction::DropColumn {
            column: first_ident(part.clone()),
            cascade: child(part, Rule::drop_behaviour)
                .map(|written| written.as_str().eq_ignore_ascii_case("CASCADE")),
        },
        Rule::rename_column => {
            let [from, to] = two_idents(part);
            AlterAction::RenameColumn { from, to }
        }
        Rule::drop_retain => AlterAction::DropRetain,
        Rule::alter_column => {
            let column = first_ident(part.clone());
            let set = part.into_inner().nth(2).expect("a clause");
            match set.as_rule() {
                Rule::signed => AlterAction::SetMaxPartitions {
                    column,
                    max_partitions: set.as_str().to_owned(),
                },
                Rule::auto_index_at => AlterAction::SetAutoIndexAt(column, signed(set)),
                Rule::auto_index_default => AlterAction::SetAutoIndexAt(column, None),
                Rule::consolidation => {
                    AlterAction::SetConsolidation(column, Some(consolidation(set)))
                }
                Rule::consolidation_default => AlterAction::SetConsolidation(column, None),
                // The one clause left: SEARCH_MODE.
                _ => AlterAction::SetSearchMode {
                    column,
                    search_mode: search_mode(&set),
                },
            }
        }
        // DEFAULT takes the declared lists away, which only an ALTER may ask.
        Rule::maintain_count
            if child(part.clone(), Rule::count_word)
                .is_some_and(|word| word.as_str().eq_ignore_ascii_case("DEFAULT")) =>
        {
            AlterAction::SetKeptCounts(None)
        }
        // SET with one option of a table, read as CREATE TABLE reads it.
        _ => {
            let mut options = TableOptions::default();
            table_option(part, &mut options)?;
            if let Some(retain) = options.retain {
                AlterAction::SetRetain(retain)
            } else if let Some(history) = options.history {
                AlterAction::SetHistory(history)
            } else if let Some(sync) = options.sync {
                AlterAction::SetSync(sync)
            } else if let Some(conflict) = options.conflict {
                AlterAction::SetConflict(conflict)
            } else {
                AlterAction::SetKeptCounts(options.kept_counts)
            }
        }
    };
    Ok(AlterTable { table, action })
}
