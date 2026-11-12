//! Who is asking, and the one filter that says which rows of a table that reader may see and
//! write.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::expression::reads;
use super::table::Source;
use crate::memory::Reservation;
use crate::schema::{Catalog, Table};
use crate::sql::Expr;
use crate::sql::ast::{BinOp, ColumnRef, Literal};
use crate::store::Read;
use crate::types::{ContextId, Error, Principal, Result, ScopeLabel, Uuid, Value};

/// Who is asking. Each rule a reader declares narrows what it is shown; `None` declares
/// nothing for that rule.
pub(crate) struct Reader {
    pub contexts: Option<BTreeSet<ContextId>>,
    pub scope_labels: Option<BTreeSet<ScopeLabel>>,
    pub principal: Option<Principal>,
}

impl Reader {
    /// The reader that declares nothing and sees everything.
    pub(crate) fn admin() -> Reader {
        Reader {
            contexts: None,
            scope_labels: None,
            principal: None,
        }
    }

    /// True for a reader that declares nothing.
    pub(crate) fn is_admin(&self) -> bool {
        self.contexts.is_none() && self.scope_labels.is_none() && self.principal.is_none()
    }
}

/// How many access ids a filter remembers the grants of.
const REMEMBERED: usize = 1024;

/// The bytes a filter is counted for what it remembers, asked for whole before its first row.
const ROOM: u64 = 64 * 1024;

/// The filter of one table for one reader, made for one statement.
pub(crate) struct Visible<'a> {
    table: &'a Table,
    reader: &'a Reader,
    /// The table's access column and the grants its rows are asked for; `None` when no row is
    /// asked for a grant. The grants are `None` when the catalog has no table of grants with
    /// the columns a grant is read from: then nobody holds one.
    acl: Option<(usize, Option<Grants<'a>>)>,
}

/// The grants of one principal: where they are found, and the ids already looked up.
struct Grants<'a> {
    /// The table of grants.
    table: &'a Table,
    /// What a row of it passes when it grants the access id bound to `$id` to the principal,
    /// the values bound in it, and the columns it reads.
    filter: Expr,
    params: HashMap<String, Value>,
    filtered: Vec<bool>,
    /// What a grant kept beside its row is loaded under.
    reservation: &'a mut Reservation,
    /// Whether the principal holds a grant, for at most the `REMEMBERED` ids looked up last.
    known: BTreeMap<Uuid, bool>,
}

impl<'a> Visible<'a> {
    /// The filter of `table` for `reader`. Refuses, before any row, a reader the table's rules
    /// cannot answer.
    pub(crate) fn new(
        table: &'a Table,
        catalog: &'a Catalog,
        reader: &'a Reader,
        reservation: &'a mut Reservation,
    ) -> Result<Visible<'a>> {
        let mut acl = None;
        if let Some((column, from, access)) = &table.rules.acl
            && !reader.is_admin()
        {
            let (kind, name) = match &reader.principal {
                Some(Principal::Agent(name)) => ("Agent", name.as_str()),
                Some(Principal::Human(name)) => ("Human", name.as_str()),
                Some(Principal::System) | None => {
                    return Err(Error::PrincipalRequired {
                        table: table.name.clone(),
                    });
                }
            };
            // A grant row holds the access id, and the principal's kind and name as text.
            let column_is = |name: &str, value: Expr| Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef {
                    table: None,
                    column: name.to_owned(),
                })),
                op: BinOp::Eq,
                right: Box::new(value),
            };
            let both = |left: Expr, right: Expr| Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
            let parameter = |name: &str| Expr::Parameter(name.to_owned());
            let of_kind = column_is(
                "principal_kind",
                Expr::Literal(Literal::Text(kind.to_owned())),
            );
            let of_name = column_is("principal_id", parameter("name"));
            let filter = both(column_is(access, parameter("id")), both(of_kind, of_name));
            let params = HashMap::from([
                ("id".to_owned(), Value::Uuid(Uuid::nil())),
                ("name".to_owned(), Value::Text(name.to_owned())),
            ]);
            // A table of grants that is missing, or that no grant can be read from, grants
            // nobody.
            let grants = catalog.table(from).and_then(|table| {
                let mut filtered = vec![false; table.meta.columns.len()];
                reads(&filter, table, &params, &mut filtered).ok()?;
                Some((table, filtered))
            });
            let grants = match grants {
                Some((table, filtered)) => {
                    reservation.grow(ROOM)?;
                    Some(Grants {
                        table,
                        filter,
                        params,
                        filtered,
                        reservation,
                        known: BTreeMap::new(),
                    })
                }
                None => None,
            };
            acl = Some((*column, grants));
        }
        Ok(Visible { table, reader, acl })
    }

    /// Marks the columns of a row that the filter reads.
    pub(crate) fn reads(&self, read: &mut [bool]) {
        let rules = &self.table.rules;
        let context = rules
            .context_column
            .filter(|_| self.reader.contexts.is_some());
        let label = rules.scope_labels.as_ref().map(|(column, _)| *column);
        let label = label.filter(|_| self.reader.scope_labels.is_some());
        let access = self.acl.as_ref().map(|(column, _)| *column);
        for column in [context, label, access].into_iter().flatten() {
            read[column] = true;
        }
    }

    /// `None` for a row the reader may see, and otherwise the refusal of the rule that hides
    /// it.
    pub(crate) fn hidden(&mut self, row: &[Value], read: &dyn Read) -> Result<Option<Error>> {
        self.refusal(row, read, false)
    }

    /// Refuses a new or changed row the reader may not write.
    pub(crate) fn may_write(&mut self, row: &[Value], read: &dyn Read) -> Result<()> {
        self.refusal(row, read, true)?.map_or(Ok(()), Err)
    }

    /// The refusal of the first rule that keeps the reader from `row`, whose values are in
    /// column order: its Context, then its label, then its grant.
    fn refusal(&mut self, row: &[Value], read: &dyn Read, write: bool) -> Result<Option<Error>> {
        let (table, reader) = (self.table, self.reader);
        if let Some(refusal) = outside(table, reader, write, |column| row.get(column)) {
            return Ok(Some(refusal));
        }
        if let Some((column, grants)) = &mut self.acl {
            let held = match (row.get(*column), grants) {
                (Some(Value::Uuid(id)), Some(grants)) => grants.holds(*id, reader, read)?,
                _ => false,
            };
            if !held {
                let table = table.name.clone();
                return Ok(Some(Error::AclDenied { table }));
            }
        }
        Ok(None)
    }
}

/// The refusal of the Context rule or the label rule of `table`, in that order, when one keeps
/// `reader` from a row whose values `value` gives by column. A row with no Context is in no
/// reader's Contexts. A row is under a label the table lists, for the reading or the writing
/// that is asked, and the reader holds; a table with one list for both hides no row.
fn outside<'v>(
    table: &Table,
    reader: &Reader,
    write: bool,
    value: impl Fn(usize) -> Option<&'v Value>,
) -> Option<Error> {
    let table_name = || table.name.clone();
    if let (Some(contexts), Some(column)) = (&reader.contexts, table.rules.context_column)
        && !matches!(value(column), Some(Value::Uuid(id)) if contexts.contains(&ContextId(*id)))
    {
        return Some(Error::ContextScopeViolation {
            table: table_name(),
        });
    }
    if let (Some(held), Some((column, labels))) = (&reader.scope_labels, &table.rules.scope_labels)
    {
        let listed = match (&labels.write, write) {
            (Some(written), true) => written,
            (Some(_), false) | (None, true) => &labels.read,
            (None, false) => return None,
        };
        if !matches!(value(*column), Some(Value::Text(label))
            if listed.contains(label) && held.iter().any(|held| held.0 == *label))
        {
            return Some(Error::ScopeLabelViolation {
                table: table_name(),
            });
        }
    }
    None
}

impl Grants<'_> {
    /// Whether the principal holds a grant for this access id.
    fn holds(&mut self, id: Uuid, reader: &Reader, read: &dyn Read) -> Result<bool> {
        if let Some(known) = self.known.get(&id) {
            return Ok(*known);
        }
        let found = self.find(id, reader, read)?;
        if self.known.len() == REMEMBERED {
            self.known.clear();
        }
        self.known.insert(id, found);
        Ok(found)
    }

    /// Looks the id up in the table of grants, as any statement reads that table: through the
    /// index its filter can use, and row by row when there is none. A grant counts when it
    /// names the principal and the reader's Contexts and labels let it see the row of the
    /// grant; the access rule of the table of grants itself is not asked.
    fn find(&mut self, id: Uuid, reader: &Reader, read: &dyn Read) -> Result<bool> {
        self.params.insert("id".to_owned(), Value::Uuid(id));
        let seen = Visible {
            table: self.table,
            reader,
            acl: None,
        };
        let source = Source::new(self.table, Some(&self.filter), &self.params, 0, read, seen)
            .reading(self.filtered.clone(), &self.filtered);
        let mut path = source.choose(&[], &[])?;
        // A grant row the reader may not see is no grant, however the lookup names it.
        path.one = false;
        let mut found = false;
        source.walk(&path, self.reservation, |_, _, bytes, reservation| {
            reservation.shrink(bytes);
            found = true;
            Ok(false)
        })?;
        Ok(found)
    }
}
