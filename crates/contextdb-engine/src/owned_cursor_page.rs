//! Private page builders and shared, originally funded immutable row storage.
use crate::executor::bounded::CursorPageInvoice;
use contextdb_core::read_contract::{CursorPage, ReadContractViolation};
use contextdb_core::read_memory::{
    NativeProduce, ReadCredit, ReadMemoryBudget, ReadMemoryOwner, ReadShared,
};
use contextdb_core::{Result, Value};
type PreparedPage =
    contextdb_core::read_contract::PreparedCursorPage<OwnedCursorPage, CursorPageInvoice>;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;
use std::sync::Arc;

// Only the public ReadSession producer opts in. OwnerReadService's private
// pages, retained templates and wire encoders never need a public shared cell.
thread_local! {
    static PUBLIC_PAGE_REQUESTED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(crate) fn with_public_page<T>(produce: impl FnOnce() -> T) -> T {
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            PUBLIC_PAGE_REQUESTED.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(PUBLIC_PAGE_REQUESTED.with(|slot| slot.replace(true)));
    produce()
}
pub(crate) fn take_public_page_request() -> bool {
    PUBLIC_PAGE_REQUESTED.with(|slot| slot.replace(false))
}

#[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
fn column_capacity_bytes(columns: &Vec<String>) -> Result<usize> {
    columns.iter().try_fold(
        columns
            .capacity()
            .checked_mul(std::mem::size_of::<String>())
            .ok_or_else(|| {
                contextdb_core::Error::Other("cursor column capacity overflow".into())
            })?,
        |bytes, column| {
            bytes.checked_add(column.capacity()).ok_or_else(|| {
                contextdb_core::Error::Other("cursor column capacity overflow".into())
            })
        },
    )
}

/// One immutable materialization, including every JSON tree in its rows.
/// Slices and copies share that allocation and its original admission.
#[derive(Default)]
pub struct SharedRows {
    working: Vec<Vec<Value>>,
    root: Option<ReadShared<Vec<Vec<Value>>>>,
    start: usize,
    end: usize,
}
impl SharedRows {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        let end = rows.len();
        Self {
            working: rows,
            root: None,
            start: 0,
            end,
        }
    }
    pub fn allocation_header_bytes() -> usize {
        ReadShared::<Vec<Vec<Value>>>::layout().size()
    }
    fn shared_copy(&self) -> Self {
        assert!(
            self.working.capacity() == 0,
            "copies require published rows"
        );
        Self {
            working: Vec::new(),
            root: self.root.clone(),
            start: self.start,
            end: self.end,
        }
    }
    fn working_rows(&mut self) -> &mut Vec<Vec<Value>> {
        assert!(
            self.root.is_none(),
            "published immutable rows cannot be mutated"
        );
        assert_eq!(self.start, 0);
        assert_eq!(self.end, self.working.len());
        &mut self.working
    }
    pub fn push(&mut self, row: Vec<Value>) {
        self.working_rows().push(row);
        self.end += 1;
    }
    pub fn pop(&mut self) -> Option<Vec<Value>> {
        if self.end == 0 {
            return None;
        }
        let row = self.working_rows().pop();
        if row.is_some() {
            self.end -= 1;
        }
        row
    }
    pub fn truncate(&mut self, len: usize) {
        self.end = self.start + len.min(self.len());
    }
    pub fn discard_prefix(&mut self, len: usize) {
        self.start += len.min(self.len());
    }
    pub fn clear(&mut self) {
        self.end = self.start;
    }
    pub fn capacity(&self) -> usize {
        self.root
            .as_ref()
            .map_or(self.working.capacity(), |root| root.capacity())
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn bind_credit(
        &mut self,
        columns: Vec<String>,
        credit: ReadCredit,
    ) -> Result<(Vec<String>, Option<ReadCredit>)> {
        assert!(self.root.is_none(), "first publication owns its rows");
        if self.working.capacity() == 0 {
            return Ok((columns, Some(credit)));
        }
        // The columns have their own original invoice. The row cell's one
        // external invoice covers its exact layout AND all nested row buffers.
        // Refusal drops the closure's payload before refunding either invoice.
        struct Pending {
            columns: Vec<String>,
            rows: Vec<Vec<Value>>,
            credit: ReadCredit,
        }
        let mut pending = Pending {
            columns,
            rows: std::mem::take(&mut self.working),
            credit,
        };
        let columns_bytes = column_capacity_bytes(&pending.columns)?;
        let rows_bytes = pending
            .credit
            .bytes()
            .checked_sub(columns_bytes)
            .ok_or_else(|| {
                contextdb_core::Error::Other("cursor columns exceed original credit".into())
            })?;
        let rows_credit = pending.credit.try_split_off(rows_bytes)?;
        let mut columns_out = None;
        let root = ReadShared::try_from_prepaid_payload_with_native(rows_credit, || {
            let Pending {
                columns,
                rows,
                credit,
            } = pending;
            columns_out = Some((columns, Some(credit)));
            Ok(rows)
        })
        .map_err(|error: NativeProduce<contextdb_core::Error>| match error {
            NativeProduce::Refused(refusal) => contextdb_core::Error::NativeEngineOther(refusal),
            NativeProduce::Producer(error) => error,
        })?;
        self.root = Some(root);
        Ok(columns_out.expect("initialized row root retains columns and their invoice"))
    }
}
impl Deref for SharedRows {
    type Target = [Vec<Value>];
    fn deref(&self) -> &Self::Target {
        self.root.as_ref().map_or_else(
            || &self.working[self.start..self.end],
            |root| &root[self.start..self.end],
        )
    }
}
impl std::fmt::Debug for SharedRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}
impl PartialEq for SharedRows {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}
impl Serialize for SharedRows {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        contextdb_core::read_contract::CursorRowsView(self.deref()).serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SharedRows {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Vec::<Vec<Value>>::deserialize(deserializer).map(Self::new)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OwnedCursorPage {
    pub columns: Vec<String>,
    pub rows: SharedRows,
    pub has_more: bool,
    #[serde(skip)]
    credit: Option<ReadCredit>,
    #[serde(skip)]
    prepared: Option<PreparedPage>,
}
impl OwnedCursorPage {
    pub fn from_parts(columns: Vec<String>, rows: Vec<Vec<Value>>, has_more: bool) -> Self {
        Self {
            columns,
            rows: SharedRows::new(rows),
            has_more,
            credit: None,
            prepared: None,
        }
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub fn with_credit(mut self, credit: ReadCredit) -> Result<Self> {
        assert!(self.credit.is_none());
        self.credit = Some(credit);
        let columns = std::mem::take(&mut self.columns);
        let credit = self
            .credit
            .take()
            .expect("unpublished page owns its credit");
        let (columns, credit) = self.rows.bind_credit(columns, credit)?;
        self.columns = columns;
        self.credit = credit;
        Ok(self)
    }
    /// A public route admits this exact cell before its producer advances.
    pub(crate) fn attach_publication(&mut self, prepared: PreparedPage) {
        assert!(self.prepared.is_none());
        self.prepared = Some(prepared);
    }
    pub(crate) fn public_carrier_bytes() -> usize {
        ReadShared::<Self, CursorPageInvoice>::layout().size()
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn try_prepare_public(owner: ReadMemoryOwner) -> Result<PreparedPage> {
        let credit = ReadCredit::try_new_owned(
            owner,
            Self::public_carrier_bytes(),
            "reader",
            "publish_cursor_page",
            "Release retained pages or raise the declared reader memory limit.",
        )?;
        Self::prepare_publication(CursorPageInvoice::new(credit))
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub(crate) fn prepare_publication(invoice: CursorPageInvoice) -> Result<PreparedPage> {
        CursorPage::try_prepare_invoice(invoice)
    }
    pub fn credit_bytes(&self) -> usize {
        self.credit
            .as_ref()
            .map_or(0, ReadCredit::bytes)
            .saturating_add(
                self.rows
                    .root
                    .as_ref()
                    .map_or(0, |root| root.credit().bytes()),
            )
            .saturating_add(
                self.prepared
                    .as_ref()
                    .map_or(0, |prepared| prepared.invoice().bytes()),
            )
    }
    pub fn validate(&self) -> std::result::Result<(), ReadContractViolation> {
        for (row, values) in self.rows.iter().enumerate() {
            if values.len() != self.columns.len() {
                return Err(ReadContractViolation::CursorRowArityMismatch {
                    row,
                    expected: self.columns.len(),
                    actual: values.len(),
                });
            }
        }
        Ok(())
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    fn columns_credit(columns: &[String], owner: impl Into<ReadMemoryOwner>) -> Result<ReadCredit> {
        let bytes = columns
            .iter()
            .fold(
                columns.len().checked_mul(std::mem::size_of::<String>()),
                |bytes, column| bytes.and_then(|n| n.checked_add(column.len())),
            )
            .ok_or_else(|| contextdb_core::Error::Other("cursor column copy overflow".into()))?;
        ReadCredit::try_new(
            owner,
            bytes,
            "read_channel",
            "copy_cursor_page",
            "Reduce the columns or raise the reader memory limit.",
        )
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub fn try_clone(&self, owner: impl Into<ReadMemoryOwner>) -> Result<Self> {
        let credit = Self::columns_credit(&self.columns, owner)?;
        let copied = Self {
            columns: self.columns.clone(),
            rows: self.rows.shared_copy(),
            has_more: self.has_more,
            credit: Some(credit),
            prepared: None,
        };
        Ok(copied)
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub fn try_empty(columns: &[String], owner: impl Into<ReadMemoryOwner>) -> Result<Self> {
        let credit = Self::columns_credit(columns, owner)?;
        Self::from_parts(columns.to_vec(), Vec::new(), false).with_credit(credit)
    }
    #[expect(clippy::result_large_err, reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating.")]
    pub fn into_public(mut self) -> Result<CursorPage> {
        if self.columns.capacity() == 0 && self.rows.capacity() == 0 && !self.has_more {
            return Ok(CursorPage::empty());
        }
        // Repeated exhausted fetches only copy the retained column template;
        // they advance no row/replay/continuation state. Their actual public
        // handoff may therefore refuse on this original payer and be retried.
        if self.prepared.is_none() && self.rows.is_empty() && !self.has_more {
            let owner = self.credit.as_ref().map(ReadCredit::owner).ok_or_else(|| {
                contextdb_core::Error::Other("terminal page lacks its original reader".into())
            })?;
            self.prepared = Some(Self::try_prepare_public(owner)?);
        }
        let prepared = self.prepared.take().ok_or_else(|| {
            contextdb_core::Error::Other("cursor page was not prepared before publication".into())
        })?;
        Ok(prepared.publish(self))
    }
}
impl contextdb_core::read_contract::CursorPageStorage for OwnedCursorPage {
    fn columns(&self) -> &[String] {
        &self.columns
    }
    fn rows(&self) -> &[Vec<Value>] {
        &self.rows
    }
    fn has_more(&self) -> bool {
        self.has_more
    }
}

impl PartialEq for OwnedCursorPage {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns && self.rows == other.rows && self.has_more == other.has_more
    }
}

impl PartialEq<CursorPage> for OwnedCursorPage {
    fn eq(&self, other: &CursorPage) -> bool {
        self.columns.as_slice() == other.columns()
            && self.rows.deref() == other.rows()
            && self.has_more == other.has_more()
    }
}
