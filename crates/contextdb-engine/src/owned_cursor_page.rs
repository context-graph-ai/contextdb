//! Private immutable page payloads. Public pages are terminal materializations.
use contextdb_core::read_contract::{CursorPage, ReadContractViolation};
use contextdb_core::read_memory::{ReadCredit, ReadMemoryBudget};
use contextdb_core::{Result, Value};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;
use std::sync::Arc;

struct RowsInner {
    rows: Vec<Vec<Value>>,
    // JSON trees and native buffers precede the credit that funds them.
    credit: Option<ReadCredit>,
}

/// One immutable materialization, including every JSON tree in its rows.
/// Slices and copies share that allocation and its original admission.
#[derive(Default)]
pub struct SharedRows {
    root: Option<Arc<RowsInner>>,
    start: usize,
    end: usize,
}
impl SharedRows {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        if rows.capacity() == 0 {
            return Self::default();
        }
        let end = rows.len();
        Self {
            root: Some(Arc::new(RowsInner { rows, credit: None })),
            start: 0,
            end,
        }
    }
    pub fn allocation_header_bytes() -> usize {
        std::mem::size_of::<RowsInner>() + 2 * std::mem::size_of::<usize>()
    }
    fn shared_copy(&self) -> Self {
        Self {
            root: self.root.clone(),
            start: self.start,
            end: self.end,
        }
    }
    fn working_rows(&mut self) -> &mut Vec<Vec<Value>> {
        let root = self
            .root
            .as_mut()
            .expect("working page has admitted row storage");
        let inner = Arc::get_mut(root).expect("published immutable rows cannot be mutated");
        assert!(
            inner.credit.is_none(),
            "published immutable rows cannot be mutated"
        );
        assert_eq!(self.start, 0);
        assert_eq!(self.end, inner.rows.len());
        &mut inner.rows
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
        self.root.as_ref().map_or(0, |root| root.rows.capacity())
    }
    fn bind_credit(&mut self, credit: ReadCredit) -> Option<ReadCredit> {
        if let Some(root) = self.root.as_mut() {
            let inner = Arc::get_mut(root).expect("first publication owns its row materialization");
            assert!(inner.credit.is_none());
            inner.credit = Some(credit);
            None
        } else {
            Some(credit)
        }
    }
    /// The sole deep copy is the public terminal-result conversion (R78).
    fn materialize_terminal(&self) -> Vec<Vec<Value>> {
        self.deref().to_vec()
    }
}
impl Drop for SharedRows {
    fn drop(&mut self) {
        if let Some(root) = self.root.take()
            && let Some(inner) = Arc::into_inner(root) {
                // Every holder uses into_inner, so exactly one concurrent drop
                // obtains the payload after releasing the Arc control block.
                let RowsInner { rows, credit } = inner;
                drop(rows);
                drop(credit);
            }
    }
}
impl Deref for SharedRows {
    type Target = [Vec<Value>];
    fn deref(&self) -> &Self::Target {
        self.root
            .as_ref()
            .map_or(&[], |root| &root.rows[self.start..self.end])
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
        self.deref().serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for SharedRows {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let rows = Vec::<Vec<Value>>::deserialize(deserializer)?;
        if rows.capacity() != 0 {
            let owner = crate::read_image_memory::active_owner()
                .map_err(|_| serde::de::Error::custom("cursor rows require a decode owner"))?;
            owner
                .try_reserve(
                    Self::allocation_header_bytes(),
                    "read_channel",
                    "decode_cursor_root",
                    "Reduce the page or raise the reader memory limit.",
                )
                .map_err(|_| serde::de::Error::custom("cursor root admission refused"))?;
        }
        Ok(Self::new(rows))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OwnedCursorPage {
    pub columns: Vec<String>,
    pub rows: SharedRows,
    pub has_more: bool,
    #[serde(skip)]
    credit: Option<ReadCredit>,
}
impl OwnedCursorPage {
    pub fn from_parts(columns: Vec<String>, rows: Vec<Vec<Value>>, has_more: bool) -> Self {
        Self {
            columns,
            rows: SharedRows::new(rows),
            has_more,
            credit: None,
        }
    }
    pub fn with_credit(mut self, credit: ReadCredit) -> Self {
        assert!(self.credit.is_none());
        self.credit = self.rows.bind_credit(credit);
        self
    }
    pub fn credit_bytes(&self) -> usize {
        self.credit
            .as_ref()
            .map_or(0, |credit| credit.bytes())
            .saturating_add(
                self.rows
                    .root
                    .as_ref()
                    .and_then(|root| root.credit.as_ref())
                    .map_or(0, |credit| credit.bytes()),
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
    fn columns_credit(columns: &[String], owner: Arc<dyn ReadMemoryBudget>) -> Result<ReadCredit> {
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
    pub fn try_clone(&self, owner: Arc<dyn ReadMemoryBudget>) -> Result<Self> {
        let credit = Self::columns_credit(&self.columns, owner)?;
        Ok(Self {
            columns: self.columns.clone(),
            rows: self.rows.shared_copy(),
            has_more: self.has_more,
            credit: Some(credit),
        })
    }
    pub fn try_empty(columns: &[String], owner: Arc<dyn ReadMemoryBudget>) -> Result<Self> {
        let credit = Self::columns_credit(columns, owner)?;
        Ok(Self::from_parts(columns.to_vec(), Vec::new(), false).with_credit(credit))
    }
    pub fn into_public(self) -> CursorPage {
        let Self {
            columns,
            rows,
            has_more,
            credit,
        } = self;
        let values = rows.materialize_terminal();
        drop(rows);
        drop(credit);
        CursorPage::from_parts(columns, values, has_more)
    }
}

impl PartialEq for OwnedCursorPage {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns && self.rows == other.rows && self.has_more == other.has_more
    }
}

impl PartialEq<CursorPage> for OwnedCursorPage {
    fn eq(&self, other: &CursorPage) -> bool {
        self.columns == other.columns
            && self.rows.deref() == other.rows.as_slice()
            && self.has_more == other.has_more
    }
}
