//! Immutable public answers and their private mutable execution payload.
use super::{CascadeReport, QueryTrace};
use crate::read_image_memory::QueryAnswerCharge;
use contextdb_core::read_memory::{NativeProduce, ReadCredit, ReadMemoryOwner, ReadShared};
use contextdb_core::{Error, Result, Value};
use std::sync::LazyLock;

/// An immutable answer. Cloning retains the same admitted allocations.
/// Borrowed components cannot outlive this handle; application copies belong
/// to the application. The engine never exposes an owning payload projection.
#[derive(Debug, Clone)]
pub struct QueryResult {
    inner: QueryResultInner,
}

#[derive(Debug, Clone)]
enum QueryResultInner {
    Empty { rows_affected: u64 },
    Shared(ReadShared<OwnedQueryResult>),
}

#[derive(Debug)]
pub(crate) struct QueryResultData {
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<Value>>,
    pub(crate) rows_affected: u64,
    pub(crate) trace: QueryTrace,
    pub(crate) cascade: Option<CascadeReport>,
}

/// Declaration order destroys all payloads before their original invoices.
#[derive(Debug)]
pub(crate) struct OwnedQueryResult {
    pub(crate) result: QueryResultData,
    pub(crate) answer_charge: QueryAnswerCharge,
    pub(crate) vector_copy_charge: QueryAnswerCharge,
    pub(crate) retained_read_memory: Option<ReadMemoryOwner>,
}

static EMPTY_TRACE: LazyLock<QueryTrace> = LazyLock::new(QueryTrace::scan);

impl QueryResult {
    pub fn empty() -> Self {
        Self::empty_with_affected(0)
    }
    pub fn empty_with_affected(rows_affected: u64) -> Self {
        Self {
            inner: QueryResultInner::Empty { rows_affected },
        }
    }
    /// Reuse the originating operation capability for later canonical output.
    pub(crate) fn encoding_owner(&self) -> Option<ReadMemoryOwner> {
        match &self.inner {
            QueryResultInner::Empty { .. } => None,
            QueryResultInner::Shared(answer) => answer
                .retained_read_memory
                .clone()
                .or_else(|| answer.answer_charge.owner())
                .or_else(|| answer.vector_copy_charge.owner())
                .or_else(|| Some(answer.credit().owner())),
        }
    }
    /// Existing answer owner for fixture serializers; never opens another bank.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn encoding_owner_for_test(&self) -> ReadMemoryOwner {
        self.encoding_owner()
            .expect("fixture supplies a funded answer")
    }
    /// Passive actual carrier/charge receipt for an independent physical-free
    /// control. No owner clone, admission, allocator hook or payload projection.
    #[cfg(all(test, feature = "test-seams"))]
    pub(super) fn original_charge_components_for_test(&self) -> [usize; 5] {
        match &self.inner {
            QueryResultInner::Empty { .. } => [0; 5],
            QueryResultInner::Shared(answer) => [
                &**answer as *const OwnedQueryResult as usize,
                answer.credit().original_payer_identity_for_test(),
                answer.credit().bytes(),
                answer.answer_charge.held(),
                answer.vector_copy_charge.held(),
            ],
        }
    }
    #[cfg(all(test, feature = "test-seams"))]
    pub(super) fn image_control_receipts_for_test(&self) -> [(usize, usize, usize); 2] {
        match &self.inner {
            QueryResultInner::Shared(answer) => {
                answer.vector_copy_charge.control_receipts_for_test()
            }
            QueryResultInner::Empty { .. } => [(0, 0, 0); 2],
        }
    }
    pub fn columns(&self) -> &[String] {
        match &self.inner {
            QueryResultInner::Empty { .. } => &[],
            QueryResultInner::Shared(answer) => &answer.columns,
        }
    }
    pub fn rows(&self) -> &[Vec<Value>] {
        match &self.inner {
            QueryResultInner::Empty { .. } => &[],
            QueryResultInner::Shared(answer) => &answer.rows,
        }
    }
    pub fn rows_affected(&self) -> u64 {
        match &self.inner {
            QueryResultInner::Empty { rows_affected } => *rows_affected,
            QueryResultInner::Shared(answer) => answer.rows_affected,
        }
    }
    pub fn trace(&self) -> &QueryTrace {
        match &self.inner {
            QueryResultInner::Empty { .. } => &EMPTY_TRACE,
            QueryResultInner::Shared(answer) => &answer.trace,
        }
    }
    pub fn cascade(&self) -> Option<&CascadeReport> {
        match &self.inner {
            QueryResultInner::Empty { .. } => None,
            QueryResultInner::Shared(answer) => answer.cascade.as_ref(),
        }
    }

    /// Test-owned parts move once into the same originally funded carrier.
    #[cfg(any(test, feature = "test-seams"))]
    #[doc(hidden)]
    pub fn from_owned_parts_for_test(
        owner: ReadMemoryOwner,
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        rows_affected: u64,
        trace: QueryTrace,
        cascade: Option<CascadeReport>,
    ) -> Result<Self> {
        Self::publish_with_owner(owner, || {
            Ok(OwnedQueryResult::new(QueryResultData {
                columns,
                rows,
                rows_affected,
                trace,
                cascade,
            }))
        })
    }

    /// Allocate the complete publication cell before running the producer.
    /// Durable writes can therefore never fail on carrier admission afterward.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_with(
        owner: impl Into<ReadMemoryOwner>,
        make: impl FnOnce() -> Result<OwnedQueryResult>,
    ) -> Result<Self> {
        Self::publish_with_owner(owner.into(), make)
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn publish_with_owner(
        owner: ReadMemoryOwner,
        make: impl FnOnce() -> Result<OwnedQueryResult>,
    ) -> Result<Self> {
        // Retain original custody outside admission: a refused reservation
        // must destroy the producer's answer before its keeper can disappear.
        let invoice = match ReadCredit::try_new_owned(
            owner.clone(),
            ReadShared::<OwnedQueryResult>::layout().size(),
            "reader",
            "publish_answer",
            "Reduce retained answers or raise the declared reader memory limit.",
        ) {
            Ok(invoice) => invoice,
            Err(error) => {
                drop(make);
                drop(owner);
                return Err(error);
            }
        };
        let answer =
            ReadShared::try_from_prepaid_with_native(invoice, make).map_err(
                |error| match error {
                    NativeProduce::Refused(refusal) => Error::NativeEngineOther(refusal),
                    NativeProduce::Producer(error) => error,
                },
            )?;
        Ok(Self {
            inner: QueryResultInner::Shared(answer),
        })
    }
}

impl OwnedQueryResult {
    pub(crate) fn new(result: QueryResultData) -> Self {
        Self {
            result,
            answer_charge: Default::default(),
            vector_copy_charge: Default::default(),
            retained_read_memory: None,
        }
    }
    pub(crate) fn empty() -> Self {
        Self::new(QueryResultData::empty())
    }
    pub(crate) fn empty_with_affected(rows_affected: u64) -> Self {
        Self::new(QueryResultData::empty_with_affected(rows_affected))
    }

    /// Read-only routes may allocate the carrier after execution, before
    /// publication. Mutating Database doors use publish_with before execution.
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn into_public(self) -> Result<QueryResult> {
        let owner = self
            .retained_read_memory
            .clone()
            .or_else(|| self.answer_charge.owner())
            .or_else(|| self.vector_copy_charge.owner())
            .ok_or_else(|| {
                Error::Other("answer publication requires its original reader".into())
            })?;
        QueryResult::publish_with_owner(owner, || Ok(self))
    }
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    pub(crate) fn into_public_with_owner(self, owner: ReadMemoryOwner) -> Result<QueryResult> {
        QueryResult::publish_with_owner(owner, || Ok(self))
    }
}
impl std::ops::Deref for OwnedQueryResult {
    type Target = QueryResultData;
    fn deref(&self) -> &QueryResultData {
        &self.result
    }
}
impl std::ops::DerefMut for OwnedQueryResult {
    fn deref_mut(&mut self) -> &mut QueryResultData {
        &mut self.result
    }
}
