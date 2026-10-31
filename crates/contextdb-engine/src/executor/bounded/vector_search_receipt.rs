//! Env-enabled observations on the synchronous bounded search worker.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static SEARCH_ORDINAL: AtomicU64 = AtomicU64::new(1);

pub(super) enum SearchReceiptState {
    Unstarted,
    Active(Box<SearchReceipt>),
    Finished,
}

pub(super) struct SearchReceipt {
    ordinal: u64,
    started: Instant,
    exact: u64,
}

impl SearchReceiptState {
    pub(super) fn begin(&mut self) {
        if !matches!(self, Self::Unstarted) {
            return;
        }
        *self = if std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH").is_some() {
            Self::Active(Box::new(SearchReceipt {
                ordinal: SEARCH_ORDINAL.fetch_add(1, Ordering::Relaxed),
                started: Instant::now(),
                exact: 0,
            }))
        } else {
            Self::Finished
        };
    }

    pub(super) fn finish_step(
        &mut self,
        exact: u64,
        complete: bool,
    ) {
        let Self::Active(receipt) = self else {
            return;
        };
        // Each fetch has its own context counters; retain only their deltas.
        receipt.exact = receipt.exact.saturating_add(exact);
        if !complete {
            return;
        }
        let Self::Active(receipt) = std::mem::replace(self, Self::Finished) else {
            unreachable!("active search receipt was inspected above");
        };
        crate::database::writer_ownership_receipt::emit_vector_search_receipt(
            receipt.ordinal,
            // The clustered index never builds or inserts into a graph.
            0,
            0,
            receipt.exact,
            receipt.started.elapsed().as_micros(),
        );
    }
}

pub(super) fn exact_candidates(context: &RequestContext) -> u64 {
    context
        .state()
        .touches
        .get(&BoundedSourceTouch::BruteForceVectorCandidate)
        .copied()
        .unwrap_or(0)
}
