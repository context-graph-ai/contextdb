//! Env-enabled observations on the synchronous bounded search worker.
use super::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

static SEARCH_ORDINAL: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct Builds {
    builds: AtomicU64,
    inserts: AtomicU64,
}
impl contextdb_vector::hnsw::HnswBuildObserver for Builds {
    fn before_hnsw_build(&self) {
        self.builds.fetch_add(1, Ordering::Relaxed);
    }
}

impl contextdb_vector::hnsw::HnswInsertObserver for Builds {
    fn after_hnsw_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) enum SearchReceiptState {
    Unstarted,
    Active(Box<SearchReceipt>),
    Finished,
}

pub(super) struct SearchReceipt {
    ordinal: u64,
    started: Instant,
    builds: Arc<Builds>,
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
                builds: Arc::new(Builds::default()),
                exact: 0,
            }))
        } else {
            Self::Finished
        };
    }

    pub(super) fn observer(&self) -> Option<Arc<dyn contextdb_vector::hnsw::HnswBuildObserver>> {
        match self {
            Self::Active(receipt) => Some(receipt.builds.clone()),
            _ => None,
        }
    }

    pub(super) fn insertion_observer(
        &self,
    ) -> Option<Arc<dyn contextdb_vector::hnsw::HnswInsertObserver>> {
        match self {
            Self::Active(receipt) => Some(receipt.builds.clone()),
            _ => None,
        }
    }

    pub(super) fn finish_step(
        &mut self,
        exact: u64,
        disclosure: Option<&VectorSearchDisclosure>,
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
        let hnsw = disclosure.map_or(0, |disclosure| {
            disclosure
                .admission_evaluations
                .unwrap_or(0)
                .saturating_add(disclosure.expansion_evaluations.unwrap_or(0)) as u64
        });
        crate::database::writer_ownership_receipt::emit_vector_search_receipt(
            receipt.ordinal,
            receipt.builds.builds.load(Ordering::Relaxed),
            receipt.builds.inserts.load(Ordering::Relaxed),
            receipt.exact.saturating_add(hnsw),
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
