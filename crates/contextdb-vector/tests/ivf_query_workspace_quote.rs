#![cfg(feature = "test-seams")]
//! The producer quote is the constructor's actual scratch debit, with unchanged cancellation order.
#[path = "support/ivf_pages.rs"]
mod ivf_pages;
use contextdb_core::{Error, Result};
use contextdb_vector::{
    MemoryBudget,
    ivf::{
        ClusteredIndex, Hit, IvfConstructorAllocationForTest, IvfConstructorObserverForTest, Mode,
        Policy, TrainingPolicy, codebook_allocation_layout, query_workspace_allocation_bytes,
        search, with_ivf_constructor_observer_for_test,
    },
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct Limit {
    cap: usize,
    live: AtomicUsize,
    peak: AtomicUsize,
}
impl std::fmt::Debug for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Limit").field("cap", &self.cap).finish()
    }
}
impl MemoryBudget for Limit {
    fn try_allocate_for(&self, n: usize, _: &str, _: &str, _: &str) -> Result<()> {
        let used = self.live.load(Ordering::SeqCst);
        if n > self.cap.saturating_sub(used) {
            return Err(Error::MemoryBudgetExceeded {
                subsystem: "fixture".into(),
                operation: "query".into(),
                requested_bytes: n,
                available_bytes: self.cap.saturating_sub(used),
                budget_limit_bytes: self.cap,
                hint: "fixture cap".into(),
            });
        }
        self.peak.fetch_max(
            self.live.fetch_add(n, Ordering::SeqCst) + n,
            Ordering::SeqCst,
        );
        Ok(())
    }
    fn release(&self, n: usize) {
        assert!(self.live.fetch_sub(n, Ordering::SeqCst) >= n);
    }
}
#[derive(Default)]
struct Seen(Mutex<Vec<usize>>);
impl IvfConstructorObserverForTest for Seen {
    fn admitted(&self, _: IvfConstructorAllocationForTest) {}
    fn debited(&self, _: u64, _: usize, n: usize, operation: &'static str) {
        if operation == "clustered_query" {
            self.0.lock().unwrap().push(n);
        }
    }
}
#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn the_query_quote_matches_real_debits_and_one_byte_short_refuses_before_construction() {
    let build = Arc::new(ivf_pages::Budget::default());
    let policy = Policy::checked(2, 4, 2, 16, 1024, true).unwrap();
    let training = TrainingPolicy {
        iterations: 1,
        reservoir_samples: 8,
        seed: 7,
        ingest_batch_keys: 8,
        phase_batch_entries: 8,
    };
    let mut index = ClusteredIndex::new(
        ivf_pages::Pages::default(),
        policy,
        training,
        build.clone().into(),
    )
    .unwrap();
    index
        .build_partition(0, (0..8).map(|row| (row, [1.0, row as f32 / 8.0])))
        .unwrap();
    let pages = index.into_storage();
    assert_eq!(build.live.load(Ordering::SeqCst), 0);
    for mode in [Mode::Exact, Mode::Indexed] {
        for k in [0, 1, 3, 8] {
            let quote = query_workspace_allocation_bytes(policy, k).unwrap();
            let book = if k != 0 && mode == Mode::Indexed {
                codebook_allocation_layout(2, 4, false)
                    .unwrap()
                    .charged_bytes
            } else {
                0
            };
            let owner = Arc::new(Limit {
                cap: quote + book,
                live: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let seen = Arc::new(Seen::default());
            let mut out = vec![Hit::default(); k];
            let result = with_ivf_constructor_observer_for_test(seen.clone(), || {
                search(
                    &pages,
                    policy,
                    &owner.clone().into(),
                    &[1.0, 0.0],
                    Some(&[0]),
                    mode,
                    &mut out,
                )
            });
            let (count, work) = result.unwrap();
            assert_eq!(count, k);
            assert_eq!(
                work.reranked_candidates,
                if mode == Mode::Indexed {
                    work.full_precision_evaluations
                } else {
                    0
                }
            );
            assert_eq!(
                work.distance_evaluations(),
                work.centroid_evaluations
                    + work.quantized_evaluations
                    + work.full_precision_evaluations
                    + work.buffer_evaluations
            );
            assert_eq!(
                *seen.0.lock().unwrap(),
                if k == 0 { vec![] } else { vec![quote] }
            );
            assert_eq!(owner.live.load(Ordering::SeqCst), 0);
            assert_eq!(owner.peak.load(Ordering::SeqCst), quote + book);
            if k != 0 {
                let short = Arc::new(Limit {
                    cap: quote - 1,
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                });
                assert!(
                    matches!(search(&pages,policy,&short.clone().into(),&[1.0,0.0],Some(&[0]),mode,&mut out),Err(Error::MemoryBudgetExceeded {requested_bytes,available_bytes,..}) if requested_bytes==quote&&available_bytes==quote-1)
                );
                assert_eq!(short.live.load(Ordering::SeqCst), 0);
                pages.cancelled.set(true);
                assert!(matches!(
                    search(
                        &pages,
                        policy,
                        &short.clone().into(),
                        &[1.0, 0.0],
                        Some(&[0]),
                        mode,
                        &mut out
                    ),
                    Err(Error::ReadCancelled)
                ));
                pages.cancelled.set(false);
            } else {
                assert_eq!(quote, 0);
            }
        }
    }
    // Row: a list scan whose records span pages stays inside the same quote.
    {
        const DIMENSIONS: usize = 4096;
        let build = Arc::new(ivf_pages::Budget::default());
        let policy = Policy::checked(DIMENSIONS, 4, 4, 16, 1 << 20, true).unwrap();
        let training = TrainingPolicy {
            iterations: 1,
            reservoir_samples: 8,
            seed: 7,
            ingest_batch_keys: 8,
            phase_batch_entries: 8,
        };
        let vector = |row: usize| {
            let mut values = vec![0.25_f32; DIMENSIONS];
            values[row] = 1.0;
            values
        };
        let mut index = ClusteredIndex::new(
            ivf_pages::Pages::default(),
            policy,
            training,
            build.clone().into(),
        )
        .unwrap();
        index
            .build_partition(0, (0..8).map(|row| (row as u64, vector(row))))
            .unwrap();
        let pages = index.into_storage();
        let query = vector(0);
        for mode in [Mode::Exact, Mode::Indexed] {
            let quote = query_workspace_allocation_bytes(policy, 3).unwrap();
            let book = if mode == Mode::Indexed {
                codebook_allocation_layout(DIMENSIONS, 4, false)
                    .unwrap()
                    .charged_bytes
            } else {
                0
            };
            let owner = Arc::new(Limit {
                cap: quote + book,
                live: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            let seen = Arc::new(Seen::default());
            let mut out = vec![Hit::default(); 3];
            let (count, work) = with_ivf_constructor_observer_for_test(seen.clone(), || {
                search(
                    &pages,
                    policy,
                    &owner.clone().into(),
                    &query,
                    Some(&[0]),
                    mode,
                    &mut out,
                )
            })
            .unwrap();
            assert_eq!(count, 3);
            assert_eq!(out[0].row, 0);
            if mode == Mode::Indexed {
                assert!(
                    work.list_pages_read >= 2,
                    "the search really read a record across its pages"
                );
            }
            assert_eq!(*seen.0.lock().unwrap(), vec![quote]);
            assert_eq!(owner.live.load(Ordering::SeqCst), 0);
            assert_eq!(owner.peak.load(Ordering::SeqCst), quote + book);
            let short = Arc::new(Limit {
                cap: quote - 1,
                live: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
            });
            assert!(matches!(
                search(
                    &pages,
                    policy,
                    &short.clone().into(),
                    &query,
                    Some(&[0]),
                    mode,
                    &mut out
                ),
                Err(Error::MemoryBudgetExceeded {
                    requested_bytes,
                    available_bytes,
                    ..
                }) if requested_bytes == quote && available_bytes == quote - 1
            ));
            assert_eq!(short.live.load(Ordering::SeqCst), 0);
            pages.cancelled.set(true);
            assert!(matches!(
                search(
                    &pages,
                    policy,
                    &owner.clone().into(),
                    &query,
                    Some(&[0]),
                    mode,
                    &mut out
                ),
                Err(Error::ReadCancelled)
            ));
            pages.cancelled.set(false);
        }
    }
}
