//! Comparing equally ranked index candidates builds a grouping of their bound
//! column sets and a size per candidate. What that grouping really allocates,
//! measured by the allocator oracle, never exceeds what it was admitted for,
//! however few or many candidates tie.

use super::*;
use crate::persistence::content_identity::proof_allocation_oracle_tests::measured;
use serial_test::serial;

#[test]
#[serial]
fn a_tie_comparison_allocates_no_more_than_it_admits() {
    for tied_count in [2usize, 3, 8, 64] {
        let names = (0..tied_count)
            .map(|n| format!("index_{n:04}"))
            .collect::<Vec<_>>();
        // Distinct bound sets, one per candidate, at positions 1, 3, 5, ...
        // among twice as many candidates.
        let tied = names
            .iter()
            .enumerate()
            .map(|(n, name)| ((n as u128 + 1, 2, 0), 2 * n + 1, name.as_str()))
            .collect::<Vec<TiedCandidate<'_>>>();
        let candidates = 2 * tied_count;
        let name_bytes = names.iter().map(String::len).sum::<usize>();
        let set_sizes = vec![7usize; tied_count];
        let admitted = TieComparison::bytes(tied_count, name_bytes, candidates);
        let (sizes, peak, _) = measured(|| {
            let comparison = TieComparison::group(&tied);
            assert_eq!(comparison.len(), tied_count);
            comparison.sizes(&tied, &set_sizes, candidates)
        });
        assert_eq!(
            sizes.of(1, &names[0]),
            7,
            "a tied candidate keeps its set's size"
        );
        assert!(
            peak <= admitted,
            "comparing {tied_count} tied candidates allocated {peak} bytes but was admitted \
             for {admitted}"
        );
    }
}
