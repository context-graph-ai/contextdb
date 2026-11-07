//! Test-only counters for bytes admitted by family-specific load producers.
//! Reset only when the measured operation has finished; callers run one
//! measurement at a time. Transfers and releases do not count as new loads.
use std::sync::atomic::{AtomicU64, Ordering};

#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum Family {
    RowProjection,
    IndexPosting,
    RowPosition,
    GraphEdge,
    SyncProvenance,
    VectorMembership,
}

static COUNTS: [AtomicU64; 6] = [const { AtomicU64::new(0) }; 6];

/// Record a successful charge at the producer that loaded these bytes.
#[doc(hidden)]
pub fn loaded(family: Family, bytes: usize) {
    let counter = &COUNTS[family as usize];
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    let _ = counter.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |old| {
        Some(old.saturating_add(bytes))
    });
}

thread_local! {
    static ACTIVE_FAMILY: std::cell::Cell<Option<Family>> = const { std::cell::Cell::new(None) };
}

/// Bind a producer's nested decode admissions to its family for this thread.
#[doc(hidden)]
pub struct FamilyScope(Option<Family>);

#[doc(hidden)]
pub fn enter(family: Family) -> FamilyScope {
    FamilyScope(ACTIVE_FAMILY.with(|slot| slot.replace(Some(family))))
}

impl Drop for FamilyScope {
    fn drop(&mut self) {
        ACTIVE_FAMILY.with(|slot| slot.set(self.0));
    }
}

/// Record an admission only when a producer has bound a family.
#[doc(hidden)]
pub fn loaded_scoped(bytes: usize) {
    if let Some(family) = ACTIVE_FAMILY.with(std::cell::Cell::get) {
        loaded(family, bytes);
    }
}

/// Read accumulated loaded bytes in the order of `Family` variants.
#[doc(hidden)]
pub fn snapshot() -> [u64; 6] {
    std::array::from_fn(|index| COUNTS[index].load(Ordering::SeqCst))
}

/// Read one family's accumulated admitted load bytes.
#[doc(hidden)]
pub fn get(family: Family) -> u64 {
    COUNTS[family as usize].load(Ordering::SeqCst)
}

/// Begin a new isolated measurement after all earlier work has completed.
#[doc(hidden)]
pub fn reset() {
    for counter in &COUNTS {
        counter.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_family_counts_successful_load_bytes_independently() {
        reset();
        for (family, bytes) in [
            (Family::RowProjection, 1),
            (Family::IndexPosting, 2),
            (Family::RowPosition, 3),
            (Family::GraphEdge, 4),
            (Family::SyncProvenance, 5),
            (Family::VectorMembership, 6),
        ] {
            loaded(family, bytes);
        }
        assert_eq!(snapshot(), [1, 2, 3, 4, 5, 6]);
        {
            let _outer = enter(Family::RowProjection);
            loaded_scoped(7);
            {
                let _inner = enter(Family::GraphEdge);
                loaded_scoped(8);
            }
            loaded_scoped(9);
        }
        loaded_scoped(100);
        assert_eq!(snapshot(), [17, 2, 3, 12, 5, 6]);
        let (slot, positions, provenance) = crate::row_side_map_family_bytes_for_test("items");
        assert_eq!(
            slot + positions + provenance,
            crate::retained_row_side_map_bytes("items")
        );
        reset();
        assert_eq!(snapshot(), [0; 6]);
    }
}
