//! Shared producer-owned catalog preparation for calibration and final publication.
use super::*;

pub(super) fn stage_catalog(
    tx: &mut impl WritePages,
    owner: &MemoryBudgetOwner,
    state: &BuildState,
    raw: &mut [u8],
    values: &mut [f32],
    encoded: &mut [u8],
) -> Result<()> {
    let partition = state.manifest.partition;
    // Seal the admitted tail and freeze this transaction's finite old catalog.
    let mut new_catalog = Catalog::new(state.manifest.slots);
    if let Some(mut old) = tx.manifest(partition)? {
        let mut old_catalog = Catalog::load(tx, old)?;
        // Later coalesces cannot delay F1. Their prepared directories remain registered scratch.
        old_catalog.coalesce_left = 0;
        old_catalog.coalesce_right = 0;
        old_catalog.coalesce_output = 0;
        old_catalog.coalesce_list = 0;
        seal_tail(tx, owner, old, &mut old_catalog, raw, values, encoded)?;
        old.sealed_slots = old.slots;
        old_catalog.store(tx, old)?;
        if old_catalog.runs != 0 {
            new_catalog.legacy = Some(old);
            new_catalog.legacy_first = old_catalog.first;
            new_catalog.legacy_runs = old_catalog.runs;
            new_catalog.frontier = state.source_lsn;
            new_catalog.switch_lsn = tx.commit_lsn()?;
            new_catalog.legacy_unserviceable = old_catalog.unserviceable.is_some();
        }
    }
    new_catalog.store(tx, state.manifest)?;
    Ok(())
}
