use contextdb_core::*;
use std::collections::HashSet;
pub(super) fn charged_bfs_bytes<T>(capacity: usize) -> Result<usize> {
    if capacity == 0 {
        return Ok(0);
    }
    capacity
        .checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| bytes.checked_add(16))
        .ok_or_else(|| Error::Other("graph storage size overflow".into()))
}
pub(super) fn charged_bfs_admit(
    credit: &mut contextdb_core::read_memory::ReadCredit,
    bytes: usize,
) -> Result<()> {
    credit.try_grow_for(
        bytes,
        "reader",
        "graph_traversal",
        "Raise the declared reader memory limit.",
    )
}
pub(super) fn charged_bfs_vec_space<T>(
    values: &mut Vec<T>,
    credit: &mut contextdb_core::read_memory::ReadCredit,
) -> Result<()> {
    if values.len() < values.capacity() {
        return Ok(());
    }
    let old = charged_bfs_bytes::<T>(values.capacity())?;
    let capacity = values
        .len()
        .checked_add(1)
        .and_then(usize::checked_next_power_of_two)
        .ok_or_else(|| Error::Other("graph result capacity overflow".into()))?;
    let new = charged_bfs_bytes::<T>(capacity)?;
    charged_bfs_admit(credit, new)?;
    let mut replacement = Vec::new();
    if replacement.try_reserve_exact(capacity).is_err() {
        drop(replacement);
        credit.try_shrink(new)?;
        return Err(Error::Other("graph result allocation failed".into()));
    }
    let actual = charged_bfs_bytes::<T>(replacement.capacity())?;
    if actual > new {
        if let Err(error) = charged_bfs_admit(credit, actual - new) {
            drop(replacement);
            credit.try_shrink(new)?;
            return Err(error);
        }
    } else {
        credit.try_shrink(new - actual)?;
    }
    replacement.append(values);
    let previous = std::mem::replace(values, replacement);
    drop(previous);
    credit.try_shrink(old)?;
    Ok(())
}

pub(super) fn charged_bfs_visit(
    visited: &mut HashSet<NodeId>,
    node: NodeId,
    credit: &mut contextdb_core::read_memory::ReadCredit,
) -> Result<bool> {
    if visited.contains(&node) {
        return Ok(false);
    }
    if visited.len() == visited.capacity() {
        let old_buckets = if visited.capacity() == 0 {
            0
        } else {
            (visited.capacity() + 1).next_power_of_two()
        };
        let new_buckets = old_buckets
            .checked_mul(2)
            .map(|n| n.max(4))
            .ok_or_else(|| Error::Other("graph visited capacity overflow".into()))?;
        let allocation = |buckets: usize| {
            if buckets == 0 {
                0
            } else {
                buckets * (std::mem::size_of::<NodeId>() + 1) + 16 + 16
            }
        };
        let new = allocation(new_buckets);
        charged_bfs_admit(credit, new)?;
        if visited.try_reserve(1).is_err() {
            credit.try_shrink(new)?;
            return Err(Error::Other("graph visited allocation failed".into()));
        }
        credit.try_shrink(allocation(old_buckets))?;
    }
    Ok(visited.insert(node))
}
