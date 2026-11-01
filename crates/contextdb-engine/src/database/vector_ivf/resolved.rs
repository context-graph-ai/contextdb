//! Resolve declarations using the producer's checked allocation geometry.
use contextdb_core::{ColumnDef, ColumnType, Error, Result, VectorIndexRef};
use contextdb_vector::ivf::{self, Policy, TrainingPolicy};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Resolved {
    pub policy: Policy,
    pub training: TrainingPolicy,
    #[cfg(feature = "test-seams")]
    pub population: u64,
    #[cfg(feature = "test-seams")]
    pub expected_vectors: u64,
    #[cfg(feature = "test-seams")]
    pub active_lists: u32,
    pub build_budget_bytes: u64,
    #[cfg(feature = "test-seams")]
    pub codebook_budget_bytes: u64,
    #[cfg(feature = "test-seams")]
    pub workspace_budget_bytes: usize,
    #[cfg(feature = "test-seams")]
    pub codebook: ivf::CodebookAllocationLayouts,
    #[cfg(feature = "test-seams")]
    pub workspace: ivf::WriterWorkspaceLayout,
}
fn memory(requested: usize, available: usize, budget: u64) -> Error {
    Error::MemoryBudgetExceeded {
        subsystem: "vector_index".into(),
        operation: "resolve_clustered_policy".into(),
        requested_bytes: requested,
        available_bytes: available,
        budget_limit_bytes: usize::try_from(budget).unwrap_or(usize::MAX),
        hint: "Reduce the declared vector workspace or raise MEMORY_LIMIT.".into(),
    }
}
fn invalid(index: &VectorIndexRef, member: &'static str) -> Error {
    Error::InvalidVectorPolicyDeclaration {
        index: index.clone(),
        issue: contextdb_core::VectorPolicyDeclarationIssue::InvalidMember { member },
    }
}
/// `workspace_available` excludes the actual native operation, source and
/// output holders. Resolution never tries a constructor or reserves memory.
pub(crate) fn resolve(
    index: &VectorIndexRef,
    column: &ColumnDef,
    population: u64,
    build_budget_bytes: u64,
    workspace_available: usize,
    source_identity: [u8; 32],
) -> Result<Resolved> {
    super::policy::validate(&index.table, column)?;
    let ColumnType::Vector(dimensions) = column.column_type else {
        return Err(invalid(index, "VECTOR"));
    };
    let dimensions = dimensions as usize;
    let p = &column.vector_engine_policy;
    let expected_vectors = column.search_expected_vectors.unwrap_or(population);
    let percent = u64::from(p.centroid_memory_percent.unwrap_or(625));
    let codebook_budget_bytes =
        u64::try_from(u128::from(build_budget_bytes) * u128::from(percent) / 10000)
            .map_err(|_| memory(usize::MAX, workspace_available, build_budget_bytes))?;
    let desired = p.min_lists.unwrap_or(64).max(
        population
            .max(expected_vectors)
            .div_ceil(p.vectors_per_list.unwrap_or(1000)),
    );
    // An omitted LISTS is engine-resolved compatibly with an explicit PROBES:
    // the derived layout has at least as many lists as probes. Explicit
    // members are never clamped.
    let desired = match (column.ivf_lists, column.ivf_probes) {
        (None, Some(probes)) => desired.max(u64::from(probes)),
        _ => desired,
    };
    let active = |lists: u32| {
        u64::from(lists)
            .min(population)
            .min(p.training_samples.unwrap_or(300000)) as u32
    };
    let lists = if let Some(lists) = column.ivf_lists {
        lists
    } else {
        let mut low = 0u32;
        let mut high = u32::try_from(desired).unwrap_or(u32::MAX);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if ivf::codebook_allocation_layout(dimensions, active(mid), true)
                .is_ok_and(|layout| layout.charged_bytes as u64 <= codebook_budget_bytes)
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        if low == 0 {
            return Err(memory(
                ivf::codebook_allocation_layout(dimensions, active(1), true)?.charged_bytes,
                codebook_budget_bytes as usize,
                build_budget_bytes,
            ));
        }
        low
    };
    // An explicit declaration is priced at its full declared capacity. A
    // presently empty partition cannot hide an impossible future codebook.
    if column.ivf_lists.is_some() {
        let requested = ivf::codebook_allocation_layout(dimensions, lists, true)?.charged_bytes;
        if requested > workspace_available {
            return Err(memory(requested, workspace_available, build_budget_bytes));
        }
    }
    let active_lists = active(lists);
    let codebook = ivf::codebook_allocation_layout(dimensions, active_lists, true)?;
    let workspace_budget_bytes = workspace_available
        .checked_sub(codebook.charged_bytes)
        .ok_or_else(|| {
            memory(
                codebook.charged_bytes,
                workspace_available,
                build_budget_bytes,
            )
        })?;
    let stride = dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(40))
        .ok_or_else(|| memory(usize::MAX, workspace_available, build_budget_bytes))?;
    let to_usize = |v: u64, member| usize::try_from(v).map_err(|_| invalid(index, member));
    let seed = p.training_seed.unwrap_or_else(|| {
        let mut h = blake3::Hasher::new();
        h.update(b"clustered-training-seed");
        h.update(index.table.as_bytes());
        h.update(index.column.as_bytes());
        h.update(&source_identity);
        h.update(&column.vector_policy_revision.to_le_bytes());
        u64::from_le_bytes(
            h.finalize().as_bytes()[..8]
                .try_into()
                .expect("eight digest bytes"),
        )
    });
    let training = TrainingPolicy {
        iterations: u32::try_from(p.training_iterations.unwrap_or(10))
            .map_err(|_| invalid(index, "TRAINING_ITERATIONS"))?,
        reservoir_samples: to_usize(
            p.training_samples.unwrap_or(300000).min(population.max(1)),
            "TRAINING_SAMPLES",
        )?,
        seed,
        ingest_batch_keys: to_usize(p.ingest_batch_keys.unwrap_or(64), "INGEST_BATCH_KEYS")?,
        phase_batch_entries: to_usize(
            p.build_batch_entries
                .unwrap_or((workspace_budget_bytes / stride).max(1) as u64),
            "BUILD_BATCH_ENTRIES",
        )?,
    };
    let probes = column.ivf_probes.unwrap_or(lists);
    if probes > lists {
        return Err(invalid(index, "PROBES"));
    }
    let max_depth = to_usize(p.calibration_max_k.unwrap_or(100), "CALIBRATION_MAX_K")?;
    let factor = column.ivf_refine_factor_hundredths.unwrap_or(100);
    let rerank = max_depth
        .checked_mul(factor as usize)
        .and_then(|n| n.checked_add(99))
        .map(|n| n / 100)
        .ok_or_else(|| memory(usize::MAX, workspace_available, build_budget_bytes))?;
    let policy_for = |buffer| -> Result<Policy> {
        Policy::checked(
            dimensions,
            lists,
            probes,
            rerank,
            buffer,
            column.ivf_probes.is_none(),
        )
        .and_then(|p| {
            p.with_max_delta_runs(column.vector_engine_policy.max_delta_runs.unwrap_or(2))
        })
        .and_then(|p| {
            p.with_coalesce_pages_per_step(
                to_usize(
                    column
                        .vector_engine_policy
                        .coalesce_pages_per_step
                        .unwrap_or(crate::persistence::STORAGE_MAINTENANCE_PAGE_BATCH as u64),
                    "COALESCE_PAGES_PER_STEP",
                )
                .ok()?,
            )
        })
        .ok_or_else(|| invalid(index, "IVF"))
    };
    let buffer = if let Some(bytes) = p.write_buffer_bytes {
        to_usize(bytes, "WRITE_BUFFER_BYTES")?
    } else {
        let mut low = 0usize;
        let mut high = usize::try_from(population.max(expected_vectors).max(1))
            .unwrap_or(usize::MAX / stride)
            .min(workspace_budget_bytes / stride);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if policy_for(mid * stride)
                .and_then(|policy| ivf::workspace_allocation_layout(policy, training))
                .is_ok_and(|layout| layout.charged_bytes <= workspace_budget_bytes)
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        if low == 0 {
            return Err(memory(
                ivf::workspace_allocation_layout(policy_for(stride)?, training)?.charged_bytes,
                workspace_budget_bytes,
                build_budget_bytes,
            ));
        }
        low * stride
    };
    let policy = policy_for(buffer)?;
    let workspace = ivf::workspace_allocation_layout(policy, training)?;
    if workspace.charged_bytes > workspace_budget_bytes {
        return Err(memory(
            workspace.charged_bytes,
            workspace_budget_bytes,
            build_budget_bytes,
        ));
    }
    Ok(Resolved {
        policy,
        training,
        #[cfg(feature = "test-seams")]
        population,
        #[cfg(feature = "test-seams")]
        expected_vectors,
        #[cfg(feature = "test-seams")]
        active_lists,
        build_budget_bytes,
        #[cfg(feature = "test-seams")]
        codebook_budget_bytes,
        #[cfg(feature = "test-seams")]
        workspace_budget_bytes,
        #[cfg(feature = "test-seams")]
        codebook,
        #[cfg(feature = "test-seams")]
        workspace,
    })
}

/// Resolve the workspace after leaving the producer-quoted native receipt
/// capacity available to the same original writer. Fixed rows describe a row
/// transaction or a complete abort-only constructor witness. Maintenance derives
/// its finite row/page batch from the declarations and the resulting workspace.
pub(crate) fn resolve_with_native_receipts(
    index: &VectorIndexRef,
    column: &ColumnDef,
    population: u64,
    build_budget_bytes: u64,
    workspace_available: usize,
    source_identity: [u8; 32],
    native_backing_bytes: usize,
    fixed_rows: Option<usize>,
) -> Result<Resolved> {
    let initial = resolve(
        index,
        column,
        population,
        build_budget_bytes,
        workspace_available,
        source_identity,
    )?;
    let dimensions = initial.policy.dimensions();
    let active = u64::from(initial.policy.lists())
        .min(population)
        .min(initial.training.reservoir_samples as u64) as u32;
    let book = ivf::codebook_allocation_layout(dimensions, active, true)?;
    let stride =
        ivf::workspace_allocation_layout(initial.policy, initial.training)?.buffer_record_bytes;
    let population_count = usize::try_from(population).unwrap_or(usize::MAX);
    let receipt_quote = |available: usize| -> Result<usize> {
        let rows = if let Some(rows) = fixed_rows {
            rows
        } else {
            let phase = column.vector_engine_policy.build_batch_entries.map_or_else(
                || {
                    available
                        .saturating_sub(book.charged_bytes)
                        .checked_div(stride)
                        .unwrap_or(0)
                        .max(1)
                },
                |n| usize::try_from(n).unwrap_or(usize::MAX),
            );
            let remap = column
                .vector_engine_policy
                .remap_batch_entries
                .map_or(phase, |n| usize::try_from(n).unwrap_or(usize::MAX));
            population_count.min(phase.max(remap).max(initial.training.ingest_batch_keys))
        };
        crate::persistence::vector_ivf::native_receipt_reserve(
            dimensions,
            rows,
            active,
            initial.training.reservoir_samples,
            native_backing_bytes,
            if fixed_rows.is_some() {
                0
            } else {
                usize::try_from(
                    column
                        .vector_engine_policy
                        .coalesce_pages_per_step
                        .unwrap_or(crate::persistence::STORAGE_MAINTENANCE_PAGE_BATCH as u64),
                )
                .unwrap_or(usize::MAX)
            },
        )
    };
    let available = if fixed_rows.is_some() {
        workspace_available
            .checked_sub(receipt_quote(workspace_available)?)
            .ok_or_else(|| memory(usize::MAX, workspace_available, build_budget_bytes))?
    } else {
        // The predicate is monotone: both the phase capacity and its receipt
        // quote grow with available workspace. Choose the largest compatible
        // workspace; no iterative margin or altered declared batch is needed.
        let mut low = 0usize;
        let mut high = workspace_available;
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if mid
                .checked_add(receipt_quote(mid)?)
                .is_some_and(|n| n <= workspace_available)
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        low
    };
    resolve(
        index,
        column,
        population,
        build_budget_bytes,
        available,
        source_identity,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;
    use std::collections::HashMap;

    fn declared_column() -> ColumnDef {
        let db = Database::open_memory();
        db.execute(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v VECTOR(3) SEARCH_MODE INDEXED)",
            &HashMap::new(),
        )
        .unwrap();
        db.table_meta("t")
            .unwrap()
            .columns
            .into_iter()
            .find(|column| column.name == "v")
            .unwrap()
    }

    #[test]
    fn an_explicit_probes_resolves_a_compatible_lists_or_is_refused_naming_probes() {
        let index = VectorIndexRef::new("t", "v");
        let mut column = declared_column();
        column.ivf_probes = Some(512);
        let roomy = resolve(&index, &column, 10, 1 << 30, 1 << 30, [0; 32]).unwrap();
        assert_eq!(
            roomy.policy.probes(),
            512,
            "an explicit PROBES is never clamped"
        );
        assert!(
            roomy.policy.lists() >= 512,
            "an omitted LISTS resolves compatibly with an explicit PROBES: {}",
            roomy.policy.lists()
        );
        // A population large enough to make every list count real, under a
        // codebook budget that cannot hold 512 of them: the pair is refused
        // naming PROBES, and PROBES is not lowered.
        let error = resolve(&index, &column, 1_000_000, 4096, 1 << 30, [0; 32]).unwrap_err();
        assert!(
            matches!(
                error,
                Error::InvalidVectorPolicyDeclaration {
                    issue: contextdb_core::VectorPolicyDeclarationIssue::InvalidMember {
                        member: "PROBES"
                    },
                    ..
                }
            ),
            "{error:?}"
        );
    }
}
