//! The clustered search kernel reads the caller's retained native snapshot.
use super::*;
use contextdb_core::{ColumnDef, VectorSearchMode};
#[cfg(feature = "test-seams")]
use contextdb_vector::ivf::storage::Manifest;
use contextdb_vector::ivf::{
    self, CandidateIdentity, Hit, Mode, Policy, Work,
    storage::{DistanceKind, ReadPages},
};
#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryWork {
    pub work: Work,
    #[cfg(feature = "test-seams")]
    pub manifest: Manifest,
    pub indexed: bool,
    pub served_depth: Option<u32>,
    pub objective_met: bool,
}
#[allow(clippy::too_many_arguments)]
pub(crate) fn search<E: From<Error>>(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    column: &ColumnDef,
    maintenance: Option<super::retained::Selection<'_>>,
    record_damage: Option<&dyn Fn(contextdb_vector::ivf::CatalogState) -> Result<()>>,
    query: &[f32],
    mode: VectorSearchMode,
    query_breadth: crate::database::vector_ivf::policy::QueryBreadth,
    checkpoint: &dyn Fn() -> Result<()>,
    distance: &dyn Fn(DistanceKind) -> Result<()>,
    eligible: &mut dyn FnMut(CandidateIdentity<'_>) -> std::result::Result<bool, E>,
    output: &mut [Hit],
    pool: Option<&mut dyn FnMut(&[Hit]) -> std::result::Result<(), E>>,
) -> std::result::Result<Option<(usize, QueryWork)>, E> {
    let authority = match read::NativeAuthority::open(root, memory) {
        Ok(authority) => authority,
        Err(error @ Error::StoreCorrupted { .. }) => {
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::Producer::current().emit(
                crate::vector_ivf_observations::NativeIvfSealEvent::QueryRejected {
                    index,
                    tuple,
                    error: &error,
                },
            );
            return Err(E::from(error));
        }
        Err(error) => return Err(E::from(error)),
    };
    let mut source = match read::PageSource::open(&authority, index, tuple, checkpoint, distance) {
        Ok(Some(source)) => source,
        Ok(None) => return Ok(None),
        Err(error @ Error::StoreCorrupted { .. }) => {
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::Producer::current().emit(
                crate::vector_ivf_observations::NativeIvfSealEvent::QueryRejected {
                    index,
                    tuple,
                    error: &error,
                },
            );
            return Err(E::from(error));
        }
        Err(error) => return Err(E::from(error)),
    };
    if !source.canonical_ready() {
        return Ok(None);
    }
    if mode == VectorSearchMode::Exact {
        source.select_canonical_authority().map_err(E::from)?;
    }
    let manifest = match source.manifest(0) {
        Ok(manifest) => manifest,
        Err(error @ (Error::IndexCorrupted { .. } | Error::StoreCorrupted { .. })) => {
            #[cfg(feature = "test-seams")]
            crate::vector_ivf_observations::Producer::current().emit(
                crate::vector_ivf_observations::NativeIvfSealEvent::QueryRejected {
                    index,
                    tuple,
                    error: &error,
                },
            );
            if mode == VectorSearchMode::Indexed {
                return Err(E::from(error));
            }
            source.select_canonical_authority().map_err(E::from)?;
            source.manifest(0).map_err(E::from)?
        }
        Err(error) => return Err(E::from(error)),
    };
    let Some(manifest) = manifest else {
        return Ok(None);
    };
    if let Some(selection) = maintenance {
        match source.activate_retained(tuple, selection) {
            Ok(()) => {}
            Err(Error::StoreCorrupted { .. } | Error::IndexCorrupted { .. })
                if mode != VectorSearchMode::Indexed => {}
            Err(error) => return Err(E::from(error)),
        }
    }
    // Verification is window-scoped, as with SHOW. The callback can record
    // only the verdict; it cannot replace this query's retained data root.
    let catalog = ivf::inspect_catalog(
        &source,
        0,
        column.vector_engine_policy.max_delta_runs.unwrap_or(2),
    )
    .map_err(E::from)?;
    if let Some(catalog) = catalog
        && catalog.unserviceable.is_some()
        && !catalog.verdict_recorded
        && let Some(record) = record_damage
    {
        record(catalog).map_err(E::from)?;
    }
    if mode == VectorSearchMode::Indexed
        && let Some(key) = catalog.and_then(|catalog| catalog.unserviceable)
    {
        return Err(E::from(Error::StoreCorrupted {
            path: "clustered vector native pages".into(),
            reason: format!(
                "the serving {:?} page is damaged (generation {}, run {}, ordinal {})",
                key.kind, key.generation, key.run, key.ordinal
            )
            .into(),
        }));
    }
    let limit = contextdb_vector::mem::search_policy_limit()
        .filter(|limit| *limit <= output.len())
        .unwrap_or(output.len());
    let route = select_route(
        &source,
        column,
        mode,
        query_breadth,
        catalog,
        manifest,
        limit,
    )
    .map_err(E::from)?;
    let probes = route.probes;
    let factor = route.factor;
    let declared_probes = query_breadth.probes.unwrap_or(column.ivf_probes);
    let mut receipt = QueryWork {
        work: Work::default(),
        #[cfg(feature = "test-seams")]
        manifest,
        indexed: route.indexed,
        served_depth: route.served_depth,
        objective_met: route.objective_met,
    };
    let indexed = route.indexed;
    if indexed && manifest.active_lists == 0 {
        return Err(E::from(Error::VectorIndexedRouteUnavailable {
            index: index.clone(),
        }));
    }
    // A next-depth receipt describes that depth's actual completion route.
    // Run it to completion before truncating to the caller's LIMIT.
    let route_depth = receipt.served_depth.map_or(output.len(), |d| d as usize);
    let rerank = route_depth
        .checked_mul(factor as usize)
        .and_then(|n| n.checked_add(99))
        .map(|n| n / 100)
        .ok_or_else(|| E::from(corrupt()))?
        .max(route_depth)
        .max(1);
    let stride = manifest
        .dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(40))
        .ok_or_else(|| E::from(corrupt()))?;
    let policy = Policy::checked(
        manifest.dimensions,
        manifest.lists,
        probes,
        rerank,
        stride,
        declared_probes.is_none(),
    )
    .and_then(|p| p.with_max_delta_runs(column.vector_engine_policy.max_delta_runs.unwrap_or(2)))
    .ok_or_else(|| E::from(corrupt()))?;
    let mut work = Work::default();
    let selected = if indexed { Mode::Indexed } else { Mode::Exact };
    // A ranked read asks for the whole refined candidate pool, which is
    // `rerank` hits at the route selected for the original limit.
    let pooled = pool.is_some() && indexed;
    let route_capacity = if pooled { rerank } else { route_depth };
    let _route_invoice = if route_capacity > output.len() {
        Some(
            super::memory::Invoice::reserve(
                authority.vector_owner(),
                std::alloc::Layout::array::<Hit>(route_capacity)
                    .map_err(|_| E::from(corrupt()))?
                    .size(),
                "clustered_served_depth",
            )
            .map_err(E::from)?,
        )
    } else {
        None
    };
    let mut route_hits = if route_capacity > output.len() {
        vec![Hit::default(); route_capacity]
    } else {
        Vec::new()
    };
    let count = if route_capacity > output.len() {
        let n = ivf::search_with_eligibility(
            &source,
            policy,
            authority.vector_owner(),
            query,
            Some(&[0]),
            selected,
            eligible,
            &mut route_hits,
            &mut work,
        )?;
        if let Some(pool) = pool
            && pooled
        {
            pool(&route_hits[..n])?;
        }
        let n = n.min(output.len());
        output[..n].copy_from_slice(&route_hits[..n]);
        n
    } else {
        ivf::search_with_eligibility(
            &source,
            policy,
            authority.vector_owner(),
            query,
            Some(&[0]),
            selected,
            eligible,
            output,
            &mut work,
        )?
    };
    let actual = source.actual_page_work();
    work.list_pages_read = actual.list_pages_read;
    work.canonical_pages_read = actual.canonical_pages_read;
    work.visibility_pages_read = actual.visibility_pages_read;
    work.directory_pages_read = actual.directory_pages_read;
    work.directory_presence_pages_read = actual.directory_presence_pages_read;
    work.catalog_pages_read = actual.catalog_pages_read;
    work.slot_metadata_pages_read = actual.slot_metadata_pages_read;
    work.tail_pages_read = actual.tail_pages_read;
    work.list_link_pages_read = actual.list_link_pages_read;
    work.row_version_pages_read = actual.row_version_pages_read;
    work.row_copy_pages_read = actual.row_copy_pages_read;
    work.codebook_pages_read = actual.codebook_pages_read;
    work.accumulator_pages_read = actual.accumulator_pages_read;
    receipt.work = work;
    receipt.objective_met |= !indexed;
    Ok(Some((count, receipt)))
}

struct SelectedRoute {
    probes: u32,
    factor: u32,
    indexed: bool,
    served_depth: Option<u32>,
    objective_met: bool,
    _qualification: Option<qualification::Selection>,
}
fn select_route(
    source: &read::PageSource<'_>,
    column: &ColumnDef,
    mode: VectorSearchMode,
    query_breadth: crate::database::vector_ivf::policy::QueryBreadth,
    catalog: Option<ivf::CatalogState>,
    manifest: ivf::storage::Manifest,
    k: usize,
) -> Result<SelectedRoute> {
    let target = query_breadth
        .target
        .unwrap_or(column.search_target_accuracy_hundredths)
        .unwrap_or(9500);
    let qualification =
        if mode != VectorSearchMode::Exact && catalog.is_none_or(|c| c.unserviceable.is_none()) {
            source.select_qualification(k, target)?
        } else {
            None
        };
    let declared_probes = query_breadth.probes.unwrap_or(column.ivf_probes);
    let declared_factor = query_breadth
        .refine_factor
        .unwrap_or(column.ivf_refine_factor_hundredths);
    let probes = declared_probes
        .or(qualification.as_ref().map(|q| q.depth.probes))
        .unwrap_or(manifest.lists);
    if probes > manifest.lists {
        return Err(Error::InvalidVectorPolicyDeclaration {
            index: source.index.clone(),
            issue: contextdb_core::VectorPolicyDeclarationIssue::InvalidMember { member: "PROBES" },
        });
    }
    let factor = declared_factor
        .or(qualification.as_ref().map(|q| q.depth.factor_hundredths))
        .unwrap_or(100);
    let objective_met = qualification.as_ref().is_some_and(|q| {
        q.covered && q.depth.probes == probes && q.depth.factor_hundredths == factor
    });
    let indexed =
        mode == VectorSearchMode::Indexed || (mode == VectorSearchMode::Auto && objective_met);
    let served_depth = if indexed {
        qualification.as_ref().map(|q| q.depth.depth)
    } else {
        None
    };
    Ok(SelectedRoute {
        probes,
        factor,
        indexed,
        served_depth,
        objective_met,
        _qualification: qualification,
    })
}

/// Passive planning selects the same authenticated receipt as execution. It
/// does no vector scoring, publication, codebook construction or repair.
pub(crate) fn predicts_indexed(
    root: &super::super::vector_paged::CapturedPagedRoot,
    memory: contextdb_core::read_memory::ReadMemoryOwner,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    column: &ColumnDef,
    mode: VectorSearchMode,
    breadth: crate::database::vector_ivf::policy::QueryBreadth,
    k: usize,
) -> Result<bool> {
    let authority = read::NativeAuthority::open(root, memory)?;
    let Some(source) = read::PageSource::open(&authority, index, tuple, &|| Ok(()), &|_| Ok(()))?
    else {
        return Ok(false);
    };
    if !source.canonical_ready() {
        return Ok(false);
    }
    let Some(manifest) = source.manifest(0)? else {
        return Ok(false);
    };
    let catalog = ivf::inspect_catalog(
        &source,
        0,
        column.vector_engine_policy.max_delta_runs.unwrap_or(2),
    )?;
    if catalog.is_some_and(|catalog| catalog.unserviceable.is_some()) {
        return Ok(false);
    }
    let route = select_route(&source, column, mode, breadth, catalog, manifest, k)?;
    Ok(route.indexed && manifest.active_lists != 0)
}
