//! Qualification executes the same production kernel against its pinned candidate.

#![expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
use super::{
    super::*,
    capture::{Capture, Sample, State},
    selector,
    workspace::Workspace,
};
use contextdb_vector::ivf::{self, Mode, Policy, Work, storage::ReadPages};

struct Geometry {
    lists: u32,
    book_bytes: usize,
}
fn geometry(source: &read::PageSource<'_>, capture: &Capture, max_runs: u64) -> Result<Geometry> {
    let manifest = capture.manifest;
    let mut lists = manifest.lists;
    let book = |m: contextdb_vector::ivf::storage::Manifest| {
        if m.active_lists == 0 {
            Ok(0)
        } else {
            ivf::codebook_allocation_layout(m.dimensions, m.lists, false)
                .map(|layout| layout.charged_bytes)
        }
    };
    let mut book_bytes = book(manifest)?;
    if let Some(catalog) = ivf::inspect_catalog(source, 0, max_runs)?
        && let Some(digest) = catalog.retained_codebook
    {
        let old = source
            .build_state(0)?
            .and_then(|build| build.serving_at_start)
            .filter(|old| {
                old.codebook == digest
                    && old.generation < manifest.generation
                    && old.dimensions == manifest.dimensions
            })
            .ok_or_else(corrupt)?;
        lists = lists.max(old.lists);
        if !catalog.legacy_unserviceable {
            book_bytes = book_bytes.max(book(old)?);
        }
    }
    Ok(Geometry { lists, book_bytes })
}
fn policy(
    capture: &Capture,
    lists: u32,
    probes: u32,
    rerank: usize,
    max_runs: u64,
    extend_to_k: bool,
) -> Result<Policy> {
    let stride = capture
        .manifest
        .dimensions
        .checked_mul(4)
        .and_then(|n| n.checked_add(40))
        .ok_or_else(corrupt)?;
    Policy::checked(
        capture.manifest.dimensions,
        lists,
        probes,
        rerank,
        stride,
        extend_to_k,
    )
    .and_then(|policy| policy.with_max_delta_runs(max_runs))
    .ok_or_else(corrupt)
}
fn with_candidate<R>(
    capture: &Capture,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    checkpoint: &dyn Fn() -> Result<()>,
    run: impl FnOnce(&read::PageSource<'_>) -> Result<R>,
) -> Result<R> {
    super::sample::check(capture, index, checkpoint)?;
    let authority = read::NativeAuthority::open_prepared(
        &capture.root,
        capture.memory.clone(),
        &capture.verification,
    )?;
    let check = || super::sample::work_checkpoint(capture, index, checkpoint);
    let distance = |_| check();
    let source = read::PageSource::open_prepared(
        &authority,
        index,
        tuple,
        &check,
        &distance,
        &capture.verification,
    )?;
    if source.select_build_authority_with_verification(&capture.verification)? != capture.manifest {
        return Err(corrupt());
    }
    run(&source)
}
/// Capacity is measured while the same caller output and source objects live.
/// The producer quote alone supplies the scratch and codebook allowance.
pub(super) fn capacity(
    capture: &Capture,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    depth: u32,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<usize> {
    with_candidate(capture, index, tuple, checkpoint, |source| {
        let column = capture.column(index)?;
        let max_runs = column.vector_engine_policy.max_delta_runs.unwrap_or(2);
        let geometry = geometry(source, capture, max_runs)?;
        let policy = policy(
            capture,
            geometry.lists,
            1,
            depth as usize,
            max_runs,
            column.ivf_probes.is_none(),
        )?;
        Ok(selector::candidate_capacity(
            policy,
            depth as usize,
            geometry.book_bytes,
            capture.original.usage().available.unwrap_or(usize::MAX),
        ))
    })
}
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    capture: &Capture,
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    state: &State,
    sample: Sample,
    depth: u32,
    point: Option<selector::Point>,
    workspace: &mut Workspace,
    checkpoint: &dyn Fn() -> Result<()>,
) -> Result<(usize, Work)> {
    capture.query_vector(
        index,
        tuple,
        sample,
        &mut workspace.query,
        &mut workspace.page,
        checkpoint,
    )?;
    with_candidate(capture, index, tuple, checkpoint, |source| {
        let column = capture.column(index)?;
        let max_runs = column.vector_engine_policy.max_delta_runs.unwrap_or(2);
        let geometry = geometry(source, capture, max_runs)?;
        let depth = depth as usize;
        if depth > workspace.hits.len() {
            return Err(corrupt());
        }
        let policy = policy(
            capture,
            geometry.lists,
            point.map_or(1, |point| point.probes),
            point.map_or(depth, |point| point.shortlist),
            max_runs,
            column.ivf_probes.is_none(),
        )?;
        let mut work = Work::default();
        let mut eligible = |candidate: ivf::CandidateIdentity<'_>| -> Result<bool> {
            Ok(state.excluded.binary_search(&candidate.row.row).is_err())
        };
        let count = ivf::search_with_eligibility(
            source,
            policy,
            &capture.owner,
            &workspace.query,
            Some(&[0]),
            if point.is_some() {
                Mode::Indexed
            } else {
                Mode::Exact
            },
            &mut eligible,
            &mut workspace.hits[..depth],
            &mut work,
        )?;
        Ok((count, work))
    })
}
