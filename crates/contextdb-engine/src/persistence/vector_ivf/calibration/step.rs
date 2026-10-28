//! A selected tuple advances through bounded sampling and measured production runs.
use super::super::qualification::{Depth, Header, RerankSummary, SampleWork};
use super::{
    super::*,
    capture::{self, Capture, Phase, State},
    selector::{self, Lattice},
    updates::{self, Update},
    workspace::Workspace,
};
use std::sync::atomic::Ordering;

type Publisher<'a> = dyn FnMut(&mut dyn FnMut() -> Result<()>) -> Result<()> + 'a;
fn rewritten(capture: &Capture) -> bool {
    #[cfg(feature = "test-seams")]
    {
        crate::vector_ivf_observations::calibration::answers_selected(
            capture.owner.as_ref().stable_funder_identity(),
            capture.route,
        )
    }
    #[cfg(not(feature = "test-seams"))]
    {
        let _ = capture;
        false
    }
}
fn forced(capture: &Capture) -> Option<(u32, u32)> {
    #[cfg(feature = "test-seams")]
    {
        crate::vector_ivf_observations::calibration::setting(
            capture.owner.as_ref().stable_funder_identity(),
            capture.route,
        )
    }
    #[cfg(not(feature = "test-seams"))]
    {
        let _ = capture;
        None
    }
}
fn rewrite_answer(
    capture: &Capture,
    sample: u32,
    depth: u32,
    exact: &[u64],
    answer: &mut [u64],
) -> Result<usize> {
    #[cfg(feature = "test-seams")]
    {
        crate::vector_ivf_observations::calibration::rewrite(
            capture.owner.as_ref().stable_funder_identity(),
            capture.route,
            sample as usize,
            depth,
            exact,
            answer,
        )
        .ok_or_else(corrupt)
    }
    #[cfg(not(feature = "test-seams"))]
    {
        let _ = (capture, sample, depth, exact, answer);
        Err(corrupt())
    }
}
/// A counterexample changes only trial execution order, never the sample set,
/// lattice order, selected pair or final measurements.
fn trial_sample(position: u32, witness: Option<u32>) -> u32 {
    if let Some(witness) = witness {
        if position == 0 {
            return witness;
        }
        if position <= witness {
            return position - 1;
        }
    }
    position
}
fn reset(state: &mut State) {
    state.cursor.query = 0;
    state.cursor.indexed_work = 0;
    state.cursor.exact_work = 0;
    state.cursor.below = 0;
    state.cursor.reranks = None;
    state.minimum_hits.fill(u32::MAX);
    state.below.fill(0);
}
fn lattice<'a>(
    capture: &Capture,
    column: &'a contextdb_core::ColumnDef,
    depth: u32,
    capacity: usize,
) -> Lattice<'a> {
    let fixed = forced(capture);
    Lattice {
        policy: &column.vector_engine_policy,
        active: capture.manifest.active_lists,
        dimensions: capture.manifest.dimensions,
        population: capture.population,
        depth,
        capacity,
        fixed_probes: fixed
            .map(|p| p.0)
            .or(column.ivf_probes)
            .map(|p| p.min(capture.manifest.active_lists)),
        fixed_factor: fixed.map(|p| p.1).or(column.ivf_refine_factor_hundredths),
    }
}
fn store_unavailable(
    capture: &Capture,
    column: &contextdb_core::ColumnDef,
    depth: u32,
    state: &mut State,
    commit: &mut dyn FnMut(&Update<'_>) -> Result<()>,
) -> Result<()> {
    let pair = lattice(capture, column, depth, 0);
    let point = pair.point(
        pair.fixed_probes
            .unwrap_or(capture.manifest.active_lists.max(1)),
        pair.fixed_factor.unwrap_or(100),
    )?;
    let row = Depth {
        depth,
        probes: point.probes,
        factor_hundredths: point.factor,
        shortlist: point.shortlist as u64,
        reranks: None,
        samples: 0,
        queries_below: 0,
        qualified: false,
        indexed_work: 0,
        exact_work: 0,
    };
    commit(&Update::Depth {
        ordinal: state.cursor.depth_ordinal,
        previous: state.cursor.previous_depth,
        row,
        minimum: &state.minimum_hits,
        below: &state.below,
    })?;
    state.cursor.previous_depth = depth;
    state.cursor.depth_ordinal = state
        .cursor
        .depth_ordinal
        .checked_add(1)
        .ok_or_else(corrupt)?;

    Ok(())
}
impl RedbPersistence {
    pub(crate) fn advance_native_ivf_calibration(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        checkpoint: &dyn Fn() -> Result<()>,
        publish: &mut Publisher<'_>,
    ) -> Result<bool> {
        let Some(capture) = self.selected_calibration(index, tuple) else {
            return Ok(false);
        };
        if capture.finished.load(Ordering::Acquire) {
            return Ok(true);
        }
        // Another driver never waits under a writer's publication boundary.
        let Some(mut state) = capture.state.try_lock() else {
            return Ok(false);
        };
        let result = self.calibration_step(&capture, index, tuple, &mut state, checkpoint, publish);
        if result.is_err() {
            self.native_ivf_cache
                .cancel_calibration_where(|active| active.epoch == capture.epoch);
        }
        result
    }
    fn calibration_step(
        &self,
        capture: &Capture,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        state: &mut State,
        checkpoint: &dyn Fn() -> Result<()>,
        publish: &mut Publisher<'_>,
    ) -> Result<bool> {
        super::sample::check(capture, index, checkpoint)?;
        let column = capture.column(index)?;
        let target = column.search_target_accuracy_hundredths.unwrap_or(9500);
        let mut commit = |update: &Update<'_>| -> Result<()> {
            let mut apply =
                || self.apply_calibration_update(capture, index, tuple, update, checkpoint);
            publish(&mut apply)
        };
        if state.cursor.phase == Phase::Samples {
            if !capture.sample_batch(
                index,
                tuple,
                state,
                capture.training.ingest_batch_keys,
                checkpoint,
            )? {
                return Ok(false);
            }
            capture.finish_samples(state, index)?;
            let samples = u32::try_from(state.samples.len()).map_err(|_| corrupt())?;
            let (_, _, frontier) = capture.root.authenticate()?;
            commit(&Update::Header(Header {
                epoch: capture.epoch,
                complete: false,
                source_digest: capture.source_digest,
                next_chunk: 0,
                generation: capture.manifest.generation,
                policy_revision: column.vector_policy_revision,
                target_hundredths: target,
                query_source: capture.source.is_some(),
                source_partitioned: capture.source_partitioned,
                source_identity: capture.source_identity,
                source_frontier: frontier.0,
                samples: 0,
                planned_samples: samples,
                selection_samples: 0,
                validation_samples: 0,
                depths: 0,
            }))?;
            let eligible = capture
                .population
                .saturating_sub(state.excluded.len() as u64);
            let full_probes = column.ivf_probes.is_some_and(|p| {
                p.min(capture.manifest.active_lists) == capture.manifest.active_lists
            });
            let proof = !rewritten(capture)
                && forced(capture).is_none()
                && (eligible == 0
                    || u64::from(capture.manifest.active_lists) >= eligible
                    || full_probes);
            state.cursor.phase = if samples == 0 || proof {
                Phase::Unavailable
            } else {
                Phase::Oracle
            };
            state.cursor.query = 0;
            return Ok(false);
        }
        if state.cursor.phase == Phase::Finished {
            return Ok(true);
        }
        if state.cursor.phase == Phase::Unavailable {
            for _ in 0..capture.training.phase_batch_entries {
                super::sample::check(capture, index, checkpoint)?;
                if let Some(depth) = capture::depth_after(column, state.cursor.previous_depth) {
                    store_unavailable(capture, column, depth, state, &mut commit)?;
                } else {
                    commit(&Update::Finish)?;
                    state.cursor.phase = Phase::Finished;
                    capture.finished.store(true, Ordering::Release);
                    return Ok(true);
                }
            }
            return Ok(false);
        }
        let mut workspace = Workspace::new(capture, rewritten(capture))?;
        for _ in 0..capture.training.phase_batch_entries {
            super::sample::check(capture, index, checkpoint)?;
            if state.cursor.phase == Phase::Oracle {
                let sample_index = state.cursor.query;
                let sample = *state
                    .samples
                    .get(sample_index as usize)
                    .ok_or_else(corrupt)?;
                let (count, work) = super::query::run(
                    capture,
                    index,
                    tuple,
                    state,
                    sample,
                    capture.maximum_depth,
                    None,
                    &mut workspace,
                    checkpoint,
                )?;
                commit(&Update::Oracle {
                    sample: sample_index,
                    identity: sample,
                    hits: &workspace.hits[..count],
                    work: work.distance_evaluations(),
                })?;
                state.cursor.query = sample_index.checked_add(1).ok_or_else(corrupt)?;
                if state.cursor.query as usize == state.samples.len() {
                    state.cursor.phase = Phase::Trial;
                    state.cursor.point = None;
                    state.cursor.first = None;
                    reset(state);
                }
                continue;
            }
            let Some(depth) = capture::depth_after(column, state.cursor.previous_depth) else {
                commit(&Update::Finish)?;
                state.cursor.phase = Phase::Finished;
                capture.finished.store(true, Ordering::Release);
                return Ok(true);
            };
            if state.cursor.phase == Phase::Unavailable {
                store_unavailable(capture, column, depth, state, &mut commit)?;
                continue;
            }
            let capacity = super::query::capacity(capture, index, tuple, depth, checkpoint)?;
            let pair = lattice(capture, column, depth, capacity);
            if state.cursor.point.is_none() {
                if capacity < depth as usize && pair.fixed_factor.is_none() {
                    // No DEFAULT constructor fits. No actual measurement is invented.
                    state.cursor.phase = Phase::Unavailable;
                    continue;
                }
                state.cursor.point = pair.next(None, checkpoint)?;
                if state.cursor.point.is_none() {
                    state.cursor.point = Some(pair.point(
                        pair.fixed_probes.unwrap_or(capture.manifest.active_lists),
                        pair.fixed_factor.unwrap_or(100),
                    )?);
                    state.cursor.phase = Phase::Final;
                }
            }
            let point = state.cursor.point.ok_or_else(corrupt)?;
            let sample_position = state.cursor.query;
            let sample_index = if state.cursor.phase == Phase::Trial {
                trial_sample(sample_position, state.cursor.failure_witness)
            } else {
                sample_position
            };
            let sample = *state
                .samples
                .get(sample_index as usize)
                .ok_or_else(corrupt)?;
            let oracle = self.read_calibration(capture, index, tuple, checkpoint, |source| {
                updates::load_oracle(
                    source,
                    capture.manifest.generation,
                    sample_index,
                    &mut workspace.exact,
                )
            })?;
            let (count, work) = super::query::run(
                capture,
                index,
                tuple,
                state,
                sample,
                depth,
                Some(point),
                &mut workspace,
                checkpoint,
            )?;
            let mut answer_count = count;
            if let Some(answer) = workspace.rewritten.as_mut() {
                let stored =
                    self.read_calibration(capture, index, tuple, checkpoint, |source| {
                        updates::load_rewrite(
                            source,
                            capture.manifest.generation,
                            state.cursor.depth_ordinal,
                            sample_index,
                            answer,
                        )
                    })?;
                if let Some(count) = stored {
                    answer_count = count;
                } else {
                    answer_count = rewrite_answer(
                        capture,
                        sample_index,
                        depth,
                        &workspace.exact[..(oracle.count as usize).min(depth as usize)],
                        &mut answer[..depth as usize],
                    )?;
                    commit(&Update::Rewrite {
                        depth_ordinal: state.cursor.depth_ordinal,
                        sample: sample_index,
                        answer: &answer[..answer_count],
                    })?;
                }
            }
            let hits_at = |k: usize| {
                if let Some(answer) = workspace.rewritten.as_ref() {
                    selector::prefix_hits(
                        &workspace.exact[..oracle.count as usize],
                        &answer[..answer_count],
                        k,
                    )
                } else {
                    workspace.hits[..count.min(k)]
                        .iter()
                        .enumerate()
                        .filter(|(at, hit)| {
                            workspace.exact[..(oracle.count as usize).min(k)].contains(&hit.row)
                                && !workspace.hits[..*at]
                                    .iter()
                                    .any(|prior| prior.row == hit.row)
                        })
                        .count() as u32
                }
            };
            let hits = hits_at(depth as usize);
            let missed = hits < selector::required_hits(target, depth)?;
            state.cursor.indexed_work = state
                .cursor
                .indexed_work
                .checked_add(work.distance_evaluations())
                .ok_or_else(corrupt)?;
            state.cursor.exact_work = state
                .cursor
                .exact_work
                .checked_add(oracle.work)
                .ok_or_else(corrupt)?;
            if missed {
                state.cursor.below = state.cursor.below.checked_add(1).ok_or_else(corrupt)?;
            }
            if state.cursor.phase == Phase::Final {
                commit(&Update::Work(SampleWork {
                    depth_ordinal: state.cursor.depth_ordinal,
                    sample: sample_index,
                    reranked_candidates: work.reranked_candidates,
                    indexed_work: work.distance_evaluations(),
                    exact_work: oracle.work,
                }))?;
                if let Some(summary) = state.cursor.reranks.as_mut() {
                    summary.include(work.reranked_candidates)?;
                } else {
                    state.cursor.reranks = Some(RerankSummary::first(work.reranked_candidates));
                }
                for k in state.cursor.previous_depth + 1..=depth {
                    let at = k as usize - 1;
                    let hits = hits_at(k as usize);
                    state.minimum_hits[at] = state.minimum_hits[at].min(hits);
                    if hits < selector::required_hits(target, k)? {
                        state.below[at] = state.below[at].checked_add(1).ok_or_else(corrupt)?;
                    }
                }
            }
            state.cursor.query = sample_position.checked_add(1).ok_or_else(corrupt)?;
            if state.cursor.phase == Phase::Trial && missed {
                state.cursor.failure_witness = Some(sample_index);
                state.cursor.point = pair.next(Some(point), checkpoint)?;
                if state.cursor.point.is_none() {
                    state.cursor.point = Some(pair.point(
                        pair.fixed_probes.unwrap_or(capture.manifest.active_lists),
                        pair.fixed_factor.unwrap_or(100),
                    )?);
                    state.cursor.phase = Phase::Final;
                }
                reset(state);
                continue;
            }
            if (state.cursor.query as usize) < state.samples.len() {
                continue;
            }
            if state.cursor.phase == Phase::Trial {
                // The frozen selector stops at the first quality qualifier.
                // Cushion once, then recheck every query; never retune afterward.
                state.cursor.first = Some(point);
                state.cursor.point = Some(pair.cushion(point)?);
                state.cursor.phase = Phase::Final;
                reset(state);
                continue;
            }
            let samples = u32::try_from(state.samples.len()).map_err(|_| corrupt())?;
            let row = Depth {
                depth,
                probes: point.probes,
                factor_hundredths: point.factor,
                shortlist: point.shortlist as u64,
                reranks: state.cursor.reranks,
                samples,
                queries_below: state.cursor.below,
                qualified: state.cursor.first.is_some()
                    && state.cursor.below == 0
                    && state.cursor.indexed_work < state.cursor.exact_work,
                indexed_work: state.cursor.indexed_work,
                exact_work: state.cursor.exact_work,
            };
            commit(&Update::Depth {
                ordinal: state.cursor.depth_ordinal,
                previous: state.cursor.previous_depth,
                row,
                minimum: &state.minimum_hits,
                below: &state.below,
            })?;
            state.cursor.previous_depth = depth;
            state.cursor.failure_witness = None;
            state.cursor.depth_ordinal = state
                .cursor
                .depth_ordinal
                .checked_add(1)
                .ok_or_else(corrupt)?;
            state.cursor.phase = Phase::Trial;
            state.cursor.point = None;
            state.cursor.first = None;
            reset(state);
        }
        Ok(false)
    }
}
