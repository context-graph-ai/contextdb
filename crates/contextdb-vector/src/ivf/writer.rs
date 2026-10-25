use super::{
    book::{Codebook, append_list, read_vector},
    catalog::{self, Catalog, Entry, Run},
    codec,
    memory::{Invoice, corrupt, dimension, overflow, phase},
    storage::{BuildPhase, BuildState, Manifest, PageKind, PageStore, ReadPages, WritePages},
    types::{BufferState, Policy, TrainingPolicy, Vector},
    versions::Version,
};
use crate::MemoryBudgetOwner;
use contextdb_core::Result;

/// Bounded writer window over the native committed canonical tail. Search reads
/// the same native slots, including after this window is evicted/reopened.
pub struct ClusteredIndex<S> {
    store: S,
    policy: Policy,
    training: TrainingPolicy,
    owner: MemoryBudgetOwner,
    buffer: Vec<u8>,
    state: BufferState,
    raw: Vec<u8>,
    values: Vec<f32>,
    encoded: Vec<u8>,
    order: Vec<usize>,
    buffer_record_bytes: usize,
    _workspace_invoice: Invoice,
}
impl<S: PageStore> ClusteredIndex<S> {
    pub fn new(
        store: S,
        policy: Policy,
        training: TrainingPolicy,
        owner: MemoryBudgetOwner,
    ) -> Result<Self> {
        for (member, value) in [
            ("TRAINING_SAMPLES", training.reservoir_samples as u64),
            ("TRAINING_ITERATIONS", u64::from(training.iterations)),
            ("INGEST_BATCH_KEYS", training.ingest_batch_keys as u64),
            ("BUILD_BATCH_ENTRIES", training.phase_batch_entries as u64),
        ] {
            if value == 0 || (member == "TRAINING_SAMPLES" && value.checked_mul(8).is_none()) {
                return Err(contextdb_core::Error::InvalidVectorPolicyDeclaration {
                    index: store.index_identity().clone(),
                    issue: contextdb_core::VectorPolicyDeclarationIssue::InvalidTrainingValue {
                        member,
                        value,
                    },
                });
            }
        }
        let raw_bytes = policy
            .dimensions
            .checked_mul(4)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(overflow)?;
        let buffer_record_bytes = raw_bytes.checked_add(32).ok_or_else(overflow)?;
        let records = policy.buffer_cap_bytes / buffer_record_bytes;
        let mut layout = std::alloc::Layout::new::<(Policy, TrainingPolicy, BufferState)>();
        for part in [
            std::alloc::Layout::array::<u8>(policy.buffer_cap_bytes),
            std::alloc::Layout::array::<u8>(raw_bytes),
            std::alloc::Layout::array::<f32>(policy.dimensions),
            std::alloc::Layout::array::<u8>(policy.dimensions + 16),
            std::alloc::Layout::array::<usize>(records),
        ] {
            layout = layout
                .extend(part.map_err(|_| overflow())?)
                .map_err(|_| overflow())?
                .0;
        }
        let total = layout
            .pad_to_align()
            .size()
            .checked_add(super::storage::PAGE_BYTES * 12 + std::mem::size_of::<Catalog>())
            .ok_or_else(overflow)?;
        let invoice = Invoice::reserve(&owner, total, "clustered_writer_workspace")?;
        let mut out = Self {
            store,
            policy,
            training,
            owner,
            buffer: Vec::with_capacity(policy.buffer_cap_bytes),
            state: BufferState {
                cap_bytes: policy.buffer_cap_bytes,
                ..BufferState::default()
            },
            raw: vec![0; raw_bytes],
            values: vec![0.0; policy.dimensions],
            encoded: vec![0; policy.dimensions + 16],
            order: Vec::with_capacity(records),
            buffer_record_bytes,
            _workspace_invoice: invoice,
        };
        let mut after = None;
        while let Some(partition) = out.store.next_partition(after)? {
            let manifest = out.store.manifest(partition)?.ok_or_else(corrupt)?;
            if manifest.dimensions != out.policy.dimensions {
                return Err(dimension(
                    &out.store,
                    manifest.dimensions,
                    out.policy.dimensions,
                ));
            }
            let c = Catalog::load(&out.store, manifest)?;
            for sequence in c.tail_head..c.tail_end {
                out.store.check_cancel()?;
                let slot = catalog::tail_slot(&out.store, manifest, sequence)?;
                let row = read_vector(&out.store, manifest, slot, &mut out.raw, &mut out.values)?;
                if out.buffer.len() + out.buffer_stride() > out.state.cap_bytes {
                    out.seal()?;
                    read_vector(&out.store, manifest, slot, &mut out.raw, &mut out.values)?;
                }
                out.buffer.extend_from_slice(&partition.to_le_bytes());
                out.buffer.extend_from_slice(&row.to_le_bytes());
                out.buffer.extend_from_slice(&slot.to_le_bytes());
                out.buffer
                    .extend_from_slice(&manifest.generation.to_le_bytes());
                out.buffer.extend_from_slice(&sequence.to_le_bytes());
                out.buffer.extend_from_slice(&out.raw[8..]);
                out.state.high_water_bytes = out.state.high_water_bytes.max(out.buffer.len());
            }
            after = Some(partition);
        }
        Ok(out)
    }
    pub fn storage(&self) -> &S {
        &self.store
    }
    pub fn into_storage(self) -> S {
        self.store
    }
    pub fn policy(&self) -> Policy {
        self.policy
    }
    pub fn buffer(&self) -> BufferState {
        self.state
    }
    fn buffer_stride(&self) -> usize {
        self.buffer_record_bytes
    }
    fn cache(&mut self, vector: Vector<'_>, slot: u64, generation: u64, sequence: u64) {
        self.buffer
            .extend_from_slice(&vector.partition.to_le_bytes());
        self.buffer.extend_from_slice(&vector.row.to_le_bytes());
        self.buffer.extend_from_slice(&slot.to_le_bytes());
        self.buffer.extend_from_slice(&generation.to_le_bytes());
        self.buffer.extend_from_slice(&sequence.to_le_bytes());
        for x in vector.values {
            self.buffer.extend_from_slice(&x.to_le_bytes());
        }
        self.state.high_water_bytes = self.state.high_water_bytes.max(self.buffer.len());
    }
    pub fn begin_build(&mut self, partition: u64) -> Result<BuildState> {
        self.seal()?;
        if let Some(current) = self.store.manifest(partition)? {
            if Catalog::load(&self.store, current)?.legacy.is_some() {
                return Err(phase("clustered replacement deferred during Remap phase"));
            }
        }
        if let Some(build) = self.store.build_state(partition)? {
            if build.phase != BuildPhase::Ready {
                return Err(phase(format!(
                    "clustered begin during {:?} phase",
                    build.phase
                )));
            }
        }
        let source_lsn = self.store.snapshot_lsn()?;
        let serving_at_start = self.store.manifest(partition)?;
        let generation = serving_at_start
            .map_or(Some(1), |m| m.generation.checked_add(1))
            .ok_or_else(overflow)?;
        let state = BuildState {
            manifest: Manifest {
                partition,
                generation,
                dimensions: self.policy.dimensions,
                lists: self.policy.lists,
                active_lists: 0,
                slots: 0,
                sealed_slots: 0,
                codebook: [0; 32],
                run: 0,
            },
            phase: BuildPhase::Ingest,
            cursor: 0,
            iteration: 0,
            training: self.training,
            serving_at_start,
            source_lsn,
        };
        let mut tx = self.store.begin()?;
        Catalog::new(0).store(&mut tx, state.manifest)?;
        tx.stage_build(state)?;
        tx.commit()?;
        Ok(state)
    }
    /// Exactly one bounded source pass. All later phases use persisted canonical slots.
    pub fn ingest_batch<V: AsRef<[f32]>>(
        &mut self,
        state: &mut BuildState,
        source: &mut impl Iterator<Item = (u64, V)>,
    ) -> Result<usize> {
        self.check_build_cursor(state)?;
        if state.phase != BuildPhase::Ingest {
            return Err(phase(format!(
                "clustered build expected Ingest; current phase {:?}",
                state.phase
            )));
        }
        let raw = &mut self.raw;
        let mut next = *state;
        let mut tx = self.store.begin()?;
        let mut count = 0;
        for (row, values) in source.take(state.training.ingest_batch_keys) {
            let values = values.as_ref();
            if values.len() != next.manifest.dimensions {
                return Err(dimension(&tx, next.manifest.dimensions, values.len()));
            }
            raw[..8].copy_from_slice(&row.to_le_bytes());
            for (i, x) in values.iter().enumerate() {
                raw[8 + i * 4..12 + i * 4].copy_from_slice(&x.to_le_bytes());
            }
            codec::write(
                &mut tx,
                next.manifest,
                PageKind::Canonical,
                0,
                next.manifest
                    .slots
                    .checked_mul(raw.len() as u64)
                    .ok_or_else(overflow)?,
                raw,
            )?;
            let mut version = Version::load(&tx, next.manifest, row)?;
            if let Some(serving) = next.serving_at_start {
                version.promote(serving.generation);
            }
            let live = !version.deleted && version.lsn <= next.source_lsn;
            codec::set_visible(&mut tx, next.manifest, next.manifest.slots, live)?;
            version.other = Some((next.manifest.generation, next.manifest.slots));
            version.store(&mut tx, next.manifest, row)?;
            Entry {
                lsn: next.source_lsn,
                next: 0,
                run: 0,
                tail: u64::MAX,
                prepared: false,
            }
            .store(&mut tx, next.manifest, next.manifest.slots)?;
            let ordinal = next.manifest.slots;
            let sample_count = next.training.reservoir_samples as u64;
            let mut hash = blake3::Hasher::new();
            hash.update(b"ivf-reservoir-v1");
            hash.update(&next.training.seed.to_le_bytes());
            hash.update(&next.manifest.partition.to_le_bytes());
            hash.update(&ordinal.to_le_bytes());
            let random = u64::from_le_bytes(
                hash.finalize().as_bytes()[..8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            let choice = if ordinal < sample_count {
                ordinal
            } else {
                random % (ordinal + 1)
            };
            if choice < sample_count {
                codec::write(
                    &mut tx,
                    next.manifest,
                    PageKind::Reservoir,
                    0,
                    choice * 8,
                    &ordinal.to_le_bytes(),
                )?;
            }
            next.manifest.slots += 1;
            count += 1;
        }
        tx.stage_build(next)?;
        tx.commit()?;
        *state = next;
        Ok(count)
    }
    pub fn finish_ingestion(&mut self, state: &mut BuildState) -> Result<()> {
        self.check_build_cursor(state)?;
        if state.phase != BuildPhase::Ingest {
            return Err(phase(format!(
                "clustered build expected Ingest; current phase {:?}",
                state.phase
            )));
        }
        let mut next = *state;
        next.manifest.active_lists =
            next.manifest
                .slots
                .min(u64::from(next.manifest.lists))
                .min(next.training.reservoir_samples as u64) as u32;
        next.phase = BuildPhase::Seed;
        next.cursor = 0;
        let mut tx = self.store.begin()?;
        tx.stage_build(next)?;
        tx.commit()?;
        *state = next;
        Ok(())
    }
    /// One admitted persisted training/assignment batch; caller drives maintenance.
    pub fn advance_build(&mut self, state: &mut BuildState) -> Result<bool> {
        self.check_build_cursor(state)?;
        if state.phase == BuildPhase::Ready {
            return Ok(true);
        }
        if state.phase == BuildPhase::Publish {
            return self.publish_ready(state);
        }
        if state.phase == BuildPhase::Ingest {
            return Err(phase(
                "clustered advance expected Seed, Train, Assign or Ready; current phase Ingest",
            ));
        }
        let mut next = *state;
        let manifest = next.manifest;
        let raw = &mut self.raw;
        let values = &mut self.values;
        let encoded = &mut self.encoded;
        let mut book = if state.phase == BuildPhase::Seed && state.cursor == 0 {
            Codebook::empty(&self.owner, manifest)?
        } else {
            Codebook::load(&self.owner, &self.store, manifest)?
        };
        let mut tx = self.store.begin()?;
        match state.phase {
            BuildPhase::Seed => {
                let end = u64::from(manifest.active_lists).min(
                    next.cursor
                        .saturating_add(next.training.phase_batch_entries as u64),
                );
                for list in next.cursor..end {
                    let sample_count = manifest.slots.min(next.training.reservoir_samples as u64);
                    let mut hash = blake3::Hasher::new();
                    hash.update(b"ivf-seeds-v1");
                    hash.update(&next.training.seed.to_le_bytes());
                    hash.update(&manifest.partition.to_le_bytes());
                    let digest = hash.finalize();
                    let start = u64::from_le_bytes(
                        digest.as_bytes()[..8].try_into().map_err(|_| corrupt())?,
                    ) % sample_count;
                    let mut stride = (u64::from_le_bytes(
                        digest.as_bytes()[8..16].try_into().map_err(|_| corrupt())?,
                    ) % sample_count)
                        .max(1);
                    while gcd(stride, sample_count) != 1 {
                        tx.check_cancel()?;
                        stride = (stride + 1) % sample_count;
                    }
                    let position = (u128::from(start) + u128::from(list) * u128::from(stride))
                        % u128::from(sample_count);
                    let mut saved = [0u8; 8];
                    codec::read(
                        &tx,
                        manifest,
                        PageKind::Reservoir,
                        0,
                        position as u64 * 8,
                        &mut saved,
                    )?;
                    let slot = u64::from_le_bytes(saved);
                    read_vector(&tx, manifest, slot, raw, values)?;
                    book.seed(list as usize, values);
                    codec::set_count(&mut tx, manifest, list as u32, 0)?;
                }
                next.cursor = end;
                next.manifest.codebook = book.store(&mut tx, manifest)?;
                if end == u64::from(manifest.active_lists) {
                    next.phase = if manifest.slots == 0 {
                        BuildPhase::Ready
                    } else {
                        BuildPhase::Train
                    };
                    next.cursor = 0;
                }
            }
            BuildPhase::Train => {
                let sample_count = manifest.slots.min(next.training.reservoir_samples as u64);
                let end = sample_count.min(
                    next.cursor
                        .saturating_add(next.training.phase_batch_entries as u64),
                );
                for slot in next.cursor..end {
                    let mut saved = [0u8; 8];
                    codec::read(&tx, manifest, PageKind::Reservoir, 0, slot * 8, &mut saved)?;
                    let original = u64::from_le_bytes(saved);
                    read_vector(&tx, manifest, original, raw, values)?;
                    book.train(&tx, values)?;
                }
                next.cursor = end;
                next.manifest.codebook = book.store(&mut tx, manifest)?;
                if end == sample_count {
                    next.iteration += 1;
                    next.cursor = 0;
                    if next.iteration == next.training.iterations {
                        next.phase = BuildPhase::Assign;
                    }
                }
            }
            BuildPhase::Assign => {
                let end = manifest.slots.min(
                    next.cursor
                        .saturating_add(next.training.phase_batch_entries as u64),
                );
                for slot in next.cursor..end {
                    let row = read_vector(&tx, manifest, slot, raw, values)?;
                    let list = book.nearest(&tx, values)? as u32;
                    append_list(&mut tx, manifest, list, slot, row, values, encoded)?;
                }
                next.cursor = end;
                if end == manifest.slots {
                    next.manifest.sealed_slots = manifest.slots;
                    next.phase = BuildPhase::Publish;
                }
            }
            BuildPhase::Ingest | BuildPhase::Publish | BuildPhase::Ready => {
                return Err(phase("clustered advance has invalid phase"));
            }
        }
        tx.stage_build(next)?;
        tx.commit()?;
        *state = next;
        if next.phase == BuildPhase::Publish || next.phase == BuildPhase::Ready {
            self.publish_ready(state)
        } else {
            Ok(false)
        }
    }
    fn check_build_cursor(&self, state: &BuildState) -> Result<()> {
        if self.store.build_state(state.manifest.partition)? != Some(*state) {
            return Err(phase(format!(
                "clustered {:?} cursor is not the current durable build cursor",
                state.phase
            )));
        }
        Ok(())
    }
    fn publish_ready(&mut self, state: &mut BuildState) -> Result<bool> {
        let partition = state.manifest.partition;
        if let Some(old) = self.store.manifest(partition)? {
            let c = Catalog::load(&self.store, old)?;
            if c.coalesce_output != 0 {
                // The integration constructor supplies COALESCE_PAGES_PER_STEP.
                // Standalone callers resolve their existing phase allowance in pages.
                let pages = self
                    .policy
                    .coalesce_pages_per_step
                    .unwrap_or(self.training.phase_batch_entries);
                self.advance_coalescing(partition, pages)?;
                return Ok(false);
            }
        }
        let mut next = *state;
        let mut tx = self.store.begin()?;
        // F1 seals the admitted tail and freezes the finite old catalog atomically.
        let mut new_catalog = Catalog::new(next.manifest.slots);
        if let Some(mut old) = tx.manifest(partition)? {
            let mut old_catalog = Catalog::load(&tx, old)?;
            seal_tail(
                &mut tx,
                &self.owner,
                old,
                &mut old_catalog,
                &mut self.raw,
                &mut self.values,
                &mut self.encoded,
            )?;
            old.sealed_slots = old.slots;
            old_catalog.store(&mut tx, old)?;
            if old_catalog.runs != 0 {
                new_catalog.legacy = Some(old);
                new_catalog.legacy_first = old_catalog.first;
                new_catalog.legacy_runs = old_catalog.runs;
                new_catalog.frontier = state.source_lsn;
                new_catalog.switch_lsn = tx.commit_lsn()?;
            }
        }
        new_catalog.store(&mut tx, next.manifest)?;
        next.phase = BuildPhase::Ready;
        tx.stage_build(next)?;
        tx.publish_manifest(next.manifest)?;
        tx.commit()?;
        let stride = self.buffer_stride();
        let mut write = 0;
        for read in (0..self.buffer.len()).step_by(stride) {
            let p = u64::from_le_bytes(
                self.buffer[read..read + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            if p != partition {
                self.buffer.copy_within(read..read + stride, write);
                write += stride;
            }
        }
        self.buffer[write..].fill(0);
        self.buffer.truncate(write);
        *state = next;
        Ok(next.phase == BuildPhase::Ready)
    }
    pub fn build_partition<V: AsRef<[f32]>>(
        &mut self,
        partition: u64,
        mut source: impl Iterator<Item = (u64, V)>,
    ) -> Result<()> {
        let mut state = self.begin_build(partition)?;
        while self.ingest_batch(&mut state, &mut source)? > 0 {}
        self.finish_ingestion(&mut state)?;
        while !self.advance_build(&mut state)? {}
        Ok(())
    }
    pub fn commit_insert(&mut self, vector: Vector<'_>) -> Result<()> {
        if vector.values.len() != self.policy.dimensions {
            return Err(dimension(
                &self.store,
                self.policy.dimensions,
                vector.values.len(),
            ));
        }
        if self.buffer.len() + self.buffer_stride() > self.state.cap_bytes {
            self.seal()?;
        }
        let raw = &mut self.raw;
        raw[..8].copy_from_slice(&vector.row.to_le_bytes());
        for (i, x) in vector.values.iter().enumerate() {
            raw[8 + i * 4..12 + i * 4].copy_from_slice(&x.to_le_bytes());
        }
        let mut manifest = self.store.manifest(vector.partition)?.unwrap_or(Manifest {
            partition: vector.partition,
            generation: 0,
            dimensions: self.policy.dimensions,
            lists: self.policy.lists,
            active_lists: 0,
            slots: 0,
            sealed_slots: 0,
            codebook: [0; 32],
            run: 0,
        });
        let slot = manifest.slots;
        let mut tx = self.store.begin()?;
        codec::write(
            &mut tx,
            manifest,
            PageKind::Canonical,
            0,
            slot.checked_mul(raw.len() as u64).ok_or_else(overflow)?,
            raw,
        )?;
        let mut version = Version::load(&tx, manifest, vector.row)?;
        version.invalidate(&mut tx, manifest)?;
        version.promote(manifest.generation);
        if version.other.is_some_and(|x| x.0 == manifest.generation) {
            version.other = None;
        }
        version.current = Some((manifest.generation, slot));
        version.lsn = tx.commit_lsn()?;
        version.deleted = false;
        version.store(&mut tx, manifest, vector.row)?;
        codec::set_visible(&mut tx, manifest, slot, true)?;
        let mut c = Catalog::load(&tx, manifest)?;
        let sequence = catalog::append_tail(&mut tx, manifest, &mut c, slot)?;
        Entry {
            lsn: tx.commit_lsn()?,
            next: 0,
            run: 0,
            tail: sequence,
            prepared: false,
        }
        .store(&mut tx, manifest, slot)?;
        c.store(&mut tx, manifest)?;
        manifest.slots = manifest.slots.checked_add(1).ok_or_else(overflow)?;
        tx.publish_manifest(manifest)?;
        tx.commit()?;
        self.cache(vector, slot, manifest.generation, sequence);
        Ok(())
    }
    pub fn seal(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let stride = self.buffer_stride();
        // Group the bounded window only. No population-sized run/member collection.
        self.order.clear();
        self.order.extend(0..self.buffer.len() / stride);
        self.order.sort_unstable_by_key(|i| {
            u64::from_le_bytes(self.buffer[i * stride..i * stride + 8].try_into().unwrap())
        });
        let mut tx = self.store.begin()?;
        let mut at = 0;
        while at < self.order.len() {
            let p = u64::from_le_bytes(
                self.buffer[self.order[at] * stride..self.order[at] * stride + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            let mut end = at + 1;
            while end < self.order.len()
                && self.buffer[self.order[end] * stride..self.order[end] * stride + 8]
                    == p.to_le_bytes()
            {
                end += 1;
            }
            let mut m = tx.manifest(p)?.ok_or_else(corrupt)?;
            let mut c = Catalog::load(&tx, m)?;
            let append_existing = c.runs > self.policy.max_delta_runs
                && c.last != c.coalesce_left
                && c.last != c.coalesce_right;
            let id = if append_existing {
                c.last
            } else {
                c.allocate_run()?
            };
            let mut run = if append_existing {
                Run::load(&tx, m, id)?
            } else {
                Run::default()
            };
            let mut run_m = m;
            run_m.run = id;
            let book = if m.active_lists > 0 {
                Some(Codebook::load_values(&self.owner, &tx, m)?)
            } else {
                None
            };
            let mut boundary = m.sealed_slots;
            for &i in &self.order[at..end] {
                let r = &self.buffer[i * stride..(i + 1) * stride];
                let row = u64::from_le_bytes(r[8..16].try_into().map_err(|_| corrupt())?);
                let slot = u64::from_le_bytes(r[16..24].try_into().map_err(|_| corrupt())?);
                let g = u64::from_le_bytes(r[24..32].try_into().map_err(|_| corrupt())?);
                let seq = u64::from_le_bytes(r[32..40].try_into().map_err(|_| corrupt())?);
                if g != m.generation {
                    return Err(phase("clustered seal buffered generation is not serving"));
                }
                for (j, x) in self.values.iter_mut().enumerate() {
                    *x = f32::from_le_bytes(
                        r[40 + j * 4..44 + j * 4]
                            .try_into()
                            .map_err(|_| corrupt())?,
                    );
                }
                let list = if let Some(book) = &book {
                    book.nearest(&tx, &self.values)? as u32
                } else {
                    0
                };
                append_list(
                    &mut tx,
                    run_m,
                    list,
                    slot,
                    row,
                    &self.values,
                    &mut self.encoded,
                )?;
                let mut e = Entry::load(&tx, m, slot)?;
                e.run = id;
                e.store(&mut tx, m, slot)?;
                run.append(&mut tx, m, slot)?;
                c.tail_head = c.tail_head.max(seq.checked_add(1).ok_or_else(overflow)?);
                boundary = boundary.max(slot.checked_add(1).ok_or_else(overflow)?);
            }
            if append_existing {
                run.store(&mut tx, m, id)?;
            } else {
                c.attach(&mut tx, m, id, run)?;
            }
            if c.runs > self.policy.max_delta_runs {
                start_coalesce(&mut tx, m, &mut c)?;
            }
            c.store(&mut tx, m)?;
            m.sealed_slots = boundary;
            tx.publish_manifest(m)?;
            at = end;
        }
        tx.commit()?;
        self.buffer.fill(0);
        self.buffer.clear();
        self.state.sealed_runs += 1;
        Ok(())
    }
    /// The authoritative row mutation and this tombstone must use the same native
    /// transaction at the engine boundary. Slots avoid population-sized reverse maps.
    pub fn delete_slot(&mut self, partition: u64, slot: u64) -> Result<()> {
        let manifest = self
            .store
            .manifest(partition)?
            .ok_or_else(|| phase("clustered Delete partition has no serving generation"))?;
        if slot >= manifest.slots {
            return Err(phase(
                "clustered delete slot is outside the serving generation",
            ));
        }
        let mut tx = self.store.begin()?;
        let row = read_vector(&tx, manifest, slot, &mut self.raw, &mut self.values)?;
        let mut version = Version::load(&tx, manifest, row)?;
        version.invalidate(&mut tx, manifest)?;
        codec::set_visible(&mut tx, manifest, slot, false)?;
        version.lsn = tx.commit_lsn()?;
        version.deleted = true;
        version.store(&mut tx, manifest, row)?;
        let mut tombstone = [0; 16];
        tombstone[..8].copy_from_slice(&slot.to_le_bytes());
        tombstone[8..].copy_from_slice(&version.lsn.to_le_bytes());
        codec::write(
            &mut tx,
            manifest,
            PageKind::Tombstones,
            0,
            slot.checked_mul(16).ok_or_else(overflow)?,
            &tombstone,
        )?;
        tx.commit()
    }
}

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let next = a % b;
        a = b;
        b = next;
    }
    a
}

fn start_coalesce(tx: &mut impl WritePages, m: Manifest, c: &mut Catalog) -> Result<()> {
    if c.coalesce_output != 0 || c.runs < 2 {
        return Ok(());
    }
    c.coalesce_left = c.first;
    let left = Run::load(tx, m, c.first)?;
    c.coalesce_right = left.next;
    if c.coalesce_right == 0 {
        return Err(corrupt());
    }
    c.coalesce_output = c.allocate_run()?;
    c.coalesce_list = 0;
    Ok(())
}

/// F1 seals only the admitted tail, not a whole historical run/population.
fn seal_tail(
    tx: &mut impl WritePages,
    owner: &MemoryBudgetOwner,
    m: Manifest,
    c: &mut Catalog,
    raw: &mut [u8],
    values: &mut [f32],
    encoded: &mut [u8],
) -> Result<()> {
    if c.tail_head == c.tail_end {
        return Ok(());
    }
    let id = c.allocate_run()?;
    let mut run = Run::default();
    let mut view = m;
    view.run = id;
    let book = if m.active_lists > 0 {
        Some(Codebook::load_values(owner, tx, m)?)
    } else {
        None
    };
    for seq in c.tail_head..c.tail_end {
        tx.check_cancel()?;
        let slot = catalog::tail_slot(tx, m, seq)?;
        let row = read_vector(tx, m, slot, raw, values)?;
        let list = if let Some(book) = &book {
            book.nearest(tx, values)? as u32
        } else {
            0
        };
        append_list(tx, view, list, slot, row, values, encoded)?;
        let mut entry = Entry::load(tx, m, slot)?;
        entry.run = id;
        entry.store(tx, m, slot)?;
        run.append(tx, m, slot)?;
    }
    c.attach(tx, m, id, run)?;
    c.tail_head = c.tail_end;
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RemapProgress {
    pub entries_examined: u64,
    pub entries_remapped: u64,
    pub run_retired: bool,
    pub complete: bool,
}

impl<S: PageStore> ClusteredIndex<S> {
    /// At most entry_limit source entries, with a durable entry cursor. Prepared
    /// output remains invisible until one atomic catalog retirement commit.
    pub fn advance_remap(&mut self, partition: u64, entry_limit: usize) -> Result<RemapProgress> {
        if entry_limit == 0 {
            return Err(phase(
                "clustered Remap requires a positive admitted entry batch",
            ));
        }
        let Some(mut m) = self.store.manifest(partition)? else {
            return Err(phase("clustered Remap has no serving generation"));
        };
        let mut c = Catalog::load(&self.store, m)?;
        let Some(old) = c.legacy else {
            return Ok(RemapProgress {
                complete: true,
                ..RemapProgress::default()
            });
        };
        let source_run = Run::load(&self.store, old, c.legacy_first)?;
        let book = if m.active_lists > 0 {
            Some(Codebook::load_values(&self.owner, &self.store, m)?)
        } else {
            None
        };
        let mut tx = self.store.begin()?;
        if c.remap_output == 0 {
            c.remap_output = c.allocate_run()?;
            c.remap_slot = source_run.first;
            c.remap_examined = 0;
            Run::default().store(&mut tx, m, c.remap_output)?;
        }
        let mut prepared = Run::load(&tx, m, c.remap_output)?;
        let mut view = m;
        view.run = c.remap_output;
        let mut progress = RemapProgress::default();
        let mut visibility = [0; super::storage::PAGE_BYTES];
        let mut loaded = None;
        while c.remap_examined < source_run.entries
            && progress.entries_examined < entry_limit as u64
        {
            tx.check_cancel()?;
            let slot = c.remap_slot;
            let e = Entry::load(&tx, old, slot)?;
            if e.lsn > c.frontier
                && e.lsn <= c.switch_lsn
                && codec::visible(&tx, old, slot, &mut visibility, &mut loaded)?
            {
                let row = read_vector(&tx, old, slot, &mut self.raw, &mut self.values)?;
                let target_slot = m.slots;
                codec::write(
                    &mut tx,
                    m,
                    PageKind::Canonical,
                    0,
                    target_slot
                        .checked_mul(self.raw.len() as u64)
                        .ok_or_else(overflow)?,
                    &self.raw,
                )?;
                let mut version = Version::load(&tx, m, row)?;
                // Invalidation also reaches prepared copies when a row changes before retirement.
                if version.lsn > e.lsn || version.deleted {
                    c.remap_slot = e.next;
                    c.remap_examined += 1;
                    progress.entries_examined += 1;
                    continue;
                }
                version.promote(m.generation);
                version.other = Some((m.generation, target_slot));
                version.store(&mut tx, m, row)?;
                codec::set_visible(&mut tx, m, target_slot, true)?;
                Entry {
                    lsn: e.lsn,
                    next: 0,
                    run: c.remap_output,
                    tail: u64::MAX,
                    prepared: true,
                }
                .store(&mut tx, m, target_slot)?;
                let list = if let Some(book) = &book {
                    book.nearest(&tx, &self.values)? as u32
                } else {
                    0
                };
                append_list(
                    &mut tx,
                    view,
                    list,
                    target_slot,
                    row,
                    &self.values,
                    &mut self.encoded,
                )?;
                prepared.append(&mut tx, m, target_slot)?;
                m.slots = m.slots.checked_add(1).ok_or_else(overflow)?;
                progress.entries_remapped += 1;
            }
            c.remap_slot = e.next;
            c.remap_examined += 1;
            progress.entries_examined += 1;
        }
        prepared.store(&mut tx, m, c.remap_output)?;
        if c.remap_examined == source_run.entries
            && (prepared.entries == 0 || c.runs <= self.policy.max_delta_runs)
        {
            if prepared.entries != 0 {
                c.attach(&mut tx, m, c.remap_output, prepared)?;
                if c.runs > self.policy.max_delta_runs {
                    start_coalesce(&mut tx, m, &mut c)?;
                }
            }
            c.legacy_first = source_run.next;
            c.legacy_runs = c.legacy_runs.checked_sub(1).ok_or_else(corrupt)?;
            c.remap_slot = 0;
            c.remap_examined = 0;
            c.remap_output = 0;
            progress.run_retired = true;
            if c.legacy_runs == 0 {
                c.legacy = None;
                c.legacy_first = 0;
                progress.complete = true;
            }
        }
        c.store(&mut tx, m)?;
        tx.publish_manifest(m)?;
        tx.commit()?;
        Ok(progress)
    }

    /// Bounded directory/link work only. The payload pages and old snapshots are
    /// reused; no historical vector is re-encoded by physical coalescing.
    /// page_limit includes conservative control-page overlap. A call too small
    /// for one atomic directory unit is refused before beginning a transaction.
    pub fn advance_coalescing(&mut self, partition: u64, page_limit: usize) -> Result<bool> {
        let Some(m) = self.store.manifest(partition)? else {
            return Ok(true);
        };
        let mut c = Catalog::load(&self.store, m)?;
        if c.coalesce_output == 0 {
            return Ok(true);
        }
        // Four descriptor/control reads, three control writes and one checked
        // directory/link unit (<=six reads/writes) fit this format-derived bound.
        const CONTROL_PAGES: usize = 20;
        const DIRECTORY_PAGES: usize = 6;
        let units = page_limit.saturating_sub(CONTROL_PAGES) / DIRECTORY_PAGES;
        if units == 0 {
            return Err(phase(format!(
                "clustered Coalesce needs an admitted page unit of at least {}; supplied {}",
                CONTROL_PAGES + DIRECTORY_PAGES,
                page_limit
            )));
        }
        let left = Run::load(&self.store, m, c.coalesce_left)?;
        let right = Run::load(&self.store, m, c.coalesce_right)?;
        let mut tx = self.store.begin()?;
        let mut a = m;
        a.run = c.coalesce_left;
        let mut b = m;
        b.run = c.coalesce_right;
        let mut out = m;
        out.run = c.coalesce_output;
        let lists = m.active_lists.max(1);
        let end = lists.min(
            c.coalesce_list
                .saturating_add(u32::try_from(units).unwrap_or(u32::MAX)),
        );
        for list in c.coalesce_list..end {
            tx.check_cancel()?;
            let x = codec::directory(&tx, a, list)?;
            let y = codec::directory(&tx, b, list)?;
            if x.count != 0 && y.count != 0 {
                codec::set_next_list_page(&mut tx, m, list, x.tail, y.head)?;
            }
            let joined = codec::Directory {
                count: x.count.checked_add(y.count).ok_or_else(overflow)?,
                head: if x.count == 0 { y.head } else { x.head },
                tail: if y.count == 0 { x.tail } else { y.tail },
            };
            codec::set_directory(&mut tx, out, list, joined)?;
        }
        c.coalesce_list = end;
        let complete = end == lists;
        if complete {
            if left.entries != 0 && right.entries != 0 {
                let mut e = Entry::load(&tx, m, left.last)?;
                e.next = right.first;
                e.store(&mut tx, m, left.last)?;
            }
            let merged = Run {
                first: if left.entries == 0 {
                    right.first
                } else {
                    left.first
                },
                last: if right.entries == 0 {
                    left.last
                } else {
                    right.last
                },
                entries: left
                    .entries
                    .checked_add(right.entries)
                    .ok_or_else(overflow)?,
                next: right.next,
            };
            merged.store(&mut tx, m, c.coalesce_output)?;
            c.first = c.coalesce_output;
            if c.last == c.coalesce_right {
                c.last = c.coalesce_output;
            }
            c.runs = c.runs.checked_sub(1).ok_or_else(corrupt)?;
            c.coalesce_left = 0;
            c.coalesce_right = 0;
            c.coalesce_output = 0;
            c.coalesce_list = 0;
            if c.runs > self.policy.max_delta_runs {
                start_coalesce(&mut tx, m, &mut c)?;
            }
        }
        let restored = c.coalesce_output == 0;
        c.store(&mut tx, m)?;
        tx.commit()?;
        Ok(restored)
    }
}
