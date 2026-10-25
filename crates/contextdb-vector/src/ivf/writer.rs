use super::{
    book::{Codebook, append_list, read_vector},
    codec,
    memory::{Invoice, corrupt, overflow},
    storage::{BuildPhase, BuildState, Manifest, PageKind, PageStore, ReadPages, WritePages},
    types::{BufferState, Policy, TrainingPolicy, Vector},
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
    _workspace_invoice: Invoice,
}
impl<S: PageStore> ClusteredIndex<S> {
    pub fn new(
        store: S,
        policy: Policy,
        training: TrainingPolicy,
        owner: MemoryBudgetOwner,
    ) -> Result<Self> {
        if training.reservoir_samples == 0
            || training.iterations == 0
            || training.ingest_batch_keys == 0
            || training.phase_batch_entries == 0
        {
            return Err(corrupt());
        }
        let raw_bytes = policy
            .dimensions
            .checked_mul(4)
            .and_then(|n| n.checked_add(8))
            .ok_or_else(overflow)?;
        let records = policy.buffer_cap_bytes / (24 + policy.dimensions * 4);
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
            .checked_add(super::storage::PAGE_BYTES * 2)
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
            _workspace_invoice: invoice,
        };
        let mut after = None;
        while let Some(tail) = out.store.next_unsealed(after)? {
            let row = read_vector(
                &out.store,
                tail.manifest,
                tail.slot,
                &mut out.raw,
                &mut out.values,
            )?;
            if out.buffer.len() + out.buffer_stride() > out.state.cap_bytes {
                out.seal()?;
                read_vector(
                    &out.store,
                    tail.manifest,
                    tail.slot,
                    &mut out.raw,
                    &mut out.values,
                )?;
            }
            out.buffer
                .extend_from_slice(&tail.manifest.partition.to_le_bytes());
            out.buffer.extend_from_slice(&row.to_le_bytes());
            out.buffer.extend_from_slice(&tail.slot.to_le_bytes());
            out.buffer.extend_from_slice(&out.raw[8..]);
            out.state.high_water_bytes = out.state.high_water_bytes.max(out.buffer.len());
            after = Some((tail.manifest.partition, tail.slot));
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
        24 + self.policy.dimensions * 4
    }
    fn cache(&mut self, vector: Vector<'_>, slot: u64) {
        self.buffer
            .extend_from_slice(&vector.partition.to_le_bytes());
        self.buffer.extend_from_slice(&vector.row.to_le_bytes());
        self.buffer.extend_from_slice(&slot.to_le_bytes());
        for x in vector.values {
            self.buffer.extend_from_slice(&x.to_le_bytes());
        }
        self.state.high_water_bytes = self.state.high_water_bytes.max(self.buffer.len());
    }
    pub fn begin_build(&mut self, partition: u64) -> Result<BuildState> {
        self.seal()?;
        let generation = self
            .store
            .manifest(partition)?
            .map_or(1, |m| m.generation + 1);
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
            },
            phase: BuildPhase::Ingest,
            cursor: 0,
            iteration: 0,
            training: self.training,
        };
        let mut tx = self.store.begin()?;
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
        if state.phase != BuildPhase::Ingest {
            return Err(corrupt());
        }
        let raw = &mut self.raw;
        let mut next = *state;
        let mut tx = self.store.begin()?;
        let mut count = 0;
        for (row, values) in source.take(state.training.ingest_batch_keys) {
            let values = values.as_ref();
            if values.len() != next.manifest.dimensions {
                return Err(corrupt());
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
            codec::set_visible(&mut tx, next.manifest, next.manifest.slots, true)?;
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
        if state.phase != BuildPhase::Ingest {
            return Err(corrupt());
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
        if state.phase == BuildPhase::Ready {
            return Ok(true);
        }
        if state.phase == BuildPhase::Ingest {
            return Err(corrupt());
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
                    next.phase = BuildPhase::Ready;
                }
            }
            BuildPhase::Ingest | BuildPhase::Ready => return Err(corrupt()),
        }
        tx.stage_build(next)?;
        if next.phase == BuildPhase::Ready {
            tx.publish_manifest(next.manifest)?;
        }
        tx.commit()?;
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
            return Err(corrupt());
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
        codec::set_visible(&mut tx, manifest, slot, true)?;
        manifest.slots += 1;
        tx.publish_manifest(manifest)?;
        tx.commit()?;
        self.cache(vector, slot);
        Ok(())
    }
    pub fn seal(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let stride = self.buffer_stride();
        let records = self.buffer.len() / stride;
        self.order.clear();
        self.order.extend(0..records);
        self.order.sort_unstable_by_key(|i| {
            u64::from_le_bytes(self.buffer[i * stride..i * stride + 8].try_into().unwrap())
        });
        let order = &self.order;
        let values = &mut self.values;
        let encoded = &mut self.encoded;
        let mut at = 0;
        let mut tx = self.store.begin()?;
        while at < order.len() {
            let first = order[at] * stride;
            let partition = u64::from_le_bytes(
                self.buffer[first..first + 8]
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            let mut end = at + 1;
            while end < order.len()
                && self.buffer[order[end] * stride..order[end] * stride + 8]
                    == self.buffer[first..first + 8]
            {
                end += 1;
            }
            let mut manifest = tx.manifest(partition)?.ok_or_else(corrupt)?;
            let book = if manifest.active_lists > 0 {
                Some(Codebook::load(&self.owner, &tx, manifest)?)
            } else {
                None
            };
            for i in &order[at..end] {
                let record = &self.buffer[i * stride..(i + 1) * stride];
                let row = u64::from_le_bytes(record[8..16].try_into().map_err(|_| corrupt())?);
                let slot = u64::from_le_bytes(record[16..24].try_into().map_err(|_| corrupt())?);
                for (j, x) in values.iter_mut().enumerate() {
                    *x = f32::from_le_bytes(
                        record[24 + j * 4..28 + j * 4]
                            .try_into()
                            .map_err(|_| corrupt())?,
                    );
                }
                let list = if let Some(book) = &book {
                    book.nearest(&tx, values)? as u32
                } else {
                    0
                };
                append_list(&mut tx, manifest, list, slot, row, values, encoded)?;
            }
            manifest.sealed_slots = manifest.slots;
            tx.publish_manifest(manifest)?;
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
        let manifest = self.store.manifest(partition)?.ok_or_else(corrupt)?;
        if slot >= manifest.slots {
            return Err(corrupt());
        }
        let mut tx = self.store.begin()?;
        codec::set_visible(&mut tx, manifest, slot, false)?;
        codec::write(
            &mut tx,
            manifest,
            PageKind::Tombstones,
            0,
            slot.checked_mul(8).ok_or_else(overflow)?,
            &slot.to_le_bytes(),
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
