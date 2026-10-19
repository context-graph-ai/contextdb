//! Root-owned vector serving supplier for one operation.
//!
//! The membership bridge captures an operation's root once and authenticates
//! its catalog and summary on that read. This module turns that exact read --
//! with the writer snapshot or committed source that owns it -- into the
//! layer and suffix loaders the vector store prices and preloads for that one
//! operation. Nothing here begins another read, recaptures a root, installs a
//! fresh tail, or enters a registry or cache: every loader holds the captured
//! lease, reads through in-read helpers, and charges the operation's own
//! funder through the existing caller-reservation protocol.
use super::*;
use crate::persistence::{RetainedReadSource};
use contextdb_vector::VectorCopyAdmission;
use contextdb_vector::store::{
    CapturedVectorGraphLayer, CapturedVectorServingSelection, CapturedVectorSuffix,
    CapturedVectorSuffixLoader, CapturedVectorSuffixSource, LoadedCapturedVectorSuffix,
    RawVectorInspectionSnapshot, ResolvedVectorPolicy,
};

fn refusal(reason: &str) -> Error {
    Error::StoreCorrupted {
        path: "vector_partition_generations".into(),
        reason: reason.into(),
    }
}

fn checkpoint(request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool) -> Result<()> {
    if request(DormantVectorLoadRequest::Checkpoint) {
        Ok(())
    } else {
        Err(Error::ReadCancelled)
    }
}

fn reserve(request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool, bytes: usize) -> Result<()> {
    if bytes == 0 || request(DormantVectorLoadRequest::Reserve(bytes)) {
        Ok(())
    } else {
        Err(Error::ReadCancelled)
    }
}

enum CapturedRootHolder {
    Writer(WriterSnapshotHandle),
    Retained(Arc<RetainedReadSource>),
}

/// One operation's captured root: the exact read the membership bridge
/// authenticated and the owner that keeps its storage version alive. Dropping
/// the last holder ends the read and releases the writer snapshot or source.
pub(crate) struct CapturedVectorRootLease {
    read: Arc<redb::ReadTransaction>,
    holder: CapturedRootHolder,
}

impl CapturedVectorRootLease {
    pub(crate) fn writable(
        read: Arc<redb::ReadTransaction>,
        writer: WriterSnapshotHandle,
    ) -> Result<Self> {
        if read.storage_snapshot_identity().user_root != writer.storage_identity().user_root {
            return Err(refusal("serving read escaped its operation physical root"));
        }
        Ok(Self {
            read,
            holder: CapturedRootHolder::Writer(writer),
        })
    }

    pub(crate) fn retained(
        read: Arc<redb::ReadTransaction>,
        source: Arc<RetainedReadSource>,
    ) -> Result<Self> {
        if read.storage_snapshot_identity() != source.storage_identity() {
            return Err(refusal("serving read escaped its retained physical root"));
        }
        Ok(Self {
            read,
            holder: CapturedRootHolder::Retained(source),
        })
    }

    /// Authoritative erasure published after capture refuses every later
    /// read of the erased table through this root with its typed refusal.
    fn check_scope(&self, table: &str) -> Result<()> {
        match &self.holder {
            CapturedRootHolder::Writer(writer) => writer.check_table_scope(table),
            CapturedRootHolder::Retained(source) => {
                if self.read.storage_snapshot_identity() != source.storage_identity() {
                    return Err(refusal("serving read escaped its retained physical root"));
                }
                Ok(())
            }
        }
    }

    /// Run one in-read step on the captured read after its scope check.
    pub(crate) fn with_read<T>(
        &self,
        index: &VectorIndexRef,
        read: impl FnOnce(&redb::ReadTransaction) -> Result<T>,
    ) -> Result<T> {
        self.check_scope(&index.table)?;
        read(&self.read)
    }

    /// Journal records past the writer snapshot's own LSN are never part of
    /// this root's suffix. A committed image's root holds nothing newer.
    pub(crate) fn through_lsn(&self) -> Option<Lsn> {
        match &self.holder {
            CapturedRootHolder::Writer(writer) => Some(writer.lsn),
            CapturedRootHolder::Retained(_) => None,
        }
    }

    /// Authenticate the read against the actual sealed lease and its original
    /// holder. Writer aliases preserve the existing equivalent-user-root rule.
    pub(crate) fn paged_expected_root(&self)
        -> Result<(redb::StorageSnapshotIdentity, SnapshotId, Lsn)> {
        match &self.holder {
            CapturedRootHolder::Writer(writer) => {
                let expected = writer.storage_identity();
                if self.read.storage_snapshot_identity().user_root != expected.user_root {
                    return Err(refusal("captured lease no longer matches its writer holder"));
                }
                Ok((self.read.storage_snapshot_identity(), writer.snapshot, writer.lsn))
            }
            CapturedRootHolder::Retained(source) => {
                if self.read.storage_snapshot_identity() != source.storage_identity() {
                    return Err(refusal("captured lease no longer matches its registered image"));
                }
                source.vector_physical_frontier()
            }
        }
    }

    /// Paged readers of a captured layer reopen tables on this same read and
    /// keep this lease alive while any of them is open.
    pub(crate) fn paged_root_capture(
        self: &Arc<Self>,
    ) -> crate::persistence::vector_paged::PagedRootCapture {
        let lease = Arc::clone(self);
        Arc::new(move || {
            let captured = crate::persistence::vector_paged::CapturedPagedRoot {
                read: Arc::clone(&lease.read),
                holder: crate::persistence::vector_paged::PagedRootHolder::Captured(Arc::clone(&lease)),
            };
            captured.authenticate()?;
            Ok(captured)
        })
    }
}

/// The identity the membership bridge reports for a root catalog descriptor.
fn catalog_identity(descriptor: VectorPartitionGenerationDescriptor) -> VectorGraphGeneration {
    VectorGraphGeneration {
        generation_id: descriptor.generation_id,
        covered_tx: descriptor.covered_tx,
        covered_lsn: descriptor.covered_lsn,
        durable_bytes: descriptor.durable_bytes,
        policy_revision: descriptor.policy_revision,
        hnsw_m: descriptor.hnsw_m,
        hnsw_ef_construction: descriptor.hnsw_ef_construction,
    }
}

/// Writable admission for one sealed layer: its resident graph plus the
/// encoded payload that coexists with it during decode.
fn writable_admission(
    identity: VectorGraphGeneration,
    resident_bytes: u64,
) -> Result<DormantVectorGraphGeneration> {
    let resident = DurableVectorGenerationLoader::descriptor_usize(
        resident_bytes,
        "captured generation resident byte declaration",
    )?;
    let durable = DurableVectorGenerationLoader::descriptor_usize(
        identity.durable_bytes,
        "captured generation durable byte declaration",
    )?;
    if resident == 0 || durable == 0 {
        return Err(refusal(
            "captured serving layer has no saved resident or durable size",
        ));
    }
    let load = DurableVectorGenerationLoader::checked_add(
        resident,
        durable,
        "captured graph load admission",
    )?;
    Ok(DormantVectorGraphGeneration {
        identity,
        resident_bytes: resident,
        load_bytes: load,
        reclaim_bytes: load,
    })
}

#[derive(Clone, Copy)]
struct BoundBase {
    generation_id: u64,
    covered_tx: TxId,
    covered_lsn: Lsn,
}

impl BoundBase {
    fn of(identity: VectorGraphGeneration) -> Self {
        Self {
            generation_id: identity.generation_id,
            covered_tx: identity.covered_tx,
            covered_lsn: identity.covered_lsn,
        }
    }
}

/// The serving identities the vector store froze, received once through
/// `bind_serving_frontier`. A dormant layer bound afterwards takes its role
/// from these identities, never from the order of its bind call.
#[derive(Clone, Copy)]
struct FrozenFrontier {
    base: Option<VectorGraphGeneration>,
    change: Option<VectorGraphGeneration>,
}

#[derive(Default)]
struct Bindings {
    /// Set on the first frontier call, whatever its outcome: the frontier is
    /// offered exactly once per operation.
    frontier_offered: bool,
    /// Set only once the offered frontier authenticated at this root.
    frontier: Option<FrozenFrontier>,
    base_bound: bool,
    change_bound: bool,
}

/// The operation-owned supplier behind one captured serving selection. It
/// retains the captured root, the root catalog the bridge authenticated, the
/// column declaration at that root and the operation's funder. It is never
/// stored outside the selection that owns it.
struct CapturedServingSupplier {
    lease: Arc<CapturedVectorRootLease>,
    partition: VectorPartitionRef,
    catalog: Option<(
        VectorPartitionGenerationDescriptor,
        Option<VectorPartitionGenerationDescriptor>,
    )>,
    captured_catalog: bool,
    snapshot_tx: TxId,
    snapshot_inserts: Option<usize>,
    dimension: usize,
    quantization: VectorQuantization,
    layout: VectorIndexLayout,
    policy: ResolvedVectorPolicy,
    funder: Arc<dyn MemoryBudget>,
    bindings: parking_lot::Mutex<Bindings>,
    _control: VectorCopyAdmission,
}

impl CapturedServingSupplier {
    /// The saved residency of a frozen identity, as this root's catalog
    /// names it. Writable registry descriptors already carry the saved
    /// value; a committed image's frozen placeholders take it from here.
    fn saved_resident_bytes(
        &self,
        descriptor: &DormantVectorGraphGeneration,
        is_base: bool,
    ) -> Result<u64> {
        let slot = self
            .catalog
            .and_then(|(base, change)| if is_base { Some(base) } else { change });
        if let Some(saved) = slot
            && saved.generation_id == descriptor.identity.generation_id
        {
            let identity = catalog_identity(saved);
            if identity.covered_tx != descriptor.identity.covered_tx
                || identity.covered_lsn != descriptor.identity.covered_lsn
                || identity.durable_bytes != descriptor.identity.durable_bytes
            {
                return Err(refusal(
                    "frozen serving layer disagrees with the captured catalog entry it names",
                ));
            }
            return Ok(saved.resident_bytes);
        }
        // A current-root predecessor the registry kept serving after it
        // refused a newer install: its saved writable admission is the one it
        // was registered with. Placeholder admissions have nothing to keep.
        let durable = usize::try_from(descriptor.identity.durable_bytes).unwrap_or(usize::MAX);
        if descriptor.resident_bytes <= 1
            || descriptor.load_bytes != descriptor.resident_bytes.saturating_add(durable)
        {
            return Err(refusal(
                "frozen serving layer has no saved residency at the captured root",
            ));
        }
        Ok(descriptor.resident_bytes as u64)
    }

    fn loader_control(
        &self,
        owner: &mut VectorCopyAdmission,
        bytes: usize,
    ) -> Result<VectorCopyAdmission> {
        let mut control = owner.scratch();
        control.reserve_for(
            bytes
                .saturating_add(self.partition.index.table.len())
                .saturating_add(self.partition.index.column.len())
                .saturating_add(self.partition.partition_key.estimated_bytes()),
            "vector",
            "captured_serving_loader",
            "Reduce the selected scope or raise the executing reader's memory limit.",
        )?;
        Ok(control)
    }

    fn layer(
        &self,
        descriptor: DormantVectorGraphGeneration,
        is_base: bool,
        base: BoundBase,
        owner: &mut VectorCopyAdmission,
    ) -> Result<CapturedVectorGraphLayer> {
        let mut control = self.loader_control(owner, std::mem::size_of::<CapturedLayerLoader>()
            .saturating_add(2 * std::mem::size_of::<usize>()))?;
        control.reserve(self.layout.partition_key_columns.len() * std::mem::size_of::<String>()
            + self.layout.partition_key_columns.iter().map(|name| name.len()).sum::<usize>())?;
        let loader: Arc<dyn DormantVectorGraphLoader> = Arc::new(CapturedLayerLoader {
            source: DurableVectorGenerationSource::CapturedRoot(Arc::clone(&self.lease)),
            lease: Arc::clone(&self.lease),
            partition: self.partition.clone(),
            descriptor,
            is_base,
            base,
            dimension: self.dimension,
            quantization: self.quantization,
            layout: self.layout.clone(),
            funder: Arc::clone(&self.funder),
            _control: control,
        });
        Ok(CapturedVectorGraphLayer { descriptor, loader })
    }
}

impl CapturedVectorSuffixSource for CapturedServingSupplier {
    fn bind_serving_layer(
        &self,
        descriptor: &DormantVectorGraphGeneration,
        owner: &mut VectorCopyAdmission,
    ) -> Result<CapturedVectorGraphLayer> {
        self.lease.check_scope(&self.partition.index.table)?;
        if self.captured_catalog {
            // An older operation serves only its own root catalog.
            return Err(refusal(
                "a captured-catalog serving selection never binds registry layers",
            ));
        }
        // The vector store hands over its frozen frontier first. Each dormant
        // descriptor is then classified by exact identity against it, so a
        // dormant change over a resident base binds as a change, and a
        // change always loads under the exact frozen base it extends.
        let mut bindings = self.bindings.lock();
        let Some(frontier) = bindings.frontier else {
            return Err(refusal(
                "serving layer bound before its frozen serving frontier",
            ));
        };
        let names_base = frontier.base == Some(descriptor.identity);
        let names_change = frontier.change == Some(descriptor.identity);
        let is_base = match (names_base, names_change) {
            (true, false) => true,
            (false, true) => false,
            (true, true) => {
                return Err(refusal(
                    "frozen serving frontier names one generation as both base and change",
                ));
            }
            (false, false) => {
                return Err(refusal(
                    "dormant serving layer is not part of the frozen serving frontier",
                ));
            }
        };
        if (is_base && bindings.base_bound) || (!is_base && bindings.change_bound) {
            return Err(refusal("dormant serving layer bound twice"));
        }
        let Some(base_identity) = frontier.base else {
            return Err(refusal("serving frontier names a change without its base"));
        };
        let base = BoundBase::of(base_identity);
        if !is_base
            && (descriptor.identity.covered_tx < base.covered_tx
                || descriptor.identity.covered_lsn < base.covered_lsn)
        {
            return Err(refusal("frozen change layer precedes its base"));
        }
        let saved = self.saved_resident_bytes(descriptor, is_base)?;
        let admission = writable_admission(descriptor.identity, saved)?;
        let layer = self.layer(admission, is_base, base, owner)?;
        if is_base {
            bindings.base_bound = true;
        } else {
            bindings.change_bound = true;
        }
        Ok(layer)
    }

    fn bind_serving_frontier(
        &self,
        base: Option<VectorGraphGeneration>,
        change: Option<VectorGraphGeneration>,
        owner: &mut VectorCopyAdmission,
    ) -> Result<CapturedVectorSuffix> {
        self.lease.check_scope(&self.partition.index.table)?;
        let mut bindings = self.bindings.lock();
        if std::mem::replace(&mut bindings.frontier_offered, true) {
            return Err(refusal("serving frontier bound twice"));
        }
        if bindings.base_bound || bindings.change_bound {
            return Err(refusal(
                "dormant serving layers bound before the frozen serving frontier",
            ));
        }
        if change.is_some() && base.is_none() {
            return Err(refusal("serving frontier names a change without its base"));
        }
        if self.captured_catalog {
            let catalog = self
                .catalog
                .map(|(base, change)| (catalog_identity(base), change.map(catalog_identity)));
            let named = base.map(|base| (base, change));
            if catalog != named {
                return Err(refusal(
                    "captured-catalog serving frontier differs from its root catalog",
                ));
            }
        }
        let frontier = change.or(base);
        let (covered_tx, covered_lsn) = frontier.map_or((TxId(0), Lsn(0)), |frontier| {
            (frontier.covered_tx, frontier.covered_lsn)
        });
        if covered_tx > self.snapshot_tx {
            return Err(refusal(
                "serving frontier is newer than the requested snapshot",
            ));
        }
        // One saved summary key at this root authenticates the frontier and
        // supplies the suffix's minimum admission. A frontier the summary does
        // not represent, including an absent catalog whose summary moved past
        // zero, is refused rather than estimated.
        let saved_inserts = self.lease.with_read(&self.partition.index, |read| {
            RedbPersistence::vector_suffix_saved_inserts_in_read(read, &self.partition, covered_tx)
        })?;
        // Keep the saved-summary authentication above. Its live count only
        // describes this root's current snapshot; historical counts were
        // streamed from the same root's retained membership intervals.
        let saved_inserts = self.snapshot_inserts.unwrap_or(saved_inserts);
        let control = self.loader_control(owner, std::mem::size_of::<CapturedSuffixLoader>())?;
        bindings.frontier = Some(FrozenFrontier { base, change });
        let loader: Arc<dyn CapturedVectorSuffixLoader> = Arc::new(CapturedSuffixLoader {
            lease: Arc::clone(&self.lease),
            partition: self.partition.clone(),
            covered_lsn,
            saved_inserts,
            admitted_load: 0,
            dimension: self.dimension,
            quantization: self.quantization,
            policy: self.policy,
            funder: Arc::clone(&self.funder),
            _control: control,
        });
        Ok(CapturedVectorSuffix {
            resident_bytes: 0,
            load_bytes: 0,
            reclaim_bytes: 0,
            loader,
        })
    }
}

/// Build the root-owned serving selection for one captured root.
pub(crate) fn captured_serving_selection(
    lease: CapturedVectorRootLease,
    partition: &VectorPartitionRef,
    generations: Option<(
        VectorPartitionGenerationDescriptor,
        Option<VectorPartitionGenerationDescriptor>,
    )>,
    inspection: RawVectorInspectionSnapshot,
    snapshot: SnapshotId,
    owner: &mut VectorCopyAdmission,
) -> Result<CapturedVectorServingSelection> {
    let historical = match &lease.holder {
        CapturedRootHolder::Writer(writer) => snapshot != writer.snapshot,
        CapturedRootHolder::Retained(source) => !source.ordered_prefix_snapshot_is_current(snapshot),
    };
    let captured_catalog = inspection.captured_catalog || historical;
    let mut control = owner.scratch();
    control.reserve_for(
        std::mem::size_of::<CapturedServingSupplier>()
            .saturating_add(std::mem::size_of::<CapturedVectorRootLease>())
            .saturating_add(partition.index.table.len().saturating_mul(2))
            .saturating_add(partition.index.column.len().saturating_mul(2))
            .saturating_add(partition.partition_key.estimated_bytes())
            .saturating_add(4 * std::mem::size_of::<usize>()),
        "vector",
        "captured_serving_supplier",
        "Reduce the selected scope or raise the executing reader's memory limit.",
    )?;
    let lease = Arc::new(lease);
    if captured_catalog {
        let named = generations.map_or((None, None), |(base, change)| {
            (Some(catalog_identity(base)), change.map(catalog_identity))
        });
        if named != (inspection.base, inspection.change) {
            return Err(refusal(
                "captured serving inspection disagrees with its root catalog",
            ));
        }
    }
    // The declaration in force at this root, read from this root.
    // Decode scratch ends here; it is not retained once per supplier.
    // The codec separately admits the table metadata it actually decodes.
    let layout = {
        let mut scratch = owner.scratch();
        scratch.reserve(4096)?;
        lease.with_read(&partition.index, |read| {
            RedbPersistence::vector_layout_in_read(read, &partition.index, &mut control)
        })?
    };
    let policy = layout.resolve_policy(inspection.summary.live_entries, 1);
    let supplier = Arc::new(CapturedServingSupplier {
        lease: Arc::clone(&lease),
        partition: partition.clone(),
        catalog: generations,
        captured_catalog,
        snapshot_tx: TxId::from_snapshot(snapshot),
        snapshot_inserts: historical.then_some(inspection.pending_inserts),
        dimension: layout.dimension,
        quantization: layout.quantization,
        policy,
        layout,
        funder: owner.funder(),
        bindings: parking_lot::Mutex::new(Bindings::default()),
        _control: control,
    });
    // An older logical snapshot also serves its captured catalog when its
    // physical root is the current publication. A current snapshot uses the
    // publication's chain, frozen by the vector store and handed over once
    // through `bind_serving_frontier`; its dormant layers then bind through
    // `bind_serving_layer`.
    let (base, change) = match (captured_catalog, generations) {
        (true, Some((base, change))) => {
            let base_bound = BoundBase::of(catalog_identity(base));
            let base_layer = supplier.layer(
                writable_admission(catalog_identity(base), base.resident_bytes)?,
                true,
                base_bound,
                owner,
            )?;
            let change_layer = change
                .map(|change| {
                    supplier.layer(
                        writable_admission(catalog_identity(change), change.resident_bytes)?,
                        false,
                        base_bound,
                        owner,
                    )
                })
                .transpose()?;
            (Some(base_layer), change_layer)
        }
        _ => (None, None),
    };
    Ok(CapturedVectorServingSelection::new(
        inspection,
        base,
        change,
        supplier as Arc<dyn CapturedVectorSuffixSource>,
    ))
}

/// One sealed layer bound to the captured root. It loads only through the
/// operation's caller reservation and returns the graph owned by the
/// operation's funder; it never replays a fresh tail.
struct CapturedLayerLoader {
    source: DurableVectorGenerationSource,
    lease: Arc<CapturedVectorRootLease>,
    partition: VectorPartitionRef,
    descriptor: DormantVectorGraphGeneration,
    is_base: bool,
    base: BoundBase,
    dimension: usize,
    quantization: VectorQuantization,
    layout: VectorIndexLayout,
    funder: Arc<dyn MemoryBudget>,
    _control: VectorCopyAdmission,
}

impl CapturedLayerLoader {
    fn catalog_descriptor(&self) -> VectorPartitionGenerationDescriptor {
        let identity = self.descriptor.identity;
        VectorPartitionGenerationDescriptor {
            generation_id: identity.generation_id,
            covered_tx: identity.covered_tx,
            covered_lsn: identity.covered_lsn,
            resident_bytes: self.descriptor.resident_bytes as u64,
            durable_bytes: identity.durable_bytes,
            policy_revision: identity.policy_revision,
            hnsw_m: identity.hnsw_m,
            hnsw_ef_construction: identity.hnsw_ef_construction,
        }
    }
}

impl DormantVectorGraphLoader for CapturedLayerLoader {
    fn load(
        &self,
        generation: DormantVectorGraphGeneration,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedVectorGraphGeneration> {
        if generation != self.descriptor || caller_reserved_load_bytes != self.descriptor.load_bytes
        {
            return Err(Error::Other((
                "captured vector loader received a descriptor or reservation other than its own"
                    .to_string()).into()));
        }
        checkpoint(request)?;
        let resident = self.descriptor.resident_bytes;
        let durable = DurableVectorGenerationLoader::descriptor_usize(
            self.descriptor.identity.durable_bytes,
            "captured graph decode workspace",
        )?;
        let metadata = vector_route_metadata_workspace(&self.partition)?;
        let record_workspace = self.source.record_bytes(
            &self.partition,
            self.base.generation_id,
            (!self.is_base).then_some(self.descriptor.identity.generation_id),
        )?;
        let mut reserved = caller_reserved_load_bytes;
        let known = DurableVectorGenerationLoader::checked_add(
            DurableVectorGenerationLoader::checked_add(
                resident,
                durable.max(record_workspace.saturating_mul(2)),
                "captured generation workspace",
            )?,
            metadata,
            "captured generation load",
        )?;
        reserve(request, known.saturating_sub(reserved))?;
        reserved = reserved.max(known);
        checkpoint(request)?;
        let (graph_bytes, paged) = if self.is_base {
            let record = self.source.load_base(
                &self.partition.index,
                &self.partition.partition_key,
                self.descriptor.identity.generation_id,
            )?;
            let paged = record
                .graph
                .bytes
                .starts_with(&crate::persistence::vector_paged::META_MAGIC)
                || record.graph_format_version
                    == crate::persistence::vector_paged::VECTOR_GRAPH_PAGED_FORMAT_VERSION;
            if record.highest_included_tx != self.descriptor.identity.covered_tx
                || record.highest_included_lsn != self.descriptor.identity.covered_lsn
                || record.dimension as usize != self.dimension
                || record.quantization != self.quantization
                || record.algorithm_version != VECTOR_GRAPH_ALGORITHM_VERSION
                || (record.graph_format_version != VECTOR_GRAPH_FORMAT_VERSION && !paged)
            {
                return Err(Error::Other((
                    "saved vector base disagrees with its catalog or column declaration"
                        .to_string()).into()));
            }
            (record.graph.bytes, paged)
        } else {
            let record = self.source.load_change(
                &self.partition.index,
                &self.partition.partition_key,
                self.base.generation_id,
                self.descriptor.identity.generation_id,
            )?;
            if record.covered_through_tx != self.descriptor.identity.covered_tx
                || record.covered_through_lsn != self.descriptor.identity.covered_lsn
                || record.dimension as usize != self.dimension
                || record.quantization != self.quantization
                || record.algorithm_version != VECTOR_GRAPH_ALGORITHM_VERSION
                || record.graph_format_version != VECTOR_GRAPH_FORMAT_VERSION
            {
                return Err(Error::Other((
                    "saved vector change graph disagrees with its catalog or column declaration"
                        .to_string()).into()));
            }
            (record.graph.bytes, true)
        };
        checkpoint(request)?;
        if graph_bytes.len() != durable {
            return Err(Error::Other((
                "saved vector graph length disagrees with its catalog".to_string()).into()));
        }
        if !paged {
            return Err(crate::persistence::vector_paged::corruption(
                "saved generation has no published topology pages",
            ));
        }
        let current_resident = if paged {
            resident
        } else {
            HnswIndex::durable_resident_bound(
                &graph_bytes,
                self.dimension,
                self.quantization,
                self.descriptor.identity.hnsw_m as usize,
            )?
        };
        let decode_total = current_resident
            .saturating_add(record_workspace.saturating_mul(2))
            .saturating_add(metadata);
        reserve(request, decode_total.saturating_sub(reserved))?;
        let graph = if paged {
            paged_generation_graph_from(
                self.lease.paged_root_capture(),
                &self.partition,
                &self.catalog_descriptor(),
                self.dimension,
                self.quantization,
                &self.layout,
                Arc::clone(&self.funder),
                &graph_bytes,
                self.is_base,
                self.base.generation_id,
                (self.base.covered_tx, self.base.covered_lsn),
            )?
        } else {
            HnswIndex::decode_durable_generation(&graph_bytes, self.dimension, self.quantization)?
        };
        // The catalog's 4KiB residency is an admission upper bound, not a
        // second lifetime owner for native page/root controls. Those carry
        // their own final-holder invoices. Retain the exact sealed Arc layout
        // here and return the unused load headroom after graph_bytes frees.
        let current_resident = LoadedVectorGraphGeneration::paged_resident_control_bytes();
        if current_resident > resident {
            return Err(Error::Other("native sealed graph exceeds its admitted control bound".into()));
        }
        // This is the find's one load of the generation: the store adopts it
        // rather than loading again, so the load is metered here.
        // The same figures the retained loader reports for a paged load: its
        // control handle is what stays resident; pages are served on demand.
        contextdb_core::find_attribution::note_saved_graph_load(
            contextdb_core::find_attribution::LoadCharge {
                saved_bytes: graph_bytes.len(),
                priced_bytes: self.descriptor.reclaim_bytes,
                minimum_bytes: current_resident,
                admitted_bytes: current_resident,
                held_bytes: current_resident,
                cache_bytes: 0,
                minimum_cache_bytes: 0,
                maximum_cache_bytes: 0,
            },
        );
        drop(graph_bytes);
        checkpoint(request)?;
        if !paged && graph.estimated_resident_bytes() != current_resident {
            return Err(Error::Other((
                "saved vector graph residency disagrees with its catalog".to_string()).into()));
        }
        // The caller's reservation already funds this residency on the
        // operation's own funder; ownership stays there and is released when
        // the operation's selection drops the graph.
        Ok(LoadedVectorGraphGeneration {
            graph,
            resident_bytes: current_resident,
            accountant: Arc::clone(&self.funder),
            caller_adopted_bytes: current_resident,
            fresh_tail: None,
        })
    }
}

/// The journal suffix after one serving frontier at the captured root.
struct CapturedSuffixLoader {
    lease: Arc<CapturedVectorRootLease>,
    partition: VectorPartitionRef,
    covered_lsn: Lsn,
    saved_inserts: usize,
    admitted_load: usize,
    dimension: usize,
    quantization: VectorQuantization,
    policy: ResolvedVectorPolicy,
    funder: Arc<dyn MemoryBudget>,
    _control: VectorCopyAdmission,
}

impl CapturedVectorSuffixLoader for CapturedSuffixLoader {
    fn load(
        &self,
        caller_reserved_load_bytes: usize,
        request: &mut dyn FnMut(DormantVectorLoadRequest) -> bool,
    ) -> Result<LoadedCapturedVectorSuffix> {
        if caller_reserved_load_bytes == 0 {
            return Ok(LoadedCapturedVectorSuffix {
                tail: None,
                caller_adopted_bytes: 0,
            });
        }
        if caller_reserved_load_bytes != self.admitted_load {
            return Err(Error::Other((
                "captured suffix loader received a reservation other than its saved admission"
                    .to_string()).into()));
        }
        checkpoint(request)?;
        let (saved_resident, saved_workspace) = vector_tail_reservation(
            self.saved_inserts,
            self.dimension,
            self.quantization,
            self.policy,
        )?;
        let mut admitted_resident = saved_resident;
        // The journal walk asks for checkpoints and for growth funding from
        // two callbacks that never run at the same time.
        let request = std::cell::RefCell::new(request);
        let loaded = self.lease.with_read(&self.partition.index, |read| {
            RedbPersistence::load_vector_partition_tail_entries_in_read(
                read,
                &self.partition.index,
                &self.partition.partition_key,
                self.covered_lsn,
                self.lease.through_lsn(),
                || {
                    let mut request = request.borrow_mut();
                    if (*request)(DormantVectorLoadRequest::Checkpoint) {
                        Ok(())
                    } else {
                        Err(Error::ReadCancelled)
                    }
                },
                |identities| {
                    if identities <= self.saved_inserts {
                        return Ok(());
                    }
                    // More upserts than the saved live count (a row moved or
                    // deleted after insert): fund the difference before any
                    // body is read.
                    let (resident, workspace) = vector_tail_reservation(
                        identities,
                        self.dimension,
                        self.quantization,
                        self.policy,
                    )?;
                    let extra = resident
                        .saturating_add(workspace)
                        .saturating_sub(saved_resident.saturating_add(saved_workspace));
                    let mut request = request.borrow_mut();
                    if (*request)(DormantVectorLoadRequest::Reserve(extra)) {
                        admitted_resident = resident;
                        Ok(())
                    } else {
                        Err(Error::ReadCancelled)
                    }
                },
            )
        });
        let request = request.into_inner();
        let (entries, replayed_through_lsn) = loaded?;
        checkpoint(request)?;
        if entries.is_empty() {
            return Ok(LoadedCapturedVectorSuffix {
                tail: None,
                caller_adopted_bytes: 0,
            });
        }
        let total = entries.len();
        let graph = HnswIndex::from_vector_entries_with_progress(
            &entries,
            self.dimension,
            self.quantization,
            self.policy,
            |done| {
                if request(DormantVectorLoadRequest::Progress { done, total }) {
                    Ok(())
                } else {
                    Err(Error::ReadCancelled)
                }
            },
        )?;
        drop(entries);
        checkpoint(request)?;
        let actual = graph.estimated_resident_bytes();
        if actual > admitted_resident {
            return Err(Error::Other((
                "vector suffix graph exceeded its admitted resident bound".to_string()).into()));
        }
        Ok(LoadedCapturedVectorSuffix {
            tail: Some(LoadedVectorFreshTail {
                graph,
                resident_bytes: actual,
                accountant: Arc::clone(&self.funder),
                replayed_through_lsn,
            }),
            caller_adopted_bytes: actual,
        })
    }

    fn as_streamed(&self) -> Option<&dyn contextdb_vector::store::StreamedVectorSuffixHandle> {
        Some(self)
    }
}

impl contextdb_vector::store::StreamedVectorSuffixHandle for CapturedSuffixLoader {
    fn stream_tail(
        &self,
        visit: &mut dyn FnMut(contextdb_vector::store::RawVectorTailEntry) -> Result<bool>,
        owner: &mut contextdb_vector::VectorCopyAdmission,
    ) -> Result<()> {
        crate::persistence::vector_membership_source::stream_partition_journal_tail(
            &self.lease,
            &self.partition,
            self.covered_lsn,
            self.lease.through_lsn(),
            visit,
            owner,
        )
    }

    fn covered_lsn(&self) -> Lsn {
        self.covered_lsn
    }

    fn through_lsn(&self) -> Option<Lsn> {
        self.lease.through_lsn()
    }

    fn saved_inserts(&self) -> usize {
        self.saved_inserts
    }

    #[cfg(any(test, feature = "test-seams"))]
    fn policy_for_test(&self) -> Option<(usize, usize, usize)> {
        Some((self.policy.hnsw_m, self.policy.hnsw_ef_construction, self.policy.hnsw_ef_search))
    }


    fn observe_vector_decode(&self, row_id: RowId, created_tx: TxId, lsn: Lsn) {
        #[cfg(any(test, feature = "test-seams"))]
        crate::vector_observations::emit(
            &self.partition.index,
            &self.partition.partition_key,
            self.covered_lsn,
            crate::vector_observations::VectorJournalReplayWork::Vector {
                row_id,
                created_tx,
                lsn,
            },
        );
    }
}


use crate::persistence::writer_source::{WriterSnapshotHandle};
