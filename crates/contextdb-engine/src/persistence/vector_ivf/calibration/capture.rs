//! One whole invoiced candidate capture and its bounded sample identity window.
use super::super::*;
use super::selector;
use crate::memory_accounting::StoreAccountantHandle;
use contextdb_core::{
    ColumnDef, ColumnType,
    read_memory::{ReadMemoryOwner, ReadShared},
};
use contextdb_vector::{
    MemoryBudgetOwner,
    ivf::storage::{BuildPhase, Manifest, ReadPages},
};
use parking_lot::Mutex;
use std::{alloc::Layout, sync::atomic::AtomicBool};

pub(in crate::persistence::vector_ivf) type Owner = ReadShared<Capture, memory::Invoice>;
pub(super) use super::super::qualification::Sample;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    Samples,
    Oracle,
    Trial,
    Final,
    Finished,
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Cursor {
    pub phase: Phase,
    pub sampled: u64,
    pub source_after: Option<[u8; 40]>,
    pub last_source_row: Option<u64>,
    pub query: u32,
    pub depth_ordinal: u32,
    pub previous_depth: u32,
    pub point: Option<selector::Point>,
    pub first: Option<selector::Point>,
    pub indexed_work: u64,
    pub exact_work: u64,
    pub below: u32,
    pub reranks: Option<super::super::qualification::RerankSummary>,
}
pub(super) struct State {
    pub cursor: Cursor,
    pub samples: Vec<Sample>,
    pub excluded: Vec<u64>,
    pub minimum_hits: Vec<u32>,
    pub below: Vec<u32>,
}
pub(super) struct Source {
    pub data: Box<SourceData>,
    // The complete source declaration frees before its original invoice.
    _invoice: memory::Invoice,
}
pub(super) struct SourceData {
    pub index: VectorIndexRef,
    pub partitioned: bool,
    pub identity: u64,
    pub digest: [u8; 32],
    pub live: u64,
}
pub(in crate::persistence::vector_ivf) struct Capture {
    pub root: super::super::super::vector_paged::CapturedPagedRoot,
    pub memory: ReadMemoryOwner,
    pub original: StoreAccountantHandle,
    pub owner: MemoryBudgetOwner,
    pub scope: [u8; 64],
    pub route: [u8; 32],
    pub index_name: [u8; 32],
    pub manifest: Manifest,
    pub seed: u64,
    pub slot_start: u64,
    pub slot_stride: u64,
    pub planned_samples: usize,
    pub maximum_depth: u32,
    pub population: u64,
    pub source: Option<Source>,
    pub source_identity: u64,
    pub source_digest: [u8; 32],
    pub epoch: u64,
    pub cancelled: AtomicBool,
    pub state: Mutex<State>,
}
fn array<T>(count: usize) -> Result<Layout> {
    Layout::array::<T>(count).map_err(|_| corrupt())
}
fn extend(layout: Layout, part: Layout) -> Result<Layout> {
    layout.extend(part).map(|(l, _)| l).map_err(|_| corrupt())
}
fn gcd(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}
pub(super) fn depth_after(column: &ColumnDef, previous: u32) -> Option<u32> {
    column
        .vector_engine_policy
        .calibration_depths
        .as_deref()
        .unwrap_or(&[1, 2, 5, 10, 20, 50, 100])
        .iter()
        .copied()
        .filter(|depth| {
            *depth > previous
                && u64::from(*depth) <= column.vector_engine_policy.calibration_max_k.unwrap_or(100)
        })
        .min()
}
pub(super) fn writer(
    root: &super::super::super::vector_paged::CapturedPagedRoot,
) -> Result<&super::super::super::writer_source::WriterSnapshotHandle> {
    match &root.holder {
        super::super::super::vector_paged::PagedRootHolder::Writer(writer) => Ok(writer),
        _ => Err(Error::InvalidStateTransition(
            "calibration requires its captured native writer source".into(),
        )),
    }
}
fn source_probe(
    main: &VectorIndexRef,
    root: &super::super::super::vector_paged::CapturedPagedRoot,
    authority: &read::NativeAuthority<'_>,
    column: &ColumnDef,
    tuple: &VectorPartitionKey,
) -> Result<Option<Source>> {
    let Some(reference) = &column.vector_engine_policy.calibration_queries else {
        return Ok(None);
    };
    let writer = writer(root)?;
    writer.check_table_scope(&reference.table)?;
    let source_meta = writer
        .borrow_captured_table_meta(&reference.table)
        .ok_or_else(|| Error::TableNotFound(reference.table.clone()))?;
    let main_meta = writer
        .borrow_captured_table_meta(&main.table)
        .ok_or_else(|| Error::TableNotFound(main.table.clone()))?;
    let source_column = source_meta
        .columns
        .iter()
        .find(|c| c.name == reference.column)
        .ok_or_else(|| {
            Error::InvalidStateTransition("the declared calibration source column is absent".into())
        })?;
    if source_column.column_type != column.column_type
        || !matches!(source_column.column_type, ColumnType::Vector(_))
        || source_column
            .partition_key_columns
            .as_ref()
            .is_some_and(|keys| {
                Some(keys) != column.partition_key_columns.as_ref()
                    || keys.iter().any(|key| {
                        source_meta
                            .columns
                            .iter()
                            .find(|c| c.name == *key)
                            .map(|c| &c.column_type)
                            != main_meta
                                .columns
                                .iter()
                                .find(|c| c.name == *key)
                                .map(|c| &c.column_type)
                    })
            })
    {
        return Err(Error::InvalidStateTransition(
            "the declared calibration source has another vector or partition shape".into(),
        ));
    }
    let names = reference
        .table
        .len()
        .checked_add(reference.column.len())
        .ok_or_else(corrupt)?;
    let layout = extend(Layout::new::<SourceData>(), array::<u8>(names)?)?.pad_to_align();
    let invoice = memory::Invoice::reserve(
        authority.vector_owner(),
        layout.size(),
        "clustered_calibration_source",
    )?;
    let index = VectorIndexRef::new(reference.table.clone(), reference.column.clone());
    let partitioned = source_column.partition_key_columns.is_some();
    let empty = VectorPartitionKey::unpartitioned();
    let tuple = if partitioned { tuple } else { &empty };
    let route_bytes = memory::route_bytes(&index, tuple)?;
    let route_layout = extend(Layout::new::<Vec<u8>>(), array::<u8>(route_bytes)?)?.pad_to_align();
    let _route_invoice = memory::Invoice::reserve(
        authority.vector_owner(),
        route_layout.size(),
        "clustered_calibration_source_route",
    )?;
    let route = memory::route_with_suffix_capacity(&index, tuple, 0)?;
    let summary = authority.source_summary(&route)?;
    let schema = authority.admission.run_original_unqualified(|| {
        RedbPersistence::vector_schema_binding_in_read(&root.read, &index)
    })?;
    let digest = control::index_digest(&index, schema);
    let identity = u64::from_le_bytes(digest[..8].try_into().map_err(|_| corrupt())?);
    if identity == 0 {
        return Err(corrupt());
    }
    Ok(Some(Source {
        data: Box::new(SourceData {
            index,
            partitioned,
            identity,
            digest: summary.source_identity(),
            live: summary.live_entries,
        }),
        _invoice: invoice,
    }))
}
impl Capture {
    /// Called after the publisher refreshed this exact root, before releasing
    /// its commit boundary. Query work happens later, outside that boundary.
    pub(super) fn new(
        root: super::super::super::vector_paged::CapturedPagedRoot,
        original: StoreAccountantHandle,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
        column: &ColumnDef,
        epoch: u64,
    ) -> Result<Owner> {
        let memory = original.read_budget()?;
        let owner = original.vector_budget()?;
        let authority = read::NativeAuthority::open(&root, memory.clone())?;
        let source = read::PageSource::open(&authority, index, tuple, &|| Ok(()), &|_| Ok(()))?
            .ok_or_else(corrupt)?;
        let manifest = source.select_build_authority()?;
        let build = source
            .build_state(0)?
            .filter(|build| build.phase == BuildPhase::Publish)
            .ok_or_else(corrupt)?;
        let source_probe = source_probe(index, &root, &authority, column, tuple)?;
        let samples = column
            .vector_engine_policy
            .calibration_samples
            .unwrap_or(200);
        let query_basis = source_probe
            .as_ref()
            .is_some_and(|source| source.data.live >= samples);
        let sample_count = if query_basis {
            samples
        } else {
            samples.min(manifest.slots)
        };
        let planned_samples = usize::try_from(sample_count).map_err(|_| corrupt())?;
        u32::try_from(planned_samples).map_err(|_| corrupt())?;
        let mut maximum_depth = 0;
        while let Some(depth) = depth_after(column, maximum_depth) {
            maximum_depth = depth;
        }
        if maximum_depth == 0 {
            return Err(corrupt());
        }
        let mut layout = Owner::layout();
        for part in [
            array::<Sample>(planned_samples)?,
            array::<u64>(planned_samples)?,
            array::<u32>(maximum_depth as usize)?,
            array::<u32>(maximum_depth as usize)?,
        ] {
            layout = extend(layout, part)?;
        }
        let invoice = memory::Invoice::reserve(
            &owner,
            layout.pad_to_align().size(),
            "clustered_calibration_capture",
        )?;
        let scope = source.scope_key;
        let population = source.source_summary.live_entries;
        let source_identity = source_probe
            .as_ref()
            .map_or(0, |source| source.data.identity);
        let source_digest = source_probe
            .as_ref()
            .map_or([0; 32], |source| source.data.digest);
        let external = if query_basis {
            source_probe
        } else {
            drop(source_probe);
            None
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"contextdb.clustered.calibration.samples.v1");
        hash.update(&build.training.seed.to_le_bytes());
        hash.update(&scope);
        let digest = hash.finalize();
        let slots = manifest.slots;
        let slot_start = if slots == 0 {
            0
        } else {
            u64::from_le_bytes(digest.as_bytes()[..8].try_into().map_err(|_| corrupt())?) % slots
        };
        let mut slot_stride = if slots == 0 {
            1
        } else {
            (u64::from_le_bytes(digest.as_bytes()[8..16].try_into().map_err(|_| corrupt())?)
                % slots)
                .max(1)
        };
        while slots != 0 && gcd(slot_stride, slots) != 1 {
            slot_stride = slot_stride.checked_add(1).ok_or_else(corrupt)? % slots;
        }
        let state = State {
            cursor: Cursor {
                phase: Phase::Samples,
                sampled: 0,
                source_after: None,
                last_source_row: None,
                query: 0,
                depth_ordinal: 0,
                previous_depth: 0,
                point: None,
                first: None,
                indexed_work: 0,
                exact_work: 0,
                below: 0,
                reranks: None,
            },
            samples: Vec::with_capacity(planned_samples),
            excluded: Vec::with_capacity(planned_samples),
            minimum_hits: vec![u32::MAX; maximum_depth as usize],
            below: vec![0; maximum_depth as usize],
        };
        let route = retained::route_identity(index, tuple);
        drop(source);
        drop(authority);
        ReadShared::try_from_invoice_native(
            Self {
                root,
                memory,
                original,
                owner,
                scope,
                route,
                index_name: retained::index_name_identity(&index.table, &index.column),
                manifest,
                seed: build.training.seed,
                slot_start,
                slot_stride,
                planned_samples,
                maximum_depth,
                population,
                source: external,
                source_identity,
                source_digest,
                epoch,
                cancelled: AtomicBool::new(false),
                state: Mutex::new(state),
            },
            invoice,
        )
        .map_err(Error::NativeEngineOther)
    }
}
