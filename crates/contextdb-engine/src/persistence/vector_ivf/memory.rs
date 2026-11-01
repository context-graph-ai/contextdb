//! Original-owner whole-object and checked private workspace admission.
use super::*;
#[cfg(feature = "test-seams")]
use crate::vector_ivf_observations::*;
use contextdb_core::VectorPartitionComponent;
use contextdb_vector::MemoryBudgetOwner;
use std::alloc::Layout;
#[cfg(feature = "test-seams")]
struct ObjectObservation {
    instance: NativeIvfObjectInstance,
    kind: NativeIvfObjectKind,
    layout: Layout,
    original_owner: usize,
    adapter_owner: usize,
    scope_key: Option<[u8; 64]>,
}

/// Covered physical fields/buffers precede this token in their final holder.
pub(super) struct Invoice {
    owner: MemoryBudgetOwner,
    bytes: usize,
    #[cfg(feature = "test-seams")]
    observer: Producer,
    #[cfg(feature = "test-seams")]
    object: Option<ObjectObservation>,
    #[cfg(feature = "test-seams")]
    reservation: u64,
    #[cfg(feature = "test-seams")]
    operation: &'static str,
}
impl Invoice {
    pub fn reserve(
        owner: &MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        #[cfg(feature = "test-seams")]
        let reservation = crate::vector_ivf_observations::instance()?;
        owner.try_allocate_for_static(
            bytes,
            "vector_index",
            operation,
            "Release retained vector objects or raise the declared memory limit.",
        )?;
        #[cfg(feature = "test-seams")]
        let observer = Producer::current();
        #[cfg(feature = "test-seams")]
        observer.emit(NativeIvfSealEvent::ReservationDebited {
            invoice: reservation,
            original_owner: original_owner(owner),
            bytes,
            operation,
        });
        Ok(Self {
            owner: owner.clone(),
            bytes,
            #[cfg(feature = "test-seams")]
            observer,
            #[cfg(feature = "test-seams")]
            object: None,
            #[cfg(feature = "test-seams")]
            reservation,
            #[cfg(feature = "test-seams")]
            operation,
        })
    }
}
impl Invoice {
    #[cfg(feature = "test-seams")]
    pub fn observe_object(
        &mut self,
        kind: NativeIvfObjectKind,
        layout: Layout,
        scope_key: Option<[u8; 64]>,
        windows: Option<&[NativeIvfWindowRange; 3]>,
    ) -> Result<()> {
        if self.object.is_some() || layout.size() != self.bytes {
            return Err(corrupt());
        }
        let (original, _, _) = self.owner.as_ref().original_read_payer_for_test();
        let adapter_owner = self.owner.accounting_identity();
        let object = ObjectObservation {
            instance: NativeIvfObjectInstance(crate::vector_ivf_observations::instance()?),
            kind,
            layout,
            original_owner: if original == 0 {
                self.owner.as_ref().stable_funder_identity()
            } else {
                original
            },
            adapter_owner,
            scope_key,
        };
        self.observer.emit(NativeIvfSealEvent::ObjectAdmitted {
            instance: object.instance,
            kind,
            layout,
            original_owner: object.original_owner,
            adapter_owner,
            scope_key,
            windows,
        });
        self.object = Some(object);
        Ok(())
    }
}
impl Drop for Invoice {
    fn drop(&mut self) {
        self.owner.release(self.bytes);
        #[cfg(feature = "test-seams")]
        self.observer.emit(NativeIvfSealEvent::ReservationReleased {
            invoice: self.reservation,
            original_owner: original_owner(&self.owner),
            bytes: self.bytes,
            operation: self.operation,
        });
        #[cfg(feature = "test-seams")]
        if let Some(o) = &self.object {
            self.observer.emit(NativeIvfSealEvent::ObjectReleased {
                instance: o.instance,
                kind: o.kind,
                layout: o.layout,
                original_owner: o.original_owner,
                adapter_owner: o.adapter_owner,
                scope_key: o.scope_key,
            });
        }
    }
}

fn overflow() -> Error {
    Error::Other("clustered native metadata layout is not representable".into())
}

fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(overflow)
}

/// One explicit variable subrange of the complete admitted scope object.
pub(super) struct ScopeConstruction {
    pub layout: Layout,
    pub route_offset: usize,
    pub route_bytes: usize,
    pub invoice: Invoice,
}
impl ScopeConstruction {
    pub fn reserve<T>(
        owner: &MemoryBudgetOwner,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<Self> {
        let route_bytes = route_bytes(index, tuple)?;
        let (layout, route_offset) = Layout::new::<T>()
            .extend(Layout::array::<u8>(route_bytes).map_err(|_| overflow())?)
            .map_err(|_| overflow())?;
        let layout = layout.pad_to_align();
        let invoice = Invoice::reserve(owner, layout.size(), "clustered_manifest")?;
        #[cfg(feature = "test-seams")]
        let invoice = {
            let mut invoice = invoice;
            invoice.observe_object(NativeIvfObjectKind::Scope, layout, None, None)?;
            invoice
        };
        Ok(Self {
            layout,
            route_offset,
            route_bytes,
            invoice,
        })
    }
    pub fn route(&self, index: &VectorIndexRef, tuple: &VectorPartitionKey) -> Result<Vec<u8>> {
        let size = route_bytes(index, tuple)?;
        if size != self.route_bytes || add(self.route_offset, size)? > self.layout.size() {
            return Err(overflow());
        }
        route_with_suffix_capacity(index, tuple, 0)
    }
}

/// The same encoded native route, with exact prepaid suffix capacity.
pub(super) fn route_with_suffix_capacity(
    index: &VectorIndexRef,
    tuple: &VectorPartitionKey,
    suffix_bytes: usize,
) -> Result<Vec<u8>> {
    let size = route_bytes(index, tuple)?;
    let capacity = size.checked_add(suffix_bytes).ok_or_else(overflow)?;
    let canonical = size
        .checked_sub(add(add(index.table.len(), index.column.len())?, 24)?)
        .ok_or_else(overflow)?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(&(index.table.len() as u64).to_be_bytes());
    out.extend_from_slice(index.table.as_bytes());
    out.extend_from_slice(&(index.column.len() as u64).to_be_bytes());
    out.extend_from_slice(index.column.as_bytes());
    out.extend_from_slice(&(canonical as u64).to_be_bytes());
    out.push(1);
    out.extend_from_slice(&(tuple.components().len() as u64).to_be_bytes());
    for value in tuple.components() {
        match value {
            VectorPartitionComponent::Uuid(v) => {
                out.push(0);
                out.extend_from_slice(v.as_bytes());
            }
            VectorPartitionComponent::Text(v) => {
                out.push(1);
                out.extend_from_slice(&(v.len() as u64).to_be_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            VectorPartitionComponent::Integer(v) => {
                out.push(2);
                out.extend_from_slice(&v.to_be_bytes());
            }
            VectorPartitionComponent::Boolean(v) => {
                out.extend_from_slice(&[3, u8::from(*v)]);
            }
            VectorPartitionComponent::Timestamp(v) => {
                out.push(4);
                out.extend_from_slice(&v.to_be_bytes());
            }
            VectorPartitionComponent::TxId(v) => {
                out.push(5);
                out.extend_from_slice(&v.0.to_be_bytes());
            }
        }
    }
    if out.len() != size || out.capacity() != capacity {
        return Err(overflow());
    }
    Ok(out)
}

pub(super) fn route_bytes(index: &VectorIndexRef, tuple: &VectorPartitionKey) -> Result<usize> {
    route_bytes_for_names(&index.table, &index.column, tuple)
}
pub(super) fn route_bytes_for_names(
    table: &str,
    column: &str,
    tuple: &VectorPartitionKey,
) -> Result<usize> {
    let mut canonical = 9usize;
    for component in tuple.components() {
        canonical = add(
            canonical,
            match component {
                VectorPartitionComponent::Uuid(_) => 17,
                VectorPartitionComponent::Text(v) => add(9, v.len())?,
                VectorPartitionComponent::Integer(_)
                | VectorPartitionComponent::Timestamp(_)
                | VectorPartitionComponent::TxId(_) => 9,
                VectorPartitionComponent::Boolean(_) => 2,
            },
        )?;
    }
    add(add(add(table.len(), column.len())?, 24)?, canonical)
}

pub(super) const OPERATION_PREFIX_BYTES: usize = 72;
pub(super) const MARKER_BYTES: usize = OPERATION_PREFIX_BYTES + 8 + header::KEY_BYTES;

/// The one native seal workspace. No field retains native page/cache charges.
pub(super) struct SealWorkspace {
    pub native_value: [u8; header::VALUE_BYTES],
    pub previous_payload: [u8; PAGE_BYTES],
    pub related_metadata: [u8; PAGE_BYTES],
    pub marker: [u8; MARKER_BYTES],
    pub control_key: [u8; super::frontier::KEY_BYTES],
    pub changed_kinds: [u16; control::ROOTS],
    pub operation: u64,
    pub original_present: bool,
    pub _invoice: Invoice,
}
impl SealWorkspace {
    pub fn reserve_for<T>(owner: &MemoryBudgetOwner, operation: u64) -> Result<Self> {
        // T is the complete operation holder, including this workspace and its
        // transaction/scope references and guards, not merely the array sum.
        let layout = Layout::new::<T>();
        if layout.size() < Layout::new::<Self>().size() {
            return Err(overflow());
        }
        let invoice = Invoice::reserve(owner, layout.size(), "clustered_native_seal")?;
        Ok(Self {
            native_value: [0; header::VALUE_BYTES],
            previous_payload: [0; PAGE_BYTES],
            related_metadata: [0; PAGE_BYTES],
            marker: [0; MARKER_BYTES],
            control_key: [0; super::frontier::KEY_BYTES],
            changed_kinds: [0; control::ROOTS],
            operation,
            original_present: false,
            _invoice: invoice,
        })
    }
    pub fn control_payload(&mut self) -> Result<&mut [u8; PAGE_BYTES]> {
        (&mut self.native_value[..PAGE_BYTES])
            .try_into()
            .map_err(|_| corrupt())
    }
}

#[cfg(feature = "test-seams")]
fn original_owner(owner: &MemoryBudgetOwner) -> usize {
    let (original, _, _) = owner.as_ref().original_read_payer_for_test();
    if original == 0 {
        owner.as_ref().stable_funder_identity()
    } else {
        original
    }
}

/// Native entries the bounded write can hold while its workspace is borrowed.
/// Packed streams use their checked page capacities. Row-version headers and
/// sampled reservoir updates may land on distinct pages, so their bound follows
/// the declared batch, not the database population. Every term names a real
/// payload, named root or finalization control; no byte margin is added.
pub(crate) fn native_receipt_reserve(
    dimensions: usize,
    rows: usize,
    active_lists: u32,
    reservoir_samples: usize,
    native_backing_bytes: usize,
    coalesce_pages: usize,
) -> Result<usize> {
    let pages = |bytes: usize| bytes.div_ceil(PAGE_BYTES);
    let mul = |a: usize, b: usize| a.checked_mul(b).ok_or_else(overflow);
    let mut entries = 0usize;
    let mut include = |count: usize| -> Result<()> {
        entries = add(entries, count)?;
        Ok(())
    };
    let canonical_stride = add(mul(dimensions, 4)?, 8)?;
    include(pages(add(mul(rows, canonical_stride)?, PAGE_BYTES - 1)?))?;
    include(pages(add(mul(rows, 40)?, PAGE_BYTES - 1)?))?; // slot metadata
    include(rows.min(pages(mul(reservoir_samples, 8)?)))?;
    include(pages(add(rows.div_ceil(8), PAGE_BYTES - 1)?))?; // visibility
    include(rows)?; // row-version header pages can have unrelated row ids
    include(pages(add(mul(rows, 24)?, PAGE_BYTES - 1)?))?; // journal copy links
    include(pages(add(mul(rows, 8)?, PAGE_BYTES - 1)?))?; // durable tail slots
    let (records_per_block, block_pages) =
        contextdb_vector::ivf::list_block_layout(dimensions).ok_or_else(overflow)?;
    let nonempty = rows.min(active_lists as usize);
    let list_blocks = if nonempty == 0 {
        0
    } else {
        add(nonempty, (rows - nonempty) / records_per_block)?
    };
    include(mul(list_blocks, block_pages)?)?;
    include(mul(pages(add(mul(list_blocks, 8)?, PAGE_BYTES - 1)?), 2)?)?; // used and next links
    include(usize::from(active_lists != 0))?; // list allocator
    // Directory records contain three u64 words and never straddle a page.
    let directory_pages =
        (active_lists as usize).div_ceil(PAGE_BYTES / std::mem::size_of::<[u64; 3]>());
    include(directory_pages)?;
    include(directory_pages.div_ceil(PAGE_BYTES * 8))?;
    let book = contextdb_vector::ivf::codebook_allocation_layout(dimensions, active_lists, true)?;
    include(pages(book.values.size()))?;
    include(usize::from(active_lists != 0))?; // codebook header
    include(pages(book.counts.size()))?;
    // Coalescing is a separate bounded step, not rows times page capacity.
    // Its declared page bound limits the payloads written in that step.
    let payload_entries = entries.max(coalesce_pages);
    let mut entries = payload_entries;
    let mut include = |count: usize| -> Result<()> {
        entries = add(entries, count)?;
        Ok(())
    };
    // Fourteen generation-local kind roots, the two global journal roots,
    // four authoritative source roots, and the scope/pending-seal roots.
    include(14 + 2 + 4 + 2)?;
    // The operation seal, compact serving descriptor and scope control.
    include(3)?;
    let receipts = super::super::native_storage_admission::pending_directory_receipt_quote(entries)
        .ok_or_else(overflow)?;
    // Each planned native entry can retain one actual dirty extent and its
    // original receipt. These are capacity quotes, never another admission.
    add(mul(entries, native_backing_bytes)?, receipts)
}
