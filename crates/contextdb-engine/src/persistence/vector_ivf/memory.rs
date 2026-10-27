//! Original-owner whole-object and checked private workspace admission.
use super::*;
use contextdb_core::VectorPartitionComponent;
use contextdb_vector::MemoryBudgetOwner;
use std::alloc::Layout;

/// Covered physical fields/buffers precede this token in their final holder.
pub(super) struct Invoice {
    owner: MemoryBudgetOwner,
    bytes: usize,
}
impl Invoice {
    pub fn reserve(
        owner: &MemoryBudgetOwner,
        bytes: usize,
        operation: &'static str,
    ) -> Result<Self> {
        owner.try_allocate_for_static(
            bytes,
            "vector_index",
            operation,
            "Release retained vector objects or raise the declared memory limit.",
        )?;
        Ok(Self {
            owner: owner.clone(),
            bytes,
        })
    }
}
impl Drop for Invoice {
    fn drop(&mut self) {
        self.owner.release(self.bytes);
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
        let canonical = size
            .checked_sub(add(add(index.table.len(), index.column.len())?, 24)?)
            .ok_or_else(overflow)?;
        let mut out = Vec::with_capacity(size);
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
        if out.len() != size || out.capacity() != size {
            return Err(overflow());
        }
        Ok(out)
    }
}

fn route_bytes(index: &VectorIndexRef, tuple: &VectorPartitionKey) -> Result<usize> {
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
    add(
        add(add(index.table.len(), index.column.len())?, 24)?,
        canonical,
    )
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
    pub prefix: [u8; OPERATION_PREFIX_BYTES],
    pub table_name: [u8; 23],
    pub captured_root: [u8; 32],
    pub changed_kinds: [u16; control::ROOTS],
    pub operation: u64,
    pub original_present: bool,
    pub invoice: Invoice,
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
            prefix: [0; OPERATION_PREFIX_BYTES],
            table_name: [0; 23],
            captured_root: [0; 32],
            changed_kinds: [0; control::ROOTS],
            operation,
            original_present: false,
            invoice,
        })
    }
    pub fn control_payload(&mut self) -> Result<&mut [u8; PAGE_BYTES]> {
        (&mut self.native_value[..PAGE_BYTES])
            .try_into()
            .map_err(|_| corrupt())
    }
}
