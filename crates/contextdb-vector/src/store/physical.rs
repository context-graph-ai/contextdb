//! Borrow actual registered owners, without hydration, source lists or encoding.
use super::*;
#[derive(Clone, Copy, Debug)]
pub enum PhysicalOwnerLocation {
    CurrentBase, CurrentChange, DormantBase, DormantChange, PendingBase,
    RetiredBase(usize), RetiredChange(usize), RetiredDormantBase(usize),
    RetiredDormantChange(usize), Tail,
}
pub enum PhysicalRawPayload<'a> {
    F32(&'a [f32]),
    SQ8 { min: f32, max: f32, len: usize, payload: &'a [u8] },
    SQ4 { min: f32, max: f32, len: usize, payload: &'a [u8] },
}
pub enum PhysicalVectorOwnerView<'a> {
    RawDirectory(&'a RawVectorDirectoryEntry),
    RawBody { row: RowId, membership: VectorMembershipProvenance,
        payload: PhysicalRawPayload<'a> },
    Generation { location: PhysicalOwnerLocation, identity: VectorGraphGeneration,
        graph: &'a HnswIndex },
    Dormant { location: PhysicalOwnerLocation, descriptor: &'a DormantVectorGraphGeneration },
    Tail(&'a HnswIndex),
    /// Actual owner exists but its private controls/pages remain unvisited.
    Unvisited(&'static str),
}
impl IndexState {
    fn visit_physical_owners<E>(&self, visit: &mut impl FnMut(PhysicalVectorOwnerView<'_>) -> std::result::Result<(), E>) -> std::result::Result<(), E> {
        // Borrow one owning container at a time; never clone payloads or graphs.
        {
            let directory = self.raw_directory.read();
            for entry in directory.iter() { visit(PhysicalVectorOwnerView::RawDirectory(entry))?; }
        }
        {
            let vectors = self.vectors.read();
            for entry in &vectors.entries {
                use crate::quantized::StoredVector;
                let payload = match &entry.vector {
                    StoredVector::F32(values) => PhysicalRawPayload::F32(values),
                    StoredVector::SQ8 { min, max, len, payload } => PhysicalRawPayload::SQ8 {
                        min: *min, max: *max, len: *len, payload },
                    StoredVector::SQ4 { min, max, len, payload } => PhysicalRawPayload::SQ4 {
                        min: *min, max: *max, len: *len, payload },
                };
                visit(PhysicalVectorOwnerView::RawBody { row: entry.row_id, membership: entry.membership, payload })?;
            }
        }
        {
            let _publication = self.generation_publication.lock();
            let sealed = self.sealed_hnsw.read();
            use PhysicalOwnerLocation::*;
            for (location, generation) in [(CurrentBase, sealed.base.as_ref()), (CurrentChange, sealed.change.as_ref())] {
                if let Some(generation) = generation {
                    visit(PhysicalVectorOwnerView::Generation { location, identity: generation.identity, graph: generation.graph() })?;
                }
            }
            for (location, generation) in [(DormantBase, sealed.dormant_base.as_ref()),
                (DormantChange, sealed.dormant_change.as_ref()), (PendingBase, sealed.pending_base.as_ref())] {
                if let Some(generation) = generation { visit(PhysicalVectorOwnerView::Dormant { location, descriptor: &generation.descriptor })?; }
            }
            for (position, chain) in sealed.retired.iter().enumerate() {
                for (location, generation) in [(RetiredBase(position), chain.base.as_ref()), (RetiredChange(position), chain.change.as_ref())] {
                    if let Some(generation) = generation {
                        visit(PhysicalVectorOwnerView::Generation { location, identity: generation.identity, graph: generation.graph() })?;
                    }
                }
                for (location, generation) in [(RetiredDormantBase(position), chain.dormant_base.as_ref()),
                    (RetiredDormantChange(position), chain.dormant_change.as_ref())] {
                    if let Some(generation) = generation { visit(PhysicalVectorOwnerView::Dormant { location, descriptor: &generation.descriptor })?; }
                }
            }
        }
        if let Some(tail) = self.hnsw.get() {
            let tail = tail.read();
            if let Some(tail) = tail.as_ref() { visit(PhysicalVectorOwnerView::Tail(tail))?; }
        }
        // These names are hard coverage gaps, never successful empty sources.
        visit(PhysicalVectorOwnerView::Unvisited("active construction/reclaimer and durable pending owners"))?;
        Ok(())
    }
}
impl VectorStore {
    /// Registered owner scope only. A callback must not re-enter mutation or
    /// source operations while these borrowed owner guards are alive. Detached
    /// Arc holders are a separate, explicitly unvisited lifecycle class.
    #[doc(hidden)]
    pub fn visit_registered_physical_owners<E>(&self, mut visit: impl FnMut(
        &VectorIndexRef, &VectorPartitionKey, PhysicalVectorOwnerView<'_>) -> std::result::Result<(), E>) -> std::result::Result<(), E> {
        self.with_bulk_read(|| {
            let registry = self.registry.read();
            for (index, column) in registry.iter() {
                let partitions = column.partitions.read();
                for (key, state) in partitions.iter() {
                    state.visit_physical_owners(&mut |owner| visit(index, key, owner))?;
                }
            }
            Ok(())
        })
    }
}
