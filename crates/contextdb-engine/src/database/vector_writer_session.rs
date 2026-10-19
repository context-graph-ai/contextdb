//! Optional import-owned vector working memory, independent of physical writers.
use super::*;
use contextdb_vector::WriterVectorTail;
use std::fmt::Write;

/// An opaque logical-writer tail cache. It never retains a database or source.
#[doc(hidden)]
#[derive(Default)]
pub struct WriterVectorSession {
    saved: Mutex<Option<CarriedWriterTail>>,
}

impl WriterVectorSession {
    pub fn clear(&self) {
        self.saved.lock().take();
    }

    pub(super) fn take(&self) -> Option<CarriedWriterTail> {
        self.saved.lock().take()
    }
}

pub(super) struct CarriedWriterTail {
    partition: VectorPartitionRef,
    physical_identity: (u64, u64),
    database_identity: contextdb_core::read_contract::DatabaseIdentity,
    frontier: [u8; 32],
    layout: [u8; 32],
    tail: Option<WriterVectorTail>,
    metadata_charge: Option<(Arc<dyn contextdb_vector::MemoryBudget>, usize)>,
}

impl CarriedWriterTail {
    pub(super) fn recharge(
        &mut self,
        accountant: Arc<dyn contextdb_vector::MemoryBudget>,
    ) -> Result<()> {
        self.tail
            .as_mut()
            .expect("carried tail")
            .reattach_budget(accountant)
    }
}

impl Drop for CarriedWriterTail {
    fn drop(&mut self) {
        // Route allocations die before their tail-owned metadata admission.
        self.partition.index = VectorIndexRef::default();
        self.partition.partition_key = VectorPartitionKey::unpartitioned();
        self.tail.take();
        if let Some((accountant, bytes)) = self.metadata_charge.take() {
            accountant.release(bytes);
        }
    }
}

struct LayoutDigest(blake3::Hasher);
impl Write for LayoutDigest {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.0.update(value.as_bytes());
        Ok(())
    }
}

impl Database {
    fn writer_tail_probe_bytes(partition: &VectorPartitionRef) -> usize {
        partition
            .partition_key
            .estimated_bytes()
            .saturating_add(partition.index.table.len())
            .saturating_add(partition.index.column.len())
            .saturating_mul(8)
            .saturating_add(64 * 1024)
    }

    fn writer_tail_layout(&self, partition: &VectorPartitionRef) -> Result<[u8; 32]> {
        self.vector_store
            .with_index_layout(&partition.index, |layout| {
                let mut digest = LayoutDigest(blake3::Hasher::new());
                write!(&mut digest, "{layout:?}").expect("digest writer cannot fail");
                *digest.0.finalize().as_bytes()
            })
    }

    /// Detach opportunistically at an import close. Failure discards the cache;
    /// normal durable recovery remains the sole authority for subsequent opens.
    #[doc(hidden)]
    pub fn detach_writer_vector_session(
        &self,
        session: &WriterVectorSession,
        partition: &VectorPartitionRef,
    ) {
        session.clear();
        let saved = self
            .tx_mgr
            .with_commit_lock(|| -> Result<Option<CarriedWriterTail>> {
                let Some(persistence) = self.persistence.as_ref() else {
                    return Ok(None);
                };
                let _workspace = crate::memory_accounting::ScopedMemoryReservation::try_new_for(
                    &self.accountant,
                    Self::writer_tail_probe_bytes(partition),
                    "vector_index",
                    "detach_writer_tail",
                    "Discard the optional import tail when validation cannot fit.",
                )?;
                let Some((database_identity, _, _, _)) =
                    crate::persistence::published_writer_identity(persistence.path())
                else {
                    return Ok(None);
                };
                let Some(frontier) = persistence.writer_vector_tail_frontier(partition)? else {
                    return Ok(None);
                };
                let layout = self.writer_tail_layout(partition)?;
                let physical_identity = persistence.physical_identity()?;
                let Some(mut tail) = self.vector_store.detach_writer_tail(partition) else {
                    return Ok(None);
                };
                let metadata = std::mem::size_of::<CarriedWriterTail>()
                    .saturating_add(partition.partition_key.estimated_bytes())
                    .saturating_add(partition.index.table.len())
                    .saturating_add(partition.index.column.len());
                if tail.admit_metadata(metadata).is_err() {
                    return Ok(None);
                }
                Ok(Some(CarriedWriterTail {
                    partition: partition.clone(),
                    physical_identity,
                    database_identity,
                    frontier,
                    layout,
                    tail: Some(tail),
                    metadata_charge: None,
                }))
            });
        if let Ok(saved) = saved {
            *session.saved.lock() = saved;
        }
    }

    #[doc(hidden)]
    pub fn reattach_writer_vector_session(&self, session: &WriterVectorSession) {
        let mut saved = session.take();
        if saved.is_none() {
            return;
        }
        let Ok(accountant) = self.accountant.vector_budget() else {
            return;
        };
        if saved
            .as_mut()
            .is_some_and(|tail| tail.recharge(accountant).is_err())
        {
            return;
        }
        self.reattach_carried_writer_tail(saved);
    }

    pub(super) fn reattach_carried_writer_tail(&self, saved: Option<CarriedWriterTail>) {
        let Some(mut saved) = saved else {
            return;
        };
        // All validation occurs at this reopened writer's committed boundary.
        // Any frontier or schema change selects ordinary reload, never stale reuse.
        self.tx_mgr.with_commit_lock(|| {
            let valid = (|| -> Result<bool> {
                let Some(persistence) = self.persistence.as_ref() else {
                    return Ok(false);
                };
                let _workspace = crate::memory_accounting::ScopedMemoryReservation::try_new_for(
                    &self.accountant,
                    Self::writer_tail_probe_bytes(&saved.partition),
                    "vector_index",
                    "reattach_writer_tail",
                    "Discard the optional import tail when validation cannot fit.",
                )?;
                let Some((identity, _, _, _)) =
                    crate::persistence::published_writer_identity(persistence.path())
                else {
                    return Ok(false);
                };
                Ok(identity == saved.database_identity
                    && persistence.physical_identity()? == saved.physical_identity
                    && persistence.writer_vector_tail_frontier(&saved.partition)?
                        == Some(saved.frontier)
                    && self.writer_tail_layout(&saved.partition)? == saved.layout)
            })()
            .unwrap_or(false);
            if valid {
                // Release the route before the vector tail releases its admission.
                if let Some(mut tail) = saved.tail.take() {
                    saved.metadata_charge = Some(tail.take_metadata_charge());
                    self.vector_store
                        .reattach_writer_tail(&saved.partition, tail);
                }
            }
        });
    }
}
