mod inspection;
mod maintenance;
pub(crate) use inspection::{ReadOnlySelection, read_only_selection};
pub(crate) mod policy;
mod query;
pub(crate) mod resolved;

#[cfg(feature = "test-seams")]
mod observations;

use super::*;
impl Database {
    #[cfg(feature = "test-seams")]
    pub(crate) fn resolve_native_ivf_policy(
        &self,
        index: &VectorIndexRef,
        tuple: &VectorPartitionKey,
    ) -> Result<resolved::Resolved> {
        let read_memory = self.declared_reader_memory()?;
        let (population, source_identity) = if let Some(persistence) = &self.persistence {
            persistence.native_ivf_source_summary(index, tuple, read_memory)?
        } else {
            let mut owner = crate::vector_read_memory::admission(read_memory);
            (
                self.vector_store
                    .try_partition_info(index, tuple, &mut owner)?
                    .map_or(0, |info| info.live_rows as u64),
                [0; 32],
            )
        };
        let metadata = self.relational_store().table_meta.read();
        let column = metadata
            .get(&index.table)
            .and_then(|table| table.columns.iter().find(|c| c.name == index.column))
            .ok_or_else(|| Error::Other("vector policy names no declared column".into()))?;
        if self.persistence.is_some() {
            // Observe resolution after the real factory has admitted its native
            // scope and transaction. Those owners include storage receipts and
            // dirty backing; a pre-factory sizeof subtraction cannot price them.
            // The production resolver remains the only policy/layout evaluator.
            return self.with_aborting_native_ivf_write_for_test(
                index,
                tuple,
                crate::vector_ivf_observations::NativeIvfDriveCallbacks {
                    checkpoint: &|| Ok(()),
                    before_distance: &|_| Ok(()),
                },
                |scope| {
                    let usage = self.finalization.accountant.usage();
                    let controls = crate::persistence::vector_ivf::writer_operation_control_bytes();
                    let ColumnType::Vector(dimensions) = column.column_type else {
                        return Err(Error::Other(
                            "vector policy requires a vector column".into(),
                        ));
                    };
                    let source_bytes = (dimensions as usize).checked_mul(8).ok_or_else(|| {
                        Error::Other("clustered source layout is not representable".into())
                    })?;
                    resolved::resolve_with_native_receipts(
                        index,
                        column,
                        population,
                        usage.limit.map_or(u64::MAX, |n| n as u64),
                        usage
                            .available
                            .unwrap_or(usize::MAX)
                            .saturating_sub(controls)
                            .saturating_sub(source_bytes),
                        crate::persistence::vector_ivf::training_identity(tuple, source_identity),
                        scope.native_entry_backing_quote()?,
                        None,
                    )
                },
            );
        }
        let usage = self.finalization.accountant.usage();
        let controls = crate::persistence::vector_ivf::writer_control_bytes(index, tuple)?;
        let available = usage
            .available
            .unwrap_or(usize::MAX)
            .saturating_sub(controls);
        resolved::resolve_with_native_receipts(
            index,
            column,
            population,
            usage.limit.map_or(u64::MAX, |n| n as u64),
            available,
            crate::persistence::vector_ivf::training_identity(tuple, source_identity),
            0,
            None,
        )
    }
}

impl Database {
    pub(crate) fn validate_native_ivf_declaration(
        &self,
        index: &VectorIndexRef,
        column: &ColumnDef,
    ) -> Result<()> {
        policy::validate(&index.table, column)?;
        if column.ivf_lists.is_none() {
            return Ok(());
        }
        let usage = self.finalization.accountant.usage();
        let controls = crate::persistence::vector_ivf::writer_control_bytes(
            index,
            &VectorPartitionKey::unpartitioned(),
        )?;
        resolved::resolve(
            index,
            column,
            0,
            usage.limit.map_or(u64::MAX, |n| n as u64),
            usage
                .available
                .unwrap_or(usize::MAX)
                .saturating_sub(controls),
            [0; 32],
        )?;
        Ok(())
    }
}
