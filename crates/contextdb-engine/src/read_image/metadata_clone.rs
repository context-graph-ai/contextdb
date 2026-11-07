//! Quotes the destinations of a standard metadata clone before it happens.
//! Native B-tree nodes use the existing library-capacity upper bound; text and
//! Vec destinations use their actual clone lengths, never encoded-byte ratios.
use crate::direct_file_reader::*;
use contextdb_core::{Error, Result};
use std::collections::BTreeMap;
pub(crate) trait CloneHeapBytes {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn clone_heap_bytes(&self) -> Result<usize>;
}
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn sum(values: impl IntoIterator<Item = usize>) -> Result<usize> {
    values.into_iter().try_fold(0usize, |sum, value| {
        sum.checked_add(value)
            .ok_or_else(|| Error::Other("metadata copy size overflow".into()))
    })
}
macro_rules! inline { ($($ty:ty),* $(,)?) => { $(impl CloneHeapBytes for $ty {
    fn clone_heap_bytes(&self) -> Result<usize> { Ok(0) }
})* }; }
inline!(
    bool,
    usize,
    u32,
    u64,
    DirectVectorQuantization,
    DirectIndexDirection,
    contextdb_core::VectorSearchMode
);
impl CloneHeapBytes for String {
    fn clone_heap_bytes(&self) -> Result<usize> {
        Ok(self.len())
    }
}
impl<T: CloneHeapBytes> CloneHeapBytes for Option<T> {
    fn clone_heap_bytes(&self) -> Result<usize> {
        self.as_ref()
            .map_or(Ok(0), CloneHeapBytes::clone_heap_bytes)
    }
}
impl<T: CloneHeapBytes> CloneHeapBytes for Vec<T> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn clone_heap_bytes(&self) -> Result<usize> {
        let slots = self
            .len()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| Error::Other("metadata copy size overflow".into()))?;
        self.iter().try_fold(slots, |bytes, value| {
            sum([bytes, value.clone_heap_bytes()?])
        })
    }
}
impl<K: CloneHeapBytes + Ord, V: CloneHeapBytes> CloneHeapBytes for BTreeMap<K, V> {
    #[expect(
        clippy::result_large_err,
        reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
    )]
    fn clone_heap_bytes(&self) -> Result<usize> {
        let nodes = crate::local_transport::query_funding::tree_bytes::<K, V>(self.len())?;
        self.iter().try_fold(nodes, |bytes, (key, value)| {
            sum([bytes, key.clone_heap_bytes()?, value.clone_heap_bytes()?])
        })
    }
}
impl CloneHeapBytes for DirectReferencePropagation {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.on_state.clone_heap_bytes()?,
            self.set_state.clone_heap_bytes()?,
            self.max_depth.clone_heap_bytes()?,
            self.abort_on_failure.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectColumnReference {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.table.clone_heap_bytes()?,
            self.column.clone_heap_bytes()?,
            self.propagation.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectRankPolicy {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.sort_key.clone_heap_bytes()?,
            self.formula.clone_heap_bytes()?,
            self.joined_table.clone_heap_bytes()?,
            self.joined_column.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectSchemaColumn {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.data_type.clone_heap_bytes()?,
            self.nullable.clone_heap_bytes()?,
            self.primary_key.clone_heap_bytes()?,
            self.unique.clone_heap_bytes()?,
            self.immutable.clone_heap_bytes()?,
            self.expires.clone_heap_bytes()?,
            self.default.clone_heap_bytes()?,
            self.references.clone_heap_bytes()?,
            self.quantization.clone_heap_bytes()?,
            self.partition_key_columns.clone_heap_bytes()?,
            self.max_partitions.clone_heap_bytes()?,
            self.search_mode.clone_heap_bytes()?,
            self.rank.clone_heap_bytes()?,
            self.scope_label.clone_heap_bytes()?,
            self.acl_ref.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectIndexColumn {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.column.clone_heap_bytes()?,
            self.direction.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectSchemaIndex {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.columns.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectStateMachine {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.column.clone_heap_bytes()?,
            self.transitions.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectRetainPolicy {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.window.clone_heap_bytes()?,
            self.unit.clone_heap_bytes()?,
            self.seconds.clone_heap_bytes()?,
            self.sync_safe.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectSchema {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.table.clone_heap_bytes()?,
            self.immutable.clone_heap_bytes()?,
            self.columns.clone_heap_bytes()?,
            self.primary_key.clone_heap_bytes()?,
            self.indexes.clone_heap_bytes()?,
            self.state_machine.clone_heap_bytes()?,
            self.retain.clone_heap_bytes()?,
            self.history.clone_heap_bytes()?,
            self.sync_direction.clone_heap_bytes()?,
            self.conflict_policy.clone_heap_bytes()?,
            self.dag_edge_types.clone_heap_bytes()?,
            self.propagate.clone_heap_bytes()?,
            self.ddl.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectConfigurationState {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.memory_limit_bytes.clone_heap_bytes()?,
            self.disk_limit_bytes.clone_heap_bytes()?,
            self.schemas.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectEventTypeStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.trigger.clone_heap_bytes()?,
            self.table.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectSinkStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.sink_type.clone_heap_bytes()?,
            self.callback_registered.clone_heap_bytes()?,
            self.delivered.clone_heap_bytes()?,
            self.queued.clone_heap_bytes()?,
            self.retried.clone_heap_bytes()?,
            self.uncertain.clone_heap_bytes()?,
            self.permanent_failures.clone_heap_bytes()?,
            self.examined.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectRouteStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.event_type.clone_heap_bytes()?,
            self.sink.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectScheduleStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.name.clone_heap_bytes()?,
            self.every.clone_heap_bytes()?,
            self.callback.clone_heap_bytes()?,
            self.callback_registered.clone_heap_bytes()?,
            self.next_fire_at_ms.clone_heap_bytes()?,
            self.last_fire_at_ms.clone_heap_bytes()?,
            self.fire_count.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectEventsStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.event_types.clone_heap_bytes()?,
            self.sinks.clone_heap_bytes()?,
            self.routes.clone_heap_bytes()?,
            self.schedules.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectMaintenanceStatus {
    fn clone_heap_bytes(&self) -> Result<usize> {
        sum([
            self.policy.clone_heap_bytes()?,
            self.running.clone_heap_bytes()?,
            self.retention_enabled.clone_heap_bytes()?,
            self.currency_compaction_enabled.clone_heap_bytes()?,
            self.active_maintenance_loops.clone_heap_bytes()?,
        ])
    }
}
impl CloneHeapBytes for DirectScopeLabelKind {
    fn clone_heap_bytes(&self) -> Result<usize> {
        match self {
            Self::Simple { write_labels } => sum([write_labels.clone_heap_bytes()?]),
            Self::Split {
                read_labels,
                write_labels,
            } => sum([
                read_labels.clone_heap_bytes()?,
                write_labels.clone_heap_bytes()?,
            ]),
        }
    }
}
impl CloneHeapBytes for DirectPropagationRule {
    fn clone_heap_bytes(&self) -> Result<usize> {
        match self {
            Self::Edge {
                edge_type,
                direction,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => sum([
                edge_type.clone_heap_bytes()?,
                direction.clone_heap_bytes()?,
                on_state.clone_heap_bytes()?,
                set_state.clone_heap_bytes()?,
                max_depth.clone_heap_bytes()?,
                abort_on_failure.clone_heap_bytes()?,
            ]),
            Self::VectorExclusion { on_state } => sum([on_state.clone_heap_bytes()?]),
            Self::ForeignKey {
                column,
                references_table,
                references_column,
                on_state,
                set_state,
                max_depth,
                abort_on_failure,
            } => sum([
                column.clone_heap_bytes()?,
                references_table.clone_heap_bytes()?,
                references_column.clone_heap_bytes()?,
                on_state.clone_heap_bytes()?,
                set_state.clone_heap_bytes()?,
                max_depth.clone_heap_bytes()?,
                abort_on_failure.clone_heap_bytes()?,
            ]),
        }
    }
}
