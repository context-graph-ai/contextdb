//! Optional writer checkpoint receipts.
use super::*;
use std::io::Write;

impl Database {
    pub(super) fn writer_receipt_observer_for_maintenance(&self) -> Option<Arc<Database>> {
        std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH")?;
        // The existing internal view shares the stores and their charges.
        // Only an enabled observer retains this view; the maintenance worker
        // drops it when close stops and joins that worker.
        let mut observer = self.scoped_with_constraints(None, None, None);
        observer.id = self.id;
        Some(Arc::new(observer))
    }

    pub(super) fn emit_writer_memory_ownership_receipt_for_test(&self, checkpoint: &str) {
        let Some(path) = std::env::var_os("CONTEXTDB_TEST_SEAMS_RECEIPT_PATH") else {
            return;
        };
        // No caller holds the commit boundary here. Serialize samples before
        // capturing their roots so concurrent commits cannot reorder the lines.
        static EMISSION: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
        let _emission = EMISSION.lock();
        let (receipt, lsn) = self.with_writer_memory_ownership_receipt_for_test(|receipt| {
            (receipt, self.tx_mgr.capture_published_read().lsn)
        });
        let rss_bytes = current_process_rss_bytes();
        let current_rows_by_table = receipt
            .current_rows_by_table
            .iter()
            .map(|(table, owner)| {
                (
                    table.clone(),
                    serde_json::json!({
                        "versions": owner.versions,
                        "values": owner.values,
                        "side_maps": owner.side_maps,
                        "index_postings": owner.index_postings,
                        "f32_projections": owner.f32_projections,
                    }),
                )
            })
            .collect::<serde_json::Map<String, serde_json::Value>>();
        let mut owners = serde_json::Map::new();
        owners.insert(
            "vector".into(),
            serde_json::json!({
                "raw_vector_bodies": receipt.vector.raw_vector_bodies,
                "raw_reverse_directory": receipt.vector.raw_reverse_directory,
                "partition_vectors": receipt.vector.partition_vectors,
                "base_graph": receipt.vector.base_graph,
                "change_graph": receipt.vector.change_graph,
                "mutable_tail": receipt.vector.mutable_tail,
                "typed_partition_keys": receipt.vector.typed_partition_keys,
                "retired_pinned_graph_bytes": receipt.vector.retired_pinned_graph_bytes,
                "temporary_workspace": receipt.vector.temporary_workspace,
                "retained_total": receipt.vector.retained_total,
                "non_vector_total": receipt.vector.non_vector_total,
                "accountant_total": receipt.vector.accountant_total,
            }),
        );
        owners.insert(
            "current_rows_by_table".into(),
            serde_json::Value::Object(current_rows_by_table),
        );
        owners.insert(
            "relational_inventory".into(),
            serde_json::json!({
                "row_versions": receipt.relational_inventory.row_versions,
                "row_positions": receipt.relational_inventory.row_positions,
                "row_version_positions": receipt.relational_inventory.row_version_positions,
                "sync_provenance": receipt.relational_inventory.sync_provenance,
                "index_trees": receipt.relational_inventory.index_trees,
                "index_exact": receipt.relational_inventory.index_exact,
                "index_exact_filters": receipt.relational_inventory.index_exact_filters,
                "index_membership": receipt.relational_inventory.index_membership,
                "index_descriptors": receipt.relational_inventory.index_descriptors,
            }),
        );
        owners.insert(
            "current_row_versions".into(),
            serde_json::json!(receipt.current_row_versions),
        );
        owners.insert(
            "historical_row_versions".into(),
            serde_json::json!(receipt.historical_row_versions),
        );
        owners.insert(
            "current_row_values".into(),
            serde_json::json!(receipt.current_row_values),
        );
        owners.insert(
            "current_row_side_maps".into(),
            serde_json::json!(receipt.current_row_side_maps),
        );
        owners.insert(
            "current_row_index_postings".into(),
            serde_json::json!(receipt.current_row_index_postings),
        );
        owners.insert(
            "current_row_f32_projections".into(),
            serde_json::json!(receipt.current_row_f32_projections),
        );
        owners.insert(
            "historical_row_priced_bytes".into(),
            serde_json::json!(receipt.historical_row_priced_bytes),
        );
        owners.insert(
            "staged_row_versions".into(),
            serde_json::json!(receipt.staged_row_versions),
        );
        owners.insert(
            "staged_row_admissions".into(),
            serde_json::json!(receipt.staged_row_admissions),
        );
        owners.insert(
            "staged_vector_admissions".into(),
            serde_json::json!(receipt.staged_vector_admissions),
        );
        owners.insert(
            "staged_vector_owner_controls".into(),
            serde_json::json!(receipt.staged_vector_owner_controls),
        );
        owners.insert(
            "staged_index_postings".into(),
            serde_json::json!(receipt.staged_index_postings),
        );
        owners.insert(
            "table_metadata".into(),
            serde_json::json!(receipt.table_metadata),
        );
        owners.insert(
            "graph_entries".into(),
            serde_json::json!(receipt.graph_entries),
        );
        owners.insert(
            "change_log_entries".into(),
            serde_json::json!(receipt.change_log_entries),
        );
        owners.insert(
            "change_log_charges".into(),
            serde_json::json!(receipt.change_log_charges),
        );
        owners.insert(
            "commit_index_entries".into(),
            serde_json::json!(receipt.commit_index_entries),
        );
        owners.insert(
            "commit_index_charge".into(),
            serde_json::json!(receipt.commit_index_charge),
        );
        owners.insert(
            "persistence_cache".into(),
            serde_json::json!(receipt.persistence_cache),
        );
        owners.insert(
            "current_membership_charge".into(),
            serde_json::json!(receipt.current_membership_charge),
        );
        owners.insert(
            "captured_membership_charge".into(),
            serde_json::json!(receipt.captured_membership_charge),
        );
        owners.insert(
            "current_membership_by_table".into(),
            serde_json::json!(receipt.current_membership_by_table),
        );
        owners.insert(
            "ddl_log_entries".into(),
            serde_json::json!(receipt.ddl_log_entries),
        );
        owners.insert(
            "ddl_log_estimated_bytes".into(),
            serde_json::json!(receipt.ddl_log_estimated_bytes),
        );
        owners.insert(
            "active_transactions".into(),
            serde_json::json!(receipt.active_transactions),
        );
        owners.insert(
            "pending_commit_metadata".into(),
            serde_json::json!(receipt.pending_commit_metadata),
        );
        owners.insert(
            "pending_schema_stages".into(),
            serde_json::json!(receipt.pending_schema_stages),
        );
        owners.insert(
            "named_owner_total".into(),
            serde_json::json!(receipt.named_owner_total),
        );
        owners.insert(
            "reconciliation_gap".into(),
            serde_json::json!(receipt.reconciliation_gap),
        );
        owners.insert(
            "published_root_charge".into(),
            serde_json::json!(receipt.published_root_charge),
        );
        owners.insert(
            "captured_root_charge".into(),
            serde_json::json!(receipt.captured_root_charge),
        );
        owners.insert(
            "commit_source_reference_charge".into(),
            serde_json::json!(receipt.commit_source_reference_charge),
        );
        owners.insert(
            "storage_maintenance_control_charge".into(),
            serde_json::json!(receipt.storage_maintenance_control_charge),
        );
        let line = serde_json::json!({
            "kind": "writer_memory_ownership",
            "checkpoint": checkpoint,
            "lsn": lsn.0,
            "pid": std::process::id(),
            "database_id": self.id.0,
            "wallclock_ms": Wallclock::now().0,
            "rss_bytes": rss_bytes,
            "receipt": owners,
        });
        let bytes = serde_json::to_vec(&line).map(|mut bytes| {
            bytes.push(b'\n');
            bytes
        });
        let result = bytes.map_err(std::io::Error::other).and_then(|bytes| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .and_then(|mut file| file.write_all(&bytes))
        });
        if let Err(error) = result {
            tracing::warn!(?path, %error, "test-seams writer receipt emission failed");
        }
    }
}

fn current_process_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                .checked_mul(1024)
        })
}
