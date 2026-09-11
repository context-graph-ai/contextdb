//! A held complete unit can predate its first custody terminal.
use super::*;
use crate::custody::{
    canonical::*,
    preparation,
    records::{Digest, RowRef},
};

impl Database {
    pub(crate) fn custody_incumbent_digest(&self, root: &RowRef) -> Result<Option<Digest>> {
        let Some(meta) = self.table_meta(&root.table) else {
            return Ok(None);
        };
        let snapshot = self.snapshot();
        let Some(row) =
            self.visible_row_by_natural_key(&root.table, &root.key, snapshot, &HashSet::new())?
        else {
            return Ok(None);
        };
        // An existing manifest owns exact membership, even when no edge has
        // delivered this locally written unit. Never grow it by foreign keys.
        if let Some(manifest) = preparation::current_manifest(self, &root.table, &root.key)? {
            return Ok(Some(manifest.seal.unit_digest));
        }
        let (_, values) = self
            .row_change_values_from_row(&root.table, &row)?
            .ok_or_else(invalid)?;
        let parent = RowChange {
            table: root.table.clone(),
            natural_key: root.key.clone(),
            values,
            deleted: false,
            lsn: row.lsn,
            created_at: row.created_at,
        };
        let mut members = BTreeMap::new();
        // This fallback is only for a held root without any custody terminal or
        // manifest. Its unmanifested membership follows the existing declared
        // foreign keys; checking just the arriving references would miss extra
        // held members and incorrectly equate a subset with the complete unit.
        for table in meta.delivery_manifest_tables.as_deref().unwrap_or_default() {
            if self.table_meta(table).is_none() {
                continue;
            }
            for row in self
                .relational
                .scan_filter_with_tx(None, table, snapshot, &|_| true)?
            {
                let (key, values) = self
                    .row_change_values_from_row(table, &row)?
                    .ok_or_else(invalid)?;
                let child = RowChange {
                    table: table.clone(),
                    natural_key: key.clone(),
                    values,
                    deleted: false,
                    lsn: row.lsn,
                    created_at: row.created_at,
                };
                if self.custody_row_references(&child, &parent) {
                    let reference = RowRef {
                        table: table.clone(),
                        key,
                    };
                    // Registration never permits the root to also be a member.
                    if &reference != root {
                        members.insert(
                            reference.order_key(),
                            (reference, content_row_digest(&child.values, &[])),
                        );
                    }
                }
            }
        }
        let mut unit = Encoder::domain("delivery-content-unit.v1");
        root.encode(&mut unit);
        unit.raw(&content_row_digest(&parent.values, &[]));
        unit.u64(members.len() as u64);
        for (reference, digest) in members.values() {
            reference.encode(&mut unit);
            unit.raw(digest);
        }
        Ok(Some(unit.digest()))
    }
}
