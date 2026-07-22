use super::*;

type CountedGraphNeighbor = (NodeId, EdgeType, NodeId, NodeId);
pub(crate) type CountedGraphScanEdge = (NodeId, NodeId);
type CountedGraphScanCandidate = (GraphScanCandidate, EdgeType, CountedGraphScanEdge);
type GraphEdgeKey = (NodeId, EdgeType, NodeId);

#[derive(Clone, Copy)]
struct GraphScanCandidate {
    start: NodeId,
    neighbor: NodeId,
    source: NodeId,
    target: NodeId,
}

impl Database {
    pub(super) fn access_is_admin(&self) -> bool {
        self.access.contexts.is_none()
            && self.access.scope_labels.is_none()
            && self.access.principal.is_none()
    }

    pub(crate) fn has_access_constraints_for_query(&self) -> bool {
        !self.access_is_admin()
    }

    pub(crate) fn has_context_or_principal_constraints(&self) -> bool {
        self.access.contexts.is_some() || self.access.principal.is_some()
    }

    fn context_column<'a>(&self, meta: &'a TableMeta) -> Option<&'a ColumnDef> {
        meta.columns.iter().find(|column| column.context_id)
    }

    fn scope_column<'a>(&self, meta: &'a TableMeta) -> Option<&'a ColumnDef> {
        meta.columns
            .iter()
            .find(|column| column.scope_label.is_some())
    }

    fn acl_column<'a>(&self, meta: &'a TableMeta) -> Option<&'a ColumnDef> {
        meta.columns.iter().find(|column| column.acl_ref.is_some())
    }

    fn meta_has_read_gate(&self, meta: &TableMeta) -> bool {
        let context_gate = self.access.contexts.is_some() && self.context_column(meta).is_some();
        let scope_gate = self.access.scope_labels.is_some()
            && self
                .scope_column(meta)
                .and_then(|column| column.scope_label.as_ref())
                .is_some_and(|scope| matches!(scope, ScopeLabelKind::Split { .. }));
        let acl_gate = !self.access_is_admin() && self.acl_column(meta).is_some();
        context_gate || scope_gate || acl_gate
    }

    fn table_has_read_gate(&self, table: &str) -> Result<bool> {
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        Ok(self.meta_has_read_gate(&meta))
    }

    pub(crate) fn assert_table_read_allowed(&self, table: &str) -> Result<()> {
        if self.access_is_admin() {
            return Ok(());
        }
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        if self.acl_column(&meta).is_some()
            && !matches!(
                self.access.principal,
                Some(Principal::Agent(_)) | Some(Principal::Human(_))
            )
        {
            return Err(Error::PrincipalRequired {
                table: table.to_string(),
            });
        }
        Ok(())
    }

    pub(crate) fn complete_insert_access_values(
        &self,
        table: &str,
        values: &mut HashMap<ColName, Value>,
    ) -> Result<()> {
        let Some(contexts) = self.access.contexts.as_ref() else {
            return Ok(());
        };
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        let Some(column) = self.context_column(&meta) else {
            return Ok(());
        };
        if values.contains_key(&column.name) {
            return Ok(());
        }
        if contexts.len() == 1 {
            let context = contexts.iter().next().expect("len checked").0;
            values.insert(column.name.clone(), Value::Uuid(context));
            return Ok(());
        }
        Err(Error::ContextScopeViolation {
            requested: ContextId::new(uuid::Uuid::from_u128(u128::MAX)),
            allowed: contexts.clone(),
        })
    }

    pub(super) fn read_allowed_for_row(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.read_allowed_for_row_cached_in_tx(None, table, meta, row, snapshot, None)
    }

    fn read_allowed_for_row_cached(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
    ) -> Result<bool> {
        self.read_allowed_for_row_cached_in_tx(None, table, meta, row, snapshot, allowed_acl_ids)
    }

    fn read_allowed_for_row_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.read_allowed_for_row_cached_in_tx(tx, table, meta, row, snapshot, None)
    }

    fn read_allowed_for_row_cached_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
    ) -> Result<bool> {
        Ok(self
            .read_denial_for_row_cached_in_tx(tx, table, meta, row, snapshot, allowed_acl_ids)?
            .is_none())
    }

    fn read_denial_for_row_cached(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
    ) -> Result<Option<Error>> {
        self.read_denial_for_row_cached_in_tx(None, table, meta, row, snapshot, allowed_acl_ids)
    }

    fn read_denial_for_row_cached_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
    ) -> Result<Option<Error>> {
        if self.access_is_admin() {
            return Ok(None);
        }
        if let (Some(contexts), Some(column)) =
            (self.access.contexts.as_ref(), self.context_column(meta))
        {
            let requested = match row.values.get(&column.name) {
                Some(Value::Uuid(context)) => ContextId::new(*context),
                _ => ContextId::new(uuid::Uuid::from_u128(u128::MAX)),
            };
            if !contexts.contains(&requested) {
                return Ok(Some(Error::ContextScopeViolation {
                    requested,
                    allowed: contexts.clone(),
                }));
            }
        }
        if let (Some(labels), Some(column)) =
            (self.access.scope_labels.as_ref(), self.scope_column(meta))
            && let Some(ScopeLabelKind::Split { read_labels, .. }) = column.scope_label.as_ref()
        {
            let requested = match row.values.get(&column.name) {
                Some(Value::Text(label)) => ScopeLabel::new(label.clone()),
                _ => ScopeLabel::new(""),
            };
            if !read_labels.iter().any(|read| read == &requested.0) {
                return Ok(Some(Error::ScopeLabelViolation {
                    requested,
                    allowed: BTreeSet::new(),
                }));
            }
            if !labels.contains(&requested) {
                return Ok(Some(Error::ScopeLabelViolation {
                    requested,
                    allowed: labels.clone(),
                }));
            }
        }
        if let Some(column) = self.acl_column(meta) {
            let Some(principal) = self.access.principal.as_ref() else {
                return Err(Error::PrincipalRequired {
                    table: table.to_string(),
                });
            };
            if matches!(principal, Principal::System) {
                return Err(Error::PrincipalRequired {
                    table: table.to_string(),
                });
            }
            let allowed = match row.values.get(&column.name) {
                Some(Value::Uuid(acl_id)) => match allowed_acl_ids {
                    Some(allowed) => allowed.contains(acl_id),
                    None => {
                        let acl_ref = column.acl_ref.as_ref().expect("acl column");
                        self.principal_has_acl_grant_in_tx(
                            tx, principal, acl_ref, *acl_id, snapshot,
                        )?
                    }
                },
                _ => false,
            };
            if !allowed {
                return Ok(Some(Error::AclDenied {
                    table: table.to_string(),
                    row_id: row.row_id,
                    principal: principal.clone(),
                }));
            }
        }
        Ok(None)
    }

    pub(crate) fn filter_rows_for_read(
        &self,
        table: &str,
        rows: Vec<VersionedRow>,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        self.filter_rows_for_read_in_tx(None, table, rows, snapshot)
    }

    pub(crate) fn filter_rows_for_read_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        rows: Vec<VersionedRow>,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        self.assert_table_read_allowed(table)?;
        if self.access_is_admin() || rows.is_empty() {
            return Ok(rows);
        }
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        let allowed_acl_ids = if let Some(column) = self.acl_column(&meta) {
            let principal = self
                .access
                .principal
                .as_ref()
                .expect("assert_table_read_allowed rejects missing principals");
            Some(self.allowed_acl_ids_for_principal_in_tx(
                tx,
                principal,
                column.acl_ref.as_ref().expect("acl column"),
                snapshot,
            )?)
        } else {
            None
        };
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            if self.read_allowed_for_row_cached(
                table,
                &meta,
                &row,
                snapshot,
                allowed_acl_ids.as_ref().map(|ids| ids.as_ref()),
            )? {
                out.push(row);
            }
        }
        Ok(out)
    }

    pub(crate) fn filter_rows_for_anchor_read(
        &self,
        table: &str,
        rows: Vec<VersionedRow>,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        self.filter_rows_for_anchor_read_in_tx(None, table, rows, snapshot)
    }

    pub(crate) fn filter_rows_for_anchor_read_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        rows: Vec<VersionedRow>,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        self.assert_table_read_allowed(table)?;
        if self.access_is_admin() || rows.is_empty() {
            return Ok(rows);
        }
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        let allowed_acl_ids = if let Some(column) = self.acl_column(&meta) {
            let principal = self
                .access
                .principal
                .as_ref()
                .expect("assert_table_read_allowed rejects missing principals");
            Some(self.allowed_acl_ids_for_principal_in_tx(
                tx,
                principal,
                column.acl_ref.as_ref().expect("acl column"),
                snapshot,
            )?)
        } else {
            None
        };
        let mut out = Vec::with_capacity(rows.len());
        let mut first_denial = None;
        for row in rows {
            match self.read_denial_for_row_cached(
                table,
                &meta,
                &row,
                snapshot,
                allowed_acl_ids.as_ref().map(|ids| ids.as_ref()),
            )? {
                Some(err) => {
                    if first_denial.is_none() {
                        first_denial = Some(err);
                    }
                }
                None => out.push(row),
            }
        }
        if out.is_empty()
            && let Some(err) = first_denial
        {
            return Err(err);
        }
        Ok(out)
    }

    fn write_allowed_for_values(
        &self,
        table: &str,
        meta: &TableMeta,
        row_id: RowId,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if self.access_is_admin() {
            return Ok(());
        }
        if let (Some(contexts), Some(column)) =
            (self.access.contexts.as_ref(), self.context_column(meta))
        {
            let requested = match values.get(&column.name) {
                Some(Value::Uuid(context)) => ContextId::new(*context),
                _ => ContextId::new(uuid::Uuid::from_u128(u128::MAX)),
            };
            if !contexts.contains(&requested) {
                return Err(Error::ContextScopeViolation {
                    requested,
                    allowed: contexts.clone(),
                });
            }
        }
        if let (Some(labels), Some(column)) =
            (self.access.scope_labels.as_ref(), self.scope_column(meta))
            && let Some(scope) = column.scope_label.as_ref()
        {
            let requested = match values.get(&column.name) {
                Some(Value::Text(label)) => ScopeLabel::new(label.clone()),
                _ => ScopeLabel::new(""),
            };
            let schema_write = match scope {
                ScopeLabelKind::Simple { write_labels } => write_labels,
                ScopeLabelKind::Split { write_labels, .. } => write_labels,
            };
            if !schema_write.iter().any(|label| label == &requested.0) {
                return Err(Error::ScopeLabelViolation {
                    requested,
                    allowed: BTreeSet::new(),
                });
            }
            if !labels.contains(&requested) {
                return Err(Error::ScopeLabelViolation {
                    requested,
                    allowed: labels.clone(),
                });
            }
        }
        if let Some(column) = self.acl_column(meta) {
            let Some(principal) = self.access.principal.as_ref() else {
                return Err(Error::PrincipalRequired {
                    table: table.to_string(),
                });
            };
            if matches!(principal, Principal::System) {
                return Err(Error::PrincipalRequired {
                    table: table.to_string(),
                });
            }
            let allowed = match values.get(&column.name) {
                Some(Value::Uuid(acl_id)) => {
                    let acl_ref = column.acl_ref.as_ref().expect("acl column");
                    self.principal_has_acl_grant(principal, acl_ref, *acl_id, snapshot)?
                }
                _ => false,
            };
            if !allowed {
                return Err(Error::AclDenied {
                    table: table.to_string(),
                    row_id,
                    principal: principal.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn assert_row_write_allowed(
        &self,
        table: &str,
        row_id: RowId,
        values: &HashMap<ColName, Value>,
        snapshot: SnapshotId,
    ) -> Result<()> {
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        self.write_allowed_for_values(table, &meta, row_id, values, snapshot)
    }

    fn principal_has_acl_grant(
        &self,
        principal: &Principal,
        acl_ref: &AclRef,
        acl_id: uuid::Uuid,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.principal_has_acl_grant_in_tx(None, principal, acl_ref, acl_id, snapshot)
    }

    fn principal_has_acl_grant_in_tx(
        &self,
        tx: Option<TxId>,
        principal: &Principal,
        acl_ref: &AclRef,
        acl_id: uuid::Uuid,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        Ok(self
            .allowed_acl_ids_for_principal_in_tx(tx, principal, acl_ref, snapshot)?
            .contains(&acl_id))
    }

    fn grant_row_context_scope_allowed(
        &self,
        table: &str,
        row: &VersionedRow,
        _snapshot: SnapshotId,
    ) -> Result<bool> {
        if self.access.contexts.is_none() && self.access.scope_labels.is_none() {
            return Ok(true);
        }
        let Some(meta) = self.table_meta(table) else {
            return Ok(false);
        };
        if let (Some(contexts), Some(column)) =
            (self.access.contexts.as_ref(), self.context_column(&meta))
        {
            match row.values.get(&column.name) {
                Some(Value::Uuid(context)) if contexts.contains(&ContextId::new(*context)) => {}
                _ => return Ok(false),
            }
        }
        if let (Some(labels), Some(column)) =
            (self.access.scope_labels.as_ref(), self.scope_column(&meta))
            && let Some(ScopeLabelKind::Split { read_labels, .. }) = column.scope_label.as_ref()
        {
            match row.values.get(&column.name) {
                Some(Value::Text(label))
                    if read_labels.iter().any(|read| read == label)
                        && labels.contains(&ScopeLabel::new(label.clone())) => {}
                _ => return Ok(false),
            }
        }
        Ok(true)
    }

    fn allowed_acl_ids_for_principal(
        &self,
        principal: &Principal,
        acl_ref: &AclRef,
        snapshot: SnapshotId,
    ) -> Result<Arc<HashSet<uuid::Uuid>>> {
        let cache_key = AclGrantCacheKey {
            principal: principal.clone(),
            ref_table: acl_ref.ref_table.clone(),
            ref_column: acl_ref.ref_column.clone(),
            snapshot,
        };
        if let Some(cached) = self.acl_grant_cache.read().get(&cache_key).cloned() {
            return Ok(cached);
        }
        let (kind, principal_id) = match principal {
            Principal::System => return Ok(Arc::new(HashSet::new())),
            Principal::Agent(id) => ("Agent", id.as_str()),
            Principal::Human(id) => ("Human", id.as_str()),
        };
        let tables = self.relational_store.tables.read();
        let Some(rows) = tables.get(&acl_ref.ref_table) else {
            return Ok(Arc::new(HashSet::new()));
        };
        let mut allowed = HashSet::new();
        for row in rows {
            if row.visible_at(snapshot)
                && self.grant_row_context_scope_allowed(&acl_ref.ref_table, row, snapshot)?
                && row.values.get("principal_kind") == Some(&Value::Text(kind.to_string()))
                && row.values.get("principal_id") == Some(&Value::Text(principal_id.to_string()))
                && let Some(Value::Uuid(acl_id)) = row.values.get(&acl_ref.ref_column)
            {
                allowed.insert(*acl_id);
            }
        }
        let allowed = Arc::new(allowed);
        self.acl_grant_cache
            .write()
            .insert(cache_key, allowed.clone());
        Ok(allowed)
    }

    fn allowed_acl_ids_for_principal_in_tx(
        &self,
        tx: Option<TxId>,
        principal: &Principal,
        acl_ref: &AclRef,
        snapshot: SnapshotId,
    ) -> Result<Arc<HashSet<uuid::Uuid>>> {
        let Some(tx) = tx else {
            return self.allowed_acl_ids_for_principal(principal, acl_ref, snapshot);
        };
        let (kind, principal_id) = match principal {
            Principal::System => return Ok(Arc::new(HashSet::new())),
            Principal::Agent(id) => ("Agent", id.as_str()),
            Principal::Human(id) => ("Human", id.as_str()),
        };
        let rows = self.acl_grant_rows_in_tx(tx, &acl_ref.ref_table, snapshot)?;
        let mut allowed = HashSet::new();
        for row in rows {
            if self.grant_row_context_scope_allowed(&acl_ref.ref_table, &row, snapshot)?
                && row.values.get("principal_kind") == Some(&Value::Text(kind.to_string()))
                && row.values.get("principal_id") == Some(&Value::Text(principal_id.to_string()))
                && let Some(Value::Uuid(acl_id)) = row.values.get(&acl_ref.ref_column)
            {
                allowed.insert(*acl_id);
            }
        }
        Ok(Arc::new(allowed))
    }

    fn acl_grant_rows_in_tx(
        &self,
        tx: TxId,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<VersionedRow>> {
        let mut rows = {
            let tables = self.relational_store.tables.read();
            tables
                .get(table)
                .map(|rows| {
                    rows.iter()
                        .filter(|row| row.visible_at(snapshot))
                        .cloned()
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        };
        let committed_row_ids = rows.iter().map(|row| row.row_id).collect::<HashSet<_>>();
        let deleted_row_ids = self.tx_mgr.with_write_set(tx, |ws| {
            ws.relational_deletes
                .iter()
                .filter(|(delete_table, _, _)| delete_table == table)
                .map(|(_, row_id, _)| *row_id)
                .collect::<HashSet<_>>()
        })?;
        rows.retain(|row| !deleted_row_ids.contains(&row.row_id));

        self.tx_mgr.with_write_set(tx, |ws| {
            let mut seen_inserts = HashSet::new();
            let mut inserts = ws
                .relational_inserts
                .iter()
                .rev()
                .filter(|(insert_table, row)| {
                    insert_table == table
                        && seen_inserts.insert(row.row_id)
                        && (!deleted_row_ids.contains(&row.row_id)
                            || committed_row_ids.contains(&row.row_id))
                })
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            inserts.reverse();
            rows.extend(inserts);
        })?;
        Ok(rows)
    }

    fn raw_row_by_id_in_tx(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<Option<VersionedRow>> {
        if let Some(tx) = tx {
            let staged = self.tx_mgr.with_write_set(tx, |ws| {
                let staged_insert = ws
                    .relational_inserts
                    .iter()
                    .rev()
                    .find(|(insert_table, row)| insert_table == table && row.row_id == row_id)
                    .map(|(_, row)| row.clone());
                if staged_insert.is_some() {
                    return staged_insert;
                }
                if ws
                    .relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
                {
                    return None;
                }
                None
            })?;
            if staged.is_some() {
                return Ok(staged);
            }
        }
        Ok(self.relational_store.row_by_id(table, row_id, snapshot))
    }

    fn raw_row_missing_due_to_staged_delete(&self, tx: TxId, table: &str, row_id: RowId) -> bool {
        self.tx_mgr
            .with_write_set(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .any(|(delete_table, deleted_row_id, _)| {
                        delete_table == table && *deleted_row_id == row_id
                    })
            })
            .unwrap_or(false)
    }

    pub(super) fn assert_row_id_write_allowed(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if let Some(row) = self.raw_row_by_id_in_tx(tx, table, row_id, snapshot)? {
            self.assert_row_write_allowed(table, row.row_id, &row.values, snapshot)?;
        } else if !tx.is_some_and(|tx| self.raw_row_missing_due_to_staged_delete(tx, table, row_id))
        {
            return Err(Error::NotFound(format!("row {row_id} in table {table}")));
        }
        Ok(())
    }

    pub(super) fn assert_existing_row_id_write_allowed(
        &self,
        tx: Option<TxId>,
        table: &str,
        row_id: RowId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        let Some(row) = self.raw_row_by_id_in_tx(tx, table, row_id, snapshot)? else {
            return Err(Error::NotFound(format!("row {row_id} in table {table}")));
        };
        self.assert_row_write_allowed(table, row.row_id, &row.values, snapshot)
    }

    fn readable_row_id_filter(
        &self,
        table: &str,
        snapshot: SnapshotId,
    ) -> Result<Option<RoaringTreemap>> {
        if !self.table_has_read_gate(table)? {
            return Ok(None);
        }
        let rows = self.relational.scan(table, snapshot)?;
        let rows = self.filter_rows_for_read(table, rows, snapshot)?;
        let mut bitmap = RoaringTreemap::new();
        for row in rows {
            bitmap.insert(row.row_id.0);
        }
        Ok(Some(bitmap))
    }

    pub(super) fn effective_read_candidates(
        &self,
        table: &str,
        snapshot: SnapshotId,
        candidates: Option<&RoaringTreemap>,
    ) -> Result<Option<RoaringTreemap>> {
        let Some(readable) = self.readable_row_id_filter(table, snapshot)? else {
            return Ok(candidates.cloned());
        };
        Ok(Some(match candidates {
            Some(existing) => {
                let mut merged = existing.clone();
                merged &= readable;
                merged
            }
            None => readable,
        }))
    }

    fn indexed_rows_for_values(
        &self,
        table: &str,
        columns: &[String],
        values: &[Value],
        snapshot: SnapshotId,
    ) -> Result<Option<Vec<VersionedRow>>> {
        if columns.is_empty() || columns.len() != values.len() {
            return Ok(Some(Vec::new()));
        }

        let row_ids = {
            let indexes = self.relational_store.indexes.read();
            let Some(storage) = indexes.get(table).and_then(|table_indexes| {
                table_indexes.values().find(|storage| {
                    storage.columns.len() >= columns.len()
                        && (!storage.exact_only() || storage.columns.len() == columns.len())
                        && storage
                            .columns
                            .iter()
                            .zip(columns.iter())
                            .all(|((indexed_column, _), wanted)| indexed_column == wanted)
                })
            }) else {
                return Ok(None);
            };

            let prefix = index_key_from_values(&storage.columns[..columns.len()], values);
            let mut row_ids = Vec::new();
            if storage.columns.len() == columns.len() {
                if let Some(entries) = storage.exact_postings(&prefix) {
                    row_ids.extend(
                        entries
                            .iter()
                            .filter(|entry| entry.visible_at(snapshot))
                            .map(|entry| entry.row_id),
                    );
                }
            } else {
                for (key, entries) in storage.tree.range(prefix.clone()..).take_while(|(key, _)| {
                    key.len() >= prefix.len() && key[..prefix.len()] == prefix[..]
                }) {
                    if key.len() < prefix.len() {
                        continue;
                    }
                    row_ids.extend(
                        entries
                            .iter()
                            .filter(|entry| entry.visible_at(snapshot))
                            .map(|entry| entry.row_id),
                    );
                }
            }
            row_ids
        };

        let mut seen = HashSet::new();
        let rows = row_ids
            .into_iter()
            .filter(|row_id| seen.insert(*row_id))
            .filter_map(|row_id| self.relational_store.row_by_id(table, row_id, snapshot))
            .collect();
        Ok(Some(rows))
    }

    fn rows_for_node(
        &self,
        node: NodeId,
        snapshot: SnapshotId,
    ) -> Result<Vec<(String, VersionedRow)>> {
        self.rows_for_node_in_tx(None, node, snapshot)
    }

    fn rows_for_node_in_tx(
        &self,
        tx: Option<TxId>,
        node: NodeId,
        snapshot: SnapshotId,
    ) -> Result<Vec<(String, VersionedRow)>> {
        let id_column = vec!["id".to_string()];
        let id_value = vec![Value::Uuid(node)];
        let table_names = self
            .relational_store
            .table_meta
            .read()
            .iter()
            .filter(|(_, meta)| Self::has_uuid_id_column(meta))
            .map(|(table, meta)| (table.clone(), self.meta_has_read_gate(meta)))
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        for (table, has_read_gate) in table_names {
            match self.indexed_rows_for_values(&table, &id_column, &id_value, snapshot)? {
                Some(mut table_rows) => {
                    if let Some(tx) = tx {
                        self.overlay_tx_node_rows(tx, &table, node, &mut table_rows)?;
                    }
                    rows.extend(table_rows.into_iter().map(|row| (table.clone(), row)))
                }
                None if has_read_gate => {
                    return Err(Error::Other(format!(
                        "graph node metadata table `{table}` requires an index on id"
                    )));
                }
                None => {}
            }
        }
        Ok(rows)
    }

    fn overlay_tx_node_rows(
        &self,
        tx: TxId,
        table: &str,
        node: NodeId,
        rows: &mut Vec<VersionedRow>,
    ) -> Result<()> {
        self.tx_mgr.with_write_set(tx, |ws| {
            let committed_row_ids = rows.iter().map(|row| row.row_id).collect::<HashSet<_>>();
            let deleted_row_ids = ws
                .relational_deletes
                .iter()
                .filter(|(delete_table, _, _)| delete_table == table)
                .map(|(_, row_id, _)| *row_id)
                .collect::<HashSet<_>>();
            rows.retain(|row| !deleted_row_ids.contains(&row.row_id));

            let mut seen_inserts = HashSet::new();
            let mut inserts = ws
                .relational_inserts
                .iter()
                .rev()
                .filter(|(insert_table, row)| {
                    insert_table == table
                        && row.values.get("id") == Some(&Value::Uuid(node))
                        && seen_inserts.insert(row.row_id)
                        && (!deleted_row_ids.contains(&row.row_id)
                            || committed_row_ids.contains(&row.row_id))
                })
                .map(|(_, row)| row.clone())
                .collect::<Vec<_>>();
            inserts.reverse();
            rows.extend(inserts);
        })?;
        Ok(())
    }

    fn has_graph_edge_columns(meta: &TableMeta) -> bool {
        Self::has_exact_column_type(meta, "source_id", &ColumnType::Uuid)
            && Self::has_exact_column_type(meta, "target_id", &ColumnType::Uuid)
            && Self::has_exact_column_type(meta, "edge_type", &ColumnType::Text)
    }

    fn edge_table_names(&self) -> Vec<(String, bool)> {
        self.relational_store
            .table_meta
            .read()
            .iter()
            .filter(|(_, meta)| Self::has_graph_edge_columns(meta))
            .map(|(table, meta)| (table.clone(), self.meta_has_read_gate(meta)))
            .collect()
    }

    fn edge_rows_for_graph_edge_in_tx(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<(String, VersionedRow)>> {
        let edge_tables = self.edge_table_names();
        let edge_table_set = edge_tables
            .iter()
            .map(|(table, _)| table.clone())
            .collect::<HashSet<_>>();
        let columns = vec![
            "source_id".to_string(),
            "target_id".to_string(),
            "edge_type".to_string(),
        ];
        let values = vec![
            Value::Uuid(source),
            Value::Uuid(target),
            Value::Text(edge_type.to_string()),
        ];
        let deleted_by_table = if let Some(tx) = tx {
            self.tx_mgr.with_write_set(tx, |ws| {
                ws.relational_deletes
                    .iter()
                    .filter(|(table, _, _)| edge_table_set.contains(table))
                    .map(|(table, row_id, _)| (table.clone(), *row_id))
                    .collect::<HashSet<_>>()
            })?
        } else {
            HashSet::new()
        };
        let mut rows = Vec::new();
        for (table, _) in &edge_tables {
            match self.indexed_rows_for_values(table, &columns, &values, snapshot)? {
                Some(table_rows) => {
                    rows.extend(table_rows.into_iter().filter_map(|row| {
                        (!deleted_by_table.contains(&(table.clone(), row.row_id)))
                            .then_some((table.clone(), row))
                    }));
                }
                None => {
                    return Err(Error::Other(format!(
                        "graph edge metadata table `{table}` requires an index on source_id, target_id, edge_type"
                    )));
                }
            }
        }

        if let Some(tx) = tx {
            self.tx_mgr.with_write_set(tx, |ws| {
                for (table, row) in &ws.relational_inserts {
                    if edge_table_set.contains(table)
                        && row.values.get("source_id") == Some(&Value::Uuid(source))
                        && row.values.get("target_id") == Some(&Value::Uuid(target))
                        && row.values.get("edge_type") == Some(&Value::Text(edge_type.to_string()))
                    {
                        rows.push((table.clone(), row.clone()));
                    }
                }
            })?;
        }
        Ok(rows)
    }

    fn reject_graph_edge_without_metadata(&self) -> Result<()> {
        if let Some(contexts) = self.access.contexts.as_ref() {
            return Err(Error::ContextScopeViolation {
                requested: ContextId::new(uuid::Uuid::from_u128(u128::MAX)),
                allowed: contexts.clone(),
            });
        }
        if let Some(labels) = self.access.scope_labels.as_ref() {
            return Err(Error::ScopeLabelViolation {
                requested: ScopeLabel::new(""),
                allowed: labels.clone(),
            });
        }
        if let Some(principal) = self.access.principal.as_ref() {
            if matches!(principal, Principal::System) {
                return Err(Error::PrincipalRequired {
                    table: "__graph_edges".to_string(),
                });
            }
            return Err(Error::AclDenied {
                table: "__graph_edges".to_string(),
                row_id: RowId(0),
                principal: principal.clone(),
            });
        }
        Ok(())
    }

    pub(super) fn edge_read_allowed(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        self.edge_read_allowed_in_tx(None, source, target, edge_type, snapshot)
    }

    fn edge_read_allowed_in_tx(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        if self.access_is_admin() {
            return Ok(true);
        }
        let edge_rows =
            self.edge_rows_for_graph_edge_in_tx(tx, source, target, edge_type, snapshot)?;
        if edge_rows.is_empty() {
            return Ok(false);
        }
        let mut visible = false;
        for (table, row) in edge_rows {
            if !self.table_has_read_gate(&table)? {
                visible = true;
                continue;
            }
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            if !self.read_allowed_for_row_in_tx(tx, &table, &meta, &row, snapshot)? {
                return Ok(false);
            }
            visible = true;
        }
        Ok(visible)
    }

    pub(super) fn assert_graph_edge_write_allowed(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if self.access_is_admin() {
            return Ok(());
        }
        let edge_rows =
            self.edge_rows_for_graph_edge_in_tx(tx, source, target, edge_type, snapshot)?;
        if edge_rows.is_empty() {
            return self.reject_graph_edge_without_metadata();
        }
        for (table, row) in edge_rows {
            self.assert_row_write_allowed(&table, row.row_id, &row.values, snapshot)?;
        }
        Ok(())
    }

    pub(super) fn graph_neighbors_with_orientation(
        &self,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<Vec<GatedGraphNeighbor>> {
        let mut results = Vec::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let fwd = self.graph_store.forward_adj.read();
            if let Some(entries) = fwd.get(&node) {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    results.push((
                        entry.target,
                        entry.edge_type.clone(),
                        entry.properties.clone(),
                        entry.source,
                        entry.target,
                    ));
                }
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
            let rev = self.graph_store.reverse_adj.read();
            if let Some(entries) = rev.get(&node) {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    results.push((
                        entry.source,
                        entry.edge_type.clone(),
                        entry.properties.clone(),
                        entry.source,
                        entry.target,
                    ));
                }
            }
        }

        Ok(results)
    }

    fn graph_neighbors_counted_in_tx(
        &self,
        tx: Option<TxId>,
        node: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<(Vec<CountedGraphNeighbor>, u64)> {
        if !self.access_is_admin() && !self.node_read_allowed_in_tx(tx, node, snapshot)? {
            return Ok((Vec::new(), 0));
        }

        let (deleted, staged_inserts) = self.graph_tx_edge_overlay(tx)?;

        let mut candidates = Vec::<CountedGraphNeighbor>::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let fwd = self.graph_store.forward_adj.read();
            if let Some(entries) = fwd.get(&node) {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    if deleted.contains(&(entry.source, entry.edge_type.clone(), entry.target)) {
                        continue;
                    }
                    candidates.push((
                        entry.target,
                        entry.edge_type.clone(),
                        entry.source,
                        entry.target,
                    ));
                }
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
            let rev = self.graph_store.reverse_adj.read();
            if let Some(entries) = rev.get(&node) {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    if deleted.contains(&(entry.source, entry.edge_type.clone(), entry.target)) {
                        continue;
                    }
                    candidates.push((
                        entry.source,
                        entry.edge_type.clone(),
                        entry.source,
                        entry.target,
                    ));
                }
            }
        }

        for entry in staged_inserts {
            if let Some(types) = edge_types
                && !types.contains(&entry.edge_type)
            {
                continue;
            }
            if matches!(direction, Direction::Outgoing | Direction::Both) && entry.source == node {
                candidates.push((
                    entry.target,
                    entry.edge_type.clone(),
                    entry.source,
                    entry.target,
                ));
            }
            if matches!(direction, Direction::Incoming | Direction::Both) && entry.target == node {
                candidates.push((
                    entry.source,
                    entry.edge_type.clone(),
                    entry.source,
                    entry.target,
                ));
            }
        }

        let mut results = Vec::new();
        let mut examined = 0_u64;
        for (neighbor, edge_type, source, target) in candidates {
            if self
                .graph_neighbor_read_allowed(tx, neighbor, source, target, &edge_type, snapshot)?
            {
                examined += 1;
                results.push((neighbor, edge_type, source, target));
            }
        }

        Ok((results, examined))
    }

    fn graph_tx_edge_overlay(
        &self,
        tx: Option<TxId>,
    ) -> Result<(HashSet<GraphEdgeKey>, Vec<AdjEntry>)> {
        let mut deleted = HashSet::<GraphEdgeKey>::new();
        let mut staged_inserts = Vec::<AdjEntry>::new();
        if let Some(tx) = tx {
            self.tx_mgr.with_write_set(tx, |ws| {
                for (source, edge_type, target, _) in &ws.adj_deletes {
                    deleted.insert((*source, edge_type.clone(), *target));
                }
                staged_inserts.extend(ws.adj_inserts.iter().cloned());
            })?;
        }
        Ok((deleted, staged_inserts))
    }

    fn graph_neighbor_read_allowed(
        &self,
        tx: Option<TxId>,
        neighbor: NodeId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        if self.access_is_admin() {
            return Ok(true);
        }
        Ok(self.node_read_allowed_in_tx(tx, neighbor, snapshot)?
            && self.edge_read_allowed_in_tx(tx, source, target, edge_type, snapshot)?)
    }

    fn graph_scan_edge_allowed(
        &self,
        tx: Option<TxId>,
        candidate: GraphScanCandidate,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        if self.access_is_admin() {
            return Ok(true);
        }
        Ok(self.node_read_allowed_in_tx(tx, candidate.start, snapshot)?
            && self.node_read_allowed_in_tx(tx, candidate.neighbor, snapshot)?
            && self.edge_read_allowed_in_tx(
                tx,
                candidate.source,
                candidate.target,
                edge_type,
                snapshot,
            )?)
    }

    pub(crate) fn graph_edges_scan_counted(
        &self,
        tx: Option<TxId>,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<(Vec<CountedGraphScanEdge>, u64)> {
        let (deleted, staged_inserts) = self.graph_tx_edge_overlay(tx)?;
        let mut candidates = Vec::<CountedGraphScanCandidate>::new();

        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let forward = self.graph_store.forward_adj.read();
            for entries in forward.values() {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    if deleted.contains(&(entry.source, entry.edge_type.clone(), entry.target)) {
                        continue;
                    }
                    candidates.push((
                        GraphScanCandidate {
                            start: entry.source,
                            neighbor: entry.target,
                            source: entry.source,
                            target: entry.target,
                        },
                        entry.edge_type.clone(),
                        (entry.source, entry.target),
                    ));
                }
            }
        }

        if matches!(direction, Direction::Incoming | Direction::Both) {
            let reverse = self.graph_store.reverse_adj.read();
            for entries in reverse.values() {
                for entry in entries {
                    if !entry.visible_at(snapshot) {
                        continue;
                    }
                    if let Some(types) = edge_types
                        && !types.contains(&entry.edge_type)
                    {
                        continue;
                    }
                    if deleted.contains(&(entry.source, entry.edge_type.clone(), entry.target)) {
                        continue;
                    }
                    candidates.push((
                        GraphScanCandidate {
                            start: entry.target,
                            neighbor: entry.source,
                            source: entry.source,
                            target: entry.target,
                        },
                        entry.edge_type.clone(),
                        (entry.target, entry.source),
                    ));
                }
            }
        }

        for entry in staged_inserts {
            if let Some(types) = edge_types
                && !types.contains(&entry.edge_type)
            {
                continue;
            }
            if matches!(direction, Direction::Outgoing | Direction::Both) {
                candidates.push((
                    GraphScanCandidate {
                        start: entry.source,
                        neighbor: entry.target,
                        source: entry.source,
                        target: entry.target,
                    },
                    entry.edge_type.clone(),
                    (entry.source, entry.target),
                ));
            }
            if matches!(direction, Direction::Incoming | Direction::Both) {
                candidates.push((
                    GraphScanCandidate {
                        start: entry.target,
                        neighbor: entry.source,
                        source: entry.source,
                        target: entry.target,
                    },
                    entry.edge_type.clone(),
                    (entry.target, entry.source),
                ));
            }
        }

        let mut results = Vec::new();
        let mut seen = HashSet::<CountedGraphScanEdge>::new();
        let mut examined = 0_u64;
        for (candidate, edge_type, edge) in candidates {
            if self.graph_scan_edge_allowed(tx, candidate, &edge_type, snapshot)? {
                examined += 1;
                if seen.insert(edge) {
                    results.push(edge);
                }
            }
        }

        Ok((results, examined))
    }

    pub(crate) fn graph_start_nodes_for_match_counted(
        &self,
        tx: Option<TxId>,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<(Vec<NodeId>, u64)> {
        let (edges, examined) =
            self.graph_edges_scan_counted(tx, edge_types, direction, snapshot)?;
        let starts = edges
            .into_iter()
            .map(|(start, _)| start)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok((starts, examined))
    }

    pub(crate) fn graph_adjacency_probe_counted(
        &self,
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<(TraversalResult, u64)> {
        let (neighbors, examined) =
            self.graph_neighbors_counted_in_tx(tx, start, edge_types, direction, snapshot)?;
        let mut seen = HashSet::new();
        let mut nodes = Vec::new();
        for (neighbor_id, edge_type, _, _) in neighbors {
            if !seen.insert(neighbor_id) {
                continue;
            }
            nodes.push(TraversalNode {
                id: neighbor_id,
                depth: 1,
                path: vec![(start, edge_type)],
            });
        }
        Ok((TraversalResult { nodes }, examined))
    }

    pub(crate) fn graph_bfs_counted(
        &self,
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        depth_range: std::ops::RangeInclusive<u32>,
        snapshot: SnapshotId,
    ) -> Result<(TraversalResult, u64)> {
        let min_depth = *depth_range.start();
        let max_depth = *depth_range.end();
        if !self.access_is_admin() && !self.node_read_allowed_in_tx(tx, start, snapshot)? {
            return Ok((TraversalResult { nodes: Vec::new() }, 0));
        }

        let gated = !self.access_is_admin();
        let max_visited = if gated { 10_000 } else { 100_000 };
        let mut visited = HashSet::new();
        visited.insert(start);
        let mut queue: VecDeque<GatedBfsEntry> = VecDeque::new();
        queue.push_back((start, 0, vec![]));
        let mut result_nodes = Vec::new();
        let mut examined = 0_u64;

        while let Some((current, depth, path)) = queue.pop_front() {
            if !gated && depth > 0 && depth >= min_depth {
                result_nodes.push(TraversalNode {
                    id: current,
                    depth,
                    path: path.clone(),
                });
            }
            if depth >= max_depth {
                continue;
            }
            let (neighbors, neighbor_examined) =
                self.graph_neighbors_counted_in_tx(tx, current, edge_types, direction, snapshot)?;
            examined += neighbor_examined;
            for (neighbor_id, edge_type, _, _) in neighbors {
                let next_depth = depth + 1;
                let mut new_path = path.clone();
                new_path.push((current, edge_type));
                if gated && next_depth >= min_depth {
                    result_nodes.push(TraversalNode {
                        id: neighbor_id,
                        depth: next_depth,
                        path: new_path.clone(),
                    });
                }
                if neighbor_id == current || visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                if visited.len() > max_visited {
                    return Err(Error::BfsVisitedExceeded(max_visited));
                }
                queue.push_back((neighbor_id, next_depth, new_path));
            }
        }

        Ok((
            TraversalResult {
                nodes: result_nodes,
            },
            examined,
        ))
    }

    pub(super) fn node_read_allowed(&self, node: NodeId, snapshot: SnapshotId) -> Result<bool> {
        self.node_read_allowed_in_tx(None, node, snapshot)
    }

    fn node_read_allowed_in_tx(
        &self,
        tx: Option<TxId>,
        node: NodeId,
        snapshot: SnapshotId,
    ) -> Result<bool> {
        let rows = self.rows_for_node_in_tx(tx, node, snapshot)?;
        if rows.is_empty() {
            return Ok(true);
        }
        for (table, row) in rows {
            if !self.table_has_read_gate(&table)? {
                continue;
            }
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            if !self.read_allowed_for_row_in_tx(tx, &table, &meta, &row, snapshot)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn readable_graph_node_column_values(
        &self,
        tx: Option<TxId>,
        node: NodeId,
        column: &str,
        snapshot: SnapshotId,
    ) -> Result<Vec<Value>> {
        if column == "id" {
            return Ok(vec![Value::Uuid(node)]);
        }
        let id_column = vec!["id".to_string()];
        let id_value = vec![Value::Uuid(node)];
        let table_names = self
            .relational_store
            .table_meta
            .read()
            .iter()
            .filter(|(_, meta)| {
                Self::has_uuid_id_column(meta)
                    && meta
                        .columns
                        .iter()
                        .any(|candidate| candidate.name == column)
            })
            .map(|(table, meta)| (table.clone(), self.meta_has_read_gate(meta)))
            .collect::<Vec<_>>();
        let mut values = Vec::new();
        for (table, has_read_gate) in table_names {
            let Some(mut table_rows) =
                self.indexed_rows_for_values(&table, &id_column, &id_value, snapshot)?
            else {
                return Err(Error::Other(format!(
                    "graph node metadata table `{table}` requires an index on id"
                )));
            };
            if let Some(tx) = tx {
                self.overlay_tx_node_rows(tx, &table, node, &mut table_rows)?;
            }
            if has_read_gate {
                let Some(meta) = self.table_meta(&table) else {
                    continue;
                };
                for row in table_rows {
                    if self.read_allowed_for_row_in_tx(tx, &table, &meta, &row, snapshot)?
                        && let Some(value) = row.values.get(column)
                    {
                        values.push(value.clone());
                    }
                }
            } else {
                values.extend(
                    table_rows
                        .into_iter()
                        .filter_map(|row| row.values.get(column).cloned()),
                );
            }
        }
        Ok(values)
    }

    fn has_uuid_id_column(meta: &TableMeta) -> bool {
        Self::has_exact_column_type(meta, "id", &ColumnType::Uuid)
    }

    fn has_exact_column_type(meta: &TableMeta, name: &str, column_type: &ColumnType) -> bool {
        let mut columns = meta.columns.iter().filter(|column| column.name == name);
        matches!(columns.next(), Some(column) if &column.column_type == column_type)
            && columns.next().is_none()
    }

    pub(crate) fn assert_graph_anchor_nodes_readable_in_tx(
        &self,
        tx: Option<TxId>,
        nodes: &[NodeId],
        snapshot: SnapshotId,
    ) -> Result<()> {
        if self.access_is_admin() {
            return Ok(());
        }
        for node in nodes {
            let rows = self.rows_for_node_in_tx(tx, *node, snapshot)?;
            if rows.is_empty() {
                continue;
            }
            let mut visible = false;
            let mut first_denial = None;
            for (table, row) in rows {
                if !self.table_has_read_gate(&table)? {
                    visible = true;
                    continue;
                }
                let Some(meta) = self.table_meta(&table) else {
                    continue;
                };
                match self
                    .read_denial_for_row_cached_in_tx(tx, &table, &meta, &row, snapshot, None)?
                {
                    Some(err) => {
                        if first_denial.is_none() {
                            first_denial = Some(err);
                        }
                    }
                    None => visible = true,
                }
            }
            if !visible && let Some(err) = first_denial {
                return Err(err);
            }
        }
        Ok(())
    }

    pub(super) fn assert_node_write_allowed(
        &self,
        node: NodeId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        for (table, row) in self.rows_for_node(node, snapshot)? {
            self.assert_graph_endpoint_allowed(&table, &row, snapshot)?;
        }
        Ok(())
    }

    fn assert_graph_endpoint_allowed(
        &self,
        table: &str,
        row: &VersionedRow,
        snapshot: SnapshotId,
    ) -> Result<()> {
        if self.access_is_admin() {
            return Ok(());
        }
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        self.write_allowed_for_values(table, &meta, row.row_id, &row.values, snapshot)
    }

    pub(crate) fn query_bfs_gated(
        &self,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
    ) -> Result<TraversalResult> {
        if !self.node_read_allowed(start, snapshot)? {
            return Ok(TraversalResult { nodes: Vec::new() });
        }

        let mut visited = HashSet::new();
        visited.insert(start);
        let mut queue: VecDeque<GatedBfsEntry> = VecDeque::new();
        queue.push_back((start, 0, vec![]));
        let mut result_nodes = Vec::new();

        while let Some((current, depth, path)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            for (neighbor_id, edge_type, _, edge_source, edge_target) in
                self.graph_neighbors_with_orientation(current, edge_types, direction, snapshot)?
            {
                if !self.node_read_allowed(neighbor_id, snapshot)? {
                    continue;
                }
                if !self.edge_read_allowed(edge_source, edge_target, &edge_type, snapshot)? {
                    continue;
                }
                let next_depth = depth + 1;
                let mut new_path = path.clone();
                new_path.push((current, edge_type.clone()));
                if next_depth >= min_depth {
                    result_nodes.push(TraversalNode {
                        id: neighbor_id,
                        depth: next_depth,
                        path: new_path.clone(),
                    });
                }
                if neighbor_id == current || visited.contains(&neighbor_id) {
                    continue;
                }
                visited.insert(neighbor_id);
                if visited.len() > 10_000 {
                    return Err(Error::BfsVisitedExceeded(10_000));
                }
                queue.push_back((neighbor_id, next_depth, new_path));
            }
        }

        Ok(TraversalResult {
            nodes: result_nodes,
        })
    }
}
