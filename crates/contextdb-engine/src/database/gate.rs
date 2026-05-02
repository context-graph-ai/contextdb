use super::*;

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

    fn table_has_read_gate(&self, table: &str) -> Result<bool> {
        let Some(meta) = self.table_meta(table) else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        let context_gate = self.access.contexts.is_some() && self.context_column(&meta).is_some();
        let scope_gate = self.access.scope_labels.is_some()
            && self
                .scope_column(&meta)
                .and_then(|column| column.scope_label.as_ref())
                .is_some_and(|scope| matches!(scope, ScopeLabelKind::Split { .. }));
        let acl_gate = !self.access_is_admin() && self.acl_column(&meta).is_some();
        Ok(context_gate || scope_gate || acl_gate)
    }

    pub(super) fn assert_table_read_allowed(&self, table: &str) -> Result<()> {
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
        self.read_allowed_for_row_cached(table, meta, row, snapshot, None)
    }

    fn read_allowed_for_row_cached(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
    ) -> Result<bool> {
        let _ = meta;
        row_payload_visible_for_access(
            &self.relational_store,
            &self.access,
            table,
            &row.values,
            snapshot,
            allowed_acl_ids,
        )
    }

    pub(crate) fn filter_rows_for_read(
        &self,
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
            Some(self.allowed_acl_ids_for_principal(
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
        Ok(self
            .allowed_acl_ids_for_principal(principal, acl_ref, snapshot)?
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

    fn rows_for_node(&self, node: NodeId, snapshot: SnapshotId) -> Vec<(String, VersionedRow)> {
        let tables = self.relational_store.tables.read();
        let mut rows = Vec::new();
        for (table, table_rows) in tables.iter() {
            for row in table_rows {
                if row.visible_at(snapshot) && row.values.get("id") == Some(&Value::Uuid(node)) {
                    rows.push((table.clone(), row.clone()));
                }
            }
        }
        rows
    }

    fn has_graph_edge_columns(meta: &TableMeta) -> bool {
        let has_source = meta.columns.iter().any(|column| column.name == "source_id");
        let has_target = meta.columns.iter().any(|column| column.name == "target_id");
        let has_type = meta.columns.iter().any(|column| column.name == "edge_type");
        has_source && has_target && has_type
    }

    fn edge_rows_for_graph_edge(
        &self,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Vec<(String, VersionedRow)> {
        self.edge_rows_for_graph_edge_in_tx(None, source, target, edge_type, snapshot)
    }

    fn edge_rows_for_graph_edge_in_tx(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
    ) -> Vec<(String, VersionedRow)> {
        let edge_tables: HashSet<String> = self
            .relational_store
            .table_meta
            .read()
            .iter()
            .filter(|(_, meta)| Self::has_graph_edge_columns(meta))
            .map(|(table, _)| table.clone())
            .collect();

        let tables = self.relational_store.tables.read();
        let mut rows = Vec::new();
        for table in &edge_tables {
            let Some(table_rows) = tables.get(table) else {
                continue;
            };
            for row in table_rows {
                if row.visible_at(snapshot)
                    && row.values.get("source_id") == Some(&Value::Uuid(source))
                    && row.values.get("target_id") == Some(&Value::Uuid(target))
                    && row.values.get("edge_type") == Some(&Value::Text(edge_type.to_string()))
                {
                    rows.push((table.clone(), row.clone()));
                }
            }
        }
        drop(tables);

        if let Some(tx) = tx {
            let _ = self.tx_mgr.with_write_set(tx, |ws| {
                for (table, row) in &ws.relational_inserts {
                    if edge_tables.contains(table)
                        && row.values.get("source_id") == Some(&Value::Uuid(source))
                        && row.values.get("target_id") == Some(&Value::Uuid(target))
                        && row.values.get("edge_type") == Some(&Value::Text(edge_type.to_string()))
                    {
                        rows.push((table.clone(), row.clone()));
                    }
                }
            });
        }
        rows
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
        if self.access_is_admin() {
            return Ok(true);
        }
        let edge_rows = self.edge_rows_for_graph_edge(source, target, edge_type, snapshot);
        if edge_rows.is_empty() {
            return Ok(false);
        }
        for (table, row) in edge_rows {
            if !self.table_has_read_gate(&table)? {
                continue;
            }
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            if !self.read_allowed_for_row(&table, &meta, &row, snapshot)? {
                return Ok(false);
            }
        }
        Ok(true)
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
            self.edge_rows_for_graph_edge_in_tx(tx, source, target, edge_type, snapshot);
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

    pub(super) fn node_read_allowed(&self, node: NodeId, snapshot: SnapshotId) -> Result<bool> {
        let rows = self.rows_for_node(node, snapshot);
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
            if !self.read_allowed_for_row(&table, &meta, &row, snapshot)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(super) fn assert_node_write_allowed(
        &self,
        node: NodeId,
        snapshot: SnapshotId,
    ) -> Result<()> {
        for (table, row) in self.rows_for_node(node, snapshot) {
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

    pub(crate) fn graph_start_nodes_for_match(
        &self,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        snapshot: SnapshotId,
    ) -> Result<Vec<NodeId>> {
        let mut candidates: Vec<(NodeId, NodeId, NodeId, EdgeType)> = Vec::new();
        if matches!(direction, Direction::Outgoing | Direction::Both) {
            let forward = self.graph_store.forward_adj.read();
            for (source, entries) in forward.iter() {
                for entry in entries {
                    if entry.visible_at(snapshot)
                        && edge_types.is_none_or(|types| types.contains(&entry.edge_type))
                    {
                        candidates.push((
                            *source,
                            entry.source,
                            entry.target,
                            entry.edge_type.clone(),
                        ));
                    }
                }
            }
        }
        if matches!(direction, Direction::Incoming | Direction::Both) {
            let reverse = self.graph_store.reverse_adj.read();
            for (target, entries) in reverse.iter() {
                for entry in entries {
                    if entry.visible_at(snapshot)
                        && edge_types.is_none_or(|types| types.contains(&entry.edge_type))
                    {
                        candidates.push((
                            *target,
                            entry.source,
                            entry.target,
                            entry.edge_type.clone(),
                        ));
                    }
                }
            }
        }
        let mut starts = BTreeSet::new();
        for (start, source, target, edge_type) in candidates {
            if !self.access_is_admin()
                && (!self.node_read_allowed(start, snapshot)?
                    || !self.edge_read_allowed(source, target, &edge_type, snapshot)?)
            {
                continue;
            }
            starts.insert(start);
        }
        let mut out = Vec::with_capacity(starts.len());
        for start in starts {
            if self.access_is_admin() || self.node_read_allowed(start, snapshot)? {
                out.push(start);
            }
        }
        Ok(out)
    }
}

pub(crate) fn row_payload_visible_for_access(
    relational_store: &RelationalStore,
    access: &AccessConstraints,
    table: &str,
    values: &HashMap<ColName, Value>,
    snapshot: SnapshotId,
    allowed_acl_ids: Option<&HashSet<uuid::Uuid>>,
) -> Result<bool> {
    if access.contexts.is_none() && access.scope_labels.is_none() && access.principal.is_none() {
        return Ok(true);
    }

    let metas = relational_store.table_meta.read();
    let Some(meta) = metas.get(table) else {
        return Ok(false);
    };

    if let (Some(contexts), Some(column)) = (
        access.contexts.as_ref(),
        meta.columns.iter().find(|column| column.context_id),
    ) {
        match values.get(&column.name) {
            Some(Value::Uuid(context)) if contexts.contains(&ContextId::new(*context)) => {}
            _ => return Ok(false),
        }
    }

    if let (Some(labels), Some(column)) = (
        access.scope_labels.as_ref(),
        meta.columns
            .iter()
            .find(|column| column.scope_label.is_some()),
    ) && let Some(ScopeLabelKind::Split { read_labels, .. }) = column.scope_label.as_ref()
    {
        match values.get(&column.name) {
            Some(Value::Text(label))
                if read_labels.iter().any(|read| read == label)
                    && labels.contains(&ScopeLabel::new(label.clone())) => {}
            _ => return Ok(false),
        }
    }

    if let Some(column) = meta.columns.iter().find(|column| column.acl_ref.is_some()) {
        let Some(principal) = access.principal.as_ref() else {
            return Err(Error::PrincipalRequired {
                table: table.to_string(),
            });
        };
        if matches!(principal, Principal::System) {
            return Err(Error::PrincipalRequired {
                table: table.to_string(),
            });
        }
        let Some(Value::Uuid(acl_id)) = values.get(&column.name) else {
            return Ok(false);
        };
        let acl_ref = column.acl_ref.as_ref().expect("acl column").clone();
        drop(metas);
        let granted = match allowed_acl_ids {
            Some(allowed) => allowed.contains(acl_id),
            None => principal_has_acl_grant_for_payload(
                relational_store,
                access,
                principal,
                &acl_ref,
                *acl_id,
                snapshot,
            ),
        };
        if !granted {
            return Ok(false);
        }
    }

    Ok(true)
}

fn principal_has_acl_grant_for_payload(
    relational_store: &RelationalStore,
    access: &AccessConstraints,
    principal: &Principal,
    acl_ref: &AclRef,
    acl_id: uuid::Uuid,
    snapshot: SnapshotId,
) -> bool {
    let (kind, principal_id) = match principal {
        Principal::System => return false,
        Principal::Agent(id) => ("Agent", id.as_str()),
        Principal::Human(id) => ("Human", id.as_str()),
    };
    let metas = relational_store.table_meta.read();
    let grant_meta = metas.get(&acl_ref.ref_table).cloned();
    drop(metas);
    let tables = relational_store.tables.read();
    let Some(rows) = tables.get(&acl_ref.ref_table) else {
        return false;
    };
    rows.iter().any(|row| {
        row.visible_at(snapshot)
            && grant_context_scope_allowed_for_payload(access, grant_meta.as_ref(), &row.values)
            && row.values.get("principal_kind") == Some(&Value::Text(kind.to_string()))
            && row.values.get("principal_id") == Some(&Value::Text(principal_id.to_string()))
            && row.values.get(&acl_ref.ref_column) == Some(&Value::Uuid(acl_id))
    })
}

fn grant_context_scope_allowed_for_payload(
    access: &AccessConstraints,
    meta: Option<&TableMeta>,
    values: &HashMap<ColName, Value>,
) -> bool {
    let Some(meta) = meta else {
        return true;
    };
    if let (Some(contexts), Some(column)) = (
        access.contexts.as_ref(),
        meta.columns.iter().find(|column| column.context_id),
    ) {
        match values.get(&column.name) {
            Some(Value::Uuid(context)) if contexts.contains(&ContextId::new(*context)) => {}
            _ => return false,
        }
    }
    if let (Some(labels), Some(column)) = (
        access.scope_labels.as_ref(),
        meta.columns
            .iter()
            .find(|column| column.scope_label.is_some()),
    ) && let Some(ScopeLabelKind::Split { read_labels, .. }) = column.scope_label.as_ref()
    {
        match values.get(&column.name) {
            Some(Value::Text(label))
                if read_labels.iter().any(|read| read == label)
                    && labels.contains(&ScopeLabel::new(label.clone())) => {}
            _ => return false,
        }
    }
    true
}
