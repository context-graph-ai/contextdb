use super::*;
use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailureLimit};

/// How many resolved grant sets the store keeps for reuse. One set is kept
/// per principal, grant reference, and snapshot, so the count tracks how many
/// distinct readers and snapshots are live rather than how much data the store
/// holds; this is the point past which keeping more stops paying for itself.
const RETAINED_GRANT_SET_CEILING: usize = 1_024;

/// A read that could not be given the memory it asked for is refused in the
/// same vocabulary as any other budget refusal, carrying the bytes it could
/// not hold.
fn bounded_memory_refusal(bytes: usize) -> Error {
    Error::ReadFailure(ReadFailure::owner_limit_exceeded(
        OwnerLimitExceededDetail {
            limit: ReadFailureLimit::Memory,
            value: u64::try_from(bytes).unwrap_or(u64::MAX),
            required: None,
            statement: None,
        },
    ))
}

/// The source a suspended read anchored itself in moved before the read came
/// back for it, so the continuation no longer describes where it left off.
fn bounded_continuation_lost(reason: &str) -> Error {
    Error::ReadFailure(ReadFailure::invalid_continuation(reason.to_string()))
}

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

#[derive(Clone, Copy, PartialEq, Eq)]
enum BoundedGraphScanPhase {
    Forward,
    Reverse,
    /// The edges the reader's own transaction has staged, walked after the
    /// committed ones and under each orientation the direction allows -- the
    /// same forward-then-reverse rule the committed phases follow, so the
    /// direction is decided in one place for both.
    StagedForward,
    StagedReverse,
    Done,
}

/// Stable source metadata for an unpinned bounded graph read. Adjacency keys
/// and their captured ends are retained once; edge payloads remain borrowed
/// from the graph store and are inspected one at a time. Within a key the
/// continuation is anchored by the identity of the last inspected entry, not
/// by a raw index: adjacency vectors are physically compacted by maintenance,
/// so a suspended position must re-locate itself by identity on the next pull
/// rather than silently skip entries the pinned snapshot is entitled to see.
pub(crate) struct BoundedGraphEdgeCursor {
    forward: Vec<(NodeId, usize)>,
    reverse: Vec<(NodeId, usize)>,
    phase: BoundedGraphScanPhase,
    /// Which orientations this scan was asked for, kept so the staged phases
    /// answer the same direction question the committed ones did.
    direction: Direction,
    /// How far through the transaction's own staged edges this walk has got.
    /// An index, not a copy: a walk that stops and starts steps over no staged
    /// edge twice and holds none of them between pulls.
    staged_position: usize,
    key_position: usize,
    edge_position: usize,
    last_inspected: Option<BoundedGraphEdgeAnchor>,
    last_emitted: Option<BoundedGraphEdgeAnchor>,
    snapshot: SnapshotId,
    retained_bytes: usize,
}

/// The first staged phase this direction has anything to walk in.
fn bounded_graph_first_staged_phase(direction: Direction) -> BoundedGraphScanPhase {
    if matches!(direction, Direction::Outgoing | Direction::Both) {
        BoundedGraphScanPhase::StagedForward
    } else if matches!(direction, Direction::Incoming) {
        BoundedGraphScanPhase::StagedReverse
    } else {
        BoundedGraphScanPhase::Done
    }
}

impl BoundedGraphEdgeCursor {
    pub(crate) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

/// Identity of one adjacency entry, owned by a suspended continuation. An
/// entry is named by its full self-owned identity (endpoints, type, creating
/// transaction, LSN) so relocation after a physical compaction resumes at
/// exactly the entry the cursor last consumed.
struct BoundedGraphEdgeAnchor {
    source: NodeId,
    target: NodeId,
    edge_type: EdgeType,
    created_tx: TxId,
    lsn: Lsn,
}

impl BoundedGraphEdgeAnchor {
    fn matches(&self, entry: &AdjEntry) -> bool {
        self.source == entry.source
            && self.target == entry.target
            && self.created_tx == entry.created_tx
            && self.lsn == entry.lsn
            && self.edge_type == entry.edge_type
    }
}

/// One edge as an anchor names it: the endpoints, the type, and the commit
/// that created it. A continuation resumes from this identity rather than
/// from a position, so it travels as one thing.
#[derive(Clone, Copy)]
struct BoundedEdgeIdentity<'a> {
    source: NodeId,
    target: NodeId,
    edge_type: &'a str,
    created_tx: TxId,
    lsn: Lsn,
}

/// Build an owned anchor after charging its retained bytes, reconciling the
/// planned charge against the allocation the string actually took — the same
/// discipline as the retained edge-type copy in `bounded_graph_edge_next`.
fn retain_bounded_edge_anchor<E>(
    identity: BoundedEdgeIdentity<'_>,
    mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
    mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(BoundedGraphEdgeAnchor, usize), E>
where
    E: From<Error>,
{
    let BoundedEdgeIdentity {
        source,
        target,
        edge_type,
        created_tx,
        lsn,
    } = identity;
    let planned = edge_type.len();
    before_retain(planned)?;
    let mut owned = String::new();
    if owned.try_reserve_exact(planned).is_err() {
        release_retained(planned)?;
        return Err(E::from(bounded_memory_refusal(planned)));
    }
    owned.push_str(edge_type);
    let actual = owned.capacity();
    if actual > planned {
        if let Err(error) = before_retain(actual - planned) {
            release_retained(planned)?;
            return Err(error);
        }
    } else if planned > actual {
        release_retained(planned - actual)?;
    }
    Ok((
        BoundedGraphEdgeAnchor {
            source,
            target,
            edge_type: owned,
            created_tx,
            lsn,
        },
        actual,
    ))
}

/// Drop one retained anchor, returning its bytes to the caller's ledger and
/// keeping the cursor's retained-byte total exact.
fn release_bounded_edge_anchor<E>(
    slot: &mut Option<BoundedGraphEdgeAnchor>,
    retained_bytes: &mut usize,
    mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    if let Some(anchor) = slot.take() {
        let bytes = anchor.edge_type.capacity();
        *retained_bytes = retained_bytes.checked_sub(bytes).ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph edge anchor accounting underflow".to_string(),
            ))
        })?;
        if bytes > 0 {
            release_retained(bytes)?;
        }
    }
    Ok(())
}

/// Move one anchor slot onto a new edge identity, keeping the buffer the slot
/// already holds. The traversal names one edge after another, so re-allocating
/// the identity's text for every edge would charge and return the same bytes
/// on every step; the slot grows only when an edge's type does not fit what it
/// already holds, and the growth is charged before it is taken.
fn set_bounded_edge_anchor<E>(
    slot: &mut Option<BoundedGraphEdgeAnchor>,
    retained_bytes: &mut usize,
    identity: BoundedEdgeIdentity<'_>,
    before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    release_retained: &mut impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let BoundedEdgeIdentity {
        source,
        target,
        edge_type,
        created_tx,
        lsn,
    } = identity;
    let Some(anchor) = slot.as_mut() else {
        let (anchor, bytes) =
            retain_bounded_edge_anchor(identity, before_retain, &mut *release_retained)?;
        // The bytes are already charged by the retain above, so a total that
        // cannot be represented must hand them back on its way out -- exactly
        // as a refused top-up does. Unreachable short of `usize::MAX` retained
        // bytes; the class stays uniform so no charge ever escapes.
        let Some(total) = retained_bytes.checked_add(bytes) else {
            if bytes > 0 {
                release_retained(bytes)?;
            }
            return Err(E::from(Error::Other(
                "bounded graph edge anchor accounting overflow".to_string(),
            )));
        };
        *slot = Some(anchor);
        *retained_bytes = total;
        return Ok(());
    };
    let held = anchor.edge_type.capacity();
    if edge_type.len() > held {
        let growth = edge_type.len() - held;
        before_retain(growth)?;
        if anchor.edge_type.try_reserve_exact(edge_type.len()).is_err() {
            release_retained(growth)?;
            return Err(E::from(bounded_memory_refusal(growth)));
        }
        let actual = anchor.edge_type.capacity();
        let taken = actual.checked_sub(held).ok_or_else(|| {
            E::from(Error::Other(
                "bounded graph edge anchor capacity moved backwards".to_string(),
            ))
        })?;
        if taken > growth {
            if let Err(error) = before_retain(taken - growth) {
                release_retained(growth)?;
                return Err(error);
            }
        } else if growth > taken {
            release_retained(growth - taken)?;
        }
        // The growth is fully settled and charged by here, so an
        // unrepresentable total releases what this step took before it
        // propagates.
        let Some(total) = retained_bytes.checked_add(taken) else {
            if taken > 0 {
                release_retained(taken)?;
            }
            return Err(E::from(Error::Other(
                "bounded graph edge anchor accounting overflow".to_string(),
            )));
        };
        *retained_bytes = total;
    }
    anchor.edge_type.clear();
    anchor.edge_type.push_str(edge_type);
    anchor.source = source;
    anchor.target = target;
    anchor.created_tx = created_tx;
    anchor.lsn = lsn;
    Ok(())
}

/// Install the inspected-entry anchor, charging any growth before it is taken
/// so a failed charge leaves the continuation unchanged.
fn install_bounded_inspected_anchor<E>(
    cursor: &mut BoundedGraphEdgeCursor,
    identity: BoundedEdgeIdentity<'_>,
    mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
    mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    set_bounded_edge_anchor(
        &mut cursor.last_inspected,
        &mut cursor.retained_bytes,
        identity,
        &mut before_retain,
        &mut release_retained,
    )
}

/// Record that the entry named by the inspected anchor was emitted to the
/// caller. The emitted anchor names an edge visible at the continuation's
/// registered snapshot — never physically removed by maintenance — so
/// relocation always has a surviving resume point strictly after the last
/// edge the caller consumed.
fn note_bounded_edge_emitted<E>(
    cursor: &mut BoundedGraphEdgeCursor,
    mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
    mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
) -> std::result::Result<(), E>
where
    E: From<Error>,
{
    let inspected = cursor.last_inspected.take().ok_or_else(|| {
        E::from(Error::Other(
            "bounded graph edge continuation lost its inspected anchor".to_string(),
        ))
    })?;
    let copied = set_bounded_edge_anchor(
        &mut cursor.last_emitted,
        &mut cursor.retained_bytes,
        BoundedEdgeIdentity {
            source: inspected.source,
            target: inspected.target,
            edge_type: &inspected.edge_type,
            created_tx: inspected.created_tx,
            lsn: inspected.lsn,
        },
        &mut before_retain,
        &mut release_retained,
    );
    cursor.last_inspected = Some(inspected);
    copied
}

fn bounded_graph_cursor_bytes(forward_capacity: usize, reverse_capacity: usize) -> Result<usize> {
    forward_capacity
        .checked_add(reverse_capacity)
        .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<(NodeId, usize)>()))
        .ok_or_else(|| {
            Error::Other(
                "bounded graph edge-source memory size exceeds the native address space"
                    .to_string(),
            )
        })
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

    pub(crate) fn bounded_read_requires_candidate_filter(&self, table: &str) -> Result<bool> {
        if self.access_is_admin() {
            return Ok(false);
        }
        self.table_has_read_gate(table)
    }

    /// Access evaluation for the bounded pull path. The callback runs before
    /// each candidate row that the gate inspects, including each ACL grant
    /// row. Ordinary callers continue through [`Self::read_allowed_for_row`]
    /// and do not acquire request-budget behavior.
    pub(crate) fn bounded_read_allowed_for_row<E>(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        self.bounded_read_allowed_for_row_with_tables(
            tx,
            None,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
        )
    }

    /// Access evaluation for a caller that already holds the relational read
    /// guard. The grant scan reads the same map, and taking that guard a
    /// second time while the first is live deadlocks as soon as a writer is
    /// queued between the two acquisitions, so such a caller lends its borrow
    /// instead.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_read_allowed_for_row_in_tables<E>(
        &self,
        // The reader's own transaction, so a grant or a denial this reader
        // staged decides its own rows here exactly as it does on every other
        // access route. Dropping it would answer from the committed
        // entitlements alone and silently ignore what the caller just wrote.
        tx: Option<TxId>,
        tables: &HashMap<TableName, Vec<VersionedRow>>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        self.bounded_read_allowed_for_row_with_tables(
            tx,
            Some(tables),
            table,
            meta,
            row,
            snapshot,
            before_access_row,
        )
    }

    /// The same answer as `bounded_read_denial_for_row_in_tables` for a caller
    /// that does not already hold the relational read guard.
    pub(crate) fn bounded_read_denial_for_row<E>(
        &self,
        tx: Option<TxId>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<Error>, E>
    where
        E: From<Error>,
    {
        self.bounded_read_denial_for_row_with_tables(
            tx,
            None,
            table,
            meta,
            row,
            snapshot,
            before_access_row,
        )
    }

    /// Access evaluation for a bounded caller that reports WHY a row was
    /// withheld. A caller branches on the refusal — an entitlement decision is
    /// not a missing row — so the decision and the reason for it are one
    /// answer, not two evaluations.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_read_denial_for_row_in_tables<E>(
        &self,
        // As above: the reader's own transaction decides its own rows.
        tx: Option<TxId>,
        tables: &HashMap<TableName, Vec<VersionedRow>>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<Error>, E>
    where
        E: From<Error>,
    {
        self.bounded_read_denial_for_row_with_tables(
            tx,
            Some(tables),
            table,
            meta,
            row,
            snapshot,
            before_access_row,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_read_allowed_for_row_with_tables<E>(
        &self,
        tx: Option<TxId>,
        tables: Option<&HashMap<TableName, Vec<VersionedRow>>>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        Ok(self
            .bounded_read_denial_for_row_with_tables(
                tx,
                tables,
                table,
                meta,
                row,
                snapshot,
                before_access_row,
            )?
            .is_none())
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_read_denial_for_row_with_tables<E>(
        &self,
        // The reader's own transaction. Its grants admit its own rows, and
        // they are composed per request on top of the committed entitlements
        // -- never written into the set other readers share.
        tx: Option<TxId>,
        tables: Option<&HashMap<TableName, Vec<VersionedRow>>>,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        snapshot: SnapshotId,
        mut before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Option<Error>, E>
    where
        E: From<Error>,
    {
        if self.access_is_admin() {
            return Ok(None);
        }
        before_access_row(row.estimated_bytes())?;
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
            if !read_labels.iter().any(|read| read == &requested.0) || !labels.contains(&requested)
            {
                return Ok(Some(Error::ScopeLabelViolation {
                    requested,
                    allowed: labels.clone(),
                }));
            }
        }
        let Some(column) = self.acl_column(meta) else {
            return Ok(None);
        };
        // An access-controlled table answers the same way on every read route:
        // the grant column is a filter the table declared, not one the reader
        // opted into, so a handle that named no principal that can hold grants
        // is refused rather than served the rows unfiltered.
        let Some(principal) = self.access.principal.as_ref() else {
            return Err(E::from(Error::PrincipalRequired {
                table: table.to_string(),
            }));
        };
        if matches!(principal, Principal::System) {
            return Err(E::from(Error::PrincipalRequired {
                table: table.to_string(),
            }));
        }
        let acl_denied = || Error::AclDenied {
            table: table.to_string(),
            row_id: row.row_id,
            principal: principal.clone(),
        };
        let Some(Value::Uuid(acl_id)) = row.values.get(&column.name) else {
            return Ok(Some(acl_denied()));
        };
        let acl_ref = column.acl_ref.as_ref().ok_or_else(|| {
            E::from(Error::Other(format!(
                "ACL column {}.{} is missing its grant reference",
                table, column.name
            )))
        })?;
        // The COMMITTED entitlements, through the one cached build. Nothing
        // below this line ever writes to that cache, which is what keeps a
        // transaction's uncommitted grants out of the set other readers share.
        let committed = match tables {
            Some(tables) => self.bounded_allowed_acl_ids_in_tables(
                tables,
                principal,
                acl_ref,
                snapshot,
                &mut before_access_row,
            )?,
            None => {
                let tables = self.relational_store.tables.read();
                self.bounded_allowed_acl_ids_in_tables(
                    &tables,
                    principal,
                    acl_ref,
                    snapshot,
                    &mut before_access_row,
                )?
            }
        };
        let allowed = self.bounded_acl_ids_with_transaction(
            tx,
            principal,
            acl_ref,
            snapshot,
            committed,
            &mut before_access_row,
        )?;
        Ok((!allowed.contains(acl_id)).then(acl_denied))
    }

    /// The entitlements this principal holds, read once and kept. Deciding a
    /// candidate row against them is one comparison; rebuilding them for every
    /// candidate would charge the read for the whole grant table once per row,
    /// so the same query served over a small grant table would be refused over
    /// a large one holding the same decisions. Every grant row the build reads
    /// is charged to the read that asked for it.
    fn bounded_allowed_acl_ids_in_tables<E>(
        &self,
        tables: &HashMap<TableName, Vec<VersionedRow>>,
        principal: &Principal,
        acl_ref: &AclRef,
        snapshot: SnapshotId,
        before_access_row: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Arc<HashSet<uuid::Uuid>>, E>
    where
        E: From<Error>,
    {
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
        let mut allowed = HashSet::new();
        if let Some(rows) = tables.get(&acl_ref.ref_table) {
            for grant in rows {
                before_access_row(grant.estimated_bytes())?;
                if grant.visible_at(snapshot)
                    && self
                        .grant_row_context_scope_allowed(&acl_ref.ref_table, grant, snapshot)
                        .map_err(E::from)?
                    && grant.values.get("principal_kind") == Some(&Value::Text(kind.to_string()))
                    && grant.values.get("principal_id")
                        == Some(&Value::Text(principal_id.to_string()))
                    && let Some(Value::Uuid(acl_id)) = grant.values.get(&acl_ref.ref_column)
                {
                    allowed.insert(*acl_id);
                }
            }
        }
        let allowed = Arc::new(allowed);
        self.remember_allowed_acl_ids(cache_key, Arc::clone(&allowed));
        Ok(allowed)
    }

    /// The entitlements this reader holds once its OWN transaction is taken
    /// into account.
    ///
    /// Composed on top of the committed set, per request, and never written
    /// back: the cache the committed build fills is shared by every reader of
    /// this store, and a grant this transaction has not committed is not
    /// theirs to see. A transaction that has staged nothing about the grant
    /// table returns that committed set untouched, so the ordinary read
    /// allocates nothing here.
    fn bounded_acl_ids_with_transaction<E>(
        &self,
        tx: Option<TxId>,
        principal: &Principal,
        acl_ref: &AclRef,
        snapshot: SnapshotId,
        committed: Arc<HashSet<uuid::Uuid>>,
        before_access_row: &mut impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<Arc<HashSet<uuid::Uuid>>, E>
    where
        E: From<Error>,
    {
        let Some(tx) = tx else {
            return Ok(committed);
        };
        let (kind, principal_id) = match principal {
            Principal::System => return Ok(committed),
            Principal::Agent(id) => ("Agent", id.as_str()),
            Principal::Human(id) => ("Human", id.as_str()),
        };
        let overlay = self
            .transaction_table_overlay(tx, &acl_ref.ref_table)
            .map_err(E::from)?;
        if overlay.is_empty() {
            return Ok(committed);
        }
        // The grant rows this transaction staged, read one at a time and
        // charged like every other access row, and the committed grants it
        // withdrew, dropped by the identity they were granted under.
        let mut staged_grants = Vec::new();
        let mut withdrawn = HashSet::new();
        for position in &overlay.staged_positions {
            let measured = self
                .tx_mgr
                .with_write_set(tx, |ws| {
                    ws.relational_inserts
                        .get(*position)
                        .map(|(_, row)| row.estimated_bytes())
                })
                .map_err(E::from)?;
            let Some(bytes) = measured else {
                continue;
            };
            before_access_row(bytes)?;
            let staged = self
                .tx_mgr
                .with_write_set(tx, |ws| {
                    ws.relational_inserts
                        .get(*position)
                        .map(|(_, row)| row.clone())
                })
                .map_err(E::from)?;
            if let Some(staged) = staged {
                staged_grants.push(staged);
            }
        }
        for row_id in &overlay.deleted {
            let withdrawn_grant = {
                let tables = self.relational_store.tables.read();
                tables
                    .get(&acl_ref.ref_table)
                    .and_then(|rows| rows.iter().find(|row| row.row_id == *row_id))
                    .and_then(|row| match row.values.get(&acl_ref.ref_column) {
                        Some(Value::Uuid(acl_id)) => Some(*acl_id),
                        _ => None,
                    })
            };
            if let Some(acl_id) = withdrawn_grant {
                withdrawn.insert(acl_id);
            }
        }
        if staged_grants.is_empty() && withdrawn.is_empty() {
            return Ok(committed);
        }
        let mut allowed = committed.as_ref().clone();
        for acl_id in withdrawn {
            allowed.remove(&acl_id);
        }
        for grant in staged_grants {
            if self
                .grant_row_context_scope_allowed(&acl_ref.ref_table, &grant, snapshot)
                .map_err(E::from)?
                && grant.values.get("principal_kind") == Some(&Value::Text(kind.to_string()))
                && grant.values.get("principal_id") == Some(&Value::Text(principal_id.to_string()))
                && let Some(Value::Uuid(acl_id)) = grant.values.get(&acl_ref.ref_column)
            {
                allowed.insert(*acl_id);
            }
        }
        Ok(Arc::new(allowed))
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
        crate::executor::observe_bounded_access_control_row();
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
            crate::executor::observe_bounded_access_control_row();
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
        self.remember_allowed_acl_ids(cache_key, Arc::clone(&allowed));
        Ok(allowed)
    }

    /// Keep a resolved grant set for later decisions, under a ceiling. The
    /// set is keyed by the snapshot it was read at, so a store that keeps
    /// answering reads produces a new key for every snapshot; without a
    /// ceiling the map grows for as long as the process lives. Reaching the
    /// ceiling clears the map rather than keeping an arbitrary subset: every
    /// entry is rebuildable from committed rows, and rebuilding is charged to
    /// the read that needs it.
    fn remember_allowed_acl_ids(
        &self,
        cache_key: AclGrantCacheKey,
        allowed: Arc<HashSet<uuid::Uuid>>,
    ) {
        let mut cache = self.acl_grant_cache.write();
        if cache.len() >= RETAINED_GRANT_SET_CEILING && !cache.contains_key(&cache_key) {
            cache.clear();
        }
        cache.insert(cache_key, allowed);
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

    /// What copying one staged edge's name out of the transaction costs: the
    /// string's own bytes plus the header that carries them.
    pub(crate) fn staged_edge_type_bytes(length: usize) -> usize {
        std::mem::size_of::<EdgeType>().saturating_add(length)
    }

    /// Whether the reader's own open transaction has removed this one edge.
    ///
    /// Asked one edge at a time and allocating nothing. A bounded read cannot
    /// build a set of every edge a transaction removed before it has been
    /// charged for a single one: the memory a read holds has to follow the
    /// walk it is doing, not the size of the transaction it is walking inside.
    pub(crate) fn transaction_hides_edge(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        edge_type: &str,
        target: NodeId,
    ) -> Result<bool> {
        let Some(tx) = tx else {
            return Ok(false);
        };
        let mut hidden = false;
        self.tx_mgr.with_write_set(tx, |ws| {
            hidden = ws
                .adj_deletes
                .iter()
                .any(|(staged_source, staged_type, staged_target, _)| {
                    *staged_source == source && *staged_target == target && staged_type == edge_type
                });
        })?;
        Ok(hidden)
    }

    /// How many edges this transaction has staged, so a walk knows when it has
    /// reached the end of them without holding any of them.
    pub(crate) fn transaction_staged_edge_count(&self, tx: Option<TxId>) -> Result<usize> {
        let Some(tx) = tx else {
            return Ok(0);
        };
        let mut count = 0usize;
        self.tx_mgr.with_write_set(tx, |ws| {
            count = ws.adj_inserts.len();
        })?;
        Ok(count)
    }

    /// One staged edge, read by its index in the transaction's own staged
    /// edges: who it joins, by what name, and how long that name is -- without
    /// cloning the name, so the caller can be charged for it before it exists.
    pub(crate) fn transaction_staged_edge_shape(
        &self,
        tx: Option<TxId>,
        index: usize,
    ) -> Result<Option<(NodeId, NodeId, usize)>> {
        let Some(tx) = tx else {
            return Ok(None);
        };
        let mut shape = None;
        self.tx_mgr.with_write_set(tx, |ws| {
            shape = ws
                .adj_inserts
                .get(index)
                .map(|entry| (entry.source, entry.target, entry.edge_type.len()));
        })?;
        Ok(shape)
    }

    /// The name of the staged edge at this index, taken once the caller has
    /// been charged for the bytes `transaction_staged_edge_shape` measured.
    pub(crate) fn transaction_staged_edge_type(
        &self,
        tx: Option<TxId>,
        index: usize,
    ) -> Result<Option<EdgeType>> {
        let Some(tx) = tx else {
            return Ok(None);
        };
        let mut edge_type = None;
        self.tx_mgr.with_write_set(tx, |ws| {
            edge_type = ws
                .adj_inserts
                .get(index)
                .map(|entry| entry.edge_type.clone());
        })?;
        Ok(edge_type)
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

    /// Decide one traversal candidate for the bounded pull path, charging the
    /// read for every row the decision inspects — the node and edge rows it
    /// reads, and the grant rows the entitlement decision reads. A decision
    /// that reads rows without charging them spends work the operator's
    /// declared ceiling never sees.
    fn bounded_graph_scan_edge_allowed<E>(
        &self,
        tx: Option<TxId>,
        candidate: GraphScanCandidate,
        edge_type: &str,
        snapshot: SnapshotId,
        before_access: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        if self.access_is_admin() {
            return Ok(true);
        }
        Ok(
            self.bounded_node_read_allowed(tx, candidate.start, snapshot, before_access)?
                && self.bounded_node_read_allowed(
                    tx,
                    candidate.neighbor,
                    snapshot,
                    before_access,
                )?
                && self.bounded_edge_read_allowed(
                    tx,
                    candidate.source,
                    candidate.target,
                    edge_type,
                    snapshot,
                    before_access,
                )?,
        )
    }

    /// The neighbour half of the same decision, for a traversal that reaches a
    /// node through an edge rather than scanning adjacency.
    #[allow(clippy::too_many_arguments)]
    fn bounded_graph_neighbor_read_allowed<E>(
        &self,
        tx: Option<TxId>,
        neighbor: NodeId,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
        before_access: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        if self.access_is_admin() {
            return Ok(true);
        }
        Ok(
            self.bounded_node_read_allowed(tx, neighbor, snapshot, before_access)?
                && self.bounded_edge_read_allowed(
                    tx,
                    source,
                    target,
                    edge_type,
                    snapshot,
                    before_access,
                )?,
        )
    }

    fn bounded_node_read_allowed<E>(
        &self,
        tx: Option<TxId>,
        node: NodeId,
        snapshot: SnapshotId,
        before_access: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let rows = self
            .rows_for_node_in_tx(tx, node, snapshot)
            .map_err(E::from)?;
        for (table, row) in rows {
            if !self.table_has_read_gate(&table).map_err(E::from)? {
                continue;
            }
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            if !self.bounded_read_allowed_for_row_with_tables(
                tx,
                None,
                &table,
                &meta,
                &row,
                snapshot,
                |_bytes| before_access(),
            )? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    fn bounded_edge_read_allowed<E>(
        &self,
        tx: Option<TxId>,
        source: NodeId,
        target: NodeId,
        edge_type: &str,
        snapshot: SnapshotId,
        before_access: &mut impl FnMut() -> std::result::Result<(), E>,
    ) -> std::result::Result<bool, E>
    where
        E: From<Error>,
    {
        let edge_rows = self
            .edge_rows_for_graph_edge_in_tx(tx, source, target, edge_type, snapshot)
            .map_err(E::from)?;
        if edge_rows.is_empty() {
            return Ok(false);
        }
        let mut visible = false;
        for (table, row) in edge_rows {
            if !self.table_has_read_gate(&table).map_err(E::from)? {
                visible = true;
                continue;
            }
            let Some(meta) = self.table_meta(&table) else {
                continue;
            };
            if !self.bounded_read_allowed_for_row_with_tables(
                tx,
                None,
                &table,
                &meta,
                &row,
                snapshot,
                |_bytes| before_access(),
            )? {
                return Ok(false);
            }
            visible = true;
        }
        Ok(visible)
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

    /// Capture only adjacency-key continuation metadata for an unpinned
    /// bounded graph read. No edge item is inspected here.
    pub(crate) fn bounded_graph_edge_cursor<E>(
        &self,
        direction: Direction,
        snapshot: SnapshotId,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<BoundedGraphEdgeCursor, E>
    where
        E: From<Error>,
    {
        let forward_adjacency = self.graph_store.forward_adj.read();
        let reverse_adjacency = self.graph_store.reverse_adj.read();
        let forward_len = if matches!(direction, Direction::Outgoing | Direction::Both) {
            forward_adjacency.len()
        } else {
            0
        };
        let reverse_len = if matches!(direction, Direction::Incoming | Direction::Both) {
            reverse_adjacency.len()
        } else {
            0
        };
        let planned_bytes =
            bounded_graph_cursor_bytes(forward_len, reverse_len).map_err(E::from)?;
        before_retain(planned_bytes)?;

        let mut forward = Vec::new();
        if forward.try_reserve_exact(forward_len).is_err() {
            release_retained(planned_bytes)?;
            return Err(E::from(bounded_memory_refusal(planned_bytes)));
        }
        let mut reverse = Vec::new();
        if reverse.try_reserve_exact(reverse_len).is_err() {
            release_retained(planned_bytes)?;
            return Err(E::from(bounded_memory_refusal(planned_bytes)));
        }

        if forward_len != 0 {
            for (node, entries) in forward_adjacency.iter() {
                forward.push((*node, entries.len()));
            }
        }
        if reverse_len != 0 {
            for (node, entries) in reverse_adjacency.iter() {
                reverse.push((*node, entries.len()));
            }
        }
        drop(reverse_adjacency);
        drop(forward_adjacency);
        forward.sort_unstable_by_key(|(node, _)| *node);
        reverse.sort_unstable_by_key(|(node, _)| *node);

        let actual_bytes =
            bounded_graph_cursor_bytes(forward.capacity(), reverse.capacity()).map_err(E::from)?;
        if actual_bytes > planned_bytes {
            if let Err(error) = before_retain(actual_bytes - planned_bytes) {
                release_retained(planned_bytes)?;
                return Err(error);
            }
        } else if planned_bytes > actual_bytes {
            release_retained(planned_bytes - actual_bytes)?;
        }
        // A store with no committed edges still has this transaction's own,
        // so the walk starts at the first phase there is anything to walk in
        // rather than finishing before it begins.
        let phase = if forward_len != 0 {
            BoundedGraphScanPhase::Forward
        } else if reverse_len != 0 {
            BoundedGraphScanPhase::Reverse
        } else {
            bounded_graph_first_staged_phase(direction)
        };
        Ok(BoundedGraphEdgeCursor {
            forward,
            reverse,
            phase,
            direction,
            staged_position: 0,
            key_position: 0,
            edge_position: 0,
            last_inspected: None,
            last_emitted: None,
            snapshot,
            retained_bytes: actual_bytes,
        })
    }

    /// Inspect at most one real adjacency item per charged iteration and
    /// return the next visible oriented edge. A failed callback leaves the
    /// cursor before that edge.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_graph_edge_next<E>(
        &self,
        cursor: &mut BoundedGraphEdgeCursor,
        edge_types: Option<&[EdgeType]>,
        // The transaction the caller is reading inside, carried in rather than
        // asked for. Its own staged edges are part of its answer and the edges
        // it removed are not there to be read.
        tx: Option<TxId>,
        mut before_edge: impl FnMut() -> std::result::Result<(), E>,
        mut before_access: impl FnMut() -> std::result::Result<(), E>,
        mut before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut release_retained: impl FnMut(usize) -> std::result::Result<(), E>,
        // One count per adjacency entry this scan reads, the same figure the
        // walker reports and the same one the uncapped door calls examined.
        mut entry_read: impl FnMut(),
    ) -> std::result::Result<Option<CountedGraphScanEdge>, E>
    where
        E: From<Error>,
    {
        let staged_count = self.transaction_staged_edge_count(tx)?;
        loop {
            if matches!(
                cursor.phase,
                BoundedGraphScanPhase::StagedForward | BoundedGraphScanPhase::StagedReverse
            ) {
                if let Some(edge) = self.bounded_graph_staged_edge_next(
                    cursor,
                    tx,
                    staged_count,
                    edge_types,
                    &mut before_edge,
                    &mut before_access,
                    &mut before_retain,
                    &mut release_retained,
                    &mut entry_read,
                )? {
                    return Ok(Some(edge));
                }
                continue;
            }
            let keys = match cursor.phase {
                BoundedGraphScanPhase::Forward => &cursor.forward,
                BoundedGraphScanPhase::Reverse => &cursor.reverse,
                BoundedGraphScanPhase::StagedForward
                | BoundedGraphScanPhase::StagedReverse
                | BoundedGraphScanPhase::Done => return Ok(None),
            };
            let Some((key, captured_end)) = keys.get(cursor.key_position).copied() else {
                cursor.phase = match cursor.phase {
                    BoundedGraphScanPhase::Forward if !cursor.reverse.is_empty() => {
                        BoundedGraphScanPhase::Reverse
                    }
                    BoundedGraphScanPhase::Forward | BoundedGraphScanPhase::Reverse => {
                        bounded_graph_first_staged_phase(cursor.direction)
                    }
                    BoundedGraphScanPhase::StagedForward
                    | BoundedGraphScanPhase::StagedReverse
                    | BoundedGraphScanPhase::Done => BoundedGraphScanPhase::Done,
                };
                cursor.key_position = 0;
                cursor.edge_position = 0;
                release_bounded_edge_anchor(
                    &mut cursor.last_inspected,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                release_bounded_edge_anchor(
                    &mut cursor.last_emitted,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                continue;
            };
            if cursor.edge_position >= captured_end {
                cursor.key_position = cursor.key_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph edge-source key position overflow".to_string(),
                    ))
                })?;
                cursor.edge_position = 0;
                release_bounded_edge_anchor(
                    &mut cursor.last_inspected,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                release_bounded_edge_anchor(
                    &mut cursor.last_emitted,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                continue;
            }

            let adjacency = match cursor.phase {
                BoundedGraphScanPhase::Forward => &self.graph_store.forward_adj,
                BoundedGraphScanPhase::Reverse => &self.graph_store.reverse_adj,
                BoundedGraphScanPhase::StagedForward
                | BoundedGraphScanPhase::StagedReverse
                | BoundedGraphScanPhase::Done => return Ok(None),
            };
            let entries_guard = adjacency.read();
            let Some(entries) = entries_guard.get(&key) else {
                // Maintenance removed every entry under this key. An edge
                // visible at this continuation's registered snapshot is never
                // physically removed, so an emitted anchor cannot be missing;
                // with nothing emitted here, the key held only entries this
                // snapshot could not see, and the walk moves on.
                if cursor.last_emitted.is_some() {
                    return Err(E::from(bounded_continuation_lost(
                        "every edge under this source was removed while the read was suspended",
                    )));
                }
                drop(entries_guard);
                cursor.key_position = cursor.key_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph edge-source key position overflow".to_string(),
                    ))
                })?;
                cursor.edge_position = 0;
                release_bounded_edge_anchor(
                    &mut cursor.last_inspected,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                continue;
            };

            // Re-anchor by identity when this key's vector was compacted
            // under the suspended continuation. Validating the anchor
            // re-reads only the identity of an entry already charged on an
            // earlier pull; each relocation candidate is charged before its
            // identity is read.
            if let (Some(anchor_position), Some(anchor)) = (
                cursor.edge_position.checked_sub(1),
                cursor.last_inspected.as_ref(),
            ) {
                let anchored = entries
                    .get(anchor_position)
                    .is_some_and(|entry| anchor.matches(entry));
                if !anchored {
                    let mut found = None;
                    if !entries.is_empty() {
                        let mut candidate = anchor_position.min(entries.len() - 1);
                        loop {
                            before_edge()?;
                            let entry = &entries[candidate];
                            if anchor.matches(entry) {
                                found = Some((candidate, true));
                                break;
                            }
                            if cursor
                                .last_emitted
                                .as_ref()
                                .is_some_and(|emitted| emitted.matches(entry))
                            {
                                found = Some((candidate, false));
                                break;
                            }
                            let Some(next) = candidate.checked_sub(1) else {
                                break;
                            };
                            candidate = next;
                        }
                    }
                    match found {
                        Some((position, exact)) => {
                            if exact {
                                // Every removed slot below the anchor
                                // tightens the captured end by the same
                                // amount; the end only ever
                                // over-approximates, so no entry the
                                // snapshot can see is cut off.
                                let adjusted =
                                    captured_end.saturating_sub(anchor_position - position);
                                match cursor.phase {
                                    BoundedGraphScanPhase::Forward => {
                                        cursor.forward[cursor.key_position].1 = adjusted;
                                    }
                                    BoundedGraphScanPhase::Reverse => {
                                        cursor.reverse[cursor.key_position].1 = adjusted;
                                    }
                                    BoundedGraphScanPhase::StagedForward
                                    | BoundedGraphScanPhase::StagedReverse
                                    | BoundedGraphScanPhase::Done => {}
                                }
                            } else {
                                // Resume from the emitted anchor: the entries
                                // between it and the lost inspected anchor
                                // were all invisible or filtered, so
                                // re-inspecting them is deterministic
                                // re-filtering, never a duplicate emission.
                                let emitted = cursor.last_emitted.take().ok_or_else(|| {
                                    E::from(bounded_continuation_lost(
                                        "the edge this read left off at was removed while it was \
                                         suspended",
                                    ))
                                })?;
                                let copied = retain_bounded_edge_anchor(
                                    BoundedEdgeIdentity {
                                        source: emitted.source,
                                        target: emitted.target,
                                        edge_type: &emitted.edge_type,
                                        created_tx: emitted.created_tx,
                                        lsn: emitted.lsn,
                                    },
                                    &mut before_retain,
                                    &mut release_retained,
                                );
                                cursor.last_emitted = Some(emitted);
                                let (copy, bytes) = copied?;
                                release_bounded_edge_anchor(
                                    &mut cursor.last_inspected,
                                    &mut cursor.retained_bytes,
                                    &mut release_retained,
                                )?;
                                cursor.last_inspected = Some(copy);
                                cursor.retained_bytes =
                                    cursor.retained_bytes.checked_add(bytes).ok_or_else(|| {
                                        E::from(Error::Other(
                                            "bounded graph edge anchor accounting overflow"
                                                .to_string(),
                                        ))
                                    })?;
                            }
                            cursor.edge_position = position.checked_add(1).ok_or_else(|| {
                                E::from(Error::Other(
                                    "bounded graph edge-source position overflow".to_string(),
                                ))
                            })?;
                        }
                        None => {
                            if cursor.last_emitted.is_some() {
                                return Err(E::from(bounded_continuation_lost(
                                    "every edge under this source was removed while the read was \
                                     suspended",
                                )));
                            }
                            cursor.edge_position = 0;
                            release_bounded_edge_anchor(
                                &mut cursor.last_inspected,
                                &mut cursor.retained_bytes,
                                &mut release_retained,
                            )?;
                        }
                    }
                    continue;
                }
            }

            if cursor.edge_position >= entries.len() {
                // Everything at or past the continuation was physically
                // removed, and a removed entry is never visible at this
                // registered snapshot: the key is genuinely finished.
                drop(entries_guard);
                cursor.key_position = cursor.key_position.checked_add(1).ok_or_else(|| {
                    E::from(Error::Other(
                        "bounded graph edge-source key position overflow".to_string(),
                    ))
                })?;
                cursor.edge_position = 0;
                release_bounded_edge_anchor(
                    &mut cursor.last_inspected,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                release_bounded_edge_anchor(
                    &mut cursor.last_emitted,
                    &mut cursor.retained_bytes,
                    &mut release_retained,
                )?;
                continue;
            }

            before_edge()?;
            entry_read();
            let entry = &entries[cursor.edge_position];
            let next_position = cursor.edge_position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph edge-source position overflow".to_string(),
                ))
            })?;
            // Anchor the continuation on the entry being inspected, emitted
            // or not: the resume point is strictly after the last inspected
            // identity, so a skipped entry is never re-charged and an
            // emitted one is never re-emitted.
            install_bounded_inspected_anchor(
                cursor,
                BoundedEdgeIdentity {
                    source: entry.source,
                    target: entry.target,
                    edge_type: &entry.edge_type,
                    created_tx: entry.created_tx,
                    lsn: entry.lsn,
                },
                &mut before_retain,
                &mut release_retained,
            )?;
            let visible = entry.visible_at(cursor.snapshot);
            let type_allowed = edge_types.is_none_or(|types| types.contains(&entry.edge_type));
            let oriented = match cursor.phase {
                BoundedGraphScanPhase::Forward if entry.source == key => {
                    Some((entry.source, entry.target))
                }
                BoundedGraphScanPhase::Reverse if entry.target == key => {
                    Some((entry.target, entry.source))
                }
                _ => None,
            };
            // An edge this transaction removed is not there to be read at all,
            // whoever is asking -- asked one edge at a time, so a walk inside a
            // transaction holds what the walk holds, never what the
            // transaction holds.
            let hidden_by_tx = visible
                && type_allowed
                && oriented.is_some()
                && self.transaction_hides_edge(tx, entry.source, &entry.edge_type, entry.target)?;
            if !visible || !type_allowed || oriented.is_none() || hidden_by_tx {
                drop(entries_guard);
                cursor.edge_position = next_position;
                continue;
            }
            let oriented = oriented.ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph edge source lost its orientation".to_string(),
                ))
            })?;
            if self.access_is_admin() {
                drop(entries_guard);
                cursor.edge_position = next_position;
                note_bounded_edge_emitted(cursor, &mut before_retain, &mut release_retained)?;
                return Ok(Some(oriented));
            }

            let candidate = GraphScanCandidate {
                start: oriented.0,
                neighbor: oriented.1,
                source: entry.source,
                target: entry.target,
            };
            drop(entries_guard);

            before_access()?;
            // The inspected anchor installed above already holds this edge's
            // type in memory this read has been charged for, so the decision
            // reads it there instead of taking a second copy of the same text
            // for every edge the traversal steps over.
            let snapshot = cursor.snapshot;
            let anchored_edge_type = cursor.last_inspected.as_ref().ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph edge continuation lost its inspected anchor".to_string(),
                ))
            })?;
            let allowed = self.bounded_graph_scan_edge_allowed(
                tx,
                candidate,
                &anchored_edge_type.edge_type,
                snapshot,
                &mut before_access,
            )?;
            cursor.edge_position = next_position;
            if allowed {
                note_bounded_edge_emitted(cursor, &mut before_retain, &mut release_retained)?;
                return Ok(Some(oriented));
            }
        }
    }

    /// One edge this transaction staged, in the orientation the current phase
    /// asks for.
    ///
    /// Resumed by index rather than gathered, exactly as the traversal walker
    /// resumes them: one charge for stepping over an edge, one reservation for
    /// the single name copied out of the transaction, taken before the copy
    /// exists and handed back the moment the edge is judged unusable. `None`
    /// means this phase is finished and the cursor has already moved on.
    #[allow(clippy::too_many_arguments)]
    fn bounded_graph_staged_edge_next<E>(
        &self,
        cursor: &mut BoundedGraphEdgeCursor,
        tx: Option<TxId>,
        staged_count: usize,
        edge_types: Option<&[EdgeType]>,
        before_edge: &mut impl FnMut() -> std::result::Result<(), E>,
        before_access: &mut impl FnMut() -> std::result::Result<(), E>,
        before_retain: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: &mut impl FnMut(usize) -> std::result::Result<(), E>,
        entry_read: &mut impl FnMut(),
    ) -> std::result::Result<Option<CountedGraphScanEdge>, E>
    where
        E: From<Error>,
    {
        while cursor.staged_position < staged_count {
            before_edge()?;
            let index = cursor.staged_position;
            cursor.staged_position = cursor.staged_position.checked_add(1).ok_or_else(|| {
                E::from(Error::Other(
                    "bounded graph staged edge position overflow".to_string(),
                ))
            })?;
            let Some((source, target, type_len)) = self
                .transaction_staged_edge_shape(tx, index)
                .map_err(E::from)?
            else {
                break;
            };
            // Charged before it is taken, measured where it still lives rather
            // than after a copy of it already exists.
            let name_bytes = Self::staged_edge_type_bytes(type_len);
            before_retain(name_bytes)?;
            let Some(edge_type) = self
                .transaction_staged_edge_type(tx, index)
                .map_err(E::from)?
            else {
                release_retained(name_bytes)?;
                break;
            };
            // An edge this transaction staged and then removed was never there
            // to walk.
            let usable = edge_types.is_none_or(|types| types.contains(&edge_type))
                && !self
                    .transaction_hides_edge(tx, source, &edge_type, target)
                    .map_err(E::from)?;
            if !usable {
                release_retained(name_bytes)?;
                continue;
            }
            // Counted on the same rule a committed entry is: an edge this walk
            // really read, whatever it then decides about it.
            entry_read();
            let oriented = match cursor.phase {
                BoundedGraphScanPhase::StagedForward => (source, target),
                BoundedGraphScanPhase::StagedReverse => (target, source),
                _ => {
                    release_retained(name_bytes)?;
                    return Ok(None);
                }
            };
            if self.access_is_admin() {
                release_retained(name_bytes)?;
                return Ok(Some(oriented));
            }
            before_access()?;
            let candidate = GraphScanCandidate {
                start: oriented.0,
                neighbor: oriented.1,
                source,
                target,
            };
            let allowed = self.bounded_graph_scan_edge_allowed(
                tx,
                candidate,
                &edge_type,
                cursor.snapshot,
                before_access,
            );
            release_retained(name_bytes)?;
            if allowed? {
                return Ok(Some(oriented));
            }
        }
        // This phase is done. A reverse pass over the same staged edges starts
        // from the beginning of them, exactly as the committed reverse pass
        // starts from the beginning of the reverse map.
        cursor.staged_position = 0;
        cursor.phase = match cursor.phase {
            BoundedGraphScanPhase::StagedForward if matches!(cursor.direction, Direction::Both) => {
                BoundedGraphScanPhase::StagedReverse
            }
            _ => BoundedGraphScanPhase::Done,
        };
        Ok(None)
    }

    /// Create a suspendable traversal for the bounded pull path. Visibility
    /// stays on the existing node/edge gates, but the request callback runs
    /// before every access candidate and no legacy visited ceiling applies.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_graph_bfs_cursor<E>(
        &self,
        // The reader's own transaction: the start it is entitled to reach can
        // rest on a grant this transaction has staged and not yet committed.
        tx: Option<TxId>,
        start: NodeId,
        edge_types: Option<&[EdgeType]>,
        direction: Direction,
        min_depth: u32,
        max_depth: u32,
        snapshot: SnapshotId,
        mut before_access: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
    ) -> std::result::Result<Option<contextdb_graph::mem::BoundedBfsCursor>, E>
    where
        E: From<Error>,
    {
        let gated = !self.access_is_admin();
        if gated {
            before_access()?;
            if !self.bounded_node_read_allowed(tx, start, snapshot, &mut before_access)? {
                return Ok(None);
            }
        }
        self.graph
            .bounded_bfs_cursor(
                start,
                edge_types,
                direction,
                min_depth,
                max_depth,
                snapshot,
                before_retain,
                release_retained,
            )
            .map(Some)
    }

    /// Advance one bounded graph continuation without materializing the rest
    /// of the traversal. The graph cursor itself owns the stable edge position
    /// and retained frontier state between fetches.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bounded_graph_bfs_next<E>(
        &self,
        cursor: &mut contextdb_graph::mem::BoundedBfsCursor,
        before_edge: impl FnMut() -> std::result::Result<(), E>,
        before_path_element: impl FnMut() -> std::result::Result<(), E>,
        mut before_access: impl FnMut() -> std::result::Result<(), E>,
        before_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        release_retained: impl FnMut(usize),
        // The staged source's own accounting: one work charge for every staged
        // edge it steps over, and a reservation for the one name it copies out
        // of the transaction, taken before the copy exists. Separate channels
        // because the walker owns the ones above for the whole call.
        mut staged_touch: impl FnMut() -> std::result::Result<(), E>,
        mut staged_retain: impl FnMut(usize) -> std::result::Result<(), E>,
        mut staged_hand_back: impl FnMut(usize),
        // One count per adjacency entry the walk reads, which is what the
        // uncapped door reports as examined.
        entry_read: impl FnMut(),
        // The standing visited ceiling this walk keeps, when the caller
        // declared no budget of its own.
        visited_cap: Option<usize>,
        // The transaction the caller is reading inside, carried in rather than
        // asked for: an explicit handle is not the session's transaction, so a
        // walk that asks the store which one is active is told none and steps
        // over the caller's own staged edges.
        tx: Option<TxId>,
    ) -> std::result::Result<Option<contextdb_graph::mem::BoundedTraversalNode>, E>
    where
        E: From<Error>,
    {
        let gated = !self.access_is_admin();
        let snapshot = cursor.snapshot();
        // Nothing about the transaction is read into memory up front. Whether
        // an edge was removed is asked one edge at a time, and the edges it
        // staged are resumed by index rather than gathered -- so a walk inside
        // a transaction holds what the walk holds, never what the transaction
        // holds.
        let staged_count = self.transaction_staged_edge_count(tx)?;
        self.graph.bounded_bfs_next_including_staged(
            cursor,
            before_edge,
            before_path_element,
            before_retain,
            release_retained,
            |neighbor, source, target, edge_type| {
                // Two separate questions, asked in the order that costs least.
                // An edge this transaction removed is not there to be read at
                // all, whoever is asking; only then does who is asking matter.
                if self.transaction_hides_edge(tx, source, edge_type, target)? {
                    return Ok(false);
                }
                if !gated {
                    return Ok(true);
                }
                before_access()?;
                self.bounded_graph_neighbor_read_allowed(
                    tx,
                    neighbor,
                    source,
                    target,
                    edge_type,
                    snapshot,
                    &mut before_access,
                )
            },
            |node, direction, from| {
                // Walk forward from where the traversal left off, charging for
                // each staged edge stepped over and stopping at the first one
                // that reaches out of this node and that this same transaction
                // has not since removed.
                let mut index = from;
                while index < staged_count {
                    staged_touch()?;
                    let Some((source, target, type_len)) =
                        self.transaction_staged_edge_shape(tx, index)?
                    else {
                        break;
                    };
                    let reaches = match direction {
                        Direction::Incoming => target == node,
                        _ => source == node,
                    };
                    index = index.saturating_add(1);
                    if !reaches {
                        continue;
                    }
                    // Charged before it is taken, measured where it still
                    // lives rather than after a copy of it already exists.
                    let name_bytes = Self::staged_edge_type_bytes(type_len);
                    staged_retain(name_bytes)?;
                    let Some(edge_type) = self.transaction_staged_edge_type(tx, index - 1)? else {
                        break;
                    };
                    // An edge this transaction staged and then removed was
                    // never there to walk.
                    if self.transaction_hides_edge(tx, source, &edge_type, target)? {
                        drop(edge_type);
                        staged_hand_back(name_bytes);
                        continue;
                    }
                    return Ok(Some((
                        contextdb_graph::mem::StagedTraversalEdge {
                            source,
                            target,
                            edge_type,
                        },
                        name_bytes,
                        index,
                    )));
                }
                Ok(None)
            },
            entry_read,
            visited_cap,
        )
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

    /// The same question, asked by a read that carries a ceiling: every grant
    /// row the decision reads is charged as it is read.
    ///
    /// The eager form answers from a cache it fills without charging anyone,
    /// so a reader given a budget could be denied on the strength of work it
    /// never paid for -- and the charge stops growing with the number of
    /// grants the gate actually consults.
    pub(crate) fn bounded_assert_graph_anchor_nodes_readable<E>(
        &self,
        tx: Option<TxId>,
        nodes: &[NodeId],
        snapshot: SnapshotId,
        mut before_access_row: impl FnMut(usize) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E>
    where
        E: From<Error>,
    {
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
                match self.bounded_read_denial_for_row_with_tables(
                    tx,
                    None,
                    &table,
                    &meta,
                    &row,
                    snapshot,
                    &mut before_access_row,
                )? {
                    Some(err) => {
                        if first_denial.is_none() {
                            first_denial = Some(err);
                        }
                    }
                    None => visible = true,
                }
            }
            if !visible && let Some(err) = first_denial {
                return Err(E::from(err));
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
