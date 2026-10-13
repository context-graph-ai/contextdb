//! Transactional physical graph evidence and immutable history occurrences.
use super::graph_history_record::*;
use super::*;

const MATCH_OLD: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_history_match_old_scratch");
const MATCH_TARGET: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_history_match_target_scratch");
const MATCH_FINAL: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_history_match_final_scratch");
const MATCH_CONSUMED: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_history_match_consumed_scratch");

fn corrupt(reason: impl Into<String>) -> Error {
    Error::StoreCorrupted {
        path: "graph_history".into(),
        reason: reason.into(),
    }
}
fn lifecycle(bytes: &[u8]) -> Result<(TxId, u64, u8)> {
    if bytes.len() != 18 || bytes[0] != 1 || bytes[17] > 1 {
        return Err(corrupt("malformed graph lifecycle receipt"));
    }
    let d = TxId(u64::from_be_bytes(bytes[1..9].try_into().unwrap()));
    let count = u64::from_be_bytes(bytes[9..17].try_into().unwrap());
    if count == 0 {
        return Err(corrupt("zero graph lifecycle reference count"));
    }
    Ok((d, count, bytes[17]))
}
fn receipt(d: TxId, count: u64, state: u8) -> [u8; 18] {
    let mut value = [0; 18];
    value[0] = 1;
    value[1..9].copy_from_slice(&d.0.to_be_bytes());
    value[9..17].copy_from_slice(&count.to_be_bytes());
    value[17] = state;
    value
}
fn location(position: &[u8; 16]) -> [u8; 17] {
    let mut value = [0; 17];
    value[0] = 1;
    value[1..].copy_from_slice(position);
    value
}
fn digest_value(digest: [u8; 32]) -> [u8; 33] {
    let mut value = [0; 33];
    value[0] = 1;
    value[1..].copy_from_slice(&digest);
    value
}
fn reverse_key(p: &[u8]) -> Result<Vec<u8>> {
    parse_physical_predecessor(p)?;
    crate::read_image_memory::reserve(p.len())?;
    let mut reverse = p.to_vec();
    reverse[..16].copy_from_slice(&p[16..32]);
    reverse[16..32].copy_from_slice(&p[..16]);
    Ok(reverse)
}
fn match_key(public: &[u8], position: &[u8; 16]) -> Result<Vec<u8>> {
    let n = u32::try_from(public.len()).map_err(|_| corrupt("public matching key overflow"))?;
    crate::read_image_memory::reserve(
        public
            .len()
            .checked_add(20)
            .ok_or_else(|| corrupt("public matching key overflow"))?,
    )?;
    let mut key = Vec::with_capacity(public.len() + 20);
    key.extend_from_slice(&n.to_be_bytes());
    key.extend_from_slice(public);
    key.extend_from_slice(position);
    Ok(key)
}

impl RedbPersistence {
    pub(super) fn initialize_graph_proof_tables_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        for table in [
            GRAPH_FWD_TABLE,
            GRAPH_REV_TABLE,
            GRAPH_CONTRIBUTIONS,
            GRAPH_OCCURRENCE_LOCATIONS,
            GRAPH_PREDECESSOR_LIFECYCLE,
        ] {
            write.open_table(table).map_err(Self::storage_error)?;
        }
        Ok(())
    }
    pub(super) fn require_graph_proof_tables_in_write(
        write: &redb::WriteTransaction,
    ) -> Result<()> {
        let mut mask = 0u8;
        for table in write.list_tables().map_err(Self::storage_error)? {
            let name = table.name();
            for (i, required) in [
                GRAPH_FWD_TABLE,
                GRAPH_REV_TABLE,
                GRAPH_CONTRIBUTIONS,
                GRAPH_OCCURRENCE_LOCATIONS,
                GRAPH_PREDECESSOR_LIFECYCLE,
            ]
            .into_iter()
            .enumerate()
            {
                if name == required.name() {
                    mask |= 1 << i;
                }
            }
        }
        if mask != 31 {
            return Err(corrupt("required graph proof table is missing"));
        }
        Ok(())
    }
    pub(super) fn graph_writer_window<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let memory = match self.proof_accountant() {
            Some(accountant) => crate::read_image_memory::ImageMemory::charging_for(
                accountant,
                "writer_source",
                "graph_history_window",
                "Raise MEMORY_LIMIT to admit graph history publication.",
            ),
            None => crate::read_image_memory::ImageMemory::new(None),
        };
        memory.scope(f)
    }
    fn require_graph_commit_in_write(
        write: &redb::WriteTransaction,
        lsn: Lsn,
        tx: TxId,
        bootstrap: bool,
        _role: &str,
    ) -> Result<()> {
        if bootstrap && lsn.0 == 0 {
            return Ok(());
        }
        let table = write
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "commit_index",
            "get",
            &lsn.0.to_be_bytes(),
            Some(contextdb_core::Lsn(lsn.0)),
            Some(_role),
        );
        let bound = table
            .get(lsn.0)
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("graph contribution required commit is absent"))?;
        if tx.0 > bound.value() {
            return Err(corrupt(
                "graph contribution exceeds its committed source position",
            ));
        }
        Ok(())
    }
    /// Called with the two projection handles owned by the caller. Only C/R
    /// are opened here; there is no second acquisition of either projection.
    pub(super) fn install_graph_creation_in_write(
        write: &redb::WriteTransaction,
        edge: &AdjEntry,
        encoded: &[u8],
    ) -> Result<()> {
        Self::require_graph_proof_tables_in_write(write)?;
        let p = Self::graph_fwd_key(edge);
        let rk = lifecycle_key(&p)?;
        {
            let mut receipts = write
                .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
                .map_err(Self::storage_error)?;
            let old = receipts
                .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                .map_err(Self::storage_error)?
                .map(|v| lifecycle(v.value()))
                .transpose()?;
            if let Some((d, count, _)) = old {
                if edge.deleted_tx != Some(d) {
                    return Err(corrupt(
                        "referenced predecessor cannot change its tombstone",
                    ));
                }
                receipts
                    .insert(
                        observed_graph_key!("graph_lifecycle", "insert", rk.as_slice(), 6),
                        receipt(d, count, 0).as_slice(),
                    )
                    .map_err(Self::storage_error)?;
            }
        }
        let ck = creation_contribution_key(&p)?;
        write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?
            .insert(
                observed_graph_key!("graph_contribution", "insert", ck.as_slice(), 4),
                digest_value(contribution_digest(true, &p, encoded)?).as_slice(),
            )
            .map_err(Self::storage_error)?;
        Ok(())
    }
    pub(super) fn reclaim_graph_creation_in_write(
        write: &redb::WriteTransaction,
        p: &[u8],
        encoded: &[u8],
    ) -> Result<()> {
        Self::require_graph_proof_tables_in_write(write)?;
        let (_, _, _, _, lsn) = parse_physical_predecessor(p)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph("graph_payload", "payload_decode", &[], None, None);

        let edge: AdjEntry = Self::decode(encoded)?;
        if Self::graph_fwd_key(&edge).as_slice() != p {
            return Err(corrupt("reclaimed predecessor key and payload disagree"));
        }
        let ck = creation_contribution_key(p)?;
        let mut contributions = write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?;
        let removed = contributions
            .remove(observed_graph_key!(
                "graph_contribution",
                "remove",
                ck.as_slice(),
                4
            ))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("reclaimed graph creation contribution is missing"))?;
        if removed.value() != digest_value(contribution_digest(true, p, encoded)?).as_slice() {
            return Err(corrupt("reclaimed graph creation digest mismatch"));
        }
        let rk = lifecycle_key(p)?;
        {
            let mut receipts = write
                .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
                .map_err(Self::storage_error)?;
            let old = receipts
                .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                .map_err(Self::storage_error)?
                .map(|v| lifecycle(v.value()))
                .transpose()?;
            if let Some((d, count, state)) = old {
                if state != 0 || edge.deleted_tx != Some(d) {
                    return Err(corrupt("reclaimed graph predecessor is already retired"));
                }
                receipts
                    .insert(
                        observed_graph_key!("graph_lifecycle", "insert", rk.as_slice(), 6),
                        receipt(d, count, 1).as_slice(),
                    )
                    .map_err(Self::storage_error)?;
            }
        }
        let mut bounds = Self::read_graph_recovery_bounds_in_write(write)?;
        bounds.graph_retirement_frontier = bounds.graph_retirement_frontier.max(lsn.0);
        Self::write_graph_recovery_bounds_in_write(write, bounds)
    }
    /// Whole projection replacement walks its explicitly replaced physical
    /// inputs once; it never visits any predecessor's referring history.
    pub(super) fn retire_graph_replacement_in_write(
        &self,
        write: &redb::WriteTransaction,
        edges: &[AdjEntry],
        selected_types: Option<&HashSet<String>>,
    ) -> Result<()> {
        const FINAL: TableDefinition<&[u8], &[u8]> =
            TableDefinition::new("graph_creation_replacement_scratch");
        Self::require_graph_proof_tables_in_write(write)?;
        if write
            .list_tables()
            .map_err(Self::storage_error)?
            .any(|t| t.name() == FINAL.name())
        {
            return Err(corrupt("published graph creation scratch"));
        }
        {
            let mut final_keys = write.open_table(FINAL).map_err(Self::storage_error)?;
            for edge in edges {
                self.graph_writer_window(|| {
                    crate::read_image_memory::reserve(edge.edge_type.len().saturating_add(49))?;
                    let key = Self::graph_fwd_key(edge);
                    final_keys
                        .insert(key.as_slice(), &[][..])
                        .map_err(Self::storage_error)?;
                    Ok(())
                })?;
            }
        }
        let fwd = write
            .open_table(GRAPH_FWD_TABLE)
            .map_err(Self::storage_error)?;
        {
            let final_keys = write.open_table(FINAL).map_err(Self::storage_error)?;
            for item in fwd.iter().map_err(Self::storage_error)?.inspect(|_entry| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok((key, _)) = _entry {
                    crate::read_work_probe::graph_storage_key(
                        "graph_fwd",
                        "iteration_step",
                        key.value(),
                    );
                }
            }) {
                let (p, body) = item.map_err(Self::storage_error)?;
                self.graph_writer_window(|| {
                    let selected = if let Some(types) = selected_types {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_graph(
                            "graph_payload",
                            "payload_decode",
                            p.value(),
                            None,
                            Some("replacement_type_selection"),
                        );
                        types.contains(&Self::decode::<AdjEntry>(body.value())?.edge_type)
                    } else {
                        true
                    };
                    if selected
                        && final_keys
                            .get(p.value())
                            .map_err(Self::storage_error)?
                            .is_none()
                    {
                        #[cfg(any(test, feature = "test-seams"))]
                        crate::read_work_probe::note_graph(
                            "graph_payload",
                            "payload_decode",
                            &[],
                            None,
                            None,
                        );

                        let edge: AdjEntry = Self::decode(body.value())?;
                        let reverse = write
                            .open_table(GRAPH_REV_TABLE)
                            .map_err(Self::storage_error)?;
                        let rev_key = Self::graph_rev_key(&edge);
                        let counterpart = reverse
                            .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
                            .map_err(Self::storage_error)?
                            .ok_or_else(|| corrupt("replacement graph counterpart missing"))?;
                        if counterpart.value() != body.value() {
                            return Err(corrupt("replacement graph projections disagree"));
                        }
                        Self::reclaim_graph_creation_in_write(write, p.value(), body.value())?;
                    }
                    Ok(())
                })?;
            }
        }
        drop(fwd);
        write.delete_table(FINAL).map_err(Self::storage_error)?;
        Ok(())
    }
    fn validate_present_predecessor_in_write(
        write: &redb::WriteTransaction,
        p: &[u8],
        d: TxId,
    ) -> Result<()> {
        let (source, target, kind, tx, lsn) = parse_physical_predecessor(p)?;
        if tx >= d {
            return Err(corrupt("predecessor tombstone order is invalid"));
        }
        let fwd = write
            .open_table(GRAPH_FWD_TABLE)
            .map_err(Self::storage_error)?;
        let body = fwd
            .get(observed_graph_key!("graph_fwd", "get", p))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("present predecessor forward projection is missing"))?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph("graph_payload", "payload_decode", &[], None, None);

        let edge: AdjEntry = Self::decode(body.value())?;
        if edge.source != source
            || edge.target != target
            || edge.edge_type != kind
            || edge.created_tx != tx
            || edge.lsn != lsn
            || edge.deleted_tx != Some(d)
        {
            return Err(corrupt("predecessor key and tombstone disagree"));
        }
        let rev_key = reverse_key(p)?;
        let rev = write
            .open_table(GRAPH_REV_TABLE)
            .map_err(Self::storage_error)?;
        let counterpart = rev
            .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("predecessor reverse projection is missing"))?;
        if counterpart.value() != body.value() {
            return Err(corrupt("graph projections disagree"));
        }
        let ck = creation_contribution_key(p)?;
        let c = write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?;
        let proof = c
            .get(observed_graph_key!(
                "graph_contribution",
                "get",
                ck.as_slice()
            ))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("predecessor creation contribution is missing"))?;
        if proof.value() != digest_value(contribution_digest(true, p, body.value())?).as_slice() {
            return Err(corrupt("predecessor creation digest disagrees"));
        }
        Self::require_graph_commit_in_write(write, lsn, tx, true, "predecessor")
    }
    fn validate_retired_predecessor_in_write(
        write: &redb::WriteTransaction,
        p: &[u8],
        d: TxId,
    ) -> Result<()> {
        let (_, _, _, tx, lsn) = parse_physical_predecessor(p)?;
        let bounds = Self::read_graph_recovery_bounds_in_write(write)?;
        if tx >= d || lsn.0 > bounds.graph_retirement_frontier {
            return Err(corrupt("retired predecessor order or frontier is invalid"));
        }
        let rev_key = reverse_key(p)?;
        if write
            .open_table(GRAPH_FWD_TABLE)
            .map_err(Self::storage_error)?
            .get(observed_graph_key!("graph_fwd", "get", p))
            .map_err(Self::storage_error)?
            .is_some()
            || write
                .open_table(GRAPH_REV_TABLE)
                .map_err(Self::storage_error)?
                .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
                .map_err(Self::storage_error)?
                .is_some()
            || write
                .open_table(GRAPH_CONTRIBUTIONS)
                .map_err(Self::storage_error)?
                .get(observed_graph_key!(
                    "graph_contribution",
                    "get",
                    creation_contribution_key(p)?.as_slice()
                ))
                .map_err(Self::storage_error)?
                .is_some()
        {
            return Err(corrupt(
                "retired predecessor still has a physical contribution",
            ));
        }
        Ok(())
    }
    fn select_graph_predecessor_in_write(
        &self,
        write: &redb::WriteTransaction,
        source: NodeId,
        target: NodeId,
        kind: &str,
        deletion_lsn: Lsn,
    ) -> Result<Option<(Vec<u8>, TxId)>> {
        crate::read_image_memory::reserve(kind.len().saturating_mul(4).saturating_add(256))?;
        let mut prefix = Vec::with_capacity(kind.len() + 33);
        prefix.extend_from_slice(source.as_bytes());
        prefix.extend_from_slice(target.as_bytes());
        prefix.extend_from_slice(kind.as_bytes());
        prefix.push(0);
        let mut winner: Option<(Vec<u8>, TxId, (Lsn, TxId))> = None;
        {
            let fwd = write
                .open_table(GRAPH_FWD_TABLE)
                .map_err(Self::storage_error)?;
            for item in fwd
                .range(prefix.as_slice()..)
                .map_err(Self::storage_error)?
                .inspect(|_entry| {
                    #[cfg(any(test, feature = "test-seams"))]
                    if let Ok((key, _)) = _entry {
                        crate::read_work_probe::graph_storage_key(
                            "graph_fwd",
                            "iteration_step",
                            key.value(),
                        );
                    }
                })
            {
                let (p, body) = item.map_err(Self::storage_error)?;
                if !p.value().starts_with(&prefix) {
                    break;
                }
                let candidate = self.graph_writer_window(|| {
                    let (s, t, k, tx, lsn) = parse_physical_predecessor(p.value())?;
                    if s != source || t != target || k != kind {
                        return Ok(None);
                    }
                    #[cfg(any(test, feature = "test-seams"))]
                    crate::read_work_probe::note_graph(
                        "graph_payload",
                        "payload_decode",
                        &[],
                        None,
                        None,
                    );

                    let edge: AdjEntry = Self::decode(body.value())?;
                    if Self::graph_fwd_key(&edge).as_slice() != p.value() {
                        return Err(corrupt("physical graph candidate is miskeyed"));
                    }
                    let Some(d) = edge.deleted_tx else {
                        return Ok(None);
                    };
                    if lsn >= deletion_lsn {
                        return Ok(None);
                    }
                    // Counterpart and contribution reads use other tables; forward
                    // equality has already been checked against this borrowed body.
                    if tx >= d {
                        return Err(corrupt("physical candidate tombstone order is invalid"));
                    }
                    let rev_key = reverse_key(p.value())?;
                    let rev = write
                        .open_table(GRAPH_REV_TABLE)
                        .map_err(Self::storage_error)?;
                    let other = rev
                        .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| corrupt("physical candidate reverse projection absent"))?;
                    if other.value() != body.value() {
                        return Err(corrupt("physical candidate projections disagree"));
                    }
                    let ck = creation_contribution_key(p.value())?;
                    let c = write
                        .open_table(GRAPH_CONTRIBUTIONS)
                        .map_err(Self::storage_error)?;
                    let proof = c
                        .get(observed_graph_key!(
                            "graph_contribution",
                            "get",
                            ck.as_slice()
                        ))
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| corrupt("physical candidate contribution absent"))?;
                    if proof.value()
                        != digest_value(contribution_digest(true, p.value(), body.value())?)
                            .as_slice()
                    {
                        return Err(corrupt("physical candidate digest mismatch"));
                    }
                    Self::require_graph_commit_in_write(write, lsn, tx, true, "predecessor")?;
                    Self::require_graph_commit_in_write(
                        write,
                        deletion_lsn,
                        d,
                        false,
                        "requested_own",
                    )?;
                    Ok(Some((d, lsn, tx)))
                })?;
                if let Some((d, lsn, tx)) = candidate
                    && winner
                        .as_ref()
                        .is_none_or(|(_, _, order)| (lsn, tx) > *order)
                {
                    if let Some((old, old_d, old_order)) = &mut winner {
                        if old.len() != p.value().len() {
                            return Err(corrupt("identity predecessor width changed"));
                        }
                        old.copy_from_slice(p.value());
                        *old_d = d;
                        *old_order = (lsn, tx);
                    } else {
                        crate::read_image_memory::reserve(p.value().len())?;
                        winner = Some((p.value().to_vec(), d, (lsn, tx)));
                    }
                }
            }
        }
        let length = u32::try_from(
            kind.len()
                .checked_add(49)
                .ok_or_else(|| corrupt("identity receipt length overflow"))?,
        )
        .map_err(|_| corrupt("identity receipt length overflow"))?;
        let mut rprefix = Vec::with_capacity(prefix.len() + 5);
        rprefix.push(1);
        rprefix.extend_from_slice(&length.to_be_bytes());
        rprefix.extend_from_slice(&prefix);
        // R is not reopened by validation; each record is resolved independently.
        let r = write
            .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
            .map_err(Self::storage_error)?;
        for item in r
            .range(rprefix.as_slice()..)
            .map_err(Self::storage_error)?
            .inspect(|_entry| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok((key, _)) = _entry {
                    crate::read_work_probe::graph_storage_key(
                        "graph_lifecycle",
                        "iteration_step",
                        key.value(),
                    );
                }
            })
        {
            let (key, value) = item.map_err(Self::storage_error)?;
            if !key.value().starts_with(&rprefix) {
                break;
            }
            if key.value().len() != length as usize + 5 {
                return Err(corrupt("malformed identity lifecycle key"));
            }
            let p = &key.value()[5..];
            let candidate = self.graph_writer_window(|| {
                let (s, t, k, tx, lsn) = parse_physical_predecessor(p)?;
                let (d, _, state) = lifecycle(value.value())?;
                if s != source || t != target || k != kind {
                    return Err(corrupt("identity lifecycle key disagrees"));
                }
                if lsn >= deletion_lsn {
                    return Ok(None);
                }
                Self::require_graph_commit_in_write(
                    write,
                    deletion_lsn,
                    d,
                    false,
                    "requested_own",
                )?;
                if state == 0 {
                    Self::validate_present_predecessor_in_write(write, p, d)?;
                } else {
                    Self::validate_retired_predecessor_in_write(write, p, d)?;
                }
                Ok(Some((d, lsn, tx)))
            })?;
            if let Some((d, lsn, tx)) = candidate {
                if winner
                    .as_ref()
                    .is_some_and(|(old, old_d, _)| old.as_slice() == p && *old_d != d)
                {
                    return Err(corrupt(
                        "physical and lifecycle candidate tombstones conflict",
                    ));
                }
                if winner
                    .as_ref()
                    .is_none_or(|(_, _, order)| (lsn, tx) > *order)
                {
                    if let Some((old, old_d, old_order)) = &mut winner {
                        if old.len() != p.len() {
                            return Err(corrupt("identity predecessor width changed"));
                        }
                        old.copy_from_slice(p);
                        *old_d = d;
                        *old_order = (lsn, tx);
                    } else {
                        crate::read_image_memory::reserve(p.len())?;
                        winner = Some((p.to_vec(), d, (lsn, tx)));
                    }
                }
            }
        }
        Ok(winner.map(|(p, d, _)| (p, d)))
    }
    pub(super) fn encode_change_log_into_in_write(
        &self,
        write: &redb::WriteTransaction,
        entry: &ChangeLogEntry,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_change_log_with_selected_predecessor_in_write(write, entry, None, encoded)
    }
    pub(super) fn encode_change_log_with_selected_predecessor_in_write(
        &self,
        write: &redb::WriteTransaction,
        entry: &ChangeLogEntry,
        selected: Option<(&[u8], TxId)>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        self.graph_writer_window(|| {
            Self::require_graph_proof_tables_in_write(write)?;
            let witness = if let ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            } = entry
            {
                let predecessor = if let Some((p, d)) = selected {
                    // Ordinary commits supply the exact version they tombstoned.
                    // A failed exact lookup cannot become public-import fallback.
                    Self::validate_present_predecessor_in_write(write, p, d)?;
                    Self::require_graph_commit_in_write(write, *lsn, d, false, "requested_own")?;
                    crate::read_image_memory::reserve(p.len())?;
                    Some((p.to_vec(), d))
                } else {
                    self.select_graph_predecessor_in_write(
                        write, *source, *target, edge_type, *lsn,
                    )?
                };
                if let Some((predecessor, deletion_tx)) = predecessor {
                    // Reference-count and id exhaustion both precede private
                    // occurrence allocation and publication writes.
                    {
                        let rk = lifecycle_key(&predecessor)?;
                        let receipts = write
                            .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
                            .map_err(Self::storage_error)?;
                        if let Some(value) = receipts
                            .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                            .map_err(Self::storage_error)?
                        {
                            let (d, count, _) = lifecycle(value.value())?;
                            if d != deletion_tx {
                                return Err(corrupt(
                                    "selected predecessor receipt tombstone mismatch",
                                ));
                            }
                            count
                                .checked_add(1)
                                .ok_or_else(|| corrupt("graph reference count overflow"))?;
                        }
                    }
                    let mut bounds = Self::read_graph_recovery_bounds_in_write(write)?;
                    let id = bounds
                        .graph_occurrence_id_floor
                        .checked_add(1)
                        .filter(|id| *id != 0)
                        .ok_or_else(|| corrupt("graph occurrence id exhausted"))?;
                    bounds.graph_occurrence_id_floor = id;
                    Self::write_graph_recovery_bounds_in_write(write, bounds)?;
                    Some(GraphDeletionWitness {
                        id,
                        predecessor,
                        deletion_tx,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            let tag = if witness.is_some() {
                2
            } else if matches!(entry, ChangeLogEntry::EdgeDelete { .. }) {
                1
            } else {
                0
            };
            Self::encode_stored_change_log_parts(entry, tag, witness.as_ref(), encoded)
        })
    }
    pub(super) fn validate_graph_occurrence_in_write(
        write: &redb::WriteTransaction,
        position: &[u8; 16],
        bytes: &[u8],
    ) -> Result<()> {
        let record = Self::decode_stored_change_log(bytes)?;
        let Some(w) = record.witness else {
            return Ok(());
        };
        let bounds = Self::read_graph_recovery_bounds_in_write(write)?;
        if w.id > bounds.graph_occurrence_id_floor {
            return Err(corrupt("occurrence exceeds allocated id floor"));
        }
        {
            let t = write
                .open_table(GRAPH_OCCURRENCE_LOCATIONS)
                .map_err(Self::storage_error)?;
            let loc = t
                .get(observed_graph_key!(
                    "graph_location",
                    "get",
                    location_key(w.id).as_slice()
                ))
                .map_err(Self::storage_error)?
                .ok_or_else(|| corrupt("graph occurrence location missing"))?;
            if loc.value() != location(position).as_slice() {
                return Err(corrupt("graph occurrence location mismatch"));
            }
        }
        let stored_lsn = Lsn(u64::from_be_bytes(position[..8].try_into().unwrap()));
        {
            let c = write
                .open_table(GRAPH_CONTRIBUTIONS)
                .map_err(Self::storage_error)?;
            let key = deletion_contribution_key(stored_lsn, &w);
            let proof = c
                .get(observed_graph_key!(
                    "graph_contribution",
                    "get",
                    key.as_slice()
                ))
                .map_err(Self::storage_error)?
                .ok_or_else(|| corrupt("graph deletion contribution missing"))?;
            if proof.value()
                != digest_value(contribution_digest(false, &w.id.to_be_bytes(), bytes)?).as_slice()
            {
                return Err(corrupt("graph deletion digest mismatch"));
            }
        }
        let (d, _, state) = {
            let rk = lifecycle_key(&w.predecessor)?;
            let r = write
                .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
                .map_err(Self::storage_error)?;
            let value = r
                .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                .map_err(Self::storage_error)?
                .ok_or_else(|| corrupt("graph predecessor receipt missing"))?;
            lifecycle(value.value())?
        };
        if d != w.deletion_tx {
            return Err(corrupt("graph witness receipt tombstone mismatch"));
        }
        if state == 0 {
            Self::validate_present_predecessor_in_write(write, &w.predecessor, d)?;
        } else {
            Self::validate_retired_predecessor_in_write(write, &w.predecessor, d)?;
        }
        Self::require_graph_commit_in_write(write, record.entry.lsn(), d, false, "requested_own")
    }
    /// Install NEW or relocate CARRIED using its immutable complete bytes.
    /// An existing T proves identity; matching coordinates mean idempotence.
    pub(super) fn install_graph_occurrence_in_write(
        write: &redb::WriteTransaction,
        position: &[u8; 16],
        bytes: &[u8],
    ) -> Result<()> {
        let record = Self::decode_stored_change_log(bytes)?;
        let Some(w) = record.witness else {
            return Ok(());
        };
        let bounds = Self::read_graph_recovery_bounds_in_write(write)?;
        if w.id > bounds.graph_occurrence_id_floor {
            return Err(corrupt("new occurrence exceeds allocated id floor"));
        }
        let tk = location_key(w.id);
        let old = {
            let t = write
                .open_table(GRAPH_OCCURRENCE_LOCATIONS)
                .map_err(Self::storage_error)?;
            t.get(observed_graph_key!("graph_location", "get", tk.as_slice()))
                .map_err(Self::storage_error)?
                .map(|v| {
                    if v.value().len() != 17 || v.value()[0] != 1 {
                        return Err(corrupt("malformed occurrence location"));
                    }
                    let mut p = [0; 16];
                    p.copy_from_slice(&v.value()[1..]);
                    Ok(p)
                })
                .transpose()?
        };
        let final_lsn = Lsn(u64::from_be_bytes(position[..8].try_into().unwrap()));
        let digest = digest_value(contribution_digest(false, &w.id.to_be_bytes(), bytes)?);
        if let Some(old) = old {
            let old_lsn = Lsn(u64::from_be_bytes(old[..8].try_into().unwrap()));
            let old_key = deletion_contribution_key(old_lsn, &w);
            let mut c = write
                .open_table(GRAPH_CONTRIBUTIONS)
                .map_err(Self::storage_error)?;
            let previous = c
                .get(observed_graph_key!(
                    "graph_contribution",
                    "get",
                    old_key.as_slice()
                ))
                .map_err(Self::storage_error)?
                .ok_or_else(|| corrupt("carried occurrence contribution missing"))?;
            if previous.value() != digest.as_slice() {
                return Err(corrupt("carried occurrence private bytes changed"));
            }
            drop(previous);
            if old_lsn != final_lsn {
                c.remove(observed_graph_key!(
                    "graph_contribution",
                    "remove",
                    old_key.as_slice(),
                    4
                ))
                .map_err(Self::storage_error)?;
            }
        } else {
            Self::require_graph_commit_in_write(
                write,
                record.entry.lsn(),
                w.deletion_tx,
                false,
                "requested_own",
            )?;
            let rk = lifecycle_key(&w.predecessor)?;
            let mut r = write
                .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
                .map_err(Self::storage_error)?;
            let old = r
                .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                .map_err(Self::storage_error)?
                .map(|v| lifecycle(v.value()))
                .transpose()?;
            let (count, state) = if let Some((d, count, state)) = old {
                if d != w.deletion_tx {
                    return Err(corrupt("new occurrence receipt tombstone mismatch"));
                }
                if state == 0 {
                    Self::validate_present_predecessor_in_write(write, &w.predecessor, d)?;
                } else {
                    Self::validate_retired_predecessor_in_write(write, &w.predecessor, d)?;
                }
                (
                    count
                        .checked_add(1)
                        .ok_or_else(|| corrupt("graph reference count overflow"))?,
                    state,
                )
            } else {
                Self::validate_present_predecessor_in_write(write, &w.predecessor, w.deletion_tx)?;
                (1, 0)
            };
            r.insert(
                observed_graph_key!("graph_lifecycle", "insert", rk.as_slice(), 6),
                receipt(w.deletion_tx, count, state).as_slice(),
            )
            .map_err(Self::storage_error)?;
        }
        write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?
            .insert(
                observed_graph_key!(
                    "graph_contribution",
                    "insert",
                    deletion_contribution_key(final_lsn, &w).as_slice(),
                    4
                ),
                digest.as_slice(),
            )
            .map_err(Self::storage_error)?;
        write
            .open_table(GRAPH_OCCURRENCE_LOCATIONS)
            .map_err(Self::storage_error)?
            .insert(
                observed_graph_key!("graph_location", "insert", tk.as_slice(), 7),
                location(position).as_slice(),
            )
            .map_err(Self::storage_error)?;
        Ok(())
    }
    pub(super) fn remove_graph_occurrence_in_write(
        write: &redb::WriteTransaction,
        position: &[u8; 16],
        bytes: &[u8],
    ) -> Result<()> {
        let record = Self::decode_stored_change_log(bytes)?;
        let Some(w) = record.witness else {
            return Ok(());
        };
        let tk = location_key(w.id);
        let mut locations = write
            .open_table(GRAPH_OCCURRENCE_LOCATIONS)
            .map_err(Self::storage_error)?;
        let loc = locations
            .remove(observed_graph_key!(
                "graph_location",
                "remove",
                tk.as_slice(),
                7
            ))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("removed occurrence location missing"))?;
        if loc.value() != location(position).as_slice() {
            return Err(corrupt("removed occurrence location mismatch"));
        }
        let key = deletion_contribution_key(
            Lsn(u64::from_be_bytes(position[..8].try_into().unwrap())),
            &w,
        );
        let mut contributions = write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?;
        let value = contributions
            .remove(observed_graph_key!(
                "graph_contribution",
                "remove",
                key.as_slice(),
                4
            ))
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("removed occurrence contribution missing"))?;
        if value.value()
            != digest_value(contribution_digest(false, &w.id.to_be_bytes(), bytes)?).as_slice()
        {
            return Err(corrupt("removed occurrence contribution mismatch"));
        }
        let rk = lifecycle_key(&w.predecessor)?;
        let mut r = write
            .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
            .map_err(Self::storage_error)?;
        let (d, count, state) = {
            let v = r
                .get(observed_graph_key!("graph_lifecycle", "get", rk.as_slice()))
                .map_err(Self::storage_error)?
                .ok_or_else(|| corrupt("removed occurrence lifecycle missing"))?;
            lifecycle(v.value())?
        };
        if d != w.deletion_tx {
            return Err(corrupt("removed occurrence tombstone mismatch"));
        }
        let next = count
            .checked_sub(1)
            .ok_or_else(|| corrupt("graph reference count underflow"))?;
        if next == 0 {
            r.remove(observed_graph_key!(
                "graph_lifecycle",
                "remove",
                rk.as_slice(),
                6
            ))
            .map_err(Self::storage_error)?;
        } else {
            r.insert(
                observed_graph_key!("graph_lifecycle", "insert", rk.as_slice(), 6),
                receipt(d, next, state).as_slice(),
            )
            .map_err(Self::storage_error)?;
        }
        Ok(())
    }
    pub(super) fn final_graph_receipts_nonempty(write: &redb::WriteTransaction) -> Result<bool> {
        Self::require_graph_proof_tables_in_write(write)?;
        let table = write
            .open_table(GRAPH_PREDECESSOR_LIFECYCLE)
            .map_err(Self::storage_error)?;
        let first = table.first().map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "graph_lifecycle",
            "support_probe",
            first.as_ref().map_or(&[][..], |(key, _)| key.value()),
            None,
            Some("final_nonempty_receipts"),
        );
        Ok(first.is_some())
    }
    pub(super) fn has_graph_creation_at_lsn(
        write: &redb::WriteTransaction,
        lsn: Lsn,
    ) -> Result<bool> {
        let mut prefix = [0; 9];
        prefix[0] = 1;
        prefix[1..].copy_from_slice(&lsn.0.to_be_bytes());
        let c = write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?;
        let mut range = c
            .range(prefix.as_slice()..)
            .map_err(Self::storage_error)?
            .inspect(|_entry| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok((key, _)) = _entry {
                    crate::read_work_probe::graph_storage_key(
                        "graph_contribution",
                        "iteration_step",
                        key.value(),
                    );
                }
            });
        let Some((key, _)) = range.next().transpose().map_err(Self::storage_error)? else {
            return Ok(false);
        };
        if !key.value().starts_with(&prefix) {
            return Ok(false);
        }
        if key.value().len() < 18 || key.value()[17] != 0 {
            return Err(corrupt(
                "final empty lifecycle has a non-creation contribution",
            ));
        }
        // In a validated empty-R publication there are no deletion C records.
        // One addressed prefix member therefore settles creation support.
        Ok(true)
    }
}

impl RedbPersistence {
    /// Public-only doors normalize final coordinate collisions first, then
    /// consume the least old numeric coordinate in each exact public class.
    /// All population state lives in transaction-local disk tables. These
    /// pages use the write transaction's allocator/disk admission owner; each
    /// comparison/codec buffer has a separate admitted window.
    pub(super) fn publish_public_change_log_in_write<'a>(
        &self,
        write: &redb::WriteTransaction,
        previous: &redb::ReadTransaction,
        whole: bool,
        assignments: impl Iterator<Item = (Lsn, usize, &'a ChangeLogEntry)>,
    ) -> Result<()> {
        Self::require_graph_proof_tables_in_write(write)?;
        let _verifier_charge = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    std::mem::size_of::<content_identity::ContentIdentityReadVerifier>()
                        .saturating_add(content_identity::READ_VERIFIER_MEMORY_BYTES),
                    "writer_source",
                    "history_authentication",
                    "Raise MEMORY_LIMIT to authenticate replaced history.",
                )
            })
            .transpose()?;
        let verifier = content_identity::ContentIdentityReadVerifier::open(
            previous,
            &self.path,
            self.proof_accountant(),
        )?;
        for definition in [MATCH_OLD, MATCH_TARGET, MATCH_FINAL, MATCH_CONSUMED] {
            // A previous transaction can never publish one of these tables.
            if write
                .list_tables()
                .map_err(Self::storage_error)?
                .any(|t| t.name() == definition.name())
            {
                return Err(corrupt("history correspondence scratch was published"));
            }
            write.open_table(definition).map_err(Self::storage_error)?;
        }
        for (order, (lsn, ordinal, entry)) in assignments.enumerate() {
            self.graph_writer_window(|| {
                let mut key = String::new();
                Self::write_change_log_entry_key(lsn, ordinal, &mut key);
                let position = Self::numeric_change_key(&key)?;
                let public = Self::encode(entry)?;
                crate::read_image_memory::reserve(
                    public
                        .len()
                        .checked_add(8)
                        .ok_or_else(|| corrupt("history assignment size overflow"))?,
                )?;
                let mut value = Vec::with_capacity(public.len() + 8);
                value.extend_from_slice(
                    &u64::try_from(order)
                        .map_err(|_| corrupt("history assignment order overflow"))?
                        .to_be_bytes(),
                );
                value.extend_from_slice(&public);
                write
                    .open_table(MATCH_TARGET)
                    .map_err(Self::storage_error)?
                    .insert(position.as_slice(), value.as_slice())
                    .map_err(Self::storage_error)?;
                Ok(())
            })?;
        }
        // Matching reads exactly the replaced numeric coordinate domain.
        {
            let source = write
                .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
                .map_err(Self::storage_error)?;
            if whole {
                let primary = write
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                if primary.len().map_err(Self::storage_error)?
                    != source.len().map_err(Self::storage_error)?
                {
                    return Err(corrupt("history primary and selector populations disagree"));
                }
                drop(primary);
                for item in source
                    .iter()
                    .map_err(Self::storage_error)?
                    .inspect(|_entry| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Ok((key, _)) = _entry {
                            crate::read_work_probe::graph_storage_key(
                                "history",
                                "iteration_step",
                                key.value(),
                            );
                        }
                    })
                {
                    let (position, bytes) = item.map_err(Self::storage_error)?;
                    self.stage_old_graph_history_match(
                        write,
                        &verifier,
                        position.value(),
                        bytes.value(),
                    )?;
                }
            } else {
                let target = write
                    .open_table(MATCH_TARGET)
                    .map_err(Self::storage_error)?;
                for item in target.iter().map_err(Self::storage_error)? {
                    let (position, _) = item.map_err(Self::storage_error)?;
                    if let Some(bytes) = source
                        .get(observed_graph_key!("history", "get", position.value()))
                        .map_err(Self::storage_error)?
                    {
                        self.stage_old_graph_history_match(
                            write,
                            &verifier,
                            position.value(),
                            bytes.value(),
                        )?;
                    } else {
                        self.graph_writer_window(|| {
                            if position.value().len() != 16 {
                                return Err(corrupt("invalid targeted physical history position"));
                            }
                            crate::read_image_memory::reserve(41)?;
                            let key = format!(
                                "{:020}:{:06}",
                                u64::from_be_bytes(position.value()[..8].try_into().unwrap()),
                                u64::from_be_bytes(position.value()[8..].try_into().unwrap())
                            );
                            if write
                                .open_table(CHANGE_LOG_TABLE)
                                .map_err(Self::storage_error)?
                                .get(observed_graph_key!("history", "get", key.as_str()))
                                .map_err(Self::storage_error)?
                                .is_some()
                            {
                                return Err(corrupt("targeted history primary has no selector"));
                            }
                            Ok(())
                        })?;
                    }
                }
            }
        }
        // Process normalized final targets in original final-assignment order,
        // without constructing a resident ordered collection.
        // Re-keying by order uses the same scratch final table temporarily.
        {
            let targets = write
                .open_table(MATCH_TARGET)
                .map_err(Self::storage_error)?;
            let mut finals = write.open_table(MATCH_FINAL).map_err(Self::storage_error)?;
            for item in targets.iter().map_err(Self::storage_error)? {
                let (p, v) = item.map_err(Self::storage_error)?;
                if v.value().len() < 8 {
                    return Err(corrupt("invalid staged assignment"));
                }
                self.graph_writer_window(|| {
                    crate::read_image_memory::reserve(
                        v.value()
                            .len()
                            .checked_add(16)
                            .ok_or_else(|| corrupt("staged assignment size overflow"))?,
                    )?;
                    let mut value = Vec::with_capacity(v.value().len() + 16);
                    value.extend_from_slice(p.value());
                    value.extend_from_slice(&v.value()[8..]);
                    finals
                        .insert(&v.value()[..8], value.as_slice())
                        .map_err(Self::storage_error)?;
                    Ok(())
                })?;
            }
        }
        let mut after = None::<[u8; 8]>;
        loop {
            let memory = match self.proof_accountant() {
                Some(a) => crate::read_image_memory::ImageMemory::charging_for(
                    a,
                    "writer_source",
                    "history_correspondence",
                    "Raise MEMORY_LIMIT to match this history occurrence.",
                ),
                None => crate::read_image_memory::ImageMemory::new(None),
            };
            let next = memory.scope(|| {
                let finals = write.open_table(MATCH_FINAL).map_err(Self::storage_error)?;
                let mut range = finals
                    .range::<&[u8]>((
                        after.as_ref().map_or(std::ops::Bound::Unbounded, |p| {
                            std::ops::Bound::Excluded(p.as_slice())
                        }),
                        std::ops::Bound::Unbounded,
                    ))
                    .map_err(Self::storage_error)?;
                let Some((order, value)) = range.next().transpose().map_err(Self::storage_error)?
                else {
                    return Ok(None);
                };
                if order.value().len() != 8 || value.value().len() < 16 {
                    return Err(corrupt("invalid staged final assignment"));
                }
                crate::read_image_memory::reserve(value.value().len())?;
                Ok(Some((
                    <[u8; 8]>::try_from(order.value()).unwrap(),
                    value.value().to_vec(),
                )))
            })?;
            let Some((order, target)) = next else {
                break;
            };
            after = Some(order);
            memory.scope(|| {
                let position: [u8; 16] = target[..16].try_into().unwrap();
                let public = &target[16..];
                let entry: ChangeLogEntry = Self::decode_config_value_exact(public)?;
                let key = match_key(public, &[0; 16])?;
                let prefix = &key[..key.len() - 16];
                let candidate = {
                    let olds = write.open_table(MATCH_OLD).map_err(Self::storage_error)?;
                    let mut range = olds.range(prefix..).map_err(Self::storage_error)?;
                    let next = range.next().transpose().map_err(Self::storage_error)?;
                    match next {
                        Some((k, v)) if k.value().starts_with(prefix) => {
                            if k.value().len() != prefix.len() + 16 {
                                return Err(corrupt("invalid matching candidate key"));
                            }
                            // Exact full public bytes, never hash-only equality.
                            if Self::encode(&Self::decode_stored_change_log(v.value())?.entry)?
                                .as_slice()
                                != public
                            {
                                return Err(corrupt("matching candidate public bytes disagree"));
                            }
                            crate::read_image_memory::reserve(
                                k.value()
                                    .len()
                                    .checked_add(v.value().len())
                                    .ok_or_else(|| corrupt("matching candidate size overflow"))?,
                            )?;
                            Some((k.value().to_vec(), v.value().to_vec()))
                        }
                        _ => None,
                    }
                };
                let bytes = if let Some((key, bytes)) = candidate {
                    let old_position = &key[key.len() - 16..];
                    write
                        .open_table(MATCH_CONSUMED)
                        .map_err(Self::storage_error)?
                        .insert(old_position, position.as_slice())
                        .map_err(Self::storage_error)?;
                    write
                        .open_table(MATCH_OLD)
                        .map_err(Self::storage_error)?
                        .remove(key.as_slice())
                        .map_err(Self::storage_error)?;
                    bytes
                } else {
                    let mut bytes = Vec::new();
                    self.encode_change_log_into_in_write(write, &entry, &mut bytes)?;
                    // Install NEW references before dropping any old reference,
                    // so a last-reference replacement retains its exact receipt.
                    Self::install_graph_occurrence_in_write(write, &position, &bytes)?;
                    bytes
                };
                write
                    .open_table(MATCH_TARGET)
                    .map_err(Self::storage_error)?
                    .insert(position.as_slice(), bytes.as_slice())
                    .map_err(Self::storage_error)?;
                Ok(())
            })?;
        }
        // Detach carried selectors without retiring their occurrences. Remove
        // every unconsumed old occurrence once; untouched append suffixes are
        // never in this domain.
        let mut after = None::<[u8; 16]>;
        loop {
            let memory = match self.proof_accountant() {
                Some(a) => crate::read_image_memory::ImageMemory::charging_for(
                    a,
                    "writer_source",
                    "history_detach",
                    "Raise MEMORY_LIMIT to publish this history occurrence.",
                ),
                None => crate::read_image_memory::ImageMemory::new(None),
            };
            let next = memory.scope(|| {
                let source = write
                    .open_table(if whole {
                        writer_history::WRITER_CHANGE_LOG_TABLE
                    } else {
                        MATCH_TARGET
                    })
                    .map_err(Self::storage_error)?;
                let mut range = source
                    .range::<&[u8]>((
                        after.as_ref().map_or(std::ops::Bound::Unbounded, |p| {
                            std::ops::Bound::Excluded(p.as_slice())
                        }),
                        std::ops::Bound::Unbounded,
                    ))
                    .map_err(Self::storage_error)?
                    .inspect(|_entry| {
                        #[cfg(any(test, feature = "test-seams"))]
                        if let Ok((key, _)) = _entry {
                            crate::read_work_probe::graph_storage_key(
                                if whole {
                                    "history"
                                } else {
                                    "history_match_target"
                                },
                                "iteration_step",
                                key.value(),
                            );
                        }
                    });
                let Some((p, _)) = range.next().transpose().map_err(Self::storage_error)? else {
                    return Ok(None);
                };

                if p.value().len() != 16 {
                    return Err(corrupt("invalid history position"));
                }
                Ok(Some(<[u8; 16]>::try_from(p.value()).unwrap()))
            })?;
            let Some(position) = next else {
                break;
            };
            after = Some(position);
            memory.scope(|| -> Result<()> {
                let key = format!(
                    "{:020}:{:06}",
                    u64::from_be_bytes(position[..8].try_into().unwrap()),
                    u64::from_be_bytes(position[8..].try_into().unwrap())
                );
                let carried = write
                    .open_table(MATCH_CONSUMED)
                    .map_err(Self::storage_error)?
                    .get(position.as_slice())
                    .map_err(Self::storage_error)?
                    .is_some();
                let removal = {
                    let old = write
                        .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?;
                    old.get(observed_graph_key!("history", "get", position.as_slice()))
                        .map_err(Self::storage_error)?
                        .map(|value| -> Result<_> {
                            let mut mutation =
                                Self::stored_change_log_mutation(&key, value.value())?;
                            mutation.logical_value = None;
                            Ok(mutation)
                        })
                        .transpose()?
                };
                Self::detach_change_log_selectors_in_write(write, &key, carried)?;
                write
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?
                    .remove(observed_graph_key!("history", "remove", key.as_str(), 8))
                    .map_err(Self::storage_error)?;
                if let Some(removal) = removal {
                    Self::apply_content_identity_mutations_in_write(
                        write,
                        &[removal],
                        &mut self.proof_admission(),
                    )?;
                }
                Ok(())
            })?;
        }
        let mut after = None::<[u8; 16]>;
        loop {
            let next = self.graph_writer_window(|| {
                // Root fingerprinting finalizes every pending table, including
                // this scratch source. Prepare the already-admitted owned
                // mutation before publication, then release its borrowed pages.
                let next = {
                    let target = write
                        .open_table(MATCH_TARGET)
                        .map_err(Self::storage_error)?;
                    let mut range = target
                        .range::<&[u8]>((
                            after.as_ref().map_or(std::ops::Bound::Unbounded, |p| {
                                std::ops::Bound::Excluded(p.as_slice())
                            }),
                            std::ops::Bound::Unbounded,
                        ))
                        .map_err(Self::storage_error)?;
                    let Some((p, bytes)) = range.next().transpose().map_err(Self::storage_error)?
                    else {
                        return Ok(None);
                    };
                    let position: [u8; 16] = p
                        .value()
                        .try_into()
                        .map_err(|_| corrupt("invalid final history position"))?;
                    let key = format!(
                        "{:020}:{:06}",
                        u64::from_be_bytes(position[..8].try_into().unwrap()),
                        u64::from_be_bytes(position[8..].try_into().unwrap())
                    );
                    write
                        .open_table(CHANGE_LOG_TABLE)
                        .map_err(Self::storage_error)?
                        .insert(
                            observed_graph_key!("history", "insert", key.as_str(), 8),
                            bytes.value(),
                        )
                        .map_err(Self::storage_error)?;
                    Self::write_change_log_selectors_in_write(write, &key, bytes.value())?;
                    (
                        position,
                        Self::stored_change_log_mutation(&key, bytes.value())?,
                    )
                };
                let (position, mutation) = next;
                Self::apply_content_identity_mutations_in_write(
                    write,
                    &[mutation],
                    &mut self.proof_admission(),
                )?;
                Ok(Some(position))
            })?;
            let Some(position) = next else {
                break;
            };
            after = Some(position);
        }
        for table in [MATCH_OLD, MATCH_TARGET, MATCH_FINAL, MATCH_CONSUMED] {
            write.delete_table(table).map_err(Self::storage_error)?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    pub(super) fn retired_commit_identity_mutations_in_write(
        write: &redb::WriteTransaction,
        lsns: &[Lsn],
    ) -> Result<Vec<content_identity::ContentIdentityMutation>> {
        crate::read_image_memory::reserve(
            lsns.len()
                .saturating_mul(std::mem::size_of::<content_identity::ContentIdentityMutation>()),
        )?;
        let table = write
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        let mut mutations = Vec::new();
        for lsn in lsns {
            if table
                .get(observed_graph_key!("commit_index", "get", lsn.0, "support"))
                .map_err(Self::storage_error)?
                .is_none()
            {
                mutations.push(content_identity::remove_commit_index_mutation(*lsn));
            }
        }
        Ok(mutations)
    }
}

impl RedbPersistence {
    fn stage_old_graph_history_match(
        &self,
        write: &redb::WriteTransaction,
        verifier: &content_identity::ContentIdentityReadVerifier,
        position: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        self.graph_writer_window(|| {
            let p: [u8; 16] = position
                .try_into()
                .map_err(|_| corrupt("invalid history position"))?;
            let key = format!(
                "{:020}:{:06}",
                u64::from_be_bytes(p[..8].try_into().unwrap()),
                u64::from_be_bytes(p[8..].try_into().unwrap())
            );
            {
                let primary = write
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let body = primary
                    .get(observed_graph_key!("history", "get", key.as_str()))
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corrupt("history selector primary is missing"))?;
                if body.value() != bytes {
                    return Err(corrupt("history primary and selector bytes disagree"));
                }
            }
            let mutation = Self::stored_change_log_mutation(&key, bytes)?;
            let mut nodes =
                content_identity::ReadAuthenticationNodes::new(0, self.proof_accountant());
            verifier.verify_with_nodes(&mut nodes, &mutation)?;
            verifier.clear_startup_proofs();
            Self::validate_graph_occurrence_in_write(write, &p, bytes)?;
            let decoded = Self::decode_stored_change_log(bytes)?;
            if let Some(w) = &decoded.witness {
                let maximum = Self::graph_maximum_in_write(
                    write,
                    Lsn(u64::from_be_bytes(p[..8].try_into().unwrap())),
                )?;
                if w.deletion_tx > maximum {
                    return Err(corrupt(
                        "requested occurrence exceeds concrete physical maximum",
                    ));
                }
            }
            let public = Self::encode(&decoded.entry)?;
            let key = match_key(&public, &p)?;
            write
                .open_table(MATCH_OLD)
                .map_err(Self::storage_error)?
                .insert(key.as_slice(), bytes)
                .map_err(Self::storage_error)?;
            Ok(())
        })
    }
}

impl RedbPersistence {
    pub(super) fn capture_graph_commit_support(
        write: &redb::WriteTransaction,
        lsn: Option<Lsn>,
    ) -> Result<Option<(Lsn, TxId)>> {
        let Some(lsn) = lsn.filter(|lsn| lsn.0 != 0) else {
            return Ok(None);
        };
        let table = write
            .open_table(COMMIT_INDEX_TABLE)
            .map_err(Self::storage_error)?;
        Ok(table
            .get(observed_graph_key!("commit_index", "get", lsn.0, "support"))
            .map_err(Self::storage_error)?
            .map(|v| (lsn, TxId(v.value()))))
    }
    pub(super) fn stage_graph_commit_support(
        write: &redb::WriteTransaction,
        prior: Option<(Lsn, TxId)>,
    ) -> Result<()> {
        if let Some((lsn, old)) = prior {
            let mut table = write
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            let current = table
                .get(observed_graph_key!("commit_index", "get", lsn.0, "support"))
                .map_err(Self::storage_error)?
                .map_or(0, |v| v.value());
            if current < old.0 {
                table.insert(lsn.0, old.0).map_err(Self::storage_error)?;
            }
        }
        Ok(())
    }
    pub(super) fn preserve_graph_commit_support(
        &self,
        write: &redb::WriteTransaction,
        prior: Option<(Lsn, TxId)>,
        requested: Option<(Lsn, TxId)>,
    ) -> Result<()> {
        let Some((lsn, old)) = prior else {
            return Ok(());
        };
        let guarded = Self::final_graph_receipts_nonempty(write)?;
        let changed = {
            let mut table = write
                .open_table(COMMIT_INDEX_TABLE)
                .map_err(Self::storage_error)?;
            let current = table
                .get(observed_graph_key!("commit_index", "get", lsn.0, "support"))
                .map_err(Self::storage_error)?
                .map_or(0, |v| v.value());
            let desired = if guarded {
                current.max(old.0)
            } else {
                requested
                    .filter(|(j, _)| *j == lsn)
                    .map_or(current, |(_, tx)| tx.0)
            };
            if current != desired {
                table.insert(lsn.0, desired).map_err(Self::storage_error)?;
                Some(TxId(desired))
            } else {
                None
            }
        };
        // The complete logical publication may have used the requested value
        // while support was staged. Rebind its actual final retained value.
        if guarded || changed.is_some() {
            let actual = if let Some(tx) = changed {
                tx
            } else {
                let table = write
                    .open_table(COMMIT_INDEX_TABLE)
                    .map_err(Self::storage_error)?;
                TxId(
                    table
                        .get(observed_graph_key!("commit_index", "get", lsn.0, "support"))
                        .map_err(Self::storage_error)?
                        .ok_or_else(|| corrupt("final graph commit support missing"))?
                        .value(),
                )
            };
            Self::apply_content_identity_mutations_in_write(
                write,
                &[content_identity::commit_index_mutation(lsn, actual)],
                &mut self.proof_admission(),
            )?;
        }
        Ok(())
    }
}

impl RedbPersistence {
    /// A concrete maximum authenticates its returned source's own public
    /// commit, which may differ from both the physical prefix and requester.
    fn graph_maximum_in_write(write: &redb::WriteTransaction, address: Lsn) -> Result<TxId> {
        let mut low = [0; 9];
        low[0] = 1;
        low[1..].copy_from_slice(&address.0.to_be_bytes());
        let mut high = [0; 9];
        if let Some(next) = address.0.checked_add(1) {
            high[0] = 1;
            high[1..].copy_from_slice(&next.to_be_bytes());
        } else {
            high[0] = 2;
        }
        let c = write
            .open_table(GRAPH_CONTRIBUTIONS)
            .map_err(Self::storage_error)?;
        #[cfg(any(test, feature = "test-seams"))]
        crate::read_work_probe::note_graph(
            "graph_contribution",
            "maximum_seek",
            &low,
            Some(contextdb_core::Lsn(address.0)),
            None,
        );
        let mut range = c
            .range::<&[u8]>(low.as_slice()..high.as_slice())
            .map_err(Self::storage_error)?
            .inspect(|_entry| {
                #[cfg(any(test, feature = "test-seams"))]
                if let Ok((key, _)) = _entry {
                    crate::read_work_probe::graph_storage_key(
                        "graph_contribution",
                        "iteration_step",
                        key.value(),
                    );
                }
            });
        let (key, digest) = range
            .next_back()
            .transpose()
            .map_err(Self::storage_error)?
            .ok_or_else(|| corrupt("graph contribution maximum missing"))?;
        let k = key.value();
        if k.len() < 18 || !k.starts_with(&low) {
            return Err(corrupt("malformed graph maximum key"));
        }
        let q = TxId(u64::from_be_bytes(k[9..17].try_into().unwrap()));
        match k[17] {
            0 => {
                if k.len() < 22 {
                    return Err(corrupt("truncated creation maximum key"));
                }
                let n = u32::from_be_bytes(k[18..22].try_into().unwrap()) as usize;
                if k.len()
                    != 22usize
                        .checked_add(n)
                        .ok_or_else(|| corrupt("creation maximum key overflow"))?
                {
                    return Err(corrupt("creation maximum key length mismatch"));
                }
                let p = &k[22..];
                let (_, _, _, created, lsn) = parse_physical_predecessor(p)?;
                if created != q || lsn != address {
                    return Err(corrupt("creation maximum coordinate mismatch"));
                }
                let fwd = write
                    .open_table(GRAPH_FWD_TABLE)
                    .map_err(Self::storage_error)?;
                let body = fwd
                    .get(observed_graph_key!("graph_fwd", "get", p))
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corrupt("creation maximum source missing"))?;
                #[cfg(any(test, feature = "test-seams"))]
                crate::read_work_probe::note_graph(
                    "graph_payload",
                    "payload_decode",
                    &[],
                    None,
                    None,
                );

                let edge: AdjEntry = Self::decode(body.value())?;
                if Self::graph_fwd_key(&edge).as_slice() != p {
                    return Err(corrupt("creation maximum source miskeyed"));
                }
                let rev_key = reverse_key(p)?;
                let rev = write
                    .open_table(GRAPH_REV_TABLE)
                    .map_err(Self::storage_error)?;
                let reverse = rev
                    .get(observed_graph_key!("graph_rev", "get", rev_key.as_slice()))
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corrupt("creation maximum counterpart missing"))?;
                if reverse.value() != body.value()
                    || digest.value()
                        != digest_value(contribution_digest(true, p, body.value())?).as_slice()
                {
                    return Err(corrupt("creation maximum source digest mismatch"));
                }
                Self::require_graph_commit_in_write(write, lsn, q, true, "maximum_source")?;
            }
            1 => {
                if k.len() != 26 {
                    return Err(corrupt("deletion maximum key length mismatch"));
                }
                let id = u64::from_be_bytes(k[18..].try_into().unwrap());
                if id == 0
                    || id
                        > Self::read_graph_recovery_bounds_in_write(write)?
                            .graph_occurrence_id_floor
                {
                    return Err(corrupt("deletion maximum id outside allocated floor"));
                }
                let locations = write
                    .open_table(GRAPH_OCCURRENCE_LOCATIONS)
                    .map_err(Self::storage_error)?;
                let loc = locations
                    .get(observed_graph_key!(
                        "graph_location",
                        "get",
                        location_key(id).as_slice()
                    ))
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corrupt("deletion maximum location missing"))?;
                if loc.value().len() != 17 || loc.value()[0] != 1 {
                    return Err(corrupt("deletion maximum location malformed"));
                }
                let a = Lsn(u64::from_be_bytes(loc.value()[1..9].try_into().unwrap()));
                let ordinal = u64::from_be_bytes(loc.value()[9..].try_into().unwrap());
                if a != address {
                    return Err(corrupt("deletion maximum physical address mismatch"));
                }
                crate::read_image_memory::reserve(41)?;
                let h = format!("{:020}:{:06}", a.0, ordinal);
                let history = write
                    .open_table(CHANGE_LOG_TABLE)
                    .map_err(Self::storage_error)?;
                let body = history
                    .get(observed_graph_key!("history", "get", h.as_str()))
                    .map_err(Self::storage_error)?
                    .ok_or_else(|| corrupt("deletion maximum history missing"))?;
                let record = Self::decode_stored_change_log(body.value())?;
                let w = record
                    .witness
                    .ok_or_else(|| corrupt("deletion maximum history has no witness"))?;
                if w.id != id
                    || w.deletion_tx != q
                    || digest.value()
                        != digest_value(contribution_digest(
                            false,
                            &id.to_be_bytes(),
                            body.value(),
                        )?)
                        .as_slice()
                {
                    return Err(corrupt("deletion maximum source digest mismatch"));
                }
                Self::require_graph_commit_in_write(
                    write,
                    record.entry.lsn(),
                    q,
                    false,
                    "maximum_source",
                )?;
            }
            _ => return Err(corrupt("unknown graph maximum contribution kind")),
        }
        Ok(q)
    }
}
