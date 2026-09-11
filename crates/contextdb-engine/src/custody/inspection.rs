//! Custody accounting reads durable metadata only.
use super::records::*;
use crate::{Database, custody_types::*};
use contextdb_core::{Result, SyncDirection};
use std::collections::BTreeMap;

// Expired authority records remain private journal evidence,
// never current credit in either per-root or administrative inspection.
struct CurrentAuthority<'a> {
    destination: Option<&'a Namespace>,
    local_hub: Option<&'a super::authority::Control>,
}
impl<'a> CurrentAuthority<'a> {
    fn read(records: &'a [Record], registered_hub: Option<&str>) -> Self {
        Self {
            destination: records.iter().find_map(|r| match r {
                Record::Destination(d) if Some(d.namespace.hub_node.as_str()) == registered_hub => {
                    Some(&d.namespace)
                }
                _ => None,
            }),
            local_hub: records.iter().find_map(|r| match r {
                Record::Control(c) if c.hub_node.is_some() => Some(c),
                _ => None,
            }),
        }
    }

    fn holds(&self, edge: bool, terminal: &TerminalRecord) -> bool {
        if edge {
            self.destination == Some(&terminal.namespace)
        } else {
            self.local_hub.is_some_and(|hub| {
                hub.hub_node.as_deref() == Some(terminal.namespace.hub_node.as_str())
                    && hub.tenant.as_ref() == Some(&terminal.namespace.tenant)
                    && hub.identity.incarnation == terminal.namespace.hub_incarnation
            })
        }
    }
}

pub(crate) fn status(db: &Database, table: &str) -> Result<DeliveryStatusCounts> {
    let mut status = DeliveryStatusCounts::default();
    let Some(meta) = db.table_meta(table) else {
        return Ok(status);
    };
    status.disabled = meta.delivery_manifest_tables.is_none()
        || matches!(
            meta.sync_direction,
            Some(SyncDirection::None | SyncDirection::Pull)
        );
    if status.disabled {
        return Ok(status);
    }
    let records = db.custody_table(table)?;
    let hub = db.retention_sync_peer();
    let authority = CurrentAuthority::read(&records, hub.as_deref());
    let manifests = records
        .iter()
        .filter_map(|record| match record {
            Record::Manifest(manifest) => Some((manifest.id, &manifest.seal.root)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let outcomes = records
        .iter()
        .filter_map(|record| match record {
            Record::Terminal { edge: true, record } if authority.holds(true, record) => {
                Some((record.submission, record.kind))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    for record in &records {
        let Record::Root {
            life, submission, ..
        } = record
        else {
            continue;
        };
        if life.original_table != table {
            continue;
        }
        let Some(root) = manifests.get(submission) else {
            continue;
        };
        if !db.delivery_root_visible(&root.table, &root.key)? {
            continue;
        }
        status.eligible += 1;
        match outcomes.get(submission).copied() {
            Some(0) => status.accepted += 1,
            Some(1) => status.equivalent += 1,
            Some(2) => status.refused += 1,
            _ => status.pending += 1,
        }
    }
    Ok(status)
}

pub(crate) fn projection(t: &TerminalRecord, block: &DiagnosticBlock) -> DeliveryOutcome {
    DeliveryOutcome {
        tenant_id: t.namespace.tenant.clone(),
        hub_node_id: t.namespace.hub_node.clone(),
        hub_incarnation: t.namespace.hub_incarnation,
        edge_node_id: t.namespace.edge_node.clone(),
        edge_incarnation: t.namespace.edge_incarnation,
        root_table: t.root.table.clone(),
        root_key: t.root.key.clone(),
        unit_digest: Some(t.unit_digest),
        kind: match t.kind {
            0 => DeliveryOutcomeKind::Accepted,
            1 => DeliveryOutcomeKind::Equivalent,
            _ => DeliveryOutcomeKind::Refused,
        },
        cursor: DeliveryOutcomeCursor {
            tenant_id: t.namespace.tenant.clone(),
            hub_node_id: t.namespace.hub_node.clone(),
            hub_incarnation: t.namespace.hub_incarnation,
            edge_node_id: t.namespace.edge_node.clone(),
            edge_incarnation: t.namespace.edge_incarnation,
            position: t.position.0,
            ordinal: t.position.1,
        },
        cause: t.cause.clone(),
        conflicts: Some(block.conflicts.clone()),
    }
}

pub(crate) fn outcome(
    db: &Database,
    table: &str,
    key: &crate::sync_types::NaturalKey,
) -> Result<Option<DeliveryOutcome>> {
    if !db.delivery_root_visible(table, key)? {
        return Ok(None);
    }
    let mut records = db.custody_root(&RowRef {
        table: table.into(),
        key: key.clone(),
    })?;
    records.extend(db.custody_authority()?);
    let hub = db.retention_sync_peer();
    let authority = CurrentAuthority::read(&records, hub.as_deref());
    let terminal = records
        .iter()
        .filter_map(|r| match r {
            Record::Terminal { edge, record: t }
                if t.root.table == table && t.root.key == *key && authority.holds(*edge, t)
                    && (!*edge || records.iter().any(|r| matches!(r, Record::Root { submission, .. } if *submission == t.submission))) =>
            {
                Some(t)
            }
            _ => None,
        })
        .max_by_key(|t| t.position);
    let Some(t) = terminal else {
        return Ok(None);
    };
    let block = records
        .iter()
        .find_map(|r| match r {
            Record::Diagnostic {
                namespace,
                submission,
                block,
            } if *namespace == t.namespace && *submission == t.submission => Some(block),
            _ => None,
        })
        .ok_or_else(super::canonical::invalid)?;
    let mut outcome = projection(t, block);
    for conflict in &block.conflicts {
        if !db.delivery_root_visible(
            conflict.table.as_deref().unwrap_or(table),
            &conflict.natural_key,
        )? {
            outcome.conflicts = None;
            break;
        }
    }
    Ok(Some(outcome))
}

pub(crate) fn show(
    db: &Database,
    table: &str,
    filter: Option<&contextdb_parser::Expr>,
    limit: Option<usize>,
    offset: usize,
    params: &std::collections::HashMap<String, contextdb_core::Value>,
) -> Result<crate::database::QueryResult> {
    super::policy::require_admin(db)?;
    use contextdb_core::Value;
    let records = db.custody_table(table)?;
    let hub = db.retention_sync_peer();
    let authority = CurrentAuthority::read(&records, hub.as_deref());
    let mut result = empty_outcome_inspection();
    let mut diagnostics = BTreeMap::new();
    for record in &records {
        if let Record::Diagnostic {
            namespace,
            submission,
            block,
        } = record
        {
            diagnostics.insert((*submission, namespace.key()?), block);
        }
    }
    let mut terminals = records
        .iter()
        .filter_map(|r| match r {
            Record::Terminal { edge, record: t }
                if t.root.table == table && authority.holds(*edge, t) =>
            {
                Some(t)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    terminals.sort_by_key(|t| t.position);
    for t in terminals {
        let block = diagnostics
            .get(&(t.submission, t.namespace.key()?))
            .copied()
            .ok_or_else(super::canonical::invalid)?;
        let row = vec![
            Value::Text(t.root.table.clone()),
            Value::Json(
                serde_json::to_value(&t.root.key).map_err(|_| super::canonical::invalid())?,
            ),
            Value::Text(super::canonical::hex(&t.unit_digest)),
            Value::Text(
                match t.kind {
                    0 => "accepted",
                    1 => "equivalent",
                    _ => "refused",
                }
                .into(),
            ),
            t.cause.clone().map_or(Value::Null, Value::Text),
            Value::Text(t.namespace.hub_node.clone()),
            Value::Text(t.namespace.hub_incarnation.to_hex()),
            Value::Text(t.namespace.edge_node.clone()),
            Value::Text(t.namespace.edge_incarnation.to_hex()),
            Value::Int64(t.position.0.0 as i64),
            Value::Json(
                serde_json::to_value(&block.conflicts).map_err(|_| super::canonical::invalid())?,
            ),
        ];
        if let Some(filter) = filter {
            let projected = contextdb_core::VersionedRow {
                row_id: contextdb_core::RowId(0),
                created_tx: contextdb_core::TxId(0),
                deleted_tx: None,
                lsn: t.position.0,
                created_at: None,
                values: result
                    .columns
                    .iter()
                    .cloned()
                    .zip(row.iter().cloned())
                    .collect(),
            };
            if !crate::executor::row_matches(&projected, filter, params)? {
                continue;
            }
        }
        result.rows.push(row);
    }
    result.rows = result
        .rows
        .into_iter()
        .skip(offset)
        .take(limit.unwrap_or(usize::MAX))
        .collect();
    Ok(result)
}

// Each route projects the same durable record through its scoped database.
pub(crate) fn metadata(
    db: &Database,
    request: crate::direct_file_reader::DirectMetadataRequest,
) -> Result<crate::direct_file_reader::DirectMetadataBody> {
    use crate::direct_file_reader::{DirectMetadataBody as Body, DirectMetadataRequest as Request};
    match request {
        Request::DeliveryStatus { root_table } => Ok(Body::DeliveryStatus {
            counts: Some(status(db, &root_table)?),
            has_more: false,
        }),
        Request::DeliveryOutcome {
            root_table,
            root_key,
        } => Ok(Body::DeliveryOutcome {
            outcome: outcome(db, &root_table, &root_key)?,
            has_more: false,
        }),
        _ => Err(super::canonical::invalid()),
    }
}
