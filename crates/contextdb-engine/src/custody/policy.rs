//! Declared authority, authenticated binding, and metadata inspection.
use super::{canonical::*, preparation::*, records::*};
use crate::{Database, custody_types::*, database::QueryResult};
use contextdb_core::{Error, Result, TableMeta, TenantId, Value};
use std::collections::BTreeMap;

#[derive(Clone)]
pub(crate) struct Runtime {
    pub tenant: TenantId,
    pub node: String,
    pub signer: LineageSigner,
}

pub(crate) fn require_admin(db: &Database) -> Result<()> {
    if db.contexts().is_some() || db.scope_labels().is_some() || db.principal().is_some() {
        use contextdb_core::read_contract::{ReadFailure, ReadFailureDetail, ReadFailureKind};
        return Err(Error::ReadFailure(
            ReadFailure::new(
                ReadFailureKind::ConstrainedHandleInspectionRefused,
                ReadFailureDetail::None,
            )
            .expect("inspection refusal has no sensitive detail"),
        ));
    }
    Ok(())
}

pub(crate) fn from_declaration(
    p: &contextdb_parser::DeclareTenantTablePolicy,
) -> ApplicationTablePolicy {
    ApplicationTablePolicy {
        sync_direction: p
            .sync_direction
            .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
        sync_conflict: p
            .conflict_policy
            .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
        immutable: p.immutable,
        retain: p.retain.as_ref().map(|r| RetentionDeclaration {
            seconds: r.duration_seconds,
            declared_unit: r.declared_unit,
            sync_safe: r.sync_safe,
        }),
        history: p.history.unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
        manifest_tables: p.delivery_manifest_tables.clone(),
        edge_discard: p.edge_discard.unwrap_or_default(),
    }
}

pub(crate) fn from_meta(m: &TableMeta) -> ApplicationTablePolicy {
    ApplicationTablePolicy {
        sync_direction: m
            .sync_direction
            .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
        sync_conflict: m
            .conflict_policy
            .unwrap_or(contextdb_core::DEFAULT_CONFLICT_POLICY),
        immutable: m.immutable,
        retain: m.default_ttl_seconds.map(|seconds| RetentionDeclaration {
            seconds,
            declared_unit: m
                .retain_declared_unit
                .unwrap_or(contextdb_core::RetainUnit::Seconds),
            sync_safe: m.sync_safe,
        }),
        history: m
            .history_policy
            .unwrap_or(contextdb_core::DEFAULT_HISTORY_POLICY),
        manifest_tables: m.delivery_manifest_tables.clone(),
        edge_discard: m.edge_discard.unwrap_or_default(),
    }
}

pub(crate) fn differing_clause(
    a: &ApplicationTablePolicy,
    b: &ApplicationTablePolicy,
) -> Option<&'static str> {
    if a.sync_direction != b.sync_direction {
        Some("direction")
    } else if a.sync_conflict != b.sync_conflict {
        Some("conflict")
    } else if a.immutable != b.immutable {
        Some("immutable")
    } else if a.retain != b.retain {
        Some("retain")
    } else if a.history != b.history {
        Some("history")
    } else if a.manifest_tables != b.manifest_tables {
        Some("manifest")
    } else if a.edge_discard != b.edge_discard {
        Some("edge_discard")
    } else {
        None
    }
}

pub(crate) fn declare(
    db: &Database,
    p: &contextdb_parser::DeclareTenantTablePolicy,
) -> Result<QueryResult> {
    require_admin(db)?;
    if let Some(hub_node_id) = db.retention_sync_peer() {
        return Err(Error::DeclareRequiresAuthoritativeHub { hub_node_id });
    }
    let policy = normalized(&p.table, from_declaration(p))?;
    let digest = policy_digest(&p.table, &policy)?;
    db.commit_delivery_metadata(|_| {
        let old = db.custody_authority()?.into_iter().find_map(|r| match r {
            Record::Policy(past) if past.table == p.table => Some(past),
            _ => None,
        });
        if old
            .as_ref()
            .is_some_and(|p| p.first_bound_position.is_some())
        {
            return Err(Error::TenantPolicyBound {
                table: p.table.clone(),
            });
        }
        Ok(vec![Record::Policy(PolicyRecord {
            table: p.table.clone(),
            policy,
            digest,
            version: old.map_or(Ok(1), |p| p.version.checked_add(1).ok_or_else(invalid))?,
            tenant: db.custody_runtime().map(|r| r.tenant),
            first_bound_at: None,
            first_bound_position: None,
        })])
    })?;
    Ok(QueryResult::empty())
}

pub(crate) fn check_binding(db: &Database, table: &str, candidate: &TableMeta) -> Result<()> {
    let candidate = normalized(table, from_meta(candidate))?;
    let records = db.custody_authority()?;
    let destination = records.iter().find_map(|r| match r {
        Record::Destination(d) => Some(&d.namespace),
        _ => None,
    });
    for record in &records {
        if let Record::Binding(b) = record {
            if destination != Some(&b.namespace) {
                continue;
            }
            if let Some(bound) = b.tables.iter().find(|p| p.table == table)
                && let Some(clause) = differing_clause(&candidate, &bound.policy)
            {
                return Err(Error::TableBindingMismatch {
                    table: table.into(),
                    clause: clause.into(),
                });
            }
        }
    }
    Ok(())
}

// DECLARE protects even a name not installed yet.
pub(crate) fn check_declared(db: &Database, table: &str, candidate: &TableMeta) -> Result<()> {
    let candidate = normalized(table, from_meta(candidate))?;
    for record in db.custody_authority()? {
        if let Record::Policy(policy) = record
            && policy.table == table
            && let Some(clause) = differing_clause(&policy.policy, &candidate)
        {
            return Err(Error::DeclaredPolicyPreserved {
                table: table.into(),
                clause: clause.into(),
            });
        }
    }
    Ok(())
}

pub(crate) fn binding_projection(b: &SignedBinding) -> AuthenticatedTenantPolicyBinding {
    AuthenticatedTenantPolicyBinding {
        tenant_id: b.namespace.tenant.clone(),
        hub_node_id: b.namespace.hub_node.clone(),
        hub_incarnation: b.namespace.hub_incarnation,
        edge_node_id: b.namespace.edge_node.clone(),
        edge_incarnation: b.namespace.edge_incarnation,
        tables: b
            .tables
            .iter()
            .map(|p| {
                (
                    p.table.clone(),
                    BoundTablePolicy {
                        policy: p.policy.clone(),
                        version: p.version,
                        digest: p.digest,
                    },
                )
            })
            .collect(),
    }
}

fn policy_cells(p: &ApplicationTablePolicy) -> Vec<Value> {
    use contextdb_core::{
        ConflictPolicy, EdgeDiscardMode, HistoryPolicy, RetainUnit, SyncDirection,
    };
    vec![
        Value::Text(
            match p.sync_direction {
                SyncDirection::None => "off",
                SyncDirection::Push => "push_only",
                SyncDirection::Pull => "pull_only",
                SyncDirection::Both => "two_way",
            }
            .into(),
        ),
        Value::Text(
            if p.sync_conflict == ConflictPolicy::KEEP_LATEST {
                "keep_latest"
            } else {
                "keep_first"
            }
            .into(),
        ),
        Value::Bool(p.immutable),
        p.retain
            .map_or(Value::Null, |r| Value::Int64(r.seconds as i64)),
        p.retain.map_or(Value::Null, |r| {
            Value::Text(
                match r.declared_unit {
                    RetainUnit::Seconds => "seconds",
                    RetainUnit::Minutes => "minutes",
                    RetainUnit::Hours => "hours",
                    RetainUnit::Days => "days",
                }
                .into(),
            )
        }),
        Value::Bool(p.retain.is_some_and(|r| r.sync_safe)),
        Value::Text(
            match p.history {
                HistoryPolicy::All => "all",
                HistoryPolicy::CurrentOnly => "current_only",
            }
            .into(),
        ),
        p.manifest_tables
            .as_ref()
            .map_or(Value::Null, |v| Value::Json(serde_json::json!(v))),
        Value::Text(
            match p.edge_discard {
                EdgeDiscardMode::Never => "never",
                EdgeDiscardMode::AfterOutcome => "after_outcome",
                EdgeDiscardMode::Always => "always",
            }
            .into(),
        ),
    ]
}

pub(crate) fn show_policy(db: &Database, table: Option<&str>) -> Result<QueryResult> {
    require_admin(db)?;
    let mut result = empty_policy_inspection();
    for record in db.custody_authority()? {
        if let Record::Policy(p) = record {
            if table.is_some_and(|name| name != p.table) {
                continue;
            }
            let mut row = vec![
                Value::Text(p.table),
                p.tenant
                    .map_or(Value::Null, |t| Value::Text(t.as_str().into())),
                Value::Int64(p.version as i64),
                Value::Text(hex(&p.digest)),
            ];
            row.extend(policy_cells(&p.policy));
            row.push(
                p.first_bound_position
                    .map_or(Value::Null, |p| Value::Int64(p.0 as i64)),
            );
            result.rows.push(row);
        }
    }
    result
        .rows
        .sort_by(|a, b| format!("{:?}", a[0]).cmp(&format!("{:?}", b[0])));
    Ok(result)
}

pub(crate) fn show_bindings(db: &Database) -> Result<QueryResult> {
    require_admin(db)?;
    let mut result = empty_binding_inspection();
    let mut tables = BTreeMap::new();
    let records = db.custody_authority()?;
    let hub = db.retention_sync_peer();
    // Render the active binding, never an arbitrary former incarnation.
    let destination = records.iter().find_map(|r| match r {
        Record::Destination(d) if Some(d.namespace.hub_node.as_str()) == hub.as_deref() => {
            Some(&d.namespace)
        }
        _ => None,
    });
    for record in &records {
        if let Record::Binding(b) = record {
            if destination != Some(&b.namespace) {
                continue;
            }
            for p in &b.tables {
                let mut row = vec![
                    Value::Text(p.table.clone()),
                    Value::Text(b.namespace.tenant.as_str().into()),
                    Value::Text(b.namespace.hub_node.clone()),
                    Value::Text(b.namespace.hub_incarnation.to_hex()),
                    Value::Text(b.namespace.edge_node.clone()),
                    Value::Text(b.namespace.edge_incarnation.to_hex()),
                    Value::Int64(p.version as i64),
                    Value::Text(hex(&p.digest)),
                ];
                row.extend(policy_cells(&p.policy));
                tables.insert(p.table.clone(), row);
            }
        }
    }
    result.rows = tables.into_values().collect();
    Ok(result)
}
