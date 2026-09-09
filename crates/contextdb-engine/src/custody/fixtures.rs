//! Authenticated test orchestration uses the same canonical commit preparation as production.
pub(crate) use super::preparation::{
    BindingInput as FixtureBindingRequest, SignedBindingPacket as FixtureSignedBinding,
    SignedOutcomePacket as FixtureSignedOutcome, TerminalInput as FixtureOutcomeRequest,
    admit_binding as install_fixture_binding, admit_terminal as install_fixture_outcome,
    commit_terminal as issue_fixture_outcome,
};
use super::{canonical::*, preparation::*, records::*};
use crate::Database;
use crate::custody_types::*;
use crate::sync_types::{NaturalKey, RowChange};
use contextdb_core::{Result, TenantId};
// Statements 9/10/11: ordinary request preparation needs no separate envelope identity.
pub(crate) fn fixture_binding_subject(t: &TenantId) -> String {
    format!("sync.{}.test-fixture.binding", t.as_str())
}
pub(crate) fn fixture_outcome_subject(t: &TenantId) -> String {
    format!("sync.{}.test-fixture.outcome", t.as_str())
}

pub(crate) fn fixture_outcome_batch_subject(t: &TenantId) -> String {
    format!("sync.{}.test-fixture.outcome-batch", t.as_str())
}

impl Database {
    #[doc(hidden)]
    pub fn __delivery_record_json_for_test(bytes: &[u8]) -> Result<serde_json::Value> {
        serde_json::to_value(super::records::decode(bytes)?).map_err(|_| invalid())
    }
    #[doc(hidden)]
    pub fn __delivery_record_bytes_for_test(value: serde_json::Value) -> Result<Vec<u8>> {
        super::records::encode(&serde_json::from_value(value).map_err(|_| invalid())?)
    }
    /// Stage authoritative local source rows without inventing a peer binding.
    #[doc(hidden)]
    pub fn __stage_local_delivery_manifest_for_test(
        &self,
        tx: contextdb_core::TxId,
        tenant: TenantId,
        identity: std::sync::Arc<crate::identity::FabricIdentity>,
        input: DeliveryManifest<'_>,
    ) -> Result<()> {
        if self.retention_sync_peer().is_some() {
            return Err(invalid());
        }
        let edge = identity.node_id();
        self.sync_incarnation(&tenant)?;
        self.stage_delivery_registration(
            tx,
            SourceRegistration {
                tenant,
                edge,
                root: RowRef {
                    table: input.root_table.into(),
                    key: input.root_key,
                },
                members: input
                    .members
                    .into_iter()
                    .map(|(table, key)| RowRef {
                        table: table.into(),
                        key,
                    })
                    .collect(),
                signer: std::sync::Arc::new(move |bytes| Ok(identity.sign_lineage(bytes))),
            },
        )
    }
    /// Statement 11: fail this unit's real storage commit, including on a receiver worker.
    #[doc(hidden)]
    pub fn __arm_delivery_commit_fault_for_test(
        &self,
        root_table: &str,
        root_key: &NaturalKey,
        unit_digest: [u8; 32],
    ) {
        super::durability::arm_commit_fault(
            RowRef {
                table: root_table.into(),
                key: root_key.clone(),
            },
            unit_digest,
        );
    }
    #[doc(hidden)]
    pub fn __delivery_commit_fault_reached_for_test(&self) -> bool {
        super::durability::commit_fault_reached()
    }
    #[doc(hidden)]
    pub fn __delivery_prerequisite_wires_for_test(
        &self,
    ) -> Result<Vec<crate::protocol::WireDeliveryOutcome>> {
        let records = self.custody_records()?;
        let mut wires = Vec::new();
        for record in &records {
            if let Record::Terminal { record: t, .. } = record {
                let block = records
                    .iter()
                    .find_map(|r| match r {
                        Record::Diagnostic {
                            namespace,
                            submission,
                            block,
                        } if *namespace == t.namespace && *submission == t.submission => {
                            Some(block)
                        }
                        _ => None,
                    })
                    .ok_or_else(invalid)?;
                wires.push(t.wire(block)?);
            }
        }
        Ok(wires)
    }
    #[doc(hidden)]
    pub fn __verify_delivery_core_for_test(node: &str, bytes: &[u8]) -> Result<()> {
        super::decoder::verify_terminal_signature(node, bytes)
    }
    #[doc(hidden)]
    pub fn __delivery_metadata_bytes_for_test(&self) -> Result<Vec<Vec<u8>>> {
        self.custody_records()?
            .iter()
            .map(super::records::encode)
            .collect()
    }
    #[doc(hidden)]
    pub fn __verify_delivery_metadata_bytes_for_test(bytes: &[u8]) -> Result<()> {
        super::records::decode(bytes)?.verify()
    }

    #[doc(hidden)]
    pub fn __delivery_encoders_started_for_test() -> u64 {
        encoders_started_for_test()
    }
    #[doc(hidden)]
    pub fn __delivery_value_commitments_for_test(
        values: &std::collections::HashMap<String, contextdb_core::Value>,
        exclude: &[String],
    ) -> ([u8; 32], [u8; 32]) {
        (full_row_digest(values), content_row_digest(values, exclude))
    }
    #[doc(hidden)]
    pub fn __delivery_diagnostic_encoding_for_test(
        conflicts: Vec<crate::sync_types::Conflict>,
    ) -> Result<Vec<u8>> {
        let subjects = conflicts
            .iter()
            .map(|c| {
                Ok((
                    RowRef {
                        table: c.table.clone().ok_or_else(invalid)?,
                        key: c.natural_key.clone(),
                    },
                    if c.mutation_kind.as_deref() == Some("delete") {
                        1
                    } else {
                        0
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        DiagnosticBlock {
            salt: [19; 32],
            subjects,
            conflicts,
        }
        .bytes()
    }
    #[doc(hidden)]
    pub fn __delivery_diagnostic_resolution_roundtrip_for_test(tag: u8) -> Result<u8> {
        Ok(diagnostic_resolution(decode_diagnostic_resolution(tag)?))
    }
    #[doc(hidden)]
    pub fn __seed_tenant_table_policies_for_test(
        &self,
        tenant: TenantId,
        expectation: ApplicationTablePolicyExpectation,
    ) -> Result<()> {
        let existing = self.custody_records()?;
        if existing
            .iter()
            .any(|r| matches!(r,Record::Policy(p) if p.tenant.as_ref().is_some_and(|t|t!=&tenant)))
        {
            return Err(invalid());
        }
        let mut records = Vec::new();
        for (table, p) in expectation.tables {
            let policy = normalized(&table, p)?;
            let prior = existing.iter().find_map(|r| match r {
                Record::Policy(p) if p.table == table => Some(p),
                _ => None,
            });
            if let Some(prior) = prior {
                if prior.policy == policy {
                    continue;
                }
                return Err(invalid());
            }
            records.push(Record::Policy(PolicyRecord {
                digest: policy_digest(&table, &policy)?,
                table,
                policy,
                version: 1,
                tenant: Some(tenant.clone()),
                first_bound_position: None,
                first_bound_at: None,
            }));
        }
        if records.is_empty() {
            return Ok(());
        }
        self.commit_delivery_metadata(|_| Ok(records))
    }
    /// Inspect actual canonical prerequisite records independently of public delivery reads.
    #[doc(hidden)]
    pub fn __delivery_prerequisites_for_test(&self) -> Result<serde_json::Value> {
        let records = self.custody_records()?;
        for r in &records {
            match r {
            Record::Root{life,submission}=>{if !records.iter().any(|r|matches!(r,Record::Manifest(m) if m.id==*submission&&m.root_life==*life)){return Err(invalid());}},
            Record::MemberOwner{member,root,submission}=>{if !records.iter().any(|r|matches!(r,Record::Manifest(m) if m.id==*submission&&m.root_life==*root&&m.member_lives.contains(member))){return Err(invalid());}},
            Record::Terminal{edge:true,record:t}=>{if !records.iter().any(|r|matches!(r,Record::Manifest(m) if m.id==t.submission&&m.seal.digest()==t.seal_digest)){return Err(invalid());}},_=>{} }
        }
        serde_json::to_value(records).map_err(|_| invalid())
    }
    #[doc(hidden)]
    pub fn __arm_erasure_boundary_persist_fault_for_test(&self) {
        crate::persistence::arm_erasure_first_table_fault_for_test();
    }
    #[doc(hidden)]
    pub fn __erasure_boundary_persist_fault_reached_for_test(&self) -> bool {
        crate::persistence::erasure_first_table_fault_reached_for_test()
    }
}

#[cfg(feature = "sync-orchestration")]
pub(crate) use super::delivery::manifested_request;

pub(crate) fn manifest_rows(db: &Database, m: &ManifestRecord) -> Result<Vec<RowChange>> {
    m.rows
        .iter()
        .map(|e| {
            let row = actual_row(db, &e.reference)?;
            if full_row_digest(&row.values) != e.row_digest {
                return Err(invalid());
            }
            Ok(RowChange {
                table: e.reference.table.clone(),
                natural_key: e.reference.key.clone(),
                values: row.values,
                deleted: false,
                lsn: row.lsn,
                created_at: row.created_at,
            })
        })
        .collect()
}

// Statement 10 fixture builder: arrange signed mismatched caller policy without
// weakening the real bind door, which now rejects that expectation.
pub(crate) fn issue_fixture_binding(
    db: &Database,
    tenant: TenantId,
    hub: String,
    edge: String,
    mut input: BindingInput,
    signer: &LineageSigner,
) -> Result<SignedBindingPacket> {
    let expected = input.expectation.clone();
    let records = db.custody_records()?;
    for (table, policy) in &mut input.expectation.tables {
        if let Some(declared) = records.iter().find_map(|r| match r {
            Record::Policy(p) if p.table == *table => Some(&p.policy),
            _ => None,
        }) {
            *policy = declared.clone();
        }
    }
    let mut packet = commit_binding(db, tenant, hub, edge, input, signer)?;
    for p in &mut packet.binding.tables {
        p.policy = normalized(&p.table, expected.tables[&p.table].clone())?;
        p.digest = policy_digest(&p.table, &p.policy)?;
    }
    packet.binding.signature = signer(&packet.binding.bytes()?)?;
    Ok(packet)
}
