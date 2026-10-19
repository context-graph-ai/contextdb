//! Signed image-prefix evidence, independent of outcome history length.
use super::{canonical::*, checkpoint, records::*};
use crate::Database;
use contextdb_core::{Incarnation, Result, TenantId};

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StatusProbe {
    pub incarnation: Incarnation,
    #[serde(default)]
    pub checkpoint: Option<checkpoint::Checkpoint>,
    pub refused_schema_unit_digests: Vec<[u8; 32]>,
}

pub(crate) fn probe(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    incarnation: Incarnation,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<StatusProbe> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    let destination = custody_hold
        .retain(db.custody_records_in(&["delivery_destination.v1".into()], read_memory.clone())?)?
        .into_iter()
        .find_map(|r| match r {
            Record::Destination(d) => Some(d.namespace),
            _ => None,
        });
    let checkpoint = if let Some(ns) = destination.filter(|ns| {
        ns.tenant == *tenant
            && ns.hub_node == hub
            && ns.edge_node == edge
            && ns.edge_incarnation == incarnation
    }) {
        custody_hold
            .retain(db.custody_records_in(&[checkpoint::edge_key(&ns)?], read_memory.clone())?)?
            .into_iter()
            .find_map(|r| match r {
                Record::EdgeCheckpoint(c) => Some(c),
                _ => None,
            })
    } else {
        None
    };
    Ok(StatusProbe {
        incarnation,
        checkpoint,
        refused_schema_unit_digests: Vec::new(),
    })
}

pub(crate) fn check(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    probe: &StatusProbe,
    vector_copy_owner: &mut contextdb_vector::VectorCopyAdmission,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Incarnation>> {
    let Some(current) = db.existing_sync_incarnation(tenant, read_memory.clone())? else {
        return Ok(None);
    };
    let Some(checkpoint) = &probe.checkpoint else {
        return Ok(Some(current));
    };
    checkpoint.verify()?;
    let ns = &checkpoint.namespace;
    if ns.tenant != *tenant
        || ns.hub_node != hub
        || ns.edge_node != edge
        || ns.edge_incarnation != probe.incarnation
    {
        return Err(invalid());
    }
    if ns.hub_incarnation != current || checkpoint::held(db, checkpoint, read_memory.clone())? {
        return Ok(Some(current));
    }
    // An authentic signed prefix is absent: unrelated writes and re-created or
    // purged recent rows cannot synthesize the lost random committed prefix.
    db.rotate_custody_incarnation(
        tenant,
        hub,
        current,
        Incarnation::mint(),
        vector_copy_owner,
        read_memory.clone(),
    )?;
    db.existing_sync_incarnation(tenant, read_memory.clone())
}
impl StatusProbe {
    pub(crate) fn pages(
        self,
        read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Vec<(Vec<u8>, contextdb_core::read_memory::ReadCredit)>> {
        use crate::protocol::{MessageType, PROTOCOL_VERSION, encode_for_version_with_read_owner};
        let (page, mut credit) = if self.checkpoint.is_none() {
            encode_for_version_with_read_owner(
                PROTOCOL_VERSION,
                MessageType::StatusRequest,
                &crate::protocol::SyncStatusRequest {
                    incarnation: self.incarnation,
                    refused_schema_unit_digests: self.refused_schema_unit_digests,
                },
                read_memory,
            )
        } else {
            encode_for_version_with_read_owner(
                PROTOCOL_VERSION,
                MessageType::StatusRequest,
                &self,
                read_memory,
            )
        }?;
        credit.try_grow_for(
            std::mem::size_of::<(Vec<u8>, contextdb_core::read_memory::ReadCredit)>()
                + crate::sync_types::held::ALLOCATION_BYTES,
            "reader",
            "encode_sync_status",
            "Reduce the status request.",
        )?;
        Ok(vec![(page, credit)])
    }
}
