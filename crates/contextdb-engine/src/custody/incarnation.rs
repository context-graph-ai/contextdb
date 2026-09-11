//! Signed image-prefix evidence, independent of outcome history length.
use super::{canonical::*, checkpoint, records::*};
use crate::Database;
use contextdb_core::{Incarnation, Result, TenantId};

#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StatusProbe {
    pub incarnation: Incarnation,
    #[serde(default)]
    pub checkpoint: Option<checkpoint::Checkpoint>,
}

pub(crate) fn probe(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    incarnation: Incarnation,
) -> Result<StatusProbe> {
    let destination = db
        .custody_records_in(&["delivery_destination.v1".into()])?
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
        db.custody_records_in(&[checkpoint::edge_key(&ns)?])?
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
    })
}

pub(crate) fn check(
    db: &Database,
    tenant: &TenantId,
    hub: &str,
    edge: &str,
    probe: &StatusProbe,
) -> Result<Option<Incarnation>> {
    let Some(current) = db.existing_sync_incarnation(tenant)? else {
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
    if ns.hub_incarnation != current || checkpoint::held(db, checkpoint)? {
        return Ok(Some(current));
    }
    // An authentic signed prefix is absent: unrelated writes and re-created or
    // purged recent rows cannot synthesize the lost random committed prefix.
    db.rotate_custody_incarnation(tenant, hub, current, Incarnation::mint())?;
    db.existing_sync_incarnation(tenant)
}
impl StatusProbe {
    pub(crate) fn pages(self) -> Result<Vec<Vec<u8>>> {
        use crate::protocol::{MessageType, encode};
        let page = if self.checkpoint.is_none() {
            encode(
                MessageType::StatusRequest,
                &crate::protocol::SyncStatusRequest {
                    incarnation: self.incarnation,
                },
            )
        } else {
            encode(MessageType::StatusRequest, &self)
        }
        .map_err(|_| invalid())?;
        Ok(vec![page])
    }
}
