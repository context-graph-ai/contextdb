//! Authenticated ownership of committed metadata. A certificate covers a state,
//! while a separate local admission ties an object to the current destination.
use super::{canonical::*, records::*};
use contextdb_core::{Incarnation, Result, TenantId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct BirthRef {
    pub logical_store_id: Uuid,
    pub incarnation: Incarnation,
    pub birth_id: Uuid,
}
impl BirthRef {
    pub fn encode(&self, e: &mut Encoder) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes());
    }
}
impl From<&StateRef> for BirthRef {
    fn from(s: &StateRef) -> Self {
        Self {
            logical_store_id: s.logical_store_id,
            incarnation: s.incarnation,
            birth_id: s.birth_id,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Birth {
    pub identity: BirthRef,
    pub predecessor: Option<StateRef>,
    pub selected_image: Option<StateRef>,
    pub triggers: Vec<Digest>,
    pub retired: Vec<BirthRef>,
}
impl Birth {
    pub fn bytes(&self) -> Vec<u8> {
        let mut e = Encoder::default();
        self.identity.encode(&mut e);
        e.option(self.predecessor.as_ref(), |e, s| s.encode(e));
        e.option(self.selected_image.as_ref(), |e, s| s.encode(e));
        e.u64(self.triggers.len() as u64);
        for id in &self.triggers {
            e.raw(id);
        }
        e.u64(self.retired.len() as u64);
        for b in &self.retired {
            b.encode(&mut e);
        }
        e.0
    }
    pub fn digest(&self) -> Digest {
        let mut e = Encoder::domain("delivery-incarnation-birth.v1");
        e.raw(&self.bytes());
        e.digest()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Control {
    pub format: u32,
    pub identity: BirthRef,
    pub hub_node: Option<String>,
    pub tenant: Option<TenantId>,
    pub head: Digest,
    pub namespace_revision: u64,
    pub active_history_root: Digest,
    pub retired_history_root: Digest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Certificate {
    pub format: u32,
    pub tenant: TenantId,
    pub hub_node: String,
    pub state: StateRecord,
    pub birth: Birth,
    pub retirement_digest: Digest,
    pub retired_births: Vec<Birth>,
    pub signature: Vec<u8>,
}
impl StateRecord {
    pub fn bytes(&self) -> Vec<u8> {
        let mut e = Encoder::default();
        self.state.encode(&mut e);
        e.option(self.parent.as_ref(), |e, t| e.raw(t));
        e.u64(self.sequence);
        e.raw(&self.retirement_root);
        e.0
    }
}
impl Certificate {
    pub fn bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::domain("delivery-committed-state.v1");
        e.u32(self.format);
        e.string(self.tenant.as_str());
        e.node(&self.hub_node)?;
        BirthRef::from(&self.state.state).encode(&mut e);
        e.raw(&self.state.bytes());
        e.raw(&self.birth.digest());
        e.raw(&self.retirement_digest);
        Ok(e.0)
    }
    pub fn id(&self) -> Result<Digest> {
        let mut e = Encoder::domain("delivery-state-certificate-id.v1");
        e.bytes(&self.bytes()?);
        e.bytes(&self.signature);
        Ok(e.digest())
    }
    pub fn verify(&self) -> Result<()> {
        if self.format != 1
            || BirthRef::from(&self.state.state) != self.birth.identity
            || self.state.retirement_root != self.retirement_digest
            || retirement_root(&self.retired_births)? != self.retirement_digest
        {
            return Err(invalid());
        }
        validate_retirement_closure(&self.birth, &self.retired_births)?;
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.hub_node,
            &self.bytes()?,
            &self.signature,
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct EdgeAuthority {
    pub destination_epoch: u64,
    pub namespace: Namespace,
    pub birth: BirthRef,
    pub validity_revision: u64,
}
impl EdgeAuthority {
    pub fn encode(&self, e: &mut Encoder) -> Result<()> {
        e.u64(self.destination_epoch);
        e.string(self.namespace.tenant.as_str());
        e.node(&self.namespace.hub_node)?;
        self.birth.encode(e);
        e.node(&self.namespace.edge_node)?;
        e.raw(&self.namespace.edge_incarnation.0.to_be_bytes());
        e.u64(self.validity_revision);
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Admission {
    pub object_digest: Digest,
    pub authority: EdgeAuthority,
    pub owning_state: StateRef,
    pub certificate: Digest,
}
impl Admission {
    pub fn key(&self) -> Result<String> {
        let mut e = Encoder::default();
        e.raw(&self.object_digest);
        self.authority.encode(&mut e)?;
        Ok(format!("delivery_object_admission.v1.{}", hex(&e.0)))
    }
}
pub(crate) fn object_digest(tag: u8, body: &[u8], signature: &[u8]) -> Digest {
    let mut e = Encoder::domain("delivery-authority-object.v1");
    e.u8(tag);
    e.bytes(body);
    e.bytes(signature);
    e.digest()
}
pub(crate) fn empty_history_root() -> Digest {
    let mut e = Encoder::domain("delivery-history-index.v1");
    e.u64(0);
    e.digest()
}
pub(crate) fn history_root(states: &[StateRecord]) -> Digest {
    let mut states: Vec<_> = states.iter().collect();
    states.sort_by_key(|s| s.state.token);
    let mut e = Encoder::domain("delivery-history-index.v1");
    e.u64(states.len() as u64);
    for s in states {
        e.raw(&s.state.token);
        let mut record = Encoder::domain("delivery-state-record.v1");
        record.raw(&s.bytes());
        e.raw(&record.digest());
    }
    e.digest()
}
fn birth_key(b: &BirthRef) -> Vec<u8> {
    let mut e = Encoder::default();
    b.encode(&mut e);
    e.0
}

/// Commit the complete ordered birth descriptors, never a list of unproven UUIDs.
pub(crate) fn retirement_root(births: &[Birth]) -> Result<Digest> {
    if births.is_empty() {
        return Ok(empty_history_root());
    }
    if births
        .windows(2)
        .any(|w| birth_key(&w[0].identity) >= birth_key(&w[1].identity))
    {
        return Err(invalid());
    }
    let mut e = Encoder::domain("delivery-retirement-index.v1");
    e.u64(births.len() as u64);
    for birth in births {
        e.bytes(&birth.bytes());
    }
    Ok(e.digest())
}

pub(crate) fn validate_retirement_closure(active: &Birth, retired: &[Birth]) -> Result<()> {
    retirement_root(retired)?;
    if active.retired.len() != retired.len()
        || active
            .retired
            .iter()
            .zip(retired)
            .any(|(id, b)| *id != b.identity)
        || active.triggers.windows(2).any(|w| w[0] >= w[1])
    {
        return Err(invalid());
    }
    for birth in std::iter::once(active).chain(retired) {
        if birth.identity.logical_store_id != active.identity.logical_store_id
            || birth
                .retired
                .windows(2)
                .any(|w| birth_key(&w[0]) >= birth_key(&w[1]))
            || birth.triggers.windows(2).any(|w| w[0] >= w[1])
            || birth
                .retired
                .iter()
                .any(|id| *id == birth.identity || !retired.iter().any(|b| b.identity == *id))
            || birth
                .predecessor
                .as_ref()
                .is_some_and(|s| !birth.retired.contains(&BirthRef::from(s)))
            || birth
                .selected_image
                .as_ref()
                .is_some_and(|s| !birth.retired.contains(&BirthRef::from(s)))
        {
            return Err(invalid());
        }
    }
    // A transition may retire ancestors, never itself through a cycle.
    for origin in std::iter::once(active).chain(retired) {
        let mut pending = origin.retired.clone();
        let mut visited = std::collections::BTreeSet::new();
        while let Some(id) = pending.pop() {
            if id == origin.identity {
                return Err(invalid());
            }
            if visited.insert(birth_key(&id)) {
                let b = retired
                    .iter()
                    .find(|b| b.identity == id)
                    .ok_or_else(invalid)?;
                pending.extend(b.retired.iter().cloned());
            }
        }
    }
    Ok(())
}

pub(crate) fn retired_descriptors(records: &[Record], active: &Birth) -> Result<Vec<Birth>> {
    active
        .retired
        .iter()
        .map(|id| {
            records
                .iter()
                .find_map(|r| match r {
                    Record::Birth(b) if b.identity == *id => Some(b.clone()),
                    _ => None,
                })
                .ok_or_else(invalid)
        })
        .collect()
}
