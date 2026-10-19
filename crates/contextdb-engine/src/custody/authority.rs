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
/// The Owned history root keeps borrowed selected states and prepaid sort and
/// encoder scratch; the ordinary history's token order and domains are exact.
pub(crate) fn history_root_owned(db: &crate::Database, states: &[&StateRecord]) -> Result<Digest> {
    use super::preparation::{OwnedPreparationValue, preparation_array_bytes};
    let sorted = OwnedPreparationValue::try_produce(
        db,
        preparation_array_bytes::<(usize, &StateRecord)>(states.len())?,
        || {
            let mut sorted = Vec::with_capacity(states.len());
            if sorted.capacity() != states.len() {
                return Err(invalid());
            }
            sorted.extend(
                states
                    .iter()
                    .enumerate()
                    .map(|(position, state)| (position, *state)),
            );
            // Preserve the original stable order for equal tokens without an
            // unadmitted stable-sort scratch allocation.
            sorted.sort_unstable_by_key(|(position, state)| (state.state.token, *position));
            Ok(sorted)
        },
    )?;
    let capacity = (8usize + "delivery-history-index.v1".len() + 8)
        .checked_add(states.len().checked_mul(64).ok_or_else(invalid)?)
        .ok_or_else(invalid)?;
    let bytes = super::canonical::prepare_encoder_owned(db, capacity, |e| {
        e.string("delivery-history-index.v1");
        e.u64(states.len() as u64);
        sorted.with_value(|states| -> Result<()> {
            for (_, state) in states {
                let state_capacity =
                    80usize + 1 + usize::from(state.parent.is_some()) * 32 + 8 + 32;
                let state_bytes =
                    super::canonical::prepare_encoder_owned(db, state_capacity, |e| {
                        state.state.encode(e);
                        e.option(state.parent.as_ref(), |e, token| e.raw(token));
                        e.u64(state.sequence);
                        e.raw(&state.retirement_root);
                        Ok(())
                    })?;
                let record = super::canonical::prepare_encoder_owned(
                    db,
                    8 + "delivery-state-record.v1".len() + state_capacity,
                    |e| {
                        e.string("delivery-state-record.v1");
                        state_bytes.with_value(|bytes| e.raw(bytes));
                        Ok(())
                    },
                )?;
                e.raw(&state.state.token);
                record.with_value(|bytes| e.raw(blake3::hash(bytes).as_bytes()));
            }
            Ok(())
        })
    })?;
    Ok(bytes.with_value(|bytes| *blake3::hash(bytes).as_bytes()))
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

impl BirthRef {
    pub(crate) fn encode_finite(&self, e: &mut FiniteEncoder<'_>) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes());
    }
}
impl Birth {
    fn bytes_finite(&self, e: &mut FiniteEncoder<'_>) {
        self.identity.encode_finite(e);
        e.option(self.predecessor.as_ref(), |e, s| s.encode_finite(e));
        e.option(self.selected_image.as_ref(), |e, s| s.encode_finite(e));
        e.u64(self.triggers.len() as u64);
        for id in &self.triggers {
            e.raw(id);
        }
        e.u64(self.retired.len() as u64);
        for b in &self.retired {
            b.encode_finite(e);
        }
    }
    fn digest_finite(&self) -> Result<Digest> {
        finite_hash(|e| {
            e.string("delivery-incarnation-birth.v1");
            self.bytes_finite(e);
            Ok(())
        })
    }
}
impl StateRecord {
    fn bytes_finite(&self, e: &mut FiniteEncoder<'_>) {
        self.state.encode_finite(e);
        e.option(self.parent.as_ref(), |e, t| e.raw(t));
        e.u64(self.sequence);
        e.raw(&self.retirement_root);
    }
}
fn compare_birth(a: &BirthRef, b: &BirthRef) -> std::cmp::Ordering {
    a.logical_store_id
        .as_bytes()
        .cmp(b.logical_store_id.as_bytes())
        .then_with(|| {
            a.incarnation
                .0
                .to_be_bytes()
                .cmp(&b.incarnation.0.to_be_bytes())
        })
        .then_with(|| a.birth_id.as_bytes().cmp(b.birth_id.as_bytes()))
}
pub(crate) fn retirement_root_finite(births: &[Birth]) -> Result<Digest> {
    retirement_root_finite_with_error(births.iter().map(Ok), invalid)
}

/// Borrowed descriptors in canonical order; no temporary collection is needed.
pub(crate) fn retirement_root_finite_with_error<'a, E>(
    births: impl ExactSizeIterator<Item = std::result::Result<&'a Birth, E>>,
    invalid: impl Fn() -> E,
) -> std::result::Result<Digest, E> {
    use super::canonical::{finite_hash_with_error, finite_len_with_error};
    finite_hash_with_error(
        |e| {
            let count = births.len();
            e.string(if count == 0 {
                "delivery-history-index.v1"
            } else {
                "delivery-retirement-index.v1"
            });
            e.u64(count as u64);
            let mut previous: Option<&BirthRef> = None;
            for birth in births {
                let birth = birth?;
                if previous
                    .is_some_and(|p| compare_birth(p, &birth.identity) != std::cmp::Ordering::Less)
                {
                    return Err(invalid());
                }
                previous = Some(&birth.identity);
                e.u64(finite_len_with_error(
                    |e| {
                        birth.bytes_finite(e);
                        Ok(())
                    },
                    &invalid,
                )? as u64);
                birth.bytes_finite(e);
            }
            Ok(())
        },
        &invalid,
    )
}
pub(crate) fn validate_retirement_closure_owned(
    db: &crate::Database,
    active: &Birth,
    retired: &[Birth],
) -> Result<()> {
    use super::preparation::{OwnedPreparationValue, preparation_array_bytes};
    retirement_root_finite(retired)?;
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
                .any(|w| compare_birth(&w[0], &w[1]) != std::cmp::Ordering::Less)
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
    let edges = retired.iter().try_fold(0usize, |n, b| {
        n.checked_add(b.retired.len()).ok_or_else(invalid)
    })?;
    for origin in std::iter::once(active).chain(retired) {
        let capacity = edges
            .checked_add(origin.retired.len())
            .ok_or_else(invalid)?;
        let mut pending = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<BirthRef>(capacity)?,
            || {
                let mut pending = Vec::with_capacity(capacity);
                if pending.capacity() != capacity {
                    return Err(invalid());
                }
                pending.extend(origin.retired.iter().cloned());
                Ok(pending)
            },
        )?;
        let mut visited = OwnedPreparationValue::try_produce(
            db,
            preparation_array_bytes::<usize>(retired.len())?,
            || {
                let capacity = retired.len();
                let value = Vec::with_capacity(capacity);
                if value.capacity() != capacity {
                    return Err(invalid());
                }
                Ok(value)
            },
        )?;
        while let Some(id) = pending.pop_slot() {
            if id == origin.identity {
                return Err(invalid());
            }
            let index = retired
                .iter()
                .position(|b| b.identity == id)
                .ok_or_else(invalid)?;
            if !visited.with_value(|v| v.contains(&index)) {
                visited.push_slot(index);
                for id in &retired[index].retired {
                    pending.push_slot(id.clone());
                }
            }
        }
    }
    Ok(())
}
pub(crate) fn certificate_bytes_finite(
    e: &mut FiniteEncoder<'_>,
    format: u32,
    tenant: &contextdb_core::TenantId,
    hub_node: &str,
    state: &StateRecord,
    birth: &Birth,
    retirement_digest: &Digest,
) -> Result<()> {
    e.string("delivery-committed-state.v1");
    e.u32(format);
    e.string(tenant.as_str());
    e.node(hub_node)?;
    BirthRef::from(&state.state).encode_finite(e);
    state.bytes_finite(e);
    e.raw(&birth.digest_finite()?);
    e.raw(retirement_digest);
    Ok(())
}
impl Certificate {
    pub(crate) fn bytes_finite(&self, e: &mut FiniteEncoder<'_>) -> Result<()> {
        certificate_bytes_finite(
            e,
            self.format,
            &self.tenant,
            &self.hub_node,
            &self.state,
            &self.birth,
            &self.retirement_digest,
        )
    }
    pub(crate) fn id_finite(&self) -> Result<Digest> {
        finite_hash(|e| {
            e.string("delivery-state-certificate-id.v1");
            e.u64(finite_len(|e| self.bytes_finite(e))? as u64);
            self.bytes_finite(e)?;
            e.bytes(&self.signature);
            Ok(())
        })
    }
}
pub(crate) fn object_digest_finite(
    tag: u8,
    walk: impl Fn(&mut FiniteEncoder<'_>) -> Result<()>,
    signature: &[u8],
) -> Result<Digest> {
    let len = finite_len(|e| walk(e))?;
    finite_hash(|e| {
        e.string("delivery-authority-object.v1");
        e.u8(tag);
        e.u64(len as u64);
        walk(e)?;
        e.bytes(signature);
        Ok(())
    })
}
