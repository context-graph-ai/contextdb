//! Borrowed committed selection -> exact paid native wire packet.
//! The caller retains native selection/source credits on this image's request
//! owner until this function returns; standing identity belongs to the server.
use super::{DecodeError, NativeOutcome, canonical};
use crate::custody::{
    authority::{Birth, BirthRef},
    checkpoint::Checkpoint,
    records::*,
};
use crate::identity::FabricIdentity;
use crate::read_image_memory::{NativeDeliveryBytes, NativeDeliveryImage};
use serde::{Serialize, ser::SerializeSeq};

fn missing() -> DecodeError {
    DecodeError::Malformed("invalid committed packet records")
}
fn selected_birth<'a>(records: &'a [Record], id: &BirthRef) -> Result<&'a Birth, DecodeError> {
    records
        .iter()
        .find_map(|record| match record {
            Record::Birth(birth) if birth.identity == *id => Some(birth),
            _ => None,
        })
        .ok_or_else(missing)
}
fn compare_birth(a: &BirthRef, b: &BirthRef) -> std::cmp::Ordering {
    a.logical_store_id
        .as_bytes()
        .cmp(b.logical_store_id.as_bytes())
        .then_with(|| a.incarnation.0.cmp(&b.incarnation.0))
        .then_with(|| a.birth_id.as_bytes().cmp(b.birth_id.as_bytes()))
}
struct Retired<'a> {
    records: &'a [Record],
    active: &'a Birth,
}
impl Retired<'_> {
    fn validate(&self, memory: &NativeDeliveryImage) -> Result<(), DecodeError> {
        // Fixed reachability bits replace Legacy's growing pending/visited sets.
        // Physical scratch is deallocated before its inline invoice on all exits.
        let mut reached = memory.allocate(self.active.retired.len())?;
        for origin in std::iter::once(&self.active.identity).chain(&self.active.retired) {
            memory.checkpoint()?;
            let birth = selected_birth(self.records, origin)?;
            if birth.identity.logical_store_id != self.active.identity.logical_store_id
                || birth
                    .retired
                    .windows(2)
                    .any(|w| compare_birth(&w[0], &w[1]) != std::cmp::Ordering::Less)
                || birth.triggers.windows(2).any(|w| w[0] >= w[1])
                || birth
                    .predecessor
                    .as_ref()
                    .is_some_and(|state| !birth.retired.contains(&BirthRef::from(state)))
                || birth
                    .selected_image
                    .as_ref()
                    .is_some_and(|state| !birth.retired.contains(&BirthRef::from(state)))
            {
                return Err(missing());
            }
            reached.as_mut_slice().fill(0);
            for id in &birth.retired {
                if *id == *origin {
                    return Err(missing());
                }
                let at = self
                    .active
                    .retired
                    .iter()
                    .position(|candidate| candidate == id)
                    .ok_or_else(missing)?;
                selected_birth(self.records, id)?;
                reached.as_mut_slice()[at] = 1;
            }
            loop {
                memory.checkpoint()?;
                let mut changed = false;
                for (at, id) in self.active.retired.iter().enumerate() {
                    if reached.as_slice()[at] == 0 {
                        continue;
                    }
                    for next in &selected_birth(self.records, id)?.retired {
                        if *next == *origin {
                            return Err(missing());
                        }
                        let next_at = self
                            .active
                            .retired
                            .iter()
                            .position(|candidate| candidate == next)
                            .ok_or_else(missing)?;
                        if reached.as_slice()[next_at] == 0 {
                            reached.as_mut_slice()[next_at] = 1;
                            changed = true;
                        }
                    }
                }
                if !changed {
                    break;
                }
            }
        }
        Ok(())
    }
    fn digest(&self) -> Result<Digest, DecodeError> {
        crate::custody::authority::retirement_root_finite_with_error(
            self.active
                .retired
                .iter()
                .map(|id| selected_birth(self.records, id)),
            missing,
        )
    }
}
impl Serialize for Retired<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.active.retired.len()))?;
        for id in &self.active.retired {
            // validate ran before either serialize pass; inputs remain borrowed.
            let birth = self
                .records
                .iter()
                .find_map(|record| match record {
                    Record::Birth(birth) if birth.identity == *id => Some(birth),
                    _ => None,
                })
                .expect("validated borrowed birth");
            seq.serialize_element(birth)?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct CertificateView<'a> {
    format: u32,
    tenant: &'a contextdb_core::TenantId,
    hub_node: &'a str,
    state: &'a StateRecord,
    birth: &'a Birth,
    retirement_digest: Digest,
    retired_births: Retired<'a>,
    // Ordinary sequence, exactly like Certificate.signature: Vec<u8>.
    signature: &'a [u8],
}
#[derive(Serialize)]
struct PacketView<'a> {
    checkpoint: Option<&'a Checkpoint>,
    record: super::wire_views::TerminalView<'a>,
    block: super::wire_views::BlockView<'a>,
    certificate: CertificateView<'a>,
}
fn canonical_bytes(
    memory: &NativeDeliveryImage,
    walk: impl Fn(&mut canonical::Encoder<'_>) -> Result<(), DecodeError>,
) -> Result<NativeDeliveryBytes, DecodeError> {
    let length = canonical::count(|e| walk(e))?;
    let mut bytes = memory.allocate(length)?;
    canonical::emit(bytes.as_mut_slice(), |e| walk(e))?;
    Ok(bytes)
}
fn verify(node: &str, bytes: &[u8], signature: &[u8]) -> Result<(), DecodeError> {
    use ed25519_dalek::Verifier as _;
    // Scalar failures; the Legacy verifier formats owned error strings.
    if node.len() != 64
        || !node
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(missing());
    }
    let key = ed25519_dalek::VerifyingKey::from_bytes(&canonical::node_bytes(node)?)
        .map_err(|_| missing())?;
    let signature = ed25519_dalek::Signature::from_slice(signature).map_err(|_| missing())?;
    key.verify(bytes, &signature).map_err(|_| missing())
}

/// Native sibling of wire_packet(packet_for(...)); selection is a separate
/// producer. All records must come from the same pinned committed source, with
/// nested allocations retained on `memory`'s original request owner. This
/// function neither acquires an owner nor certifies borrowed input invoices.
pub(crate) fn encode_packet(
    memory: &NativeDeliveryImage,
    identity: &FabricIdentity,
    terminal: &TerminalRecord,
    submission_records: &[Record],
    authority_records: &[Record],
    checkpoint: Option<&Checkpoint>,
) -> Result<NativeOutcome, DecodeError> {
    memory.checkpoint()?;
    let ns = &terminal.namespace;
    if terminal.kind > 2
        || ns.hub_incarnation != terminal.state.incarnation
        || canonical::node_bytes(&ns.hub_node)? != identity.public_key_bytes()
    {
        return Err(missing());
    }
    // Validate before shared walkers so malformed nodes retain scalar refusals.
    canonical::node_bytes(&ns.edge_node)?;
    let block = submission_records
        .iter()
        .find_map(|record| match record {
            Record::Diagnostic {
                namespace,
                submission,
                block,
            } if *namespace == *ns && *submission == terminal.submission => Some(block),
            _ => None,
        })
        .ok_or_else(missing)?;
    let state = authority_records
        .iter()
        .find_map(|record| match record {
            Record::State(state) if state.state == terminal.state => Some(state),
            _ => None,
        })
        .ok_or_else(missing)?;
    let birth = selected_birth(authority_records, &BirthRef::from(&state.state))?;
    let retired = Retired {
        records: authority_records,
        active: birth,
    };
    retired.validate(memory)?;
    let retirement_digest = retired.digest()?;
    if retirement_digest != state.retirement_root {
        return Err(missing());
    }
    // Match checkpoint::current: copy the stored checkpoint unchecked.
    let diagnostic_body = canonical_bytes(memory, |e| canonical::diagnostic(e, block))?;
    if terminal.diagnostics.subject_count != block.subjects.len() as u64
        || terminal.diagnostics.conflict_count != block.conflicts.len() as u64
        || terminal.diagnostics.canonical_bytes != diagnostic_body.as_slice().len() as u64
        || terminal.diagnostics.commitment
            != canonical::hash(|e| {
                e.string("delivery-diagnostics.v1");
                e.raw(diagnostic_body.as_slice());
                Ok(())
            })?
    {
        return Err(missing());
    }
    let terminal_length = canonical::count(|e| canonical::terminal(e, terminal))?;
    let core_length = terminal_length
        .checked_add(16)
        .and_then(|n| n.checked_add(terminal.signature.len()))
        .ok_or_else(missing)?;
    let mut signed_core = memory.allocate(core_length)?;
    canonical::emit(signed_core.as_mut_slice(), |e| {
        e.u64(terminal_length as u64);
        canonical::terminal(e, terminal)?;
        e.bytes(&terminal.signature);
        Ok(())
    })?;
    verify(
        &ns.hub_node,
        &signed_core.as_slice()[8..8 + terminal_length],
        &terminal.signature,
    )?;
    let certificate_bytes = canonical_bytes(memory, |e| {
        canonical::certificate(e, ns, state, birth, &retirement_digest)
    })?;
    let signature = identity.sign_lineage_fixed(certificate_bytes.as_slice());
    let json = super::wire_views::prepare_json(memory, terminal, block)?;
    let wire_context = super::wire_views::Context::new(terminal, block, json.as_slice());
    let packet = PacketView {
        checkpoint,
        record: super::wire_views::terminal(&wire_context),
        block: super::wire_views::block(&wire_context),
        certificate: CertificateView {
            format: 1,
            tenant: &ns.tenant,
            hub_node: &ns.hub_node,
            state,
            birth,
            retirement_digest,
            retired_births: retired,
            signature: &signature,
        },
    };
    // Count then fallibly admit the complete original named-codec packet. The
    // fixed certificate signature is copied as a sequence into this exact buffer.
    let packet_length = super::named_size(&packet)?;
    wire_context.finish_json_pass()?;
    let mut lookup_source = memory.allocate(packet_length)?;
    wire_context.reset_json_pass();
    let mut destination = lookup_source.as_mut_slice();
    packet
        .serialize(&mut rmp_serde::Serializer::new(&mut destination).with_struct_map())
        .map_err(DecodeError::Encode)?;
    wire_context.finish_json_pass()?;
    if !destination.is_empty() {
        return Err(DecodeError::Malformed("packet size changed"));
    }
    drop(certificate_bytes);
    drop(json);
    let mut lookup_submission = memory.allocate(17)?;
    lookup_submission.as_mut_slice()[0] = 0;
    lookup_submission.as_mut_slice()[1..].copy_from_slice(terminal.submission.as_bytes());
    memory.checkpoint()?;
    // All output allocations own inline invoices; drop order frees physical
    // storage before its credit, even if a later admission/checkpoint refuses.
    Ok(NativeOutcome {
        lookup_submission,
        lookup_seal_digest: terminal.seal_digest,
        lookup_origin_life_digest: terminal.origin_life_digest,
        lookup_source,
        signed_core,
        diagnostic_body,
    })
}
