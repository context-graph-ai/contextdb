//! Signed packets from original-request committed custody windows.
//! Typed fields cannot outlive the checked erasure guards retained in Views.
use crate::custody::{
    native_decode::{Decoded, Invalid, Value},
    native_record_codec::{self as codec, BirthDirectory},
    native_records::*,
    native_selection::{Failure, NativeBuffer, NativeSelectionHold, Selected},
    store::{NativeCustodySelection, NativeCustodyView},
};
use crate::database::{Database, native_custody_selection::NativeOutcomeSelection};
use crate::executor::bounded::NativeReadOpenContext;
use crate::identity::FabricIdentity;
use crate::owner_read::NativeReadRequest;
use contextdb_core::{
    Incarnation, Lsn, TenantId,
    read_memory::{ReadCredit, ReadMemoryOwner},
};
use serde::{Serialize, ser::SerializeSeq};

/// The exact owned input payloads are already paid before construction.
/// Tenant conversion moves the original UTF-8 buffer, never copies a String.
pub(crate) struct NativePacketNamespace {
    tenant: TenantId,
    tenant_invoice: ReadCredit,
    hub: NativeBuffer,
    edge: NativeBuffer,
    cursor: Option<NativeBuffer>,
}
impl NativePacketNamespace {
    pub(crate) fn from_buffers(
        tenant: NativeBuffer,
        hub: NativeBuffer,
        edge: NativeBuffer,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<Self> {
        same_request(operation, context)?;
        let owner = operation.owner();
        if !tenant.is_funded_by(&owner) || !hub.is_funded_by(&owner) || !edge.is_funded_by(&owner) {
            return Err(Failure::OwnerMismatch);
        }
        codec::node_bytes(hub.text()?)?;
        codec::node_bytes(edge.text()?)?;
        let (text, tenant_invoice) = tenant.into_text_parts()?;
        Ok(Self {
            tenant: TenantId::from(text),
            tenant_invoice,
            hub,
            edge,
            cursor: None,
        })
    }
    pub(super) fn with_cursor(mut self, cursor: Option<NativeBuffer>, operation: &NativeReadRequest,
        context: &NativeReadOpenContext) -> Selected<Self> {
        same_request(operation, context)?;
        if cursor.as_ref().is_some_and(|token| !token.is_funded_by(&operation.owner())) {
            return Err(Failure::OwnerMismatch);
        }
        self.cursor = cursor;
        Ok(self)
    }
    pub(super) fn text(&self) -> Selected<(&str, &str, &str)> {
        Ok((self.tenant.as_str(), self.hub.text()?, self.edge.text()?))
    }
    fn is_funded_by(&self, owner: &ReadMemoryOwner) -> bool {
        self.tenant_invoice.is_funded_by(owner)
            && self.hub.is_funded_by(owner)
            && self.edge.is_funded_by(owner)
            && self.cursor.as_ref().is_none_or(|token| token.is_funded_by(owner))
    }
}
pub(crate) type NativeBorrowedOutcome = super::NativeOutcome<NativeBuffer>;
pub(crate) struct NativeBorrowedOutcomeBatch {
    // The physical packets/slot array drop before the original capability.
    pub(crate) outcomes: NativeSelectionHold<NativeBorrowedOutcome>,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) response_incarnation: Incarnation,
    operation: NativeReadRequest,
    context: NativeReadOpenContext,
}
impl NativeBorrowedOutcomeBatch {
    pub(crate) fn checkpoint(&self) -> Selected<()> {
        same_request(&self.operation, &self.context)
    }
}
fn same_request(operation: &NativeReadRequest, context: &NativeReadOpenContext) -> Selected<()> {
    if !operation
        .owner()
        .same_owner(&context.owner().map_err(Failure::Unqualified)?)
    {
        return Err(Failure::OwnerMismatch);
    }
    context.checkpoint().map_err(Failure::Unqualified)
}
struct Views<'window> {
    guards: NativeSelectionHold<NativeCustodyView<'window>>,
}
impl<'window> Views<'window> {
    fn pin(selection: &'window NativeCustodySelection, owner: &ReadMemoryOwner) -> Selected<Self> {
        if !selection.is_funded_by(owner) {
            return Err(Failure::OwnerMismatch);
        }
        let mut guards = NativeSelectionHold::try_new(owner, selection.windows().len())?;
        for window in selection.windows() {
            guards.retain(window.view()?)?;
        }
        Ok(Self { guards })
    }
    fn birth(&self, id: &BirthRef<'_>) -> Decoded<Birth<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::Birth(birth) = view.record()? {
                if codec::birth_eq(&birth.identity, id) {
                    return Ok(birth);
                }
            }
        }
        Err(Invalid::Companions)
    }
    fn state(&self, id: &StateRef<'_>) -> Decoded<StateRecord<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::State(state) = view.record()? {
                if codec::state_eq(&state.state, id) {
                    return Ok(state);
                }
            }
        }
        Err(Invalid::Companions)
    }
    fn state_birth(&self, state: &StateRef<'_>) -> Decoded<Birth<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::Birth(birth) = view.record()? {
                if codec::state_birth_eq(state, &birth.identity) {
                    return Ok(birth);
                }
            }
        }
        Err(Invalid::Companions)
    }
    fn diagnostic(&self, terminal: &TerminalRecord<'_>) -> Decoded<DiagnosticBlock<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::Diagnostic {
                namespace,
                submission,
                block,
            } = view.record()?
            {
                if codec::namespace_eq(&namespace, &terminal.namespace)
                    && submission == terminal.submission
                {
                    return Ok(block);
                }
            }
        }
        Err(Invalid::Companions)
    }
}
#[derive(Clone, Copy)]
struct Retired<'a, 'window> {
    records: &'a Views<'window>,
    active: Birth<'a>,
}
impl<'a, 'window: 'a> BirthDirectory<'a> for Retired<'a, 'window> {
    fn len(self) -> usize {
        self.active.retired.len()
    }
    fn get(self, index: usize) -> Decoded<Birth<'a>> {
        self.records.birth(&self.active.retired.get(index)?)
    }
}
impl Serialize for Retired<'_, '_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for index in 0..self.len() {
            let birth = self
                .get(index)
                .map_err(|_| serde::ser::Error::custom("invalid checked retirement directory"))?;
            seq.serialize_element(&birth)?;
        }
        seq.end()
    }
}
#[derive(Serialize)]
struct CertificateView<'a, 'window> {
    format: u32,
    tenant: &'a str,
    hub_node: &'a str,
    state: StateRecord<'a>,
    birth: Birth<'a>,
    retirement_digest: Digest,
    retired_births: Retired<'a, 'window>,
    signature: &'a [u8],
}
#[derive(Serialize)]
struct PacketView<'a, 'window> {
    checkpoint: Option<Checkpoint<'a>>,
    record: TerminalRecord<'a>,
    block: DiagnosticBlock<'a>,
    certificate: CertificateView<'a, 'window>,
}

// Native Value::Json borrows the actual paid canonical source string. Unlike
// the owned Value serializer it never builds another String or JSON tree.
fn validate_key_json(key: NaturalKey<'_>) -> Decoded<()> {
    fn value(value: Value<'_>) -> Decoded<()> {
        if let Value::Json(text) = value {
            crate::custody::native_json::validate(text)?;
        }
        Ok(())
    }
    value(key.value)?;
    for pair in key.rest.iter()? {
        value(pair?.1)?;
    }
    Ok(())
}
fn validate_json(terminal: TerminalRecord<'_>, block: DiagnosticBlock<'_>) -> Decoded<()> {
    validate_key_json(terminal.root.key)?;
    for subject in block.subjects.iter()? {
        validate_key_json(subject?.0.key)?;
    }
    for conflict in block.conflicts.iter()? {
        let conflict = conflict?;
        validate_key_json(conflict.natural_key)?;
        if let Some(cause) = conflict.refusal_cause {
            validate_key_json(cause.natural_key)?;
        }
    }
    Ok(())
}
struct NamedCounter(usize);
impl std::io::Write for NamedCounter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or(std::io::ErrorKind::InvalidData)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn named_bytes(owner: &ReadMemoryOwner, packet: &PacketView<'_, '_>) -> Selected<NativeBuffer> {
    let mut count = NamedCounter(0);
    packet
        .serialize(&mut rmp_serde::Serializer::new(&mut count).with_struct_map())
        .map_err(|_| Failure::Invalid(Invalid::Codec))?;
    let mut bytes = NativeBuffer::allocate(owner, count.0)?;
    let mut destination = bytes.bytes_mut();
    packet
        .serialize(&mut rmp_serde::Serializer::new(&mut destination).with_struct_map())
        .map_err(|_| Failure::Invalid(Invalid::Codec))?;
    if !destination.is_empty() {
        return Err(Invalid::Canonical.into());
    }
    Ok(bytes)
}
fn encode_packet(
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
    identity: &FabricIdentity,
    terminal: TerminalRecord<'_>,
    submission: &Views<'_>,
    authority: &Views<'_>,
    checkpoint: Option<Checkpoint<'_>>,
) -> Selected<NativeBorrowedOutcome> {
    same_request(operation, context)?;
    let owner = operation.owner();
    let ns = terminal.namespace;
    if terminal.kind > 2
        || ns.hub_incarnation != terminal.state.incarnation
        || codec::node_bytes(ns.hub_node)? != identity.public_key_bytes()
    {
        return Err(Invalid::Companions.into());
    }
    codec::node_bytes(ns.edge_node)?;
    let block = submission.diagnostic(&terminal)?;
    let state = authority.state(&terminal.state)?;
    let birth = authority.state_birth(&state.state)?;
    let retired = Retired {
        records: authority,
        active: birth,
    };
    codec::retirement_closure_with_checkpoint(&owner, &birth, retired, || {
        same_request(operation, context)
    })?;
    let retirement_digest = codec::retirement_root(retired)?;
    if retirement_digest != state.retirement_root {
        return Err(Invalid::Companions.into());
    }
    validate_json(terminal, block)?;
    let diagnostic_body = codec::finite_bytes(&owner, |e| block.bytes_finite(e))?;
    if terminal.diagnostics.subject_count != block.subjects.len() as u64
        || terminal.diagnostics.conflict_count != block.conflicts.len() as u64
        || terminal.diagnostics.canonical_bytes != diagnostic_body.bytes().len() as u64
        || terminal.diagnostics.commitment
            != codec::finite_hash(|e| {
                e.string("delivery-diagnostics.v1");
                e.raw(diagnostic_body.bytes());
                Ok(())
            })?
    {
        return Err(Invalid::Companions.into());
    }
    let terminal_length = codec::finite_len(|e| terminal.bytes_finite(e))?;
    let signed_core = codec::finite_bytes(&owner, |e| {
        e.u64(terminal_length as u64);
        terminal.bytes_finite(e)?;
        e.bytes(&terminal.signature);
        Ok(())
    })?;
    // The scalar native verifier copies the fallible signature into stack bytes.
    codec::verify_signature(&owner, ns.hub_node, terminal.signature, |e| {
        terminal.bytes_finite(e)
    })?;
    let certificate_bytes = codec::finite_bytes(&owner, |e| {
        codec::certificate_fields_finite(
            e,
            1,
            ns.tenant,
            ns.hub_node,
            &state,
            &birth,
            &retirement_digest,
        )
    })?;
    let signature = identity.sign_lineage_fixed(certificate_bytes.bytes());
    let packet = PacketView {
        // Selection validated the actual source. Legacy current copies this
        // checkpoint without extra namespace/root/terminal-position checks.
        checkpoint,
        record: terminal,
        block,
        certificate: CertificateView {
            format: 1,
            tenant: ns.tenant,
            hub_node: ns.hub_node,
            state,
            birth,
            retirement_digest,
            retired_births: retired,
            signature: &signature,
        },
    };
    let lookup_source = named_bytes(&owner, &packet)?;
    drop(certificate_bytes);
    let mut lookup_submission = NativeBuffer::allocate(&owner, 17)?;
    lookup_submission.bytes_mut()[0] = 0;
    lookup_submission.bytes_mut()[1..].copy_from_slice(terminal.submission.as_bytes());
    same_request(operation, context)?;
    Ok(NativeBorrowedOutcome {
        lookup_submission,
        lookup_seal_digest: terminal.seal_digest,
        lookup_origin_life_digest: terminal.origin_life_digest,
        lookup_source,
        signed_core,
        diagnostic_body,
    })
}

fn coherent(selection: &NativeOutcomeSelection) -> Selected<()> {
    let mut descriptor = None;
    let mut check = |records: &NativeCustodySelection| -> Selected<()> {
        for window in records.windows() {
            let actual = window.committed_view();
            if descriptor
                .as_ref()
                .is_some_and(|expected| *expected != actual)
            {
                return Err(Invalid::Companions.into());
            }
            descriptor = Some(actual);
        }
        Ok(())
    };
    check(&selection.terminals)?;
    for submission in selection.submissions.as_slice() {
        check(submission)?;
    }
    check(&selection.authority)?;
    if let Some(checkpoint) = &selection.checkpoint {
        check(checkpoint)?;
    }
    Ok(())
}
fn encode_selection(
    selection: &NativeOutcomeSelection,
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
    identity: &FabricIdentity,
) -> Selected<NativeBorrowedOutcomeBatch> {
    same_request(operation, context)?;
    let owner = operation.owner();
    if !selection.positions.is_funded_by(&owner) || !selection.submissions.is_funded_by(&owner) {
        return Err(Failure::OwnerMismatch);
    }
    coherent(selection)?;
    if selection.positions.as_slice().len() != selection.submissions.as_slice().len() {
        return Err(Invalid::Companions.into());
    }
    let mut outcomes = NativeSelectionHold::try_new(&owner, selection.positions.as_slice().len())?;
    let authority = Views::pin(&selection.authority, &owner)?;
    let checkpoint_views = selection
        .checkpoint
        .as_ref()
        .map(|s| Views::pin(s, &owner))
        .transpose()?;
    let checkpoint = match &checkpoint_views {
        None => None,
        Some(views) => {
            if views.guards.as_slice().len() != 1 {
                return Err(Invalid::Companions.into());
            }
            match views.guards.as_slice()[0].record()? {
                RecordView::CheckpointFrontier(frontier) => Some(frontier.checkpoint),
                _ => return Err(Invalid::Companions.into()),
            }
        }
    };
    if !selection.terminals.is_funded_by(&owner) {
        return Err(Failure::OwnerMismatch);
    }
    for ((index, position), submission) in selection
        .positions
        .as_slice()
        .iter()
        .zip(selection.submissions.as_slice())
    {
        same_request(operation, context)?;
        let window = selection
            .terminals
            .windows()
            .get(*index)
            .ok_or(Invalid::Companions)?;
        let view = window.view()?;
        let terminal = match view.record()? {
            RecordView::Terminal {
                edge: false,
                record,
            } if record.position == *position
                && record.namespace.hub_incarnation == selection.hub_incarnation =>
            {
                record
            }
            _ => return Err(Invalid::Companions.into()),
        };
        let submission = Views::pin(submission, &owner)?;
        outcomes.retain(encode_packet(
            operation,
            context,
            identity,
            terminal,
            &submission,
            &authority,
            checkpoint,
        )?)?;
    }
    same_request(operation, context)?;
    Ok(NativeBorrowedOutcomeBatch {
        outcomes,
        hub_incarnation: selection.hub_incarnation,
        response_incarnation: selection.response_incarnation,
        operation: operation.share(),
        context: context.clone(),
    })
}

/// Private real caller of the native selection and fixed same-key signer.
/// The caller owns admitted request text; no child owner or legacy fetch is made.
pub(crate) fn encode_custody_outcomes(
    db: &Database,
    identity: &FabricIdentity,
    namespace: &NativePacketNamespace,
    edge_incarnation: Incarnation,
    after: Option<(Lsn, u32)>,
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
) -> Selected<NativeBorrowedOutcomeBatch> {
    same_request(operation, context)?;
    if !namespace.is_funded_by(&operation.owner()) {
        return Err(Failure::OwnerMismatch);
    }
    // All incarnation, root/cursor and other cold calls finish without views.
    let selection = match &namespace.cursor {
        None => db.custody_outcomes_native(
            &namespace.tenant, namespace.hub.text()?, namespace.edge.text()?,
            edge_incarnation, after, operation, context,
        )?,
        Some(token) => {
            if after.is_some() { return Err(Invalid::Canonical.into()); }
            db.custody_outcomes_native_with_after(
                &namespace.tenant, namespace.hub.text()?, namespace.edge.text()?,
                edge_incarnation, operation, context,
                |incarnation| super::paid_request::cursor_after(token, &namespace.tenant,
                    namespace.hub.text()?, incarnation, namespace.edge.text()?,
                    edge_incarnation, operation, context),
            )?
        }
    };
    encode_selection(&selection, operation, context, identity)
}
