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
    pub(super) fn with_cursor(
        mut self,
        cursor: Option<NativeBuffer>,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<Self> {
        same_request(operation, context)?;
        if cursor
            .as_ref()
            .is_some_and(|token| !token.is_funded_by(&operation.owner()))
        {
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
            && self
                .cursor
                .as_ref()
                .is_none_or(|token| token.is_funded_by(owner))
    }
}
pub(crate) type NativeBorrowedOutcome = super::NativeOutcome<NativeBuffer>;
pub(crate) struct NativeBorrowedOutcomeBatch {
    // The physical packets/slot array drop before the original capability.
    pub(crate) outcomes: NativeSelectionHold<NativeBorrowedOutcome>,
    pub(crate) response_incarnation: Incarnation,
    /// More outcomes follow this page's last outcome.
    pub(crate) more: bool,
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
    fn birth(&self, id: &BirthRef) -> Decoded<Birth<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::Birth(birth) = view.record()?
                && codec::birth_eq(&birth.identity, id)
            {
                return Ok(birth);
            }
        }
        Err(Invalid::Companions)
    }
    fn state(&self, id: &StateRef) -> Decoded<StateRecord> {
        for view in self.guards.as_slice() {
            if let RecordView::State(state) = view.record()?
                && codec::state_eq(&state.state, id)
            {
                return Ok(state);
            }
        }
        Err(Invalid::Companions)
    }
    fn state_birth(&self, state: &StateRef) -> Decoded<Birth<'_>> {
        for view in self.guards.as_slice() {
            if let RecordView::Birth(birth) = view.record()?
                && codec::state_birth_eq(state, &birth.identity)
            {
                return Ok(birth);
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
                && codec::namespace_eq(&namespace, &terminal.namespace)
                && submission == terminal.submission
            {
                return Ok(block);
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
    state: StateRecord,
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
/// Each outcome size the page cut saw, in order: `(built, bytes)`, counted
/// (`false`) by the counting pass or built (`true`) by the building pass.
#[cfg(test)]
pub(crate) static PAGE_CUT_SIZES: std::sync::Mutex<Vec<(bool, usize)>> =
    std::sync::Mutex::new(Vec::new());

/// The byte bound of one reply page and how a page that cannot fit refuses.
/// `base` gives the reply frame's fixed parts for the response incarnation;
/// `size` counts one outcome's exact named encoding without allocating.
pub(crate) struct PageCut<'a> {
    pub(crate) bound: usize,
    pub(crate) limit: contextdb_core::read_contract::ReadFailureLimit,
    pub(crate) value: u64,
    pub(crate) base: &'a dyn Fn(Incarnation) -> Selected<super::PageFrameBase>,
    pub(crate) size: &'a dyn Fn(&NativeBorrowedOutcome) -> Selected<usize>,
    /// The same exact size, counted from an outcome's field lengths before any
    /// of its buffers exists.
    pub(crate) count: &'a dyn Fn(&OutcomeLengths) -> Selected<usize>,
}

/// One outcome's encoded field lengths and digests, counted from borrowed
/// views: enough to size its named encoding without building it.
pub(crate) struct OutcomeLengths {
    pub(crate) seal_digest: [u8; 32],
    pub(crate) origin_life_digest: [u8; 32],
    pub(crate) submission: usize,
    pub(crate) source: usize,
    pub(crate) core: usize,
    pub(crate) diagnostic: usize,
}

/// Count what `encode_packet` would build for one outcome, from borrowed
/// views. The diagnostic body, signed core and lookup source are counted and
/// never built; the certificate is the one buffer built here, because the
/// signature's encoding depends on its bytes, and it is freed before return.
fn measure_packet(
    owner: &ReadMemoryOwner,
    identity: &FabricIdentity,
    terminal: TerminalRecord<'_>,
    submission: &Views<'_>,
    authority: &Views<'_>,
    checkpoint: Option<Checkpoint<'_>>,
) -> Selected<OutcomeLengths> {
    let ns = terminal.namespace;
    let block = submission.diagnostic(&terminal)?;
    let state = authority.state(&terminal.state)?;
    let birth = authority.state_birth(&state.state)?;
    let retired = Retired {
        records: authority,
        active: birth,
    };
    let retirement_digest = codec::retirement_root(retired)?;
    let diagnostic = codec::finite_len(|e| block.bytes_finite(e))?;
    let terminal_length = codec::finite_len(|e| terminal.bytes_finite(e))?;
    let core = codec::finite_len(|e| {
        e.u64(terminal_length as u64);
        terminal.bytes_finite(e)?;
        e.bytes(&terminal.signature);
        Ok(())
    })?;
    let certificate_bytes = codec::finite_bytes(owner, |e| {
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
    drop(certificate_bytes);
    let packet = PacketView {
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
    let mut source = NamedCounter(0);
    packet
        .serialize(&mut rmp_serde::Serializer::new(&mut source).with_struct_map())
        .map_err(|_| Failure::Invalid(Invalid::Codec))?;
    Ok(OutcomeLengths {
        seal_digest: terminal.seal_digest,
        origin_life_digest: terminal.origin_life_digest,
        submission: 17,
        source: source.0,
        core,
        diagnostic,
    })
}

impl PageCut<'_> {
    /// The typed refusal for a page that cannot hold even its first outcome,
    /// or whose empty reply already exceeds the bound. It owns no heap value.
    fn refusal(&self) -> Failure {
        use contextdb_core::read_contract::{OwnerLimitExceededDetail, ReadFailure};
        Failure::Unqualified(contextdb_core::Error::ReadFailure(
            ReadFailure::owner_limit_exceeded(OwnerLimitExceededDetail {
                limit: self.limit,
                value: self.value,
                required: None,
                statement: None,
            }),
        ))
    }
}

fn encode_selection(
    selection: &NativeOutcomeSelection,
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
    identity: &FabricIdentity,
    cut: &PageCut<'_>,
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
    // The page is the longest prefix, in delivery order, whose reply frame
    // stays within the bound. A counting pass sizes each candidate from its
    // borrowed views, building no packet, and stops at the first that would
    // cross the bound; only then are exactly that many slots admitted and
    // exactly that prefix built. An excluded candidate is never materialized,
    // and a first candidate over the bound refuses before any packet exists.
    let base = (cut.base)(selection.response_incarnation)?;
    if base.upper(0, 0).is_none_or(|upper| upper > cut.bound) {
        return Err(cut.refusal());
    }
    let candidates = selection
        .positions
        .as_slice()
        .iter()
        .zip(selection.submissions.as_slice());
    let mut outcome_bytes = 0usize;
    let mut included = 0usize;
    for ((index, position), submission) in candidates.clone() {
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
        let lengths = measure_packet(
            &owner,
            identity,
            terminal,
            &submission,
            &authority,
            checkpoint,
        )?;
        drop(submission);
        let counted = (cut.count)(&lengths)?;
        #[cfg(test)]
        PAGE_CUT_SIZES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((false, counted));
        let total = outcome_bytes.checked_add(counted).ok_or(Invalid::Codec)?;
        if base
            .upper(total, included + 1)
            .is_none_or(|upper| upper > cut.bound)
        {
            if included == 0 {
                return Err(cut.refusal());
            }
            break;
        }
        outcome_bytes = total;
        included += 1;
    }
    let stopped = included < selection.positions.as_slice().len();
    // Cancellation is observed between the counting and the building pass.
    same_request(operation, context)?;
    let mut outcomes = NativeSelectionHold::try_new(&owner, included)?;
    let mut built_bytes = 0usize;
    for ((index, position), submission) in candidates.take(included) {
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
        let packet = encode_packet(
            operation,
            context,
            identity,
            terminal,
            &submission,
            &authority,
            checkpoint,
        )?;
        let built = (cut.size)(&packet)?;
        #[cfg(test)]
        PAGE_CUT_SIZES
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push((true, built));
        built_bytes = built_bytes.checked_add(built).ok_or(Invalid::Codec)?;
        outcomes.retain(packet)?;
    }
    // The built page is exactly the counted one.
    if built_bytes != outcome_bytes {
        return Err(Invalid::Canonical.into());
    }
    same_request(operation, context)?;
    Ok(NativeBorrowedOutcomeBatch {
        outcomes,
        response_incarnation: selection.response_incarnation,
        more: selection.more || stopped,
        operation: operation.share(),
        context: context.clone(),
    })
}

/// Private real caller of the native selection and fixed same-key signer.
/// The caller owns admitted request text; no child owner or legacy fetch is made.
#[allow(clippy::too_many_arguments)] // Original capabilities and the page bound stay explicit.
pub(crate) fn encode_custody_outcomes(
    db: &Database,
    identity: &FabricIdentity,
    namespace: &NativePacketNamespace,
    edge_incarnation: Incarnation,
    after: Option<(Lsn, u32)>,
    page: usize,
    cut: &PageCut<'_>,
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
            &namespace.tenant,
            namespace.hub.text()?,
            namespace.edge.text()?,
            edge_incarnation,
            after,
            page,
            operation,
            context,
        )?,
        Some(token) => {
            if after.is_some() {
                return Err(Invalid::Canonical.into());
            }
            db.custody_outcomes_native_with_after(
                &namespace.tenant,
                namespace.hub.text()?,
                namespace.edge.text()?,
                edge_incarnation,
                page,
                operation,
                context,
                |incarnation| {
                    super::paid_request::cursor_after(
                        token,
                        &namespace.tenant,
                        namespace.hub.text()?,
                        incarnation,
                        namespace.edge.text()?,
                        edge_incarnation,
                        operation,
                        context,
                    )
                },
            )?
        }
    };
    encode_selection(&selection, operation, context, identity, cut)
}
