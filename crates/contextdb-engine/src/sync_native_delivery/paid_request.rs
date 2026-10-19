//! Private paid request/cursor adapter for the existing delivery wire format.
//! Every input buffer is checked against the original operation before parsing.
use super::{
    DeliveryMemory, decode_envelope_raw, decode_request, encode_response, validate_cursor_after,
};
use super::{NativePacketNamespace, encode_custody_outcomes};
use crate::custody::native_decode::Invalid;
use crate::custody::native_selection::{Failure, NativeBuffer, Selected};
use crate::executor::bounded::NativeReadOpenContext;
use crate::owner_read::NativeReadRequest;
use crate::protocol::MessageType;
use contextdb_core::{Incarnation, Lsn, TenantId};

// One-shot test action run on this thread right after custody selection.
#[cfg(test)]
thread_local! {
    pub(crate) static AFTER_SELECTION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

// The same one-shot action for a selection running on a transport's own
// worker thread.
#[cfg(test)]
#[allow(
    clippy::type_complexity,
    reason = "a one-shot test hook; the closure type is its whole contract"
)]
pub(crate) static AFTER_SELECTION_ANY_THREAD: std::sync::Mutex<
    Option<Box<dyn FnOnce(&NativeReadRequest) + Send>>,
> = std::sync::Mutex::new(None);

struct PaidMemory<'a> {
    operation: &'a NativeReadRequest,
    context: &'a NativeReadOpenContext,
}
impl PaidMemory<'_> {
    fn checkpoint(&self) -> Selected<()> {
        if !self
            .operation
            .owner()
            .same_owner(&self.context.owner().map_err(Failure::Unqualified)?)
        {
            return Err(Failure::OwnerMismatch);
        }
        self.context.checkpoint().map_err(Failure::Unqualified)
    }
    fn allocate(&self, len: usize) -> Selected<NativeBuffer> {
        self.checkpoint()?;
        NativeBuffer::allocate(&self.operation.owner(), len)
    }
    fn input(&self, bytes: &NativeBuffer) -> Selected<()> {
        self.checkpoint()?;
        if !bytes.is_funded_by(&self.operation.owner()) {
            return Err(Failure::OwnerMismatch);
        }
        Ok(())
    }
}
impl DeliveryMemory for PaidMemory<'_> {
    type Buffer = NativeBuffer;
    type Error = Failure;
    fn checkpoint(&self) -> Selected<()> {
        self.checkpoint()
    }
    fn allocate(&self, length: usize) -> Selected<NativeBuffer> {
        self.allocate(length)
    }
    fn owns(&self, buffer: &NativeBuffer) -> bool {
        buffer.is_funded_by(&self.operation.owner())
    }
    fn malformed(_: &'static str) -> Failure {
        Invalid::Codec.into()
    }
    fn canonical(_: &'static str) -> Failure {
        Invalid::Canonical.into()
    }
    fn owner_mismatch() -> Failure {
        Failure::OwnerMismatch
    }
    fn encode(_: rmp_serde::encode::Error) -> Failure {
        Invalid::Codec.into()
    }
}

/// Validate the original unsigned cursor tuple against the first incarnation read.
/// Cursor tokens are namespace/position metadata, not signed outcome packets.
#[allow(clippy::too_many_arguments)] // Explicit original namespace and custody capabilities.
pub(super) fn cursor_after(
    token: &NativeBuffer,
    tenant: &TenantId,
    hub: &str,
    hub_incarnation: Incarnation,
    edge: &str,
    edge_incarnation: Incarnation,
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
) -> Selected<Option<(Lsn, u32)>> {
    let memory = PaidMemory { operation, context };
    memory.input(token)?;
    let after = validate_cursor_after(
        &memory,
        token.bytes(),
        tenant.as_str(),
        hub,
        hub_incarnation,
        edge,
        edge_incarnation,
    )?;
    memory.checkpoint()?;
    Ok(Some(after))
}

/// The byte bound of one native reply page and the typed limit it refuses
/// with: the reader's declared page bytes, or the transport's frame ceiling
/// when that binds tighter.
pub(crate) fn page_bound(
    page_bytes: u64,
) -> (u64, contextdb_core::read_contract::ReadFailureLimit, u64) {
    use contextdb_core::read_contract::ReadFailureLimit;
    let frame = crate::transport::MAX_FRAME_BYTES as u64;
    if page_bytes <= frame {
        (page_bytes, ReadFailureLimit::CursorPageBytes, page_bytes)
    } else {
        (frame, ReadFailureLimit::ResultBytes, frame)
    }
}

/// Concrete synchronous wire-terminal consumer; transport/future activation is separate.
/// Server tenant/hub and authenticated edge are already original-operation buffers.
/// This consumes no Legacy request, record, JSON tree, provider or budget child.
#[allow(clippy::too_many_arguments)] // Private paid buffers and original capabilities stay explicit.
pub(crate) fn paid_delivery_response(
    db: &crate::Database,
    identity: &crate::identity::FabricIdentity,
    input: NativeBuffer,
    tenant: NativeBuffer,
    hub: NativeBuffer,
    edge: NativeBuffer,
    page: usize,
    page_bytes: u64,
    operation: &NativeReadRequest,
    context: &NativeReadOpenContext,
) -> Selected<NativeBuffer> {
    let memory = PaidMemory { operation, context };
    memory.input(&input)?;
    memory.input(&tenant)?;
    memory.input(&hub)?;
    memory.input(&edge)?;
    let envelope = decode_envelope_raw(&memory, input.bytes())?;
    if !crate::protocol::supports_protocol_version(envelope.version)
        || envelope.message_type != MessageType::FetchDeliveryOutcomesRequest
    {
        return Err(Invalid::Codec.into());
    }
    let request = decode_request(&memory, envelope.payload.bytes())?;
    if request.format != 1 || request.tenant_id != tenant.text()? {
        return Err(Invalid::Canonical.into());
    }
    let namespace = NativePacketNamespace::from_buffers(tenant, hub, edge, operation, context)?
        .with_cursor(request.since, operation, context)?;
    // One page is bounded by the reader's declared page bytes and by the
    // transport's frame ceiling, whichever binds; each refuses with its own
    // typed limit.
    let (bound, limit, value) = page_bound(page_bytes);
    let (tenant_text, hub_text, edge_text) = namespace.text()?;
    let base = |incarnation: Incarnation| {
        super::page_frame_base(
            &memory,
            request.request_nonce,
            hub_text,
            incarnation,
            tenant_text,
            edge_text,
            request.edge_incarnation,
        )
    };
    let size = |outcome: &super::borrowed_packet::NativeBorrowedOutcome| {
        super::outcome_named_size(&memory, outcome)
    };
    let count = |lengths: &super::borrowed_packet::OutcomeLengths| {
        super::outcome_named_size_of(&memory, lengths)
    };
    let cut = super::borrowed_packet::PageCut {
        bound: usize::try_from(bound).unwrap_or(usize::MAX),
        limit,
        value,
        base: &base,
        size: &size,
        count: &count,
    };
    let batch = encode_custody_outcomes(
        db,
        identity,
        &namespace,
        request.edge_incarnation,
        None,
        page,
        &cut,
        operation,
        context,
    )?;
    #[cfg(test)]
    {
        AFTER_SELECTION.with(|hook| {
            if let Some(hook) = hook.borrow_mut().take() {
                hook();
            }
        });
        let shared = AFTER_SELECTION_ANY_THREAD
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(hook) = shared {
            hook(operation);
        }
    }
    batch.checkpoint()?;
    let (tenant, hub, edge) = namespace.text()?;
    let output = encode_response(
        &memory,
        request.request_nonce,
        hub,
        batch.response_incarnation,
        tenant,
        edge,
        request.edge_incarnation,
        batch.outcomes.as_slice(),
        batch.more,
    )?;
    batch.checkpoint()?;
    memory.checkpoint()?;
    Ok(output)
}
