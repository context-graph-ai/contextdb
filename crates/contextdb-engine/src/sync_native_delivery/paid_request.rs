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
    let batch = encode_custody_outcomes(
        db,
        identity,
        &namespace,
        request.edge_incarnation,
        None,
        operation,
        context,
    )?;
    #[cfg(test)]
    AFTER_SELECTION.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
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
    )?;
    batch.checkpoint()?;
    memory.checkpoint()?;
    Ok(output)
}
