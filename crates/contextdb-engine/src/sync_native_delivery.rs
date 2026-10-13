//! Private delivery wire producers. This module does not activate a transport.
//! Owned byte fields have exact prepaid fallible storage; text borrows the
//! admitted input. No general serde visitor may construct a nested container.
use crate::protocol::{MessageType, PROTOCOL_VERSION};
use crate::read_image_memory::{NativeDeliveryBytes, NativeDeliveryImage};
use contextdb_core::{Incarnation, Lsn};
use serde::Serialize;
mod borrowed_packet;
mod canonical;
mod packet;
mod paid_request;
pub(crate) use borrowed_packet::{
    NativeBorrowedOutcomeBatch, NativePacketNamespace, encode_custody_outcomes,
};
#[cfg(test)]
pub(crate) use paid_request::AFTER_SELECTION;
pub(crate) use paid_request::paid_delivery_response;
mod wire_views;
pub(crate) use packet::encode_packet;

#[derive(Debug)]
pub(crate) enum DecodeError {
    Resource(contextdb_core::Error),
    Allocation(crate::read_image_memory::NativeDeliveryAllocationError),
    Malformed(&'static str),
    Version {
        received: u8,
        oldest: u8,
        newest: u8,
    },
    Encode(rmp_serde::encode::Error),
}
impl From<contextdb_core::Error> for DecodeError {
    fn from(error: contextdb_core::Error) -> Self {
        Self::Resource(error)
    }
}

impl From<crate::read_image_memory::NativeDeliveryAllocationError> for DecodeError {
    fn from(error: crate::read_image_memory::NativeDeliveryAllocationError) -> Self {
        Self::Allocation(error)
    }
}

// The wire grammar owns no allocator or request capability. Its two concrete
// memory adapters retain the existing image errors or original-request failures.
pub(crate) trait DeliveryBuffer {
    fn bytes(&self) -> &[u8];
    fn bytes_mut(&mut self) -> &mut [u8];
}
impl DeliveryBuffer for NativeDeliveryBytes {
    fn bytes(&self) -> &[u8] {
        self.as_slice()
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}
impl DeliveryBuffer for crate::custody::native_selection::NativeBuffer {
    fn bytes(&self) -> &[u8] {
        self.bytes()
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        self.bytes_mut()
    }
}
pub(crate) trait DeliveryMemory {
    type Buffer: DeliveryBuffer;
    type Error;
    fn checkpoint(&self) -> Result<(), Self::Error>;
    fn allocate(&self, length: usize) -> Result<Self::Buffer, Self::Error>;
    fn owns(&self, buffer: &Self::Buffer) -> bool;
    fn malformed(message: &'static str) -> Self::Error;
    fn canonical(message: &'static str) -> Self::Error;
    fn owner_mismatch() -> Self::Error;
    fn encode(error: rmp_serde::encode::Error) -> Self::Error;
}
impl DeliveryMemory for NativeDeliveryImage {
    type Buffer = NativeDeliveryBytes;
    type Error = DecodeError;
    fn checkpoint(&self) -> Result<(), DecodeError> {
        self.checkpoint().map_err(Into::into)
    }
    fn allocate(&self, length: usize) -> Result<Self::Buffer, DecodeError> {
        self.allocate(length).map_err(Into::into)
    }
    fn owns(&self, buffer: &Self::Buffer) -> bool {
        self.owns(buffer)
    }
    fn malformed(message: &'static str) -> DecodeError {
        DecodeError::Malformed(message)
    }
    fn canonical(message: &'static str) -> DecodeError {
        DecodeError::Malformed(message)
    }
    fn owner_mismatch() -> DecodeError {
        DecodeError::Malformed("outcome has a different request owner")
    }
    fn encode(error: rmp_serde::encode::Error) -> DecodeError {
        DecodeError::Encode(error)
    }
}

pub(crate) struct NativeEnvelope<B = NativeDeliveryBytes> {
    pub(crate) version: u8,
    pub(crate) message_type: MessageType,
    pub(crate) payload: B,
}
pub(crate) struct NativeOutcomesRequest<'a, B = NativeDeliveryBytes> {
    pub(crate) format: u8,
    pub(crate) tenant_id: &'a str,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) request_nonce: [u8; 16],
    pub(crate) since: Option<B>,
}
pub(crate) struct NativeOutcomeCursor<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) hub_node_id: &'a str,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) edge_node_id: &'a str,
    pub(crate) edge_incarnation: Incarnation,
    pub(crate) position: Lsn,
    pub(crate) ordinal: u32,
}

struct Input<'a, 'm, M: DeliveryMemory> {
    bytes: &'a [u8],
    at: usize,
    memory: &'m M,
}
impl<'a, 'm, M: DeliveryMemory> Input<'a, 'm, M> {
    fn new(bytes: &'a [u8], memory: &'m M) -> Result<Self, M::Error> {
        memory.checkpoint()?;
        Ok(Self {
            bytes,
            at: 0,
            memory,
        })
    }
    fn take(&mut self, length: usize) -> Result<&'a [u8], M::Error> {
        let end = self
            .at
            .checked_add(length)
            .ok_or(M::malformed("MessagePack length overflow"))?;
        let value = self
            .bytes
            .get(self.at..end)
            .ok_or(M::malformed("truncated MessagePack"))?;
        self.at = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8, M::Error> {
        Ok(self.take(1)?[0])
    }
    fn peek(&self) -> Result<u8, M::Error> {
        self.bytes
            .get(self.at)
            .copied()
            .ok_or(M::malformed("truncated MessagePack"))
    }
    fn be(&mut self, length: usize) -> Result<u64, M::Error> {
        let mut value = 0;
        for byte in self.take(length)? {
            value = (value << 8) | u64::from(*byte);
        }
        Ok(value)
    }
    fn uint(&mut self) -> Result<u64, M::Error> {
        let marker = self.byte()?;
        match marker {
            0x00..=0x7f => Ok(u64::from(marker)),
            0xcc => self.be(1),
            0xcd => self.be(2),
            0xce => self.be(4),
            0xcf => self.be(8),
            0xd0..=0xd3 => {
                let length = 1usize << (marker - 0xd0);
                let value = self.be(length)?;
                if value & (1u64 << (length * 8 - 1)) != 0 {
                    return Err(M::malformed("negative unsigned integer"));
                }
                Ok(value)
            }
            _ => Err(M::malformed("expected unsigned integer")),
        }
    }
    fn u8(&mut self) -> Result<u8, M::Error> {
        u8::try_from(self.uint()?).map_err(|_| M::malformed("u8 out of range"))
    }
    fn u32(&mut self) -> Result<u32, M::Error> {
        u32::try_from(self.uint()?).map_err(|_| M::malformed("u32 out of range"))
    }
    fn count(&mut self, map: bool) -> Result<usize, M::Error> {
        let marker = self.byte()?;
        let count = match (map, marker) {
            (false, 0x90..=0x9f) => u64::from(marker & 15),
            (true, 0x80..=0x8f) => u64::from(marker & 15),
            (false, 0xdc) | (true, 0xde) => self.be(2)?,
            (false, 0xdd) | (true, 0xdf) => self.be(4)?,
            _ => return Err(M::malformed("expected MessagePack container")),
        };
        usize::try_from(count).map_err(|_| M::malformed("container length overflow"))
    }
    fn text(&mut self) -> Result<&'a str, M::Error> {
        let marker = self.byte()?;
        let length = match marker {
            0xa0..=0xbf => u64::from(marker & 31),
            0xd9 | 0xc4 => self.be(1)?,
            0xda | 0xc5 => self.be(2)?,
            0xdb | 0xc6 => self.be(4)?,
            _ => return Err(M::malformed("expected MessagePack text")),
        };
        let length = usize::try_from(length).map_err(|_| M::malformed("text length overflow"))?;
        std::str::from_utf8(self.take(length)?).map_err(|_| M::malformed("invalid UTF-8"))
    }
    fn binary(&mut self) -> Result<&'a [u8], M::Error> {
        let marker = self.byte()?;
        let length = match marker {
            0xc4 => self.be(1)?,
            0xc5 => self.be(2)?,
            0xc6 => self.be(4)?,
            _ => return Err(M::malformed("expected MessagePack binary")),
        };
        self.take(usize::try_from(length).map_err(|_| M::malformed("binary length overflow"))?)
    }
    fn incarnation(&mut self) -> Result<Incarnation, M::Error> {
        // rmp-serde's deserialize_tuple permits bin as a u8 sequence too.
        if matches!(self.peek()?, 0xc4..=0xc6) {
            let pair = self.binary()?;
            if pair.len() != 2 {
                return Err(M::malformed("incarnation tuple length"));
            }
            return Ok(Incarnation(
                (u128::from(pair[0]) << 64) | u128::from(pair[1]),
            ));
        }
        if self.count(false)? != 2 {
            return Err(M::malformed("incarnation tuple length"));
        }
        let hi = self.uint()?;
        let lo = self.uint()?;
        Ok(Incarnation((u128::from(hi) << 64) | u128::from(lo)))
    }
    fn nonce(&mut self) -> Result<[u8; 16], M::Error> {
        if matches!(self.peek()?, 0xc4..=0xc6) {
            return self
                .binary()?
                .try_into()
                .map_err(|_| M::malformed("nonce tuple length"));
        }
        if self.count(false)? != 16 {
            return Err(M::malformed("nonce tuple length"));
        }
        let mut value = [0; 16];
        for byte in &mut value {
            *byte = self.u8()?;
        }
        Ok(value)
    }
    fn vector(&mut self) -> Result<M::Buffer, M::Error> {
        // Original emitted Vec<u8> bytes are a sequence. rmp-serde's reader
        // additionally accepts bin as a u8 sequence; preserve both forms.
        if matches!(self.peek()?, 0xc4..=0xc6) {
            let borrowed = self.binary()?;
            let mut value = self.memory.allocate(borrowed.len())?;
            value.bytes_mut().copy_from_slice(borrowed);
            return Ok(value);
        }
        // Validate every element before allocating the exact owned array.
        let length = self.count(false)?;
        let start = self.at;
        for _ in 0..length {
            self.memory.checkpoint()?;
            self.u8()?;
        }
        let end = self.at;
        let mut value = self.memory.allocate(length)?;
        self.at = start;
        for byte in value.bytes_mut() {
            self.memory.checkpoint()?;
            *byte = self.u8()?;
        }
        debug_assert_eq!(self.at, end);
        Ok(value)
    }
    fn optional_vector(&mut self) -> Result<Option<M::Buffer>, M::Error> {
        if self.peek()? == 0xc0 {
            self.byte()?;
            Ok(None)
        } else {
            self.vector().map(Some)
        }
    }
    fn field_key(&mut self, fields: &[&str]) -> Result<Option<usize>, M::Error> {
        match self.peek()? {
            0x00..=0x7f | 0xcc..=0xcf => {
                let index = usize::try_from(self.uint()?)
                    .map_err(|_| M::malformed("field index overflow"))?;
                if index >= fields.len() {
                    return Err(M::malformed("unknown numeric field index"));
                }
                Ok(Some(index))
            }
            _ => {
                let key = self.text()?;
                Ok(fields.iter().position(|field| *field == key))
            }
        }
    }
    fn message_type(&mut self) -> Result<MessageType, M::Error> {
        // Derived unit enums accept their string name or numeric variant index.
        let fields = [
            "PushRequest",
            "DependencyCompletePushRequest",
            "PushResponse",
            "PullRequest",
            "PullResponse",
            "DependencyCompletePullResponse",
            "Chunk",
            "ChunkAck",
            "StatusRequest",
            "StatusResponse",
            "BindApplicationTablePolicyRequest",
            "BindApplicationTablePolicyResponse",
            "FetchDeliveryOutcomesRequest",
            "FetchDeliveryOutcomesResponse",
        ];
        let mapped = matches!(self.peek()?, 0x80..=0x8f | 0xde | 0xdf);
        if mapped && self.count(true)? != 1 {
            return Err(M::malformed("enum map length"));
        }
        let index = self
            .field_key(&fields)?
            .ok_or(M::malformed("unknown message type"))?;
        if mapped && self.byte()? != 0xc0 {
            return Err(M::malformed("expected unit enum nil"));
        }
        Ok(match index {
            0 => MessageType::PushRequest,
            1 => MessageType::DependencyCompletePushRequest,
            2 => MessageType::PushResponse,
            3 => MessageType::PullRequest,
            4 => MessageType::PullResponse,
            5 => MessageType::DependencyCompletePullResponse,
            6 => MessageType::Chunk,
            7 => MessageType::ChunkAck,
            8 => MessageType::StatusRequest,
            9 => MessageType::StatusResponse,
            10 => MessageType::BindApplicationTablePolicyRequest,
            11 => MessageType::BindApplicationTablePolicyResponse,
            12 => MessageType::FetchDeliveryOutcomesRequest,
            13 => MessageType::FetchDeliveryOutcomesResponse,
            _ => unreachable!("bounded variant index"),
        })
    }
    fn skip(&mut self, depth: usize) -> Result<(), M::Error> {
        self.memory.checkpoint()?;
        // rmp-serde 1.3.1 Deserializer's existing default recursion cut. This
        // is the codec's grammar safety limit, never a reader-memory budget.
        if depth == 1024 {
            return Err(M::malformed("MessagePack depth limit exceeded"));
        }
        let marker = self.peek()?;
        match marker {
            0x90..=0x9f | 0xdc | 0xdd => {
                let count = self.count(false)?;
                for _ in 0..count {
                    self.skip(depth + 1)?;
                }
            }
            0x80..=0x8f | 0xde | 0xdf => {
                let count = self.count(true)?;
                for _ in 0..count {
                    self.skip(depth + 1)?;
                    self.skip(depth + 1)?;
                }
            }
            0xa0..=0xbf | 0xd9..=0xdb | 0xc4..=0xc6 => {
                self.byte()?;
                let length = match marker {
                    0xa0..=0xbf => u64::from(marker & 31),
                    0xd9 | 0xc4 => self.be(1)?,
                    0xda | 0xc5 => self.be(2)?,
                    _ => self.be(4)?,
                };
                self.take(
                    usize::try_from(length)
                        .map_err(|_| M::malformed("ignored value length overflow"))?,
                )?;
            }
            0xc7..=0xc9 | 0xd4..=0xd8 => {
                self.byte()?;
                let length = match marker {
                    0xc7 => self.be(1)?,
                    0xc8 => self.be(2)?,
                    0xc9 => self.be(4)?,
                    _ => 1u64 << (marker - 0xd4),
                };
                let length = length
                    .checked_add(1)
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or(M::malformed("extension length overflow"))?;
                self.take(length)?;
            }
            0xca | 0xcb | 0xcc..=0xcf | 0xd0..=0xd3 => {
                self.byte()?;
                let length = match marker {
                    0xca => 4,
                    0xcb => 8,
                    0xcc..=0xcf => 1 << (marker - 0xcc),
                    _ => 1 << (marker - 0xd0),
                };
                self.take(length)?;
            }
            0xc1 => return Err(M::malformed("reserved MessagePack marker")),
            _ => {
                self.byte()?;
            }
        }
        Ok(())
    }
}

impl<M: DeliveryMemory> Input<'_, '_, M> {
    fn lsn(&mut self) -> Result<Lsn, M::Error> {
        self.uint().map(Lsn)
    }
}

pub(crate) fn decode_envelope_raw<'a, M: DeliveryMemory>(
    memory: &M,
    bytes: &'a [u8],
) -> Result<NativeEnvelope<M::Buffer>, M::Error> {
    let mut input = Input::new(bytes, memory)?;
    let mut version: Option<u8> = None;
    let mut message_type: Option<MessageType> = None;
    let mut payload: Option<M::Buffer> = None;
    match input.peek()? {
        0x90..=0x9f | 0xdc | 0xdd => {
            if input.count(false)? != 3 {
                return Err(M::malformed("struct tuple length"));
            }
            version = Some(input.u8()?);
            message_type = Some(input.message_type()?);
            payload = Some(input.vector()?);
        }
        0x80..=0x8f | 0xde | 0xdf => {
            let count = input.count(true)?;
            for _ in 0..count {
                memory.checkpoint()?;
                match input.field_key(&["version", "message_type", "payload"])? {
                    Some(0) => {
                        if version.is_some() {
                            return Err(M::malformed("duplicate version"));
                        }
                        version = Some(input.u8()?);
                    }
                    Some(1) => {
                        if message_type.is_some() {
                            return Err(M::malformed("duplicate message_type"));
                        }
                        message_type = Some(input.message_type()?);
                    }
                    Some(2) => {
                        if payload.is_some() {
                            return Err(M::malformed("duplicate payload"));
                        }
                        payload = Some(input.vector()?);
                    }
                    _ => input.skip(0)?,
                }
            }
        }
        _ => return Err(M::malformed("expected struct map or tuple")),
    }
    memory.checkpoint()?;
    Ok(NativeEnvelope {
        version: version.ok_or(M::malformed("missing version"))?,
        message_type: message_type.ok_or(M::malformed("missing message_type"))?,
        payload: payload.ok_or(M::malformed("missing payload"))?,
    })
}

pub(crate) fn decode_request<'a, M: DeliveryMemory>(
    memory: &M,
    bytes: &'a [u8],
) -> Result<NativeOutcomesRequest<'a, M::Buffer>, M::Error> {
    let mut input = Input::new(bytes, memory)?;
    let mut format: Option<u8> = None;
    let mut tenant_id: Option<&'a str> = None;
    let mut edge_incarnation: Option<Incarnation> = None;
    let mut request_nonce: Option<[u8; 16]> = None;
    let mut since: Option<Option<M::Buffer>> = None;
    match input.peek()? {
        0x90..=0x9f | 0xdc | 0xdd => {
            if input.count(false)? != 5 {
                return Err(M::malformed("struct tuple length"));
            }
            format = Some(input.u8()?);
            tenant_id = Some(input.text()?);
            edge_incarnation = Some(input.incarnation()?);
            request_nonce = Some(input.nonce()?);
            since = Some(input.optional_vector()?);
        }
        0x80..=0x8f | 0xde | 0xdf => {
            let count = input.count(true)?;
            for _ in 0..count {
                memory.checkpoint()?;
                match input.field_key(&[
                    "format",
                    "tenant_id",
                    "edge_incarnation",
                    "request_nonce",
                    "since",
                ])? {
                    Some(0) => {
                        if format.is_some() {
                            return Err(M::malformed("duplicate format"));
                        }
                        format = Some(input.u8()?);
                    }
                    Some(1) => {
                        if tenant_id.is_some() {
                            return Err(M::malformed("duplicate tenant_id"));
                        }
                        tenant_id = Some(input.text()?);
                    }
                    Some(2) => {
                        if edge_incarnation.is_some() {
                            return Err(M::malformed("duplicate edge_incarnation"));
                        }
                        edge_incarnation = Some(input.incarnation()?);
                    }
                    Some(3) => {
                        if request_nonce.is_some() {
                            return Err(M::malformed("duplicate request_nonce"));
                        }
                        request_nonce = Some(input.nonce()?);
                    }
                    Some(4) => {
                        if since.is_some() {
                            return Err(M::malformed("duplicate since"));
                        }
                        since = Some(input.optional_vector()?);
                    }
                    _ => return Err(M::malformed("unknown request field")),
                }
            }
        }
        _ => return Err(M::malformed("expected struct map or tuple")),
    }
    memory.checkpoint()?;
    Ok(NativeOutcomesRequest {
        format: format.ok_or(M::malformed("missing format"))?,
        tenant_id: tenant_id.ok_or(M::malformed("missing tenant_id"))?,
        edge_incarnation: edge_incarnation.ok_or(M::malformed("missing edge_incarnation"))?,
        request_nonce: request_nonce.ok_or(M::malformed("missing request_nonce"))?,
        since: since.ok_or(M::malformed("missing since"))?,
    })
}

pub(crate) fn decode_cursor<'a, M: DeliveryMemory>(
    memory: &M,
    bytes: &'a [u8],
) -> Result<NativeOutcomeCursor<'a>, M::Error> {
    let mut input = Input::new(bytes, memory)?;
    let mut tenant_id: Option<&'a str> = None;
    let mut hub_node_id: Option<&'a str> = None;
    let mut hub_incarnation: Option<Incarnation> = None;
    let mut edge_node_id: Option<&'a str> = None;
    let mut edge_incarnation: Option<Incarnation> = None;
    let mut position: Option<Lsn> = None;
    let mut ordinal: Option<u32> = None;
    match input.peek()? {
        0x90..=0x9f | 0xdc | 0xdd => {
            if input.count(false)? != 7 {
                return Err(M::malformed("struct tuple length"));
            }
            tenant_id = Some(input.text()?);
            hub_node_id = Some(input.text()?);
            hub_incarnation = Some(input.incarnation()?);
            edge_node_id = Some(input.text()?);
            edge_incarnation = Some(input.incarnation()?);
            position = Some(input.lsn()?);
            ordinal = Some(input.u32()?);
        }
        0x80..=0x8f | 0xde | 0xdf => {
            let count = input.count(true)?;
            for _ in 0..count {
                memory.checkpoint()?;
                match input.field_key(&[
                    "tenant_id",
                    "hub_node_id",
                    "hub_incarnation",
                    "edge_node_id",
                    "edge_incarnation",
                    "position",
                    "ordinal",
                ])? {
                    Some(0) => {
                        if tenant_id.is_some() {
                            return Err(M::malformed("duplicate tenant_id"));
                        }
                        tenant_id = Some(input.text()?);
                    }
                    Some(1) => {
                        if hub_node_id.is_some() {
                            return Err(M::malformed("duplicate hub_node_id"));
                        }
                        hub_node_id = Some(input.text()?);
                    }
                    Some(2) => {
                        if hub_incarnation.is_some() {
                            return Err(M::malformed("duplicate hub_incarnation"));
                        }
                        hub_incarnation = Some(input.incarnation()?);
                    }
                    Some(3) => {
                        if edge_node_id.is_some() {
                            return Err(M::malformed("duplicate edge_node_id"));
                        }
                        edge_node_id = Some(input.text()?);
                    }
                    Some(4) => {
                        if edge_incarnation.is_some() {
                            return Err(M::malformed("duplicate edge_incarnation"));
                        }
                        edge_incarnation = Some(input.incarnation()?);
                    }
                    Some(5) => {
                        if position.is_some() {
                            return Err(M::malformed("duplicate position"));
                        }
                        position = Some(input.lsn()?);
                    }
                    Some(6) => {
                        if ordinal.is_some() {
                            return Err(M::malformed("duplicate ordinal"));
                        }
                        ordinal = Some(input.u32()?);
                    }
                    _ => input.skip(0)?,
                }
            }
        }
        _ => return Err(M::malformed("expected struct map or tuple")),
    }
    memory.checkpoint()?;
    Ok(NativeOutcomeCursor {
        tenant_id: tenant_id.ok_or(M::malformed("missing tenant_id"))?,
        hub_node_id: hub_node_id.ok_or(M::malformed("missing hub_node_id"))?,
        hub_incarnation: hub_incarnation.ok_or(M::malformed("missing hub_incarnation"))?,
        edge_node_id: edge_node_id.ok_or(M::malformed("missing edge_node_id"))?,
        edge_incarnation: edge_incarnation.ok_or(M::malformed("missing edge_incarnation"))?,
        position: position.ok_or(M::malformed("missing position"))?,
        ordinal: ordinal.ok_or(M::malformed("missing ordinal"))?,
    })
}

/// Namespace comparison against the caller's first committed incarnation read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn validate_cursor_after<M: DeliveryMemory>(
    memory: &M,
    bytes: &[u8],
    tenant: &str,
    hub: &str,
    hub_incarnation: Incarnation,
    edge: &str,
    edge_incarnation: Incarnation,
) -> Result<(Lsn, u32), M::Error> {
    let cursor = decode_cursor(memory, bytes)?;
    if cursor.tenant_id != tenant
        || cursor.hub_node_id != hub
        || cursor.hub_incarnation != hub_incarnation
        || cursor.edge_node_id != edge
        || cursor.edge_incarnation != edge_incarnation
    {
        return Err(M::canonical("invalid canonical delivery metadata"));
    }
    Ok((cursor.position, cursor.ordinal))
}

pub(crate) fn decode_envelope(
    memory: &NativeDeliveryImage,
    bytes: &[u8],
    expected: Option<u8>,
) -> Result<NativeEnvelope, DecodeError> {
    let envelope = decode_envelope_raw(memory, bytes)?;
    if !crate::protocol::supports_protocol_version(envelope.version)
        || expected.is_some_and(|version| envelope.version != version)
    {
        let (oldest, newest) = expected.map_or(
            (
                crate::protocol::OLDEST_SUPPORTED_PROTOCOL_VERSION,
                PROTOCOL_VERSION,
            ),
            |version| (version, version),
        );
        return Err(DecodeError::Version {
            received: envelope.version,
            oldest,
            newest,
        });
    }
    Ok(envelope)
}

// These buffers are produced by the concrete custody sibling, never a Legacy
// Vec wrapped as native after construction. Each independently keeps its invoice.
pub(crate) struct NativeOutcome<B = NativeDeliveryBytes> {
    pub(crate) lookup_submission: B,
    pub(crate) lookup_seal_digest: [u8; 32],
    pub(crate) lookup_origin_life_digest: [u8; 32],
    pub(crate) lookup_source: B,
    pub(crate) signed_core: B,
    pub(crate) diagnostic_body: B,
}
struct Binary<'a>(&'a [u8]);
impl Serialize for Binary<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}
impl<B: DeliveryBuffer> Serialize for NativeOutcome<B> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let mut value = serializer.serialize_struct("WireDeliveryOutcome", 6)?;
        value.serialize_field("lookup_submission", &Binary(self.lookup_submission.bytes()))?;
        value.serialize_field("lookup_seal_digest", &self.lookup_seal_digest)?;
        value.serialize_field("lookup_origin_life_digest", &self.lookup_origin_life_digest)?;
        value.serialize_field("lookup_source", &Binary(self.lookup_source.bytes()))?;
        value.serialize_field("signed_core", &Binary(self.signed_core.bytes()))?;
        value.serialize_field("diagnostic_body", &Binary(self.diagnostic_body.bytes()))?;
        value.end()
    }
}
#[derive(Serialize)]
#[serde(bound(serialize = "B: DeliveryBuffer"))]
struct NativeOutcomePage<'a, B> {
    outcomes: &'a [NativeOutcome<B>],
}
#[derive(Serialize)]
#[serde(bound(serialize = "B: DeliveryBuffer"))]
struct Response<'a, B> {
    format: u8,
    request_nonce: [u8; 16],
    hub_node_id: &'a str,
    hub_incarnation: Incarnation,
    tenant_id: &'a str,
    edge_node_id: &'a str,
    edge_incarnation: Incarnation,
    result: Option<NativeOutcomePage<'a, B>>,
    error: Option<&'a str>,
}
#[derive(Serialize)]
struct ReplyEnvelope<'a> {
    version: u8,
    message_type: MessageType,
    // Envelope's original Vec<u8> representation is a sequence, not bin.
    payload: &'a [u8],
}
#[derive(Default)]
struct Counter(usize);
impl std::io::Write for Counter {
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
// A serializer frame checks the same concrete memory capability before writing.
// The scalar I/O sentinel never formats a resource error into an owned string;
// the original error is returned after serde unwinds the borrowed write frame.
struct FrameWriter<'a, M: DeliveryMemory, W> {
    memory: &'a M,
    writer: W,
    failure: Option<M::Error>,
}
impl<M: DeliveryMemory, W: std::io::Write> std::io::Write for FrameWriter<'_, M, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.failure.is_some() {
            return Err(std::io::ErrorKind::Other.into());
        }
        if let Err(error) = self.memory.checkpoint() {
            self.failure = Some(error);
            return Err(std::io::ErrorKind::Other.into());
        }
        self.writer.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
fn serialize_frame<M: DeliveryMemory, T: Serialize, W: std::io::Write>(
    memory: &M,
    value: &T,
    writer: W,
    named: bool,
) -> Result<(), M::Error> {
    memory.checkpoint()?;
    let mut frame = FrameWriter {
        memory,
        writer,
        failure: None,
    };
    let result = if named {
        value.serialize(&mut rmp_serde::Serializer::new(&mut frame).with_struct_map())
    } else {
        value.serialize(&mut rmp_serde::Serializer::new(&mut frame))
    };
    result.map_err(|error| frame.failure.take().unwrap_or_else(|| M::encode(error)))
}
fn named_size<M: DeliveryMemory, T: Serialize>(memory: &M, value: &T) -> Result<usize, M::Error> {
    let mut count = Counter::default();
    serialize_frame(memory, value, &mut count, true)?;
    Ok(count.0)
}

pub(crate) fn encode_response<M: DeliveryMemory>(
    memory: &M,
    request_nonce: [u8; 16],
    hub_node_id: &str,
    hub_incarnation: Incarnation,
    tenant_id: &str,
    edge_node_id: &str,
    edge_incarnation: Incarnation,
    outcomes: &[NativeOutcome<M::Buffer>],
) -> Result<M::Buffer, M::Error> {
    memory.checkpoint()?;
    for outcome in outcomes {
        for buffer in [
            &outcome.lookup_submission,
            &outcome.lookup_source,
            &outcome.signed_core,
            &outcome.diagnostic_body,
        ] {
            if !memory.owns(buffer) {
                return Err(M::owner_mismatch());
            }
        }
    }
    let response = Response {
        format: 1,
        request_nonce,
        hub_node_id,
        hub_incarnation,
        tenant_id,
        edge_node_id,
        edge_incarnation,
        result: Some(NativeOutcomePage { outcomes }),
        error: None,
    };
    // Count using this closed producer's borrowed/scalar serializers. The exact
    // destination is paid before either pass writes a byte. Slice writers cannot
    // grow or allocate; an incorrect count returns an error rather than resizing.
    let mut payload = memory.allocate(named_size(memory, &response)?)?;
    let mut destination = payload.bytes_mut();
    serialize_frame(memory, &response, &mut destination, true)?;
    if !destination.is_empty() {
        return Err(M::canonical("response size changed"));
    }
    memory.checkpoint()?;
    let envelope = ReplyEnvelope {
        version: PROTOCOL_VERSION,
        message_type: MessageType::FetchDeliveryOutcomesResponse,
        payload: payload.bytes(),
    };
    let mut count = Counter::default();
    serialize_frame(memory, &envelope, &mut count, false)?;
    let mut reply = memory.allocate(count.0)?;
    let mut destination = reply.bytes_mut();
    serialize_frame(memory, &envelope, &mut destination, false)?;
    if !destination.is_empty() {
        return Err(M::canonical("envelope size changed"));
    }
    memory.checkpoint()?;
    Ok(reply)
}

/// The closed native request validation kernel used before custody selection.
/// The local hub and edge are borrowed from standing server and authenticated
/// transport custody respectively; request text cannot supply either identity.
/// A consumer closure cannot retain borrowed request text after input release.
pub(crate) fn with_authenticated_request<T>(
    memory: &NativeDeliveryImage,
    bytes: &[u8],
    tenant: &contextdb_core::TenantId,
    local_hub: Option<&str>,
    authenticated_edge: Option<&str>,
    consume: impl FnOnce(NativeOutcomesRequest<'_>, &str, &str) -> Result<T, DecodeError>,
) -> Result<T, DecodeError> {
    let envelope = crate::sync_client::decode_native_sync_envelope(memory, bytes, None)?;
    let request = decode_request(memory, envelope.payload.as_slice())?;
    if envelope.message_type != MessageType::FetchDeliveryOutcomesRequest
        || request.format != 1
        || request.tenant_id != tenant.as_str()
    {
        return Err(DecodeError::Malformed(
            "invalid authenticated outcome request",
        ));
    }
    let hub = local_hub.ok_or(DecodeError::Malformed("authenticated hub required"))?;
    let edge = authenticated_edge.ok_or(DecodeError::Malformed("authenticated edge required"))?;
    memory.checkpoint()?;
    consume(request, hub, edge)
}
