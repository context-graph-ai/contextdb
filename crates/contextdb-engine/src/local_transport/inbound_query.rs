//! The authenticated reader's bounded prefix and discard decoder.
//!
//! No request object is constructed by this pass. A Query prefix identifies
//! the acquisition point; the rest is either decoded on that acquired child
//! or validated here before a deferred refusal enters the connection FIFO.
use super::{FrameViolation, LocalTransportError, MAX_FRAME_BYTES, invalid_channel_data};
use contextdb_core::read_contract::ReadLimits;
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use std::io::{self, Read};

// Nine maximum-width u64s and one maximum-width u32 discriminant. This is
// wire syntax, not a request-size or policy limit.
const PREFIX_BYTES: usize = 9 * 9 + 5;

/// Pinned serde_json's error block contains an enum (largest payload:
/// Box<str> or io::Error) and two positions. A full-word discriminant plus
/// the larger payload is a conservative layout bound, including alignment.
/// fix_position may construct its replacement before dropping the old box.
/// This visitor never calls Error::custom, so no owned message is created.
pub(crate) fn drain_scratch_bytes() -> usize {
    use std::alloc::Layout;
    let payload = Layout::new::<Box<str>>();
    let io_error = Layout::new::<io::Error>();
    let payload = Layout::from_size_align(payload.size().max(io_error.size()),
        payload.align().max(io_error.align())).expect("JSON error payload layout");
    let code = Layout::new::<usize>().extend(payload).expect("JSON error code layout").0;
    let error = code.extend(Layout::new::<[usize; 2]>()).expect("JSON error positions layout").0
        .pad_to_align();
    2 * error.size() + std::mem::size_of::<RequestPrefix>()
        + std::mem::size_of::<Wire<'_, io::Empty>>() + 256
}

pub(crate) struct RequestPrefix {
    pub(crate) length: usize,
    pub(crate) limits: ReadLimits,
    pub(crate) query: bool,
    bytes: [u8; PREFIX_BYTES],
    used: usize,
}

impl RequestPrefix {
    pub(crate) fn read(stream: &mut impl Read) -> Result<Self, LocalTransportError> {
        let mut length = [0; 4];
        stream.read_exact(&mut length).map_err(|_| {
            LocalTransportError::Frame(FrameViolation::TruncatedLength)
        })?;
        let length = u32::from_be_bytes(length) as usize;
        if length > MAX_FRAME_BYTES {
            return Err(LocalTransportError::Frame(FrameViolation::LengthExceedsMaximum));
        }
        let mut result = Self {
            length, limits: ReadLimits::default(), query: false,
            bytes: [0; PREFIX_BYTES], used: 0,
        };
        let mut wire = Wire::new(stream, length, false);
        let mut fields = [0; 9];
        for field in &mut fields {
            *field = result.integer(&mut wire, 8)?;
        }
        let tag = result.integer(&mut wire, 4)?;
        if tag > 8 { return Err(invalid_channel_data()); }
        result.query = tag == 0;
        result.limits = ReadLimits {
            result_rows: fields[0], result_bytes: fields[1], work: fields[2],
            active_ms: fields[3], memory: fields[4], cursor_page_rows: fields[5],
            cursor_page_bytes: fields[6], cursor_idle_ms: fields[7],
            cursor_lifetime_ms: fields[8],
        };
        Ok(result)
    }

    fn integer(&mut self, wire: &mut Wire<'_, impl Read>, width: usize)
        -> Result<u64, LocalTransportError>
    {
        let tag = wire.byte().map_err(|_| invalid_channel_data())?;
        self.bytes[self.used] = tag;
        self.used += 1;
        let count = integer_width(tag, width).map_err(|_| invalid_channel_data())?;
        if count == 0 { return Ok(tag as u64); }
        let mut value = [0; 8];
        wire.read_exact(&mut value[..count]).map_err(|_| invalid_channel_data())?;
        self.bytes[self.used..self.used + count].copy_from_slice(&value[..count]);
        self.used += count;
        Ok(u64::from_le_bytes(value))
    }

    pub(crate) fn complete(&self, stream: &mut impl Read, output: &mut [u8])
        -> Result<(), LocalTransportError>
    {
        if output.len() != self.length { return Err(invalid_channel_data()); }
        output[..self.used].copy_from_slice(&self.bytes[..self.used]);
        stream.read_exact(&mut output[self.used..]).map_err(|_| {
            LocalTransportError::Frame(FrameViolation::TruncatedPayload)
        })
    }

    /// The payload was completed from this prefix. A typed decoder can have
    /// stopped on admission before reaching its end; replay only this frame's
    /// body through the same full validator used before body allocation.
    pub(crate) fn validate_completed_query(&self, payload: &[u8])
        -> Result<(), LocalTransportError>
    {
        if payload.len() != self.length { return Err(invalid_channel_data()); }
        let mut body = payload.get(self.used..).ok_or_else(invalid_channel_data)?;
        self.discard_query(&mut body)
    }

    pub(crate) fn discard_query(&self, stream: &mut impl Read)
        -> Result<(), LocalTransportError>
    {
        if !self.query { return Err(invalid_channel_data()); }
        let mut wire = Wire::new(stream, self.length - self.used, true);
        let validated = (|| -> io::Result<()> {
            wire.string()?;
            let members = wire.length()?;
            // Every map member consumes at least a key length and a value tag.
            if members > wire.remaining { return Err(malformed()); }
            for _ in 0..members {
                wire.string()?;
                wire.value()?;
            }
            if wire.remaining != 0 { return Err(malformed()); }
            Ok(())
        })();
        validated.map_err(|_| invalid_channel_data())?;
        self.limits.validate().map_err(|_| invalid_channel_data())
    }
}

fn malformed() -> io::Error { io::ErrorKind::InvalidData.into() }

fn integer_width(tag: u8, maximum: usize) -> io::Result<usize> {
    match tag {
        0..=250 => Ok(0),
        251 if maximum >= 2 => Ok(2),
        252 if maximum >= 4 => Ok(4),
        253 if maximum >= 8 => Ok(8),
        _ => Err(malformed()),
    }
}

// Only the refused-body validator enables read-ahead. Prefix reads never
// copy a successful request's body before its child has been acquired. A
// refill is bounded by this frame's remaining bytes and cannot consume any
// part of the following request.
struct Wire<'a, R> {
    input: &'a mut R,
    remaining: usize,
    buffer: [u8; 4096],
    start: usize,
    end: usize,
    buffered: bool,
}
impl<'a, R> Wire<'a, R> {
    fn new(input: &'a mut R, remaining: usize, buffered: bool) -> Self {
        Self { input, remaining, buffer: [0; 4096], start: 0, end: 0, buffered }
    }
}
impl<R: Read> Read for Wire<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let length = output.len().min(self.remaining);
        if length == 0 { return Ok(0); }
        let read = if self.buffered {
            if self.start == self.end {
                let size = self.remaining.min(self.buffer.len());
                self.end = self.input.read(&mut self.buffer[..size])?;
                self.start = 0;
            }
            let read = length.min(self.end - self.start);
            output[..read].copy_from_slice(&self.buffer[self.start..self.start + read]);
            self.start += read;
            read
        } else {
            self.input.read(&mut output[..length])?
        };
        self.remaining -= read;
        Ok(read)
    }
}
impl<R: Read> Wire<'_, R> {
    fn byte(&mut self) -> io::Result<u8> {
        let mut byte = [0];
        self.read_exact(&mut byte)?;
        Ok(byte[0])
    }
    fn unsigned(&mut self, maximum: usize) -> io::Result<u64> {
        let tag = self.byte()?;
        let count = integer_width(tag, maximum)?;
        if count == 0 { return Ok(tag as u64); }
        let mut bytes = [0; 8];
        self.read_exact(&mut bytes[..count])?;
        Ok(u64::from_le_bytes(bytes))
    }
    fn length(&mut self) -> io::Result<usize> {
        usize::try_from(self.unsigned(8)?).map_err(|_| malformed())
    }
    fn skip(&mut self, mut length: usize) -> io::Result<()> {
        if length > self.remaining { return Err(malformed()); }
        let mut scratch = [0; 256];
        while length != 0 {
            let take = length.min(scratch.len());
            self.read_exact(&mut scratch[..take])?;
            length -= take;
        }
        Ok(())
    }
    fn string(&mut self) -> io::Result<()> {
        let length = self.length()?;
        if length > self.remaining { return Err(malformed()); }
        let mut utf8 = Utf8::default();
        for _ in 0..length { utf8.byte(self.byte()?)?; }
        utf8.finish()
    }
    fn value(&mut self) -> io::Result<()> {
        match self.unsigned(4)? {
            0 => Ok(()),
            1 => match self.byte()? { 0 | 1 => Ok(()), _ => Err(malformed()) },
            2 | 6 | 9 => self.unsigned(8).map(|_| ()),
            3 => self.skip(8),
            4 => self.string(),
            5 => {
                if self.length()? != 16 { return Err(malformed()); }
                self.skip(16)
            }
            7 => {
                let length = self.length()?;
                if length > self.remaining { return Err(malformed()); }
                let mut source = JsonStrings {
                    input: self.take(length as u64), closing_quote: false,
                };
                let mut parser = serde_json::Deserializer::from_reader(&mut source);
                JsonValue::deserialize(&mut parser).map_err(|_| malformed())?;
                parser.end().map_err(|_| malformed())?;
                drop(parser);
                if source.input.limit() != 0 { return Err(malformed()); }
                Ok(())
            }
            8 => {
                let length = self.length()?.checked_mul(4).ok_or_else(malformed)?;
                self.skip(length)
            }
            _ => Err(malformed()),
        }
    }
}

/// A UTF-8 scalar can cross any carrier read boundary. The first continuation
/// bound excludes overlong encodings, surrogates and values above U+10FFFF.
#[derive(Default)]
struct Utf8 { left: u8, low: u8, high: u8 }
impl Utf8 {
    fn byte(&mut self, byte: u8) -> io::Result<()> {
        if self.left != 0 {
            if byte < self.low || byte > self.high { return Err(malformed()); }
            self.left -= 1;
            self.low = 0x80;
            self.high = 0xbf;
            return Ok(());
        }
        let (left, low, high) = match byte {
            0..=0x7f => (0, 0, 0),
            0xc2..=0xdf => (1, 0x80, 0xbf),
            0xe0 => (2, 0xa0, 0xbf),
            0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
            0xed => (2, 0x80, 0x9f),
            0xf0 => (3, 0x90, 0xbf),
            0xf1..=0xf3 => (3, 0x80, 0xbf),
            0xf4 => (3, 0x80, 0x8f),
            _ => return Err(malformed()),
        };
        self.left = left; self.low = low; self.high = high;
        Ok(())
    }
    fn finish(&self) -> io::Result<()> {
        if self.left == 0 { Ok(()) } else { Err(malformed()) }
    }
}

/// Replace only fully validated string contents with the empty string. The
/// typed serde parser still sees every delimiter, literal and numeric byte,
/// and remains the recursion and numeric-conversion authority. Unlike
/// IgnoredAny, this visitor invokes deserialize_any recursively and therefore
/// rejects numbers that serde_json::Value cannot represent.
struct JsonStrings<R> { input: R, closing_quote: bool }
impl<R: Read> JsonStrings<R> {
    fn byte(&mut self) -> io::Result<u8> {
        let mut byte = [0]; self.input.read_exact(&mut byte)?; Ok(byte[0])
    }
    fn hex(&mut self) -> io::Result<u16> {
        let mut code = 0;
        for _ in 0..4 {
            let digit = match self.byte()? {
                byte @ b'0'..=b'9' => byte - b'0',
                byte @ b'a'..=b'f' => byte - b'a' + 10,
                byte @ b'A'..=b'F' => byte - b'A' + 10,
                _ => return Err(malformed()),
            };
            code = (code << 4) | u16::from(digit);
        }
        Ok(code)
    }
    fn string(&mut self) -> io::Result<()> {
        let mut utf8 = Utf8::default();
        loop {
            let byte = self.byte()?;
            if utf8.left != 0 { utf8.byte(byte)?; continue; }
            match byte {
                b'"' => return Ok(()),
                0..=0x1f => return Err(malformed()),
                b'\\' => match self.byte()? {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {},
                    b'u' => match self.hex()? {
                        0xdc00..=0xdfff => return Err(malformed()),
                        0xd800..=0xdbff => {
                            if self.byte()? != b'\\' || self.byte()? != b'u' {
                                return Err(malformed());
                            }
                            if !(0xdc00..=0xdfff).contains(&self.hex()?) {
                                return Err(malformed());
                            }
                        }
                        _ => {},
                    },
                    _ => return Err(malformed()),
                },
                _ => utf8.byte(byte)?,
            }
        }
    }
}
impl<R: Read> Read for JsonStrings<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() { return Ok(0); }
        if self.closing_quote {
            output[0] = b'"'; self.closing_quote = false; return Ok(1);
        }
        let count = self.input.read(&mut output[..1])?;
        if count == 1 && output[0] == b'"' {
            self.string()?;
            self.closing_quote = true;
        }
        Ok(count)
    }
}

struct JsonValue;
impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D: Deserializer<'de>>(parser: D) -> Result<Self, D::Error> {
        struct ValueVisitor;
        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = JsonValue;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a JSON value")
            }
            fn visit_unit<E>(self) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_bool<E>(self, _: bool) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_i64<E>(self, _: i64) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_u64<E>(self, _: u64) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_f64<E>(self, _: f64) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_str<E>(self, _: &str) -> Result<JsonValue, E> { Ok(JsonValue) }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<JsonValue, A::Error> {
                while seq.next_element::<JsonValue>()?.is_some() {}
                Ok(JsonValue)
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<JsonValue, A::Error> {
                while map.next_key::<JsonValue>()?.is_some() {
                    map.next_value::<JsonValue>()?;
                }
                Ok(JsonValue)
            }
        }
        parser.deserialize_any(ValueVisitor)
    }
}
