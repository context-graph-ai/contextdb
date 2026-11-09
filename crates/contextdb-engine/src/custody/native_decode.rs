//! Two-pass, allocation-free inspection of the actual named MessagePack codec.
//! The first pass validates every scalar/container and typed field. The second
//! emits borrowed shapes; sequences retain raw spans, never nested owned data.
use contextdb_core::{EdgeDiscardMode, HistoryPolicy, Incarnation, Lsn, RetainUnit, SyncDirection};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Invalid {
    Codec,
    Canonical,
    Key,
    Signature,
    Companions,
}
pub(crate) type Decoded<T> = std::result::Result<T, Invalid>;
pub(crate) trait Decode<'a>: Sized {
    fn decode(n: NodeView<'a>) -> Decoded<Self>;
}
#[derive(Clone, Copy)]
pub(crate) struct NodeView<'a> {
    raw: &'a [u8],
}
struct Header {
    bytes: usize,
    children: usize,
    kind: u8,
    payload: usize,
}
fn header(b: &[u8]) -> Decoded<Header> {
    let tag = *b.first().ok_or(Invalid::Codec)?;
    let number = |start: usize, len: usize| -> Decoded<usize> {
        let end = start.checked_add(len).ok_or(Invalid::Codec)?;
        let mut n = 0usize;
        for x in b.get(start..end).ok_or(Invalid::Codec)? {
            n = n
                .checked_mul(256)
                .and_then(|n| n.checked_add(*x as usize))
                .ok_or(Invalid::Codec)?;
        }
        Ok(n)
    };
    let (base, payload, children, kind): (usize, usize, usize, u8) = match tag {
        0x00..=0x7f | 0xe0..=0xff => (1, 0, 0, b'i'),
        0x80..=0x8f => (1, 0, 2 * (tag as usize & 15), b'm'),
        0x90..=0x9f => (1, 0, tag as usize & 15, b'a'),
        0xa0..=0xbf => (1, tag as usize & 31, 0, b's'),
        0xc0 => (1, 0, 0, b'n'),
        0xc2 | 0xc3 => (1, 0, 0, b'b'),
        0xc4 => (2, number(1, 1)?, 0, b'x'),
        0xc5 => (3, number(1, 2)?, 0, b'x'),
        0xc6 => (5, number(1, 4)?, 0, b'x'),
        0xca => (1, 4, 0, b'f'),
        0xcb => (1, 8, 0, b'f'),
        0xcc | 0xd0 => (1, 1, 0, b'i'),
        0xcd | 0xd1 => (1, 2, 0, b'i'),
        0xce | 0xd2 => (1, 4, 0, b'i'),
        0xcf | 0xd3 => (1, 8, 0, b'i'),
        0xd9 => (2, number(1, 1)?, 0, b's'),
        0xda => (3, number(1, 2)?, 0, b's'),
        0xdb => (5, number(1, 4)?, 0, b's'),
        0xdc => (3, 0, number(1, 2)?, b'a'),
        0xdd => (5, 0, number(1, 4)?, b'a'),
        0xde => (
            3,
            0,
            number(1, 2)?.checked_mul(2).ok_or(Invalid::Codec)?,
            b'm',
        ),
        0xdf => (
            5,
            0,
            number(1, 4)?.checked_mul(2).ok_or(Invalid::Codec)?,
            b'm',
        ),
        // No custody field uses MessagePack extension values or the reserved tag.
        _ => return Err(Invalid::Codec),
    };
    let bytes = base.checked_add(payload).ok_or(Invalid::Codec)?;
    if bytes > b.len() {
        return Err(Invalid::Codec);
    }
    if kind == b's' {
        std::str::from_utf8(&b[base..bytes]).map_err(|_| Invalid::Codec)?;
    }
    Ok(Header {
        bytes,
        children,
        kind,
        payload,
    })
}
// A scalar pending-child counter validates a whole subtree without recursion,
// an owned node arena, a path Vec, or a stack proportional to the stored bytes.
fn take(b: &[u8]) -> Decoded<(&[u8], &[u8])> {
    let mut left = 1usize;
    let mut offset = 0usize;
    while left != 0 {
        let h = header(b.get(offset..).ok_or(Invalid::Codec)?)?;
        offset = offset.checked_add(h.bytes).ok_or(Invalid::Codec)?;
        left = left
            .checked_sub(1)
            .and_then(|n| n.checked_add(h.children))
            .ok_or(Invalid::Codec)?;
        // Every pending child consumes at least one remaining input byte.
        if left > b.len() - offset {
            return Err(Invalid::Codec);
        }
    }
    Ok((&b[..offset], &b[offset..]))
}
impl<'a> NodeView<'a> {
    pub(crate) fn parse(raw: &'a [u8]) -> Decoded<Self> {
        let (node, rest) = take(raw)?;
        if !rest.is_empty() {
            return Err(Invalid::Codec);
        }
        Ok(Self { raw: node })
    }
    pub(crate) fn nil() -> Self {
        Self { raw: &[0xc0] }
    }
    pub(crate) fn empty_array() -> Self {
        Self { raw: &[0x90] }
    }
    pub(crate) fn is_nil(self) -> bool {
        self.raw == [0xc0]
    }
    pub(crate) fn string(self) -> Decoded<&'a str> {
        let h = header(self.raw)?;
        if h.kind != b's' {
            return Err(Invalid::Codec);
        }
        std::str::from_utf8(&self.raw[h.bytes - h.payload..h.bytes]).map_err(|_| Invalid::Codec)
    }
    pub(crate) fn unsigned(self) -> Decoded<u64> {
        let tag = self.raw[0];
        if tag <= 0x7f {
            return Ok(tag as u64);
        }
        let len = match tag {
            0xcc => 1,
            0xcd => 2,
            0xce => 4,
            0xcf => 8,
            _ => return Err(Invalid::Codec),
        };
        let mut n = 0u64;
        for x in self.raw.get(1..1 + len).ok_or(Invalid::Codec)? {
            n = (n << 8) | *x as u64;
        }
        Ok(n)
    }
    pub(crate) fn signed(self) -> Decoded<i64> {
        let tag = self.raw[0];
        if tag >= 0xe0 {
            return Ok((tag as i8) as i64);
        }
        if tag <= 0x7f || matches!(tag, 0xcc..=0xcf) {
            return i64::try_from(self.unsigned()?).map_err(|_| Invalid::Codec);
        }
        let len = match tag {
            0xd0 => 1,
            0xd1 => 2,
            0xd2 => 4,
            0xd3 => 8,
            _ => return Err(Invalid::Codec),
        };
        let mut buf = [if self.raw[1] & 0x80 != 0 { 0xff } else { 0 }; 8];
        buf[8 - len..].copy_from_slice(&self.raw[1..1 + len]);
        Ok(i64::from_be_bytes(buf))
    }
    pub(crate) fn float(self) -> Decoded<f64> {
        match self.raw[0] {
            0xca => {
                let mut a = [0; 4];
                a.copy_from_slice(&self.raw[1..5]);
                Ok(f32::from_bits(u32::from_be_bytes(a)) as f64)
            }
            0xcb => {
                let mut a = [0; 8];
                a.copy_from_slice(&self.raw[1..9]);
                Ok(f64::from_bits(u64::from_be_bytes(a)))
            }
            _ => Err(Invalid::Codec),
        }
    }
    pub(crate) fn array(self) -> Decoded<Nodes<'a>> {
        let h = header(self.raw)?;
        if h.kind != b'a' {
            return Err(Invalid::Codec);
        }
        Ok(Nodes {
            rest: &self.raw[h.bytes..],
            left: h.children,
        })
    }
    pub(crate) fn map(self) -> Decoded<Nodes<'a>> {
        let h = header(self.raw)?;
        if h.kind != b'm' {
            return Err(Invalid::Codec);
        }
        Ok(Nodes {
            rest: &self.raw[h.bytes..],
            left: h.children,
        })
    }
    pub(crate) fn binary(self) -> Decoded<&'a [u8]> {
        let h = header(self.raw)?;
        if h.kind != b'x' {
            return Err(Invalid::Codec);
        }
        Ok(&self.raw[h.bytes - h.payload..h.bytes])
    }
    pub(crate) fn field(self, key: &str) -> Decoded<Self> {
        self.optional_field(key)?.ok_or(Invalid::Codec)
    }
    pub(crate) fn optional_field(self, key: &str) -> Decoded<Option<Self>> {
        let mut found = None;
        let mut nodes = self.map()?;
        while let Some(k) = nodes.next() {
            let k = k?.string()?;
            let v = nodes.next().ok_or(Invalid::Codec)??;
            if k == key {
                if found.is_some() {
                    return Err(Invalid::Codec);
                }
                found = Some(v)
            }
        }
        Ok(found)
    }
    pub(crate) fn variant(self) -> Decoded<(&'a str, Self)> {
        let mut nodes = self.map()?;
        if nodes.len() != 2 {
            return Err(Invalid::Codec);
        }
        Ok((
            nodes.next().ok_or(Invalid::Codec)??.string()?,
            nodes.next().ok_or(Invalid::Codec)??,
        ))
    }
}
pub(crate) struct Nodes<'a> {
    rest: &'a [u8],
    left: usize,
}
impl Nodes<'_> {
    pub(crate) fn len(&self) -> usize {
        self.left
    }
}
impl<'a> Iterator for Nodes<'a> {
    type Item = Decoded<NodeView<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(take(self.rest).map(|(node, rest)| {
            self.rest = rest;
            NodeView { raw: node }
        }))
    }
}
#[derive(Clone, Copy)]
pub(crate) struct Bytes<'a> {
    node: NodeView<'a>,
    length: usize,
}
impl<'a> Bytes<'a> {
    pub(crate) fn len(self) -> usize {
        self.length
    }
    pub(crate) fn copy_into(self, out: &mut [u8]) -> Decoded<()> {
        if out.len() != self.length {
            return Err(Invalid::Codec);
        }
        for (dst, n) in out.iter_mut().zip(self.node.array()?) {
            *dst = u8::decode(n?)?;
        }
        Ok(())
    }
    pub(crate) fn iter(self) -> Decoded<impl Iterator<Item = Decoded<u8>> + 'a> {
        Ok(self.node.array()?.map(|n| u8::decode(n?)))
    }
}
impl<'a> Decode<'a> for Bytes<'a> {
    fn decode(node: NodeView<'a>) -> Decoded<Self> {
        let a = node.array()?;
        let length = a.len();
        for n in a {
            u8::decode(n?)?;
        }
        Ok(Self { node, length })
    }
}
impl Serialize for Bytes<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.length))?;
        for n in self
            .iter()
            .map_err(|_| serde::ser::Error::custom("invalid borrowed bytes"))?
        {
            seq.serialize_element(
                &n.map_err(|_| serde::ser::Error::custom("invalid borrowed byte"))?,
            )?;
        }
        seq.end()
    }
}
pub(crate) struct Seq<'a, T> {
    node: NodeView<'a>,
    length: usize,
    marker: std::marker::PhantomData<T>,
}
impl<T> Copy for Seq<'_, T> {}
impl<T> Clone for Seq<'_, T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<'a, T: Decode<'a>> Seq<'a, T> {
    pub(crate) fn len(self) -> usize {
        self.length
    }
    pub(crate) fn iter(self) -> Decoded<impl Iterator<Item = Decoded<T>> + 'a>
    where
        T: 'a,
    {
        Ok(self.node.array()?.map(|n| T::decode(n?)))
    }
    pub(crate) fn get(self, index: usize) -> Decoded<T> {
        T::decode(self.node.array()?.nth(index).ok_or(Invalid::Codec)??)
    }
}
impl<'a, T: Decode<'a>> Decode<'a> for Seq<'a, T> {
    fn decode(node: NodeView<'a>) -> Decoded<Self> {
        let a = node.array()?;
        let length = a.len();
        for n in a {
            T::decode(n?)?;
        }
        Ok(Self {
            node,
            length,
            marker: std::marker::PhantomData,
        })
    }
}
impl<'a, T: Decode<'a> + Serialize + 'a> Serialize for Seq<'a, T> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.length))?;
        for n in self
            .iter()
            .map_err(|_| serde::ser::Error::custom("invalid borrowed sequence"))?
        {
            seq.serialize_element(
                &n.map_err(|_| serde::ser::Error::custom("invalid borrowed field"))?,
            )?;
        }
        seq.end()
    }
}
impl<'a> Decode<'a> for &'a str {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.string()
    }
}
impl<'a> Decode<'a> for bool {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        match n.raw {
            [0xc2] => Ok(false),
            [0xc3] => Ok(true),
            _ => Err(Invalid::Codec),
        }
    }
}
macro_rules! unsigned {($($t:ty),*)=>{$(impl<'a> Decode<'a> for $t{fn decode(n:NodeView<'a>)->Decoded<Self>{<$t>::try_from(n.unsigned()?).map_err(|_|Invalid::Codec)}})*}}
unsigned!(u8, u32, u64);
impl<'a> Decode<'a> for i64 {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.signed()
    }
}
impl<'a> Decode<'a> for f64 {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        n.float()
    }
}
impl<'a> Decode<'a> for f32 {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        Ok(n.float()? as f32)
    }
}
impl<'a> Decode<'a> for Lsn {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        Ok(Lsn(n.unsigned()?))
    }
}
impl<'a> Decode<'a> for Uuid {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        Uuid::from_slice(n.binary()?).map_err(|_| Invalid::Codec)
    }
}
impl<'a> Decode<'a> for Incarnation {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        let (hi, lo): (u64, u64) = Decode::decode(n)?;
        Ok(Incarnation((hi as u128) << 64 | lo as u128))
    }
}
impl<'a, T: Decode<'a>> Decode<'a> for Option<T> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        if n.is_nil() {
            Ok(None)
        } else {
            T::decode(n).map(Some)
        }
    }
}
impl<'a, A: Decode<'a>, B: Decode<'a>> Decode<'a> for (A, B) {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        let mut a = n.array()?;
        if a.len() != 2 {
            return Err(Invalid::Codec);
        }
        Ok((
            A::decode(a.next().ok_or(Invalid::Codec)??)?,
            B::decode(a.next().ok_or(Invalid::Codec)??)?,
        ))
    }
}
impl<'a> Decode<'a> for [u8; 32] {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        let bytes = Bytes::decode(n)?;
        let mut a = [0; 32];
        bytes.copy_into(&mut a)?;
        Ok(a)
    }
}
macro_rules! words{($t:ty,$($s:literal=>$v:path),*)=>{impl<'a> Decode<'a> for $t{fn decode(n:NodeView<'a>)->Decoded<Self>{match n.string()?{$($s=>Ok($v),)*_=>Err(Invalid::Codec)}}}}}
words!(SyncDirection,"None"=>SyncDirection::None,"Push"=>SyncDirection::Push,"Pull"=>SyncDirection::Pull,"Both"=>SyncDirection::Both);
words!(HistoryPolicy,"All"=>HistoryPolicy::All,"CurrentOnly"=>HistoryPolicy::CurrentOnly);
words!(EdgeDiscardMode,"Never"=>EdgeDiscardMode::Never,"AfterOutcome"=>EdgeDiscardMode::AfterOutcome,"Always"=>EdgeDiscardMode::Always);
words!(RetainUnit,"Seconds"=>RetainUnit::Seconds,"Minutes"=>RetainUnit::Minutes,"Hours"=>RetainUnit::Hours,"Days"=>RetainUnit::Days);
// The stored conflict codec also carries the private role-relative diagnostics.
#[derive(Clone, Copy, Serialize)]
pub(crate) enum Resolution {
    InsertIfNotExists,
    ServerWins,
    EdgeWins,
    LatestWins,
}
words!(Resolution,"InsertIfNotExists"=>Resolution::InsertIfNotExists,"ServerWins"=>Resolution::ServerWins,"EdgeWins"=>Resolution::EdgeWins,"LatestWins"=>Resolution::LatestWins);
#[derive(Clone, Copy, Serialize)]
pub(crate) enum Value<'a> {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Text(&'a str),
    Uuid(Uuid),
    Timestamp(i64),
    Json(&'a str),
    Vector(Seq<'a, f32>),
    TxId(u64),
}
impl<'a> Decode<'a> for Value<'a> {
    fn decode(n: NodeView<'a>) -> Decoded<Self> {
        if n.string().ok() == Some("Null") {
            return Ok(Self::Null);
        }
        let (tag, v) = n.variant()?;
        Ok(match tag {
            "Bool" => Self::Bool(Decode::decode(v)?),
            "Int64" => Self::Int64(Decode::decode(v)?),
            "Float64" => Self::Float64(Decode::decode(v)?),
            "Text" => Self::Text(Decode::decode(v)?),
            "Uuid" => Self::Uuid(Decode::decode(v)?),
            "Timestamp" => Self::Timestamp(Decode::decode(v)?),
            "Json" => {
                let text = v.string()?;
                super::native_json::validate(text)?;
                Self::Json(text)
            }
            "Vector" => Self::Vector(Decode::decode(v)?),
            "TxId" => Self::TxId(Decode::decode(v)?),
            _ => return Err(Invalid::Codec),
        })
    }
}

// Used only to re-emit validated positional DDL bytes for exact canonical
// comparison. Typed custody records use their derived schema serialization.
impl Serialize for NodeView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let bad = || <S::Error as serde::ser::Error>::custom("invalid borrowed MessagePack node");
        match header(self.raw).map_err(|_| bad())?.kind {
            b'n' => s.serialize_unit(),
            b'b' => s.serialize_bool(bool::decode(*self).map_err(|_| bad())?),
            b's' => s.serialize_str(self.string().map_err(|_| bad())?),
            b'x' => s.serialize_bytes(self.binary().map_err(|_| bad())?),
            b'i' => {
                if self.raw[0] >= 0xe0 || matches!(self.raw[0], 0xd0..=0xd3) {
                    s.serialize_i64(self.signed().map_err(|_| bad())?)
                } else {
                    s.serialize_u64(self.unsigned().map_err(|_| bad())?)
                }
            }
            b'f' => {
                if self.raw[0] == 0xca {
                    s.serialize_f32(self.float().map_err(|_| bad())? as f32)
                } else {
                    s.serialize_f64(self.float().map_err(|_| bad())?)
                }
            }
            b'a' => {
                let nodes = self.array().map_err(|_| bad())?;
                let mut seq = s.serialize_seq(Some(nodes.len()))?;
                for n in nodes {
                    seq.serialize_element(&n.map_err(|_| bad())?)?;
                }
                seq.end()
            }
            b'm' => {
                let mut nodes = self.map().map_err(|_| bad())?;
                let mut map = s.serialize_map(Some(nodes.len() / 2))?;
                while let Some(k) = nodes.next() {
                    let k = k.map_err(|_| bad())?;
                    let v = nodes.next().ok_or_else(bad)?.map_err(|_| bad())?;
                    map.serialize_entry(&k, &v)?;
                }
                map.end()
            }
            _ => Err(bad()),
        }
    }
}
