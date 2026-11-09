//! Borrow graph identities while validating, rather than retaining, properties.
use super::*;
use serde::Deserialize;
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use std::fmt;

pub(super) struct Header<'a> {
    source: &'a [u8],
    target: &'a [u8],
    edge_type: &'a str,
    created_tx: TxId,
    pub(super) deleted_tx: Option<TxId>,
    lsn: Lsn,
}
impl Header<'_> {
    pub(super) fn matches_forward_key(&self, key: &[u8]) -> bool {
        let Some(len) = self.edge_type.len().checked_add(49) else {
            return false;
        };
        key.len() == len
            && key[..16] == *self.source
            && key[16..32] == *self.target
            && key[32..len - 17] == *self.edge_type.as_bytes()
            && key[len - 17] == 0
            && key[len - 16..len - 8] == self.created_tx.0.to_be_bytes()
            && key[len - 8..] == self.lsn.0.to_be_bytes()
    }
}
fn malformed() -> Error {
    Error::StoreCorrupted {
        path: "graph_history".into(),
        reason: "malformed reclaimed graph predecessor".into(),
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Preserve inline typed admission and corruption errors."
)]
pub(super) fn decode(bytes: &[u8]) -> Result<Header<'_>> {
    let mut failure = None;
    let result = bincode::serde::seed_decode_from_slice(
        HeaderSeed(&mut failure),
        bytes,
        bincode::config::standard(),
    );
    if let Some(error) = failure {
        return Err(error);
    }
    match result {
        Ok((header, consumed)) if consumed == bytes.len() => Ok(header),
        _ => Err(malformed()),
    }
}
struct HeaderSeed<'a>(&'a mut Option<Error>);
impl<'de> DeserializeSeed<'de> for HeaderSeed<'_> {
    type Value = Header<'de>;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        d: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        d.deserialize_tuple(7, self)
    }
}
impl<'de> Visitor<'de> for HeaderSeed<'_> {
    type Value = Header<'de>;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a stored adjacency entry")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<Self::Value, A::Error> {
        macro_rules! take {
            ($t:ty) => {
                a.next_element::<$t>()?
                    .ok_or_else(|| de::Error::custom(""))?
            };
        }
        let source = take!(&'de [u8]);
        let target = take!(&'de [u8]);
        if source.len() != 16 || target.len() != 16 {
            return Err(de::Error::custom(""));
        }
        let edge_type = take!(&'de str);
        a.next_element_seed(Properties(self.0))?
            .ok_or_else(|| de::Error::custom(""))?;
        Ok(Header {
            source,
            target,
            edge_type,
            created_tx: take!(TxId),
            deleted_tx: take!(Option<TxId>),
            lsn: take!(Lsn),
        })
    }
}
struct Properties<'a>(&'a mut Option<Error>);
impl<'de> DeserializeSeed<'de> for Properties<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Properties<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stored properties")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> std::result::Result<(), A::Error> {
        while a.next_key::<&'de str>()?.is_some() {
            a.next_value_seed(PropertyValue(&mut *self.0))?;
        }
        Ok(())
    }
}
struct PropertyValue<'a>(&'a mut Option<Error>);
impl<'de> DeserializeSeed<'de> for PropertyValue<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        // Value is a closed bincode enum; every payload shape is visited.
        d.deserialize_enum(
            "Value",
            &[
                "Null",
                "Bool",
                "Int64",
                "Float64",
                "Text",
                "Uuid",
                "Timestamp",
                "Json",
                "Vector",
                "TxId",
            ],
            self,
        )
    }
}
impl<'de> Visitor<'de> for PropertyValue<'_> {
    type Value = ();
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a stored Value")
    }
    fn visit_enum<A: EnumAccess<'de>>(self, a: A) -> std::result::Result<(), A::Error> {
        let (tag, v) = a.variant::<u32>()?;
        match tag {
            0 => v.unit_variant()?,
            1 => {
                v.newtype_variant::<bool>()?;
            }
            2 | 6 => {
                v.newtype_variant::<i64>()?;
            }
            3 => {
                v.newtype_variant::<f64>()?;
            }
            4 => {
                v.newtype_variant::<&'de str>()?;
            }
            5 => {
                if v.newtype_variant::<&'de [u8]>()?.len() != 16 {
                    return Err(de::Error::custom(""));
                }
            }
            7 => {
                let json = v.newtype_variant::<&'de str>()?;
                if let Err(error) = validate_json(json) {
                    *self.0 = Some(error);
                    return Err(de::Error::custom(""));
                }
            }
            8 => {
                v.newtype_variant::<Floats>()?;
            }
            9 => {
                v.newtype_variant::<TxId>()?;
            }
            _ => return Err(de::Error::custom("")),
        }
        Ok(())
    }
}
struct Floats;
impl<'de> Deserialize<'de> for Floats {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_seq(Self)
    }
}
impl<'de> Visitor<'de> for Floats {
    type Value = Self;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("stored f32 values")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<Self, A::Error> {
        while a.next_element::<f32>()?.is_some() {}
        Ok(self)
    }
}
// Ordinary recursive JSON visits retain serde_json's numeric, escape and depth
// checks. Strings and map entries are consumed immediately, never collected.
struct Json;
impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: de::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        d.deserialize_any(Self)
    }
}
impl<'de> Visitor<'de> for Json {
    type Value = Self;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_bool<E: de::Error>(self, _: bool) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_str<E: de::Error>(self, _: &str) -> std::result::Result<Self, E> {
        Ok(self)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> std::result::Result<Self, A::Error> {
        while a.next_element::<Json>()?.is_some() {}
        Ok(self)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> std::result::Result<Self, A::Error> {
        while a.next_key::<Json>()?.is_some() {
            a.next_value::<Json>()?;
        }
        Ok(self)
    }
}
struct JsonWorkspace(crate::read_image_memory::ImageMemoryHandle, usize);
impl Drop for JsonWorkspace {
    fn drop(&mut self) {
        self.0.release(self.1);
    }
}
#[expect(
    clippy::result_large_err,
    reason = "Preserve inline typed admission and corruption errors."
)]
fn validate_json(text: &str) -> Result<()> {
    // Pinned serde_json1.0.149: one amortized byte scratch and recursion128.
    // Each native failure frame uses the existing calibrated error bound.
    let bytes = text
        .len()
        .checked_mul(2)
        .map(|n| n.max(8))
        .and_then(|n| {
            crate::funded_error::json_native_error_workspace_bytes()
                .checked_mul(128)
                .and_then(|errors| n.checked_add(errors))
        })
        .ok_or_else(malformed)?;
    let owner = crate::read_image_memory::active_owner()?;
    owner.reserve(bytes)?;
    let _workspace = JsonWorkspace(owner, bytes);
    let mut parser = serde_json::Deserializer::from_str(text);
    let result = Json::deserialize(&mut parser).and_then(|_| parser.end());
    drop(parser);
    match result {
        Ok(()) => Ok(()),
        Err(error) => {
            drop(error);
            Err(malformed())
        }
    }
}
