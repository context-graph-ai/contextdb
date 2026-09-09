//! Canonical commitments for delivery metadata. Wire/config serialization is
//! deliberately separate from these length-delimited, typed hash inputs.
use crate::custody_types::ApplicationTablePolicy;
use crate::sync_types::NaturalKey;
use contextdb_core::{
    ConflictPolicy, EdgeDiscardMode, Error, HistoryPolicy, Result, SyncDirection, Value,
};
use std::collections::HashMap;

#[track_caller]
pub(crate) fn invalid() -> Error {
    #[cfg(feature = "test-seams")]
    if std::env::var_os("CONTEXTDB_CUSTODY_TRACE_INVALID").is_some() {
        eprintln!("custody invalid at {}", std::panic::Location::caller());
    }

    Error::SyncError("invalid canonical delivery metadata".into())
}

#[cfg_attr(not(feature = "test-seams"), derive(Default))]
pub(crate) struct Encoder(pub Vec<u8>);
#[cfg(feature = "test-seams")]
thread_local! { static ENCODERS_STARTED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) }; }
#[cfg(feature = "test-seams")]
pub(crate) fn encoders_started_for_test() -> u64 {
    ENCODERS_STARTED.with(std::cell::Cell::get)
}
#[cfg(feature = "test-seams")]
impl Default for Encoder {
    fn default() -> Self {
        #[cfg(feature = "test-seams")]
        ENCODERS_STARTED.with(|count| count.set(count.get() + 1));
        Self(Vec::new())
    }
}
impl Encoder {
    pub fn domain(name: &str) -> Self {
        let mut e = Self::default();
        e.string(name);
        e
    }
    pub fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    pub fn u32(&mut self, v: u32) {
        self.0.extend(v.to_be_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.0.extend(v.to_be_bytes());
    }
    pub fn raw(&mut self, v: &[u8]) {
        self.0.extend(v);
    }
    pub fn bytes(&mut self, v: &[u8]) {
        self.u64(v.len() as u64);
        self.raw(v);
    }
    pub fn string(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }
    pub fn node(&mut self, v: &str) -> Result<()> {
        self.raw(&node_bytes(v)?);
        Ok(())
    }
    pub fn option<T>(&mut self, v: Option<&T>, write: impl FnOnce(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                write(self, v);
            }
        }
    }
    pub fn json(&mut self, v: &serde_json::Value) {
        use serde_json::Value as J;
        match v {
            J::Null => self.u8(0),
            J::Bool(b) => {
                self.u8(1);
                self.u8(u8::from(*b));
            }
            J::String(s) => {
                self.u8(2);
                self.string(s);
            }
            J::Array(a) => {
                self.u8(3);
                self.u64(a.len() as u64);
                for v in a {
                    self.json(v);
                }
            }
            J::Object(o) => {
                self.u8(4);
                self.u64(o.len() as u64);
                let mut keys: Vec<_> = o.keys().collect();
                keys.sort();
                for k in keys {
                    self.string(k);
                    self.json(&o[k]);
                }
            }
            J::Number(n) => {
                if let Some(v) = n.as_i64() {
                    self.u8(5);
                    self.raw(&v.to_be_bytes());
                } else if let Some(v) = n.as_u64() {
                    self.u8(6);
                    self.u64(v);
                } else {
                    self.u8(7);
                    self.u64(n.as_f64().expect("JSON number is representable").to_bits());
                }
            }
        }
    }
    pub fn value(&mut self, v: &Value) {
        match v {
            Value::Null => self.u8(0),
            Value::Bool(b) => {
                self.u8(1);
                self.u8(u8::from(*b));
            }
            Value::Int64(v) => {
                self.u8(2);
                self.raw(&v.to_be_bytes());
            }
            Value::Float64(v) => {
                self.u8(3);
                self.u64(v.to_bits());
            }
            Value::Text(v) => {
                self.u8(4);
                self.string(v);
            }
            Value::Uuid(v) => {
                self.u8(5);
                self.raw(v.as_bytes());
            }
            Value::Timestamp(v) => {
                self.u8(6);
                self.raw(&v.to_be_bytes());
            }
            Value::Json(v) => {
                self.u8(7);
                self.json(v);
            }
            Value::Vector(v) => {
                self.u8(8);
                self.u64(v.len() as u64);
                for f in v {
                    self.u32(f.to_bits());
                }
            }
            Value::TxId(v) => {
                self.u8(9);
                self.u64(v.0);
            }
        }
    }
    pub fn key(&mut self, key: &NaturalKey) {
        self.u64(key.pairs().len() as u64);
        for (column, value) in key.pairs() {
            self.string(&column);
            self.value(&value);
        }
    }
    pub fn reference(&mut self, table: &str, key: &NaturalKey) {
        self.string(table);
        self.key(key);
    }
    pub fn row(&mut self, values: &HashMap<String, Value>, exclude: &[String]) {
        let mut columns: Vec<_> = values.keys().filter(|k| !exclude.contains(k)).collect();
        columns.sort();
        self.u64(columns.len() as u64);
        for column in columns {
            self.string(column);
            self.value(&values[column]);
        }
    }
    pub fn strings(&mut self, values: &[String]) {
        self.u64(values.len() as u64);
        for value in values {
            self.string(value);
        }
    }
    pub fn policy(&mut self, p: &ApplicationTablePolicy) -> Result<()> {
        self.u8(match p.sync_direction {
            SyncDirection::None => 0,
            SyncDirection::Push => 1,
            SyncDirection::Pull => 2,
            SyncDirection::Both => 3,
        });
        self.u8(match p.sync_conflict {
            ConflictPolicy::KEEP_FIRST => 0,
            ConflictPolicy::KEEP_LATEST => 1,
            _ => return Err(invalid()),
        });
        self.u8(u8::from(p.immutable));
        self.option(p.retain.as_ref(), |e, r| {
            e.u64(r.seconds);
            e.u8(u8::from(r.sync_safe));
        });
        self.u8(match p.history {
            HistoryPolicy::All => 0,
            HistoryPolicy::CurrentOnly => 1,
        });
        self.option(p.manifest_tables.as_ref(), |e, t| e.strings(t));
        self.u8(match p.edge_discard {
            EdgeDiscardMode::Never => 0,
            EdgeDiscardMode::AfterOutcome => 1,
            EdgeDiscardMode::Always => 2,
        });
        Ok(())
    }
    pub fn digest(&self) -> [u8; 32] {
        *blake3::hash(&self.0).as_bytes()
    }
}

pub(crate) fn node_bytes(node: &str) -> Result<[u8; 32]> {
    if node.len() != 64 {
        return Err(invalid());
    }
    let mut bytes = [0; 32];
    for (i, pair) in node.as_bytes().chunks_exact(2).enumerate() {
        let a = (pair[0] as char).to_digit(16).ok_or_else(invalid)?;
        let b = (pair[1] as char).to_digit(16).ok_or_else(invalid)?;
        bytes[i] = ((a << 4) | b) as u8;
    }
    Ok(bytes)
}
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("String write");
    }
    out
}
pub(crate) fn reference_key(table: &str, key: &NaturalKey) -> Vec<u8> {
    let mut e = Encoder::default();
    e.reference(table, key);
    e.0
}
pub(crate) fn full_row_digest(values: &HashMap<String, Value>) -> [u8; 32] {
    let mut e = Encoder::domain("row.v1");
    e.row(values, &[]);
    e.digest()
}
pub(crate) fn content_row_digest(values: &HashMap<String, Value>, exclude: &[String]) -> [u8; 32] {
    let mut e = Encoder::domain("delivery-content-row.v1");
    e.strings(exclude);
    e.row(values, exclude);
    e.digest()
}
pub(crate) fn policy_digest(table: &str, policy: &ApplicationTablePolicy) -> Result<[u8; 32]> {
    let mut e = Encoder::domain("tenant-table-policy.v1");
    e.string(table);
    e.policy(policy)?;
    Ok(e.digest())
}
pub(crate) fn diagnostic_resolution(policy: crate::sync_types::ConflictPolicy) -> u8 {
    match policy {
        crate::sync_types::ConflictPolicy::InsertIfNotExists => 0,
        crate::sync_types::ConflictPolicy::ServerWins => 1,
        crate::sync_types::ConflictPolicy::EdgeWins => 2,
        crate::sync_types::ConflictPolicy::LatestWins => 3,
    }
}
pub(crate) fn decode_diagnostic_resolution(tag: u8) -> Result<crate::sync_types::ConflictPolicy> {
    match tag {
        0 => Ok(crate::sync_types::ConflictPolicy::InsertIfNotExists),
        1 => Ok(crate::sync_types::ConflictPolicy::ServerWins),
        2 => Ok(crate::sync_types::ConflictPolicy::EdgeWins),
        3 => Ok(crate::sync_types::ConflictPolicy::LatestWins),
        _ => Err(invalid()),
    }
}
