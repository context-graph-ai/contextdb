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

/// Emit primitive canonical fields into an exact, originally admitted buffer.
/// The finite callers supply a checked nonallocating plan; no JSON/key/row
/// allocating walk is permitted without its own separately retained scratch.
pub(crate) fn prepare_encoder_owned(
    db: &crate::Database,
    capacity: usize,
    emit: impl FnOnce(&mut Encoder) -> Result<()>,
) -> Result<super::preparation::OwnedPreparationValue<Vec<u8>>> {
    super::preparation::OwnedPreparationValue::try_produce(db, capacity, || {
        let mut encoder = Encoder(Vec::with_capacity(capacity));
        if encoder.0.capacity() != capacity {
            return Err(invalid());
        }
        emit(&mut encoder)?;
        if encoder.0.len() != capacity || encoder.0.capacity() != capacity {
            return Err(invalid());
        }
        Ok(encoder.0)
    })
}

/// Count and emit the same fmt arguments without constructing an intermediate
/// key. Output capacity is admitted before String construction.
pub(crate) fn prepare_string_owned(
    db: &crate::Database,
    args: std::fmt::Arguments<'_>,
) -> Result<super::preparation::OwnedPreparationValue<String>> {
    struct Count(usize);
    impl std::fmt::Write for Count {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            self.0 = self.0.checked_add(value.len()).ok_or(std::fmt::Error)?;
            Ok(())
        }
    }
    use std::fmt::Write;
    let mut count = Count(0);
    count.write_fmt(args).map_err(|_| invalid())?;
    super::preparation::OwnedPreparationValue::try_produce(db, count.0, || {
        let mut output = String::with_capacity(count.0);
        output.write_fmt(args).map_err(|_| invalid())?;
        if output.len() != count.0 || output.capacity() != count.0 {
            return Err(invalid());
        }
        Ok(output)
    })
}

pub(crate) fn node_bytes(node: &str) -> Result<[u8; 32]> {
    node_bytes_with_error(node, invalid)
}
pub(crate) fn node_bytes_with_error<E>(
    node: &str,
    invalid: impl Fn() -> E,
) -> std::result::Result<[u8; 32], E> {
    if node.len() != 64 {
        return Err(invalid());
    }
    let mut bytes = [0; 32];
    for (i, pair) in node.as_bytes().chunks_exact(2).enumerate() {
        let a = (pair[0] as char).to_digit(16).ok_or_else(&invalid)?;
        let b = (pair[1] as char).to_digit(16).ok_or_else(&invalid)?;
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
#[cfg(feature = "test-seams")]
pub(crate) fn decode_diagnostic_resolution(tag: u8) -> Result<crate::sync_types::ConflictPolicy> {
    match tag {
        0 => Ok(crate::sync_types::ConflictPolicy::InsertIfNotExists),
        1 => Ok(crate::sync_types::ConflictPolicy::ServerWins),
        2 => Ok(crate::sync_types::ConflictPolicy::EdgeWins),
        3 => Ok(crate::sync_types::ConflictPolicy::LatestWins),
        _ => Err(invalid()),
    }
}

// Private canonical sink. Count, hash and exact-buffer emission share the
// existing field order and tags; no native allocation occurs in this walk.
enum FiniteSink<'a> {
    Count(usize),
    Hash(blake3::Hasher),
    Buffer(&'a mut Vec<u8>),
    Slice { bytes: &'a mut [u8], at: usize },
}
pub(crate) struct FiniteEncoder<'a> {
    sink: FiniteSink<'a>,
    failed: bool,
}
impl FiniteEncoder<'_> {
    pub fn u8(&mut self, v: u8) {
        self.raw(&[v]);
    }
    pub fn u32(&mut self, v: u32) {
        self.raw(&v.to_be_bytes());
    }
    pub fn u64(&mut self, v: u64) {
        self.raw(&v.to_be_bytes());
    }
    pub fn raw(&mut self, v: &[u8]) {
        match &mut self.sink {
            FiniteSink::Count(count) => match count.checked_add(v.len()) {
                Some(next) => *count = next,
                None => self.failed = true,
            },
            FiniteSink::Hash(hash) => {
                hash.update(v);
            }
            FiniteSink::Slice { bytes, at } => {
                if let Some(end) = at.checked_add(v.len()).filter(|end| *end <= bytes.len()) {
                    bytes[*at..end].copy_from_slice(v);
                    *at = end;
                } else {
                    self.failed = true;
                }
            }
            FiniteSink::Buffer(buffer) => {
                if v.len() > buffer.capacity() - buffer.len() {
                    self.failed = true;
                } else {
                    buffer.extend_from_slice(v);
                }
            }
        }
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
                // The pinned serde_json Map is the same lexical BTreeMap
                // priced by the native Value copy kernel.
                for (key, value) in o {
                    self.string(key);
                    self.json(value);
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
        self.u64(1 + key.rest.len() as u64);
        self.string(&key.column);
        self.value(&key.value);
        for (column, value) in &key.rest {
            self.string(column);
            self.value(value);
        }
    }
    pub fn reference(&mut self, table: &str, key: &NaturalKey) {
        self.string(table);
        self.key(key);
    }
    pub fn row(&mut self, values: &HashMap<String, Value>, exclude: &[String]) {
        let count = values.keys().filter(|key| !exclude.contains(key)).count();
        self.u64(count as u64);
        let mut previous: Option<&String> = None;
        for _ in 0..count {
            let column = values
                .keys()
                .filter(|key| !exclude.contains(key) && previous.is_none_or(|p| *key > p))
                .min()
                .expect("unique row columns");
            previous = Some(column);
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

    pub(crate) fn finish(&self) -> Result<()> {
        if self.failed { Err(invalid()) } else { Ok(()) }
    }
}
pub(crate) fn finite_bytes_owned(
    db: &crate::Database,
    walk: impl Fn(&mut FiniteEncoder<'_>) -> Result<()>,
) -> Result<super::preparation::OwnedPreparationValue<Vec<u8>>> {
    let mut count = FiniteEncoder {
        sink: FiniteSink::Count(0),
        failed: false,
    };
    walk(&mut count)?;
    count.finish()?;
    let FiniteSink::Count(capacity) = count.sink else {
        unreachable!()
    };
    super::preparation::OwnedPreparationValue::try_produce(db, capacity, || {
        let mut value = Vec::with_capacity(capacity);
        if value.capacity() != capacity {
            return Err(invalid());
        }
        {
            let mut emit = FiniteEncoder {
                sink: FiniteSink::Buffer(&mut value),
                failed: false,
            };
            walk(&mut emit)?;
            emit.finish()?;
        }
        if value.len() != capacity || value.capacity() != capacity {
            return Err(invalid());
        }
        Ok(value)
    })
}
pub(crate) fn finite_hash(
    walk: impl FnOnce(&mut FiniteEncoder<'_>) -> Result<()>,
) -> Result<[u8; 32]> {
    finite_hash_with_error(walk, invalid)
}

// Native callers keep scalar refusals; Legacy callers retain their error type.
pub(crate) fn finite_hash_with_error<E>(
    walk: impl FnOnce(&mut FiniteEncoder<'_>) -> std::result::Result<(), E>,
    invalid: impl FnOnce() -> E,
) -> std::result::Result<[u8; 32], E> {
    let mut encoder = FiniteEncoder {
        sink: FiniteSink::Hash(blake3::Hasher::new()),
        failed: false,
    };
    walk(&mut encoder)?;
    if encoder.failed {
        return Err(invalid());
    }
    let FiniteSink::Hash(hash) = encoder.sink else {
        unreachable!()
    };
    Ok(*hash.finalize().as_bytes())
}
pub(crate) fn finite_row_digest(
    values: &HashMap<String, Value>,
    content: Option<&[String]>,
) -> Result<[u8; 32]> {
    finite_hash(|e| {
        e.string(if content.is_some() {
            "delivery-content-row.v1"
        } else {
            "row.v1"
        });
        if let Some(exclude) = content {
            e.strings(exclude);
        }
        e.row(values, content.unwrap_or(&[]));
        Ok(())
    })
}
pub(crate) fn finite_policy_digest(
    table: &str,
    policy: &ApplicationTablePolicy,
) -> Result<[u8; 32]> {
    finite_hash(|e| {
        e.string("tenant-table-policy.v1");
        e.string(table);
        e.policy(policy)
    })
}

pub(crate) fn finite_len(walk: impl FnOnce(&mut FiniteEncoder<'_>) -> Result<()>) -> Result<usize> {
    finite_len_with_error(walk, invalid)
}

pub(crate) fn finite_len_with_error<E>(
    walk: impl FnOnce(&mut FiniteEncoder<'_>) -> std::result::Result<(), E>,
    invalid: impl FnOnce() -> E,
) -> std::result::Result<usize, E> {
    let mut encoder = FiniteEncoder {
        sink: FiniteSink::Count(0),
        failed: false,
    };
    walk(&mut encoder)?;
    if encoder.failed {
        return Err(invalid());
    }
    let FiniteSink::Count(count) = encoder.sink else {
        unreachable!()
    };
    Ok(count)
}

/// Fill the exact caller-admitted slice, refusing both overflow and short fill.
pub(crate) fn finite_emit_with_error<E>(
    bytes: &mut [u8],
    walk: impl FnOnce(&mut FiniteEncoder<'_>) -> std::result::Result<(), E>,
    invalid: impl FnOnce() -> E,
) -> std::result::Result<(), E> {
    let mut encoder = FiniteEncoder {
        sink: FiniteSink::Slice { bytes, at: 0 },
        failed: false,
    };
    walk(&mut encoder)?;
    let FiniteSink::Slice { bytes, at } = encoder.sink else {
        unreachable!()
    };
    if encoder.failed || at != bytes.len() {
        return Err(invalid());
    }
    Ok(())
}
