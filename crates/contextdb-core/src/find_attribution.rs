//! Where one statement's read work actually went.
//!
//! A measurement seam, not a product surface. It is compiled in always and
//! costs one relaxed atomic load per call site when it is off, which it is
//! unless `CONTEXTDB_FIND_ATTRIBUTION` names a file to append records to. A
//! record is one statement: its named work counts, its named stage times and
//! one entry per partition the vector search entered. Nothing here changes an
//! answer, a budget or a receipt.
//!
//! The sink deliberately holds one record at a time. A statement is executed
//! by one thread, and a measurement that silently interleaved two of them
//! would attribute work to the wrong statement.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

static ENABLED: AtomicBool = AtomicBool::new(false);
static SINK: OnceLock<Option<Mutex<Sink>>> = OnceLock::new();

struct Sink {
    file: File,
    record: Option<Record>,
}

#[derive(Default)]
struct Record {
    label: String,
    counts: BTreeMap<String, u64>,
    stage_nanos: BTreeMap<String, u64>,
    partitions: Vec<Vec<(String, Field)>>,
    loads: Vec<Vec<(String, Field)>>,
    started: Option<Instant>,
    // Inline observation state has no per-request heap admission or refund.
    native_reclaim: NativeReclaimMeters,
    native_reclaim_present: bool,
}

fn sink() -> Option<&'static Mutex<Sink>> {
    SINK.get_or_init(|| {
        let path = std::env::var_os("CONTEXTDB_FIND_ATTRIBUTION")?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()?;
        ENABLED.store(true, Ordering::Relaxed);
        Some(Mutex::new(Sink { file, record: None }))
    })
    .as_ref()
}

/// Whether records are being collected at all. Every other call in this module
/// is a no-op when this is false.
#[inline]
pub fn enabled() -> bool {
    if ENABLED.load(Ordering::Relaxed) {
        return true;
    }
    // The first call resolves the environment once; afterwards the atomic
    // answers on its own.
    static RESOLVED: OnceLock<bool> = OnceLock::new();
    *RESOLVED.get_or_init(|| sink().is_some())
}

fn with_record(note: impl FnOnce(&mut Record)) {
    if !enabled() {
        return;
    }
    let Some(sink) = sink() else {
        return;
    };
    let Ok(mut sink) = sink.lock() else {
        return;
    };
    if let Some(record) = sink.record.as_mut() {
        note(record);
    }
}

/// Open a record for one statement. An already-open record is kept: the outer
/// statement owns the attribution and a nested read is its work, not its own.
pub fn begin(label: &str) {
    if !enabled() {
        return;
    }
    let Some(sink) = sink() else {
        return;
    };
    let Ok(mut sink) = sink.lock() else {
        return;
    };
    if sink.record.is_some() {
        return;
    }
    sink.record = Some(Record {
        label: label.to_string(),
        started: Some(Instant::now()),
        ..Record::default()
    });
}

/// Close the record opened by the matching `begin` and append it to the sink.
pub fn finish() {
    if finish_native_record() {
        return;
    }
    #[cfg(test)]
    find_attribution_native_tests::observe_legacy_finish_lock_gap();
    if !enabled() {
        return;
    }
    let Some(sink) = sink() else {
        return;
    };
    let Ok(mut sink) = sink.lock() else {
        return;
    };
    let Some(record) = sink.record.take() else {
        return;
    };
    if record.native_reclaim_present {
        let total_nanos = record
            .started
            .map(|started| started.elapsed().as_nanos() as u64)
            .unwrap_or_default();
        let _appended = write_native_record(&mut sink.file, &record, total_nanos);
        let _flushed = sink.file.flush();
        return;
    }
    let total_nanos = record
        .started
        .map(|started| started.elapsed().as_nanos() as u64)
        .unwrap_or_default();
    let line = serde_json::json!({
        "label": record.label,
        "total_ms": total_nanos as f64 / 1e6,
        "counts": record.counts,
        "stage_ms": record
            .stage_nanos
            .iter()
            .map(|(name, nanos)| (name.clone(), *nanos as f64 / 1e6))
            .collect::<BTreeMap<_, _>>(),
        "partitions": record
            .partitions
            .iter()
            .map(|entry| {
                entry
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_json()))
                    .collect::<serde_json::Map<_, _>>()
            })
            .collect::<Vec<_>>(),
        "loads": record
            .loads
            .iter()
            .map(|entry| {
                entry
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_json()))
                    .collect::<serde_json::Map<_, _>>()
            })
            .collect::<Vec<_>>(),
    });
    let _appended = writeln!(sink.file, "{line}");
    let _flushed = sink.file.flush();
}

// This sibling streams only an open record that observed native pressure.
// It never resolves the environment or opens a file; all other records use
// the unchanged legacy finish body.
fn finish_native_record() -> bool {
    if !ENABLED.load(Ordering::Relaxed) {
        return false;
    }
    let Some(Some(sink)) = SINK.get() else {
        return false;
    };
    let Ok(mut sink) = sink.lock() else {
        return false;
    };
    if sink
        .record
        .as_ref()
        .is_none_or(|record| !record.native_reclaim_present)
    {
        return false;
    }
    let record = sink.record.take().expect("open native attribution record");
    let total_nanos = record
        .started
        .map(|started| started.elapsed().as_nanos() as u64)
        .unwrap_or_default();
    let _appended = write_native_record(&mut sink.file, &record, total_nanos);
    let _flushed = sink.file.flush();
    true
}

// Write borrowed fields directly. Even an output error needs no allocating JSON
// error wrapper, Value, copied key or intermediate collection in this path.
fn write_json_string(writer: &mut impl Write, value: &str) -> std::io::Result<()> {
    writer.write_all(b"\"")?;
    let bytes = value.as_bytes();
    let mut start = 0;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (index, &byte) in bytes.iter().enumerate() {
        if byte == b'"' || byte == b'\\' || byte < 0x20 {
            writer.write_all(&bytes[start..index])?;
            match byte {
                b'"' => writer.write_all(b"\\\"")?,
                b'\\' => writer.write_all(b"\\\\")?,
                b'\x08' => writer.write_all(b"\\b")?,
                b'\t' => writer.write_all(b"\\t")?,
                b'\n' => writer.write_all(b"\\n")?,
                b'\x0c' => writer.write_all(b"\\f")?,
                b'\r' => writer.write_all(b"\\r")?,
                _ => writer.write_all(&[
                    b'\\', b'u', b'0', b'0',
                    HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize],
                ])?,
            }
            start = index + 1;
        }
    }
    writer.write_all(&bytes[start..])?;
    writer.write_all(b"\"")
}

fn write_json_millis(writer: &mut impl Write, value: f64) -> std::io::Result<()> {
    if value.is_finite() {
        // Use the existing JSON formatter's fixed float buffer directly,
        // without Serializer's allocating error conversion on I/O failure.
        serde_json::ser::Formatter::write_f64(
            &mut serde_json::ser::CompactFormatter, writer, value,
        )
    } else {
        writer.write_all(b"null")
    }
}

fn write_json_key(
    writer: &mut impl Write, name: &str, first: &mut bool,
) -> std::io::Result<()> {
    if !*first {
        writer.write_all(b",")?;
    }
    *first = false;
    write_json_string(writer, name)?;
    writer.write_all(b":")
}

fn write_native_counts(writer: &mut impl Write, record: &Record) -> std::io::Result<()> {
    writer.write_all(b"{")?;
    let mut first = true;
    let mut legacy = record.counts.iter().peekable();
    // Both streams are sorted. Equal keys are emitted once with their sum;
    // nothing is inserted into the preexisting dynamic record.
    for (name, amount) in record.native_reclaim.labels() {
        let Some(mut amount) = amount else {
            continue;
        };
        while legacy.peek().is_some_and(|(key, _)| key.as_str() < name) {
            let (key, value) = legacy.next().expect("peeked legacy count");
            write_json_key(writer, key, &mut first)?;
            write!(writer, "{value}")?;
        }
        if legacy.peek().is_some_and(|(key, _)| key.as_str() == name) {
            amount += *legacy.next().expect("matching legacy count").1;
        }
        write_json_key(writer, name, &mut first)?;
        write!(writer, "{amount}")?;
    }
    for (name, amount) in legacy {
        write_json_key(writer, name, &mut first)?;
        write!(writer, "{amount}")?;
    }
    writer.write_all(b"}")
}

fn write_native_entries(
    writer: &mut impl Write, entries: &[Vec<(String, Field)>],
) -> std::io::Result<()> {
    writer.write_all(b"[")?;
    let mut first_entry = true;
    for entry in entries {
        if !first_entry {
            writer.write_all(b",")?;
        }
        first_entry = false;
        writer.write_all(b"{")?;
        let mut first = true;
        let mut previous: Option<&str> = None;
        // The legacy JSON map sorts keys and retains the last duplicate. Do
        // the same over borrowed views, without building another map.
        while let Some(name) = entry.iter().map(|(name, _)| name.as_str())
            .filter(|name| previous.is_none_or(|previous| *name > previous)).min()
        {
            let value = &entry.iter().rev().find(|(key, _)| key.as_str() == name)
                .expect("borrowed entry key").1;
            write_json_key(writer, name, &mut first)?;
            match value {
                Field::Text(value) => write_json_string(writer, value)?,
                Field::Count(value) => write!(writer, "{value}")?,
                Field::Millis(value) => write_json_millis(writer, *value)?,
            }
            previous = Some(name);
        }
        writer.write_all(b"}")?;
    }
    writer.write_all(b"]")
}

fn write_native_record(
    writer: &mut impl Write, record: &Record, total_nanos: u64,
) -> std::io::Result<()> {
    writer.write_all(b"{\"counts\":")?;
    write_native_counts(writer, record)?;
    writer.write_all(b",\"label\":")?;
    write_json_string(writer, &record.label)?;
    writer.write_all(b",\"loads\":")?;
    write_native_entries(writer, &record.loads)?;
    writer.write_all(b",\"partitions\":")?;
    write_native_entries(writer, &record.partitions)?;
    writer.write_all(b",\"stage_ms\":{")?;
    let mut first = true;
    for (name, nanos) in &record.stage_nanos {
        write_json_key(writer, name, &mut first)?;
        write_json_millis(writer, *nanos as f64 / 1e6)?;
    }
    writer.write_all(b"},\"total_ms\":")?;
    write_json_millis(writer, total_nanos as f64 / 1e6)?;
    writer.write_all(b"}\n")
}

/// Add to a named work count for the open record.
pub fn count(name: &str, amount: u64) {
    with_record(|record| {
        *record.counts.entry(name.to_string()).or_default() += amount;
    });
}

/// One named value of a partition entry. Kept deliberately small so the
/// crates that record work do not take a serialization dependency for it.
#[derive(Debug, Clone)]
pub enum Field {
    Text(String),
    Count(u64),
    Millis(f64),
}

impl Field {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Field::Text(value) => serde_json::Value::from(value.as_str()),
            Field::Count(value) => serde_json::Value::from(*value),
            Field::Millis(value) => serde_json::Value::from(*value),
        }
    }
}

/// Record one partition the vector search entered, in the order it entered it.
pub fn note_partition(entry: Vec<(String, Field)>) {
    with_record(|record| record.partitions.push(entry));
}

/// Append one standalone line outside any statement's record: work a reader
/// does before its first statement, such as opening its image, which no
/// `begin`/`finish` pair brackets.
pub fn note_event(label: &str, fields: Vec<(String, Field)>) {
    if !enabled() {
        return;
    }
    let Some(sink) = sink() else {
        return;
    };
    let Ok(mut sink) = sink.lock() else {
        return;
    };
    let line = serde_json::json!({
        "event": label,
        "fields": fields
            .iter()
            .map(|(name, value)| (name.clone(), value.to_json()))
            .collect::<serde_json::Map<_, _>>(),
    });
    let _appended = writeln!(sink.file, "{line}");
    let _flushed = sink.file.flush();
}

/// Record one saved graph the statement loaded, in the order it loaded it:
/// what the load was priced at, what it admitted, what it holds, what it
/// hashed and how long it took.
pub fn note_load(entry: Vec<(String, Field)>) {
    with_record(|record| record.loads.push(entry));
}

/// Accumulate wall time into a named stage until the returned guard drops.
/// Stages are named so they do not nest; a stage entered inside another is
/// counted in both, and the report says so.
#[must_use]
pub fn stage(name: &'static str) -> StageGuard {
    StageGuard {
        name,
        started: enabled().then(Instant::now),
    }
}

pub struct StageGuard {
    name: &'static str,
    started: Option<Instant>,
}

impl Drop for StageGuard {
    fn drop(&mut self) {
        let Some(started) = self.started else {
            return;
        };
        let nanos = started.elapsed().as_nanos() as u64;
        let name = self.name;
        with_record(|record| {
            *record.stage_nanos.entry(name.to_string()).or_default() += nanos;
        });
    }
}

/// Serving-page meters.
///
/// A graph walk reaches a stored vector through a page of the serving cache.
/// Whether that page was already resident is what separates a walk that reads
/// its partition once from one that re-reads it, so the meters are process
/// counters the partition entries difference rather than per-call records:
/// a walk touches pages far too often for a record each.
static PAGE_LOOKUPS: AtomicU64 = AtomicU64::new(0);
static PAGE_HITS: AtomicU64 = AtomicU64::new(0);
static PAGE_FILLS: AtomicU64 = AtomicU64::new(0);
static PAGE_FILL_BYTES: AtomicU64 = AtomicU64::new(0);

#[inline]
pub fn note_page_lookup(hit: bool) {
    if !enabled() {
        return;
    }
    PAGE_LOOKUPS.fetch_add(1, Ordering::Relaxed);
    if hit {
        PAGE_HITS.fetch_add(1, Ordering::Relaxed);
    }
}

#[inline]
pub fn note_page_fill(bytes: usize) {
    if !enabled() {
        return;
    }
    PAGE_FILLS.fetch_add(1, Ordering::Relaxed);
    PAGE_FILL_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
}

/// Lookups, hits, fills and filled bytes so far in this process.
pub fn page_meters() -> (u64, u64, u64, u64) {
    (
        PAGE_LOOKUPS.load(Ordering::Relaxed),
        PAGE_HITS.load(Ordering::Relaxed),
        PAGE_FILLS.load(Ordering::Relaxed),
        PAGE_FILL_BYTES.load(Ordering::Relaxed),
    )
}

/// Saved-graph load meters.
///
/// How many saved graphs this thread loaded, how many bytes they held, and
/// every full pass a load made over those bytes to check them -- so a load
/// entry, or a proof, can say how many passes one load made and over how many
/// bytes. Unlike the rest of this module they are always on: a load happens a
/// handful of times per statement, and a proof must be able to read them
/// without a sink. They are per thread because a statement's loads run on the
/// thread that executes it, and a proof must not see another test's loads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadMeters {
    pub loads: u64,
    pub saved_bytes: u64,
    pub hash_passes: u64,
    pub hashed_bytes: u64,
    /// What admission priced the loads at before reading them.
    pub priced_bytes: u64,
    /// The least each load could have been admitted at: its topology and a
    /// one-page serving cache.
    pub minimum_bytes: u64,
    /// What each load was admitted at, its serving-cache share included.
    pub admitted_bytes: u64,
    /// What each loaded graph holds once decoded.
    pub held_bytes: u64,
    /// Loads the memory budget refused after they were sized.
    pub refused: u64,
    /// The serving cache each load was given.
    pub cache_bytes: u64,
    /// Serving pages resident generations gave back so a load could fit.
    pub given_back_bytes: u64,
    /// Loads left with the least serving cache, one page, although their own
    /// pages and the reader's ceiling allowed them more.
    pub least_cache_loads: u64,
}

/// Fixed counters recorded by native pressure reclaimers.
///
/// These stay outside the allocating attribution record because a native
/// refusal may be running after its memory admission has failed.  They are
/// per-thread for the same reason as `LoadMeters`: a caller can sample only
/// the reclamation it caused without observing another statement.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NativeReclaimMeters {
    pub serving_page_give_back_short: u64,
    pub serving_page_bytes_given_back: u64,
    pub serving_page_give_back_reached_final_pass: bool,
    pub graph_reclaims_over_budget: u64,
    pub graph_reclaim_required_bytes: u64,
    pub graph_reclaim_available_bytes: u64,
    pub sealed_generations_evicted: u64,
}

/// A fixed, nestable native-pressure meter scope.
#[doc(hidden)]
pub struct NativeReclaimMeterScope {
    prior: NativeReclaimMeters,
    active: bool,
}

/// What one saved-graph load was priced at, admitted at and holds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LoadCharge {
    pub saved_bytes: usize,
    pub priced_bytes: usize,
    pub minimum_bytes: usize,
    pub admitted_bytes: usize,
    pub held_bytes: usize,
    pub cache_bytes: usize,
    /// The least serving cache this load could have been given.
    pub minimum_cache_bytes: usize,
    /// The most serving cache this load could have used.
    pub maximum_cache_bytes: usize,
}

thread_local! {
    static LOAD_METERS: std::cell::Cell<LoadMeters> =
        const {
            std::cell::Cell::new(LoadMeters {
                loads: 0,
                saved_bytes: 0,
                hash_passes: 0,
                hashed_bytes: 0,
                priced_bytes: 0,
                minimum_bytes: 0,
                admitted_bytes: 0,
                held_bytes: 0,
                refused: 0,
                cache_bytes: 0,
                given_back_bytes: 0,
                least_cache_loads: 0,
            })
        };
    static NATIVE_RECLAIM_METERS: std::cell::Cell<NativeReclaimMeters> =
        const { std::cell::Cell::new(NativeReclaimMeters {
            serving_page_give_back_short: 0,
            serving_page_bytes_given_back: 0,
            serving_page_give_back_reached_final_pass: false,
            graph_reclaims_over_budget: 0,
            graph_reclaim_required_bytes: 0,
            graph_reclaim_available_bytes: 0,
            sealed_generations_evicted: 0,
        }) };
}

#[inline]
#[doc(hidden)]
pub fn note_native_serving_page_give_back_short() {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.serving_page_give_back_short = now.serving_page_give_back_short.saturating_add(1);
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_serving_page_bytes_given_back(bytes: usize) {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.serving_page_bytes_given_back = now
            .serving_page_bytes_given_back
            .saturating_add(bytes as u64);
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_serving_page_give_back_reached_final_pass() {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.serving_page_give_back_reached_final_pass = true;
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_graph_reclaims_over_budget() {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.graph_reclaims_over_budget = now.graph_reclaims_over_budget.saturating_add(1);
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_graph_reclaim_required_bytes(bytes: usize) {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.graph_reclaim_required_bytes = now
            .graph_reclaim_required_bytes
            .saturating_add(bytes as u64);
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_graph_reclaim_available_bytes(bytes: usize) {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.graph_reclaim_available_bytes = now
            .graph_reclaim_available_bytes
            .saturating_add(bytes as u64);
        meters.set(now);
    });
}

#[inline]
#[doc(hidden)]
pub fn note_native_sealed_generations_evicted(evicted: usize) {
    NATIVE_RECLAIM_METERS.with(|meters| {
        let mut now = meters.get();
        now.sealed_generations_evicted = now.sealed_generations_evicted.saturating_add(evicted as u64);
        meters.set(now);
    });
}

/// Start a fixed, non-allocating meter scope around native pressure work.
#[doc(hidden)]
pub fn native_reclaim_meter_scope() -> NativeReclaimMeterScope {
    let prior = NATIVE_RECLAIM_METERS.with(|meters| {
        let prior = meters.get();
        meters.set(NativeReclaimMeters::default());
        prior
    });
    NativeReclaimMeterScope { prior, active: true }
}

impl NativeReclaimMeterScope {
    /// Bind this scope's counters to the currently open record, restore the
    /// enclosing scope, and return an observational copy. No heap admission is
    /// needed.
    #[doc(hidden)]
    pub fn finish(mut self) -> NativeReclaimMeters {
        self.active = false;
        restore_native_reclaim_meter_scope(self.prior)
    }
}

impl Drop for NativeReclaimMeterScope {
    fn drop(&mut self) {
        if self.active {
            let _ = restore_native_reclaim_meter_scope(self.prior);
        }
    }
}

fn restore_native_reclaim_meter_scope(prior: NativeReclaimMeters) -> NativeReclaimMeters {
    let local = NATIVE_RECLAIM_METERS.with(|meters| {
        let local = meters.get();
        meters.set(prior);
        local
    });
    // Normal completion and unwind both bind local work exactly once. The
    // enclosing active scope is restored separately, so nested work is never
    // counted again when that scope closes. With no open record, drop the
    // observation just as legacy count does; never initialize the sink here.
    if ENABLED.load(Ordering::Relaxed) {
        if let Some(Some(sink)) = SINK.get() {
            if let Ok(mut sink) = sink.lock() {
                if let Some(record) = sink.record.as_mut() {
                    record.native_reclaim.merge(local);
                    record.native_reclaim_present = true;
                }
            }
        }
    }
    local
}

impl NativeReclaimMeters {
    fn merge(&mut self, other: Self) {
        // Completed scopes aggregate like the legacy count map's u64 +=;
        // the native note functions retain their existing saturating meters.
        self.serving_page_give_back_short += other.serving_page_give_back_short;
        self.serving_page_bytes_given_back += other.serving_page_bytes_given_back;
        self.serving_page_give_back_reached_final_pass |=
            other.serving_page_give_back_reached_final_pass;
        self.graph_reclaims_over_budget += other.graph_reclaims_over_budget;
        self.graph_reclaim_required_bytes += other.graph_reclaim_required_bytes;
        self.graph_reclaim_available_bytes += other.graph_reclaim_available_bytes;
        self.sealed_generations_evicted += other.sealed_generations_evicted;
    }

    fn labels(self) -> [(&'static str, Option<u64>); 6] {
        let graph = self.graph_reclaims_over_budget != 0;
        [
            ("graph_reclaim_available_bytes", graph.then_some(self.graph_reclaim_available_bytes)),
            ("graph_reclaim_required_bytes", graph.then_some(self.graph_reclaim_required_bytes)),
            ("graph_reclaims_over_budget", graph.then_some(self.graph_reclaims_over_budget)),
            ("sealed_generations_evicted", graph.then_some(self.sealed_generations_evicted)),
            ("serving_page_bytes_given_back", self.serving_page_give_back_reached_final_pass
                .then_some(self.serving_page_bytes_given_back)),
            ("serving_page_give_back_short", (self.serving_page_give_back_short != 0)
                .then_some(self.serving_page_give_back_short)),
        ]
    }
}

/// Fixed native pressure counters for this thread. This read allocates no
/// diagnostic state and does not initialize the attribution sink.
#[doc(hidden)]
pub fn native_reclaim_meters() -> NativeReclaimMeters {
    NATIVE_RECLAIM_METERS.with(std::cell::Cell::get)
}

#[cfg(test)]
#[path = "find_attribution_native_tests.rs"]
mod find_attribution_native_tests;

/// One full pass over `bytes` saved bytes to check them; `name` says which
/// step of a load paid for it.
#[inline]
pub fn note_hash_pass(name: &'static str, bytes: usize) {
    LOAD_METERS.with(|meters| {
        let mut now = meters.get();
        now.hash_passes = now.hash_passes.saturating_add(1);
        now.hashed_bytes = now.hashed_bytes.saturating_add(bytes as u64);
        meters.set(now);
    });
    if !enabled() {
        return;
    }
    count("graph_load_hash_passes", 1);
    count("graph_load_hashed_bytes", bytes as u64);
    count(name, bytes as u64);
}

/// One saved graph loaded by this thread, and what its load was charged.
#[inline]
pub fn note_saved_graph_load(charge: LoadCharge) {
    LOAD_METERS.with(|meters| {
        let mut now = meters.get();
        now.loads = now.loads.saturating_add(1);
        now.saved_bytes = now.saved_bytes.saturating_add(charge.saved_bytes as u64);
        now.priced_bytes = now.priced_bytes.saturating_add(charge.priced_bytes as u64);
        now.minimum_bytes = now
            .minimum_bytes
            .saturating_add(charge.minimum_bytes as u64);
        now.admitted_bytes = now
            .admitted_bytes
            .saturating_add(charge.admitted_bytes as u64);
        now.held_bytes = now.held_bytes.saturating_add(charge.held_bytes as u64);
        now.cache_bytes = now.cache_bytes.saturating_add(charge.cache_bytes as u64);
        if charge.cache_bytes <= charge.minimum_cache_bytes
            && charge.maximum_cache_bytes > charge.minimum_cache_bytes
        {
            now.least_cache_loads = now.least_cache_loads.saturating_add(1);
        }
        meters.set(now);
    });
}

/// Serving pages resident generations gave back so this thread's load fits.
#[inline]
pub fn note_serving_pages_given_back(bytes: usize) {
    LOAD_METERS.with(|meters| {
        let mut now = meters.get();
        now.given_back_bytes = now.given_back_bytes.saturating_add(bytes as u64);
        meters.set(now);
    });
}

/// One saved-graph load the memory budget refused after sizing it.
#[inline]
pub fn note_refused_saved_graph_load() {
    LOAD_METERS.with(|meters| {
        let mut now = meters.get();
        now.refused = now.refused.saturating_add(1);
        meters.set(now);
    });
}

/// What this thread has loaded and hashed so far.
pub fn load_meters() -> LoadMeters {
    LOAD_METERS.with(std::cell::Cell::get)
}
