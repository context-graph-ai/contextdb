//! Bounded restore evidence on the existing status lane.
//!
//! Each per-edge outcome commit appends fresh random entropy to a binary prefix
//! index. Completed subtree hashes contain no row reference, value, lineage or
//! outcome digest. Purge erases those objects without erasing evidence of which
//! committed image served this edge. There is no traversed history chain: checking
//! any signed prefix takes at most 64 indexed nodes (the width of the counter),
//! and the current prefix is one key read. Ordinary writes cannot rebuild a lost
//! prefix; even a replayed outcome receives fresh entropy on a diverged image.
use super::{canonical::*, preparation::LineageSigner, records::*};
use crate::Database;
use contextdb_core::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Checkpoint {
    pub namespace: Namespace,
    pub count: u64,
    pub digest: Digest,
    pub signature: Vec<u8>,
}
impl Checkpoint {
    fn bytes(&self) -> Result<Vec<u8>> {
        let mut e = Encoder::domain("delivery-prefix-checkpoint.v1");
        self.namespace.encode(&mut e)?;
        e.u64(self.count);
        e.raw(&self.digest);
        Ok(e.0)
    }
    pub fn verify(&self) -> Result<()> {
        if self.count == 0 {
            return Err(invalid());
        }
        crate::identity::FabricIdentity::verify_lineage_by_node_id(
            &self.namespace.hub_node,
            &self.bytes()?,
            &self.signature,
        )
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Frontier {
    pub checkpoint: Checkpoint,
    pub peaks: Vec<Option<Digest>>,
}
impl Frontier {
    pub fn verify(&self) -> Result<()> {
        self.checkpoint.verify()?;
        if prefix_digest(self.checkpoint.count, &self.peaks)? != self.checkpoint.digest {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Node {
    pub namespace: Namespace,
    pub height: u8,
    pub start: u64,
    pub digest: Digest,
    pub checksum: Digest,
}
impl Node {
    pub fn key(&self) -> Result<String> {
        node_key(&self.namespace, self.height, self.start)
    }
    fn checksum(&self) -> Result<Digest> {
        let mut e = Encoder::domain("delivery-prefix-node.v1");
        self.namespace.encode(&mut e)?;
        e.u8(self.height);
        e.u64(self.start);
        e.raw(&self.digest);
        Ok(e.digest())
    }
    pub fn verify(&self) -> Result<()> {
        if self.height >= 64
            || self.start & ((1u64 << self.height) - 1) != 0
            || self.checksum()? != self.checksum
        {
            return Err(invalid());
        }
        Ok(())
    }
}
fn node_key(ns: &Namespace, height: u8, start: u64) -> Result<String> {
    Ok(format!(
        "delivery_prefix_node.v1.{}.{height:02x}.{start:016x}",
        ns.key()?
    ))
}
fn frontier_key(ns: &Namespace) -> Result<String> {
    Ok(format!("delivery_prefix_frontier.v1.{}", ns.key()?))
}
fn prefix_digest(count: u64, peaks: &[Option<Digest>]) -> Result<Digest> {
    if peaks.len() != 64 {
        return Err(invalid());
    }
    let mut e = Encoder::domain("delivery-prefix-root.v1");
    e.u64(count);
    for (height, peak) in peaks.iter().enumerate() {
        if peak.is_some() != (count & (1u64 << height) != 0) {
            return Err(invalid());
        }
        if let Some(hash) = peak {
            e.u8(height as u8);
            e.raw(hash);
        }
    }
    Ok(e.digest())
}
fn read(
    db: &Database,
    key: &str,
    ws: Option<&contextdb_tx::WriteSet>,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Record>> {
    let mut custody_hold = crate::custody::store::SelectionHold::new(read_memory.clone());

    if let Some((_, bytes)) =
        ws.and_then(|ws| ws.config_writes.iter().rev().find(|(k, _)| k == key))
    {
        return decode(bytes).map(Some);
    }
    Ok(custody_hold
        .retain(db.custody_records_in(&[key.into()], read_memory.clone())?)?
        .into_iter()
        .find(|r| r.key().ok().as_deref() == Some(key)))
}
pub(crate) fn current(
    db: &Database,
    ns: &Namespace,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Checkpoint>> {
    match read(db, &frontier_key(ns)?, None, read_memory.clone())? {
        Some(Record::CheckpointFrontier(f)) => Ok(Some(f.checkpoint)),
        None => Ok(None),
        _ => Err(invalid()),
    }
}
// The writes join the exact terminal transaction, including several terminals
// sharing a source transaction in the existing private test adapter.
pub(crate) fn append(
    db: &Database,
    ws: &mut contextdb_tx::WriteSet,
    ns: &Namespace,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    let key = frontier_key(ns)?;
    let (old_count, mut peaks) = match read(db, &key, Some(ws), read_memory.clone())? {
        Some(Record::CheckpointFrontier(f)) => (f.checkpoint.count, f.peaks),
        None => (0, vec![None; 64]),
        _ => return Err(invalid()),
    };
    let count = old_count.checked_add(1).ok_or_else(invalid)?;
    let mut hash = super::preparation::random_digest();
    let mut start = old_count;
    for height in 0..64u8 {
        let mut node = Node {
            namespace: ns.clone(),
            height,
            start,
            digest: hash,
            checksum: [0; 32],
        };
        node.checksum = node.checksum()?;
        let record = Record::CheckpointNode(node);
        ws.config_writes.push((record.key()?, encode(&record)?));
        if old_count & (1u64 << height) == 0 {
            peaks[height as usize] = Some(hash);
            break;
        }
        let left = peaks[height as usize].take().ok_or_else(invalid)?;
        let mut e = Encoder::domain("delivery-prefix-branch.v1");
        e.u8(height);
        e.raw(&left);
        e.raw(&hash);
        hash = e.digest();
        start -= 1u64 << height;
    }
    let mut checkpoint = Checkpoint {
        namespace: ns.clone(),
        count,
        digest: prefix_digest(count, &peaks)?,
        signature: Vec::new(),
    };
    checkpoint.signature = signer(&checkpoint.bytes()?)?;
    let record = Record::CheckpointFrontier(Frontier { checkpoint, peaks });
    ws.config_writes.push((key, encode(&record)?));
    Ok(())
}
// Owned append uses a fixed stack frontier. Every emitted native namespace,
// signature, encoder and source ConfigWrite has its own original admission.
fn namespace_capacity(ns: &Namespace) -> Result<usize> {
    // Namespace::encode writes a length-delimited tenant, two fixed nodes and
    // two inline u128 incarnations. Validate nodes before constructing output.
    node_bytes(&ns.hub_node)?;
    node_bytes(&ns.edge_node)?;
    (8usize + 32 + 16 + 32 + 16)
        .checked_add(ns.tenant.as_str().len())
        .ok_or_else(invalid)
}
fn namespace_copy_bytes(ns: &Namespace) -> Result<usize> {
    ns.tenant
        .as_str()
        .len()
        .checked_add(ns.hub_node.len())
        .and_then(|bytes| bytes.checked_add(ns.edge_node.len()))
        .ok_or_else(invalid)
}
fn namespace_key_owned(
    db: &Database,
    ns: &Namespace,
) -> Result<super::preparation::OwnedPreparationValue<String>> {
    let capacity = namespace_capacity(ns)?;
    let encoded = prepare_encoder_owned(db, capacity, |e| ns.encode(e))?;
    super::preparation::OwnedPreparationValue::try_produce(
        db,
        capacity.checked_mul(2).ok_or_else(invalid)?,
        || {
            let key = encoded.with_value(|bytes| hex(bytes));
            if key.len() != capacity * 2 || key.capacity() != key.len() {
                return Err(invalid());
            }
            Ok(key)
        },
    )
}
fn prefix_digest_owned(db: &Database, count: u64, peaks: &[Option<Digest>]) -> Result<Digest> {
    if peaks.len() != 64 {
        return Err(invalid());
    }
    for (height, peak) in peaks.iter().enumerate() {
        if peak.is_some() != (count & (1u64 << height) != 0) {
            return Err(invalid());
        }
    }
    let bytes = prepare_encoder_owned(
        db,
        8 + "delivery-prefix-root.v1".len() + 8 + count.count_ones() as usize * 33,
        |e| {
            e.string("delivery-prefix-root.v1");
            e.u64(count);
            for (height, peak) in peaks.iter().enumerate() {
                if let Some(hash) = peak {
                    e.u8(height as u8);
                    e.raw(hash);
                }
            }
            Ok(())
        },
    )?;
    Ok(bytes.with_value(|bytes| *blake3::hash(bytes).as_bytes()))
}
fn checkpoint_bytes_owned(
    db: &Database,
    checkpoint: &Checkpoint,
) -> Result<super::preparation::OwnedPreparationValue<Vec<u8>>> {
    let capacity = (8 + "delivery-prefix-checkpoint.v1".len() + 8 + 32usize)
        .checked_add(namespace_capacity(&checkpoint.namespace)?)
        .ok_or_else(invalid)?;
    prepare_encoder_owned(db, capacity, |e| {
        e.string("delivery-prefix-checkpoint.v1");
        checkpoint.namespace.encode(e)?;
        e.u64(checkpoint.count);
        e.raw(&checkpoint.digest);
        Ok(())
    })
}
fn stage_checkpoint_record_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    record: &Record,
    key: &str,
) -> Result<()> {
    let encoded_capacity = count_named_encoding(record)?;
    let bytes = key
        .len()
        .checked_add(encoded_capacity)
        .ok_or_else(invalid)?;
    db.stage_owned_delivery_aux(guard, bytes, || {
        let key = key.to_owned();
        let mut encoded = Vec::with_capacity(encoded_capacity);
        if key.capacity() != key.len() || encoded.capacity() != encoded_capacity {
            return Err(invalid());
        }
        emit_named_encoding(record, &mut encoded)?;
        if encoded.len() != encoded_capacity {
            return Err(invalid());
        }
        Ok(contextdb_tx::staged_source::AuxBody::ConfigWrite((
            key, encoded,
        )))
    })
}
struct SelectedFrontier {
    record: Record,
    memory: std::sync::Arc<crate::read_image_memory::ImageMemory>,
}
fn read_frontier_owned(
    db: &Database,
    guard: &contextdb_tx::manager::EngineGuard<'_>,
    key: &str,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<(u64, [Option<Digest>; 64])> {
    let verify = |record: &Record| -> Result<(u64, [Option<Digest>; 64])> {
        let Record::CheckpointFrontier(frontier) = record else {
            return Err(invalid());
        };
        if frontier.checkpoint.count == 0 || frontier.peaks.len() != 64 {
            return Err(invalid());
        }
        if prefix_digest_owned(db, frontier.checkpoint.count, &frontier.peaks)?
            != frontier.checkpoint.digest
        {
            return Err(invalid());
        }
        let bytes = checkpoint_bytes_owned(db, &frontier.checkpoint)?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &frontier.checkpoint.namespace.hub_node,
                bytes,
                &frontier.checkpoint.signature,
            )
        })?;
        let mut peaks = [None; 64];
        peaks.copy_from_slice(&frontier.peaks);
        Ok((frontier.checkpoint.count, peaks))
    };
    let view = guard.read();
    if let Some((_, bytes)) = view
        .config_writes
        .iter()
        .rev()
        .find(|(candidate, _)| candidate == key)
    {
        let memory = crate::read_image_memory::ImageMemory::charging_reader_for(
            db.bounded_read_accountant(),
            read_memory,
            "reader",
            "decode_custody_metadata",
            "Reduce selected custody metadata.",
        );
        let record: Record = memory.decode_msgpack(bytes)?;
        let selected = SelectedFrontier { record, memory };
        // Preserve decode's canonical named-codec equality before verification.
        let encoded = prepare_named_encoding_owned(db, &selected.record)?;
        if !encoded.with_value(|encoded| encoded.as_slice() == bytes.as_slice()) {
            return Err(invalid());
        }
        return verify(&selected.record);
    }
    // The key argument array is stack-native; its sole copied String is funded
    // independently of the operation's selected-record lease.
    let keys = super::preparation::OwnedPreparationValue::try_produce(db, key.len(), || {
        Ok([key.to_owned()])
    })?;
    let selected = keys.with_value(|keys| db.custody_records_in(keys, read_memory))?;
    if let Some(record) = selected.iter().next() {
        verify(record)
    } else {
        Ok((0, [None; 64]))
    }
}

pub(crate) fn append_owned(
    db: &Database,
    guard: &mut contextdb_tx::manager::EngineGuard<'_>,
    ns: &Namespace,
    signer: &LineageSigner,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<()> {
    use super::preparation::{OwnedPreparationValue, preparation_array_bytes};
    let namespace_key = namespace_key_owned(db, ns)?;
    let key = namespace_key.with_value(|key| {
        prepare_string_owned(db, format_args!("delivery_prefix_frontier.v1.{}", key))
    })?;
    let (old_count, mut peaks) =
        key.with_value(|key| read_frontier_owned(db, guard, key, read_memory))?;
    let count = old_count.checked_add(1).ok_or_else(invalid)?;
    let mut hash = super::preparation::random_digest();
    let mut start = old_count;
    for height in 0..64u8 {
        let record = OwnedPreparationValue::try_produce(db, namespace_copy_bytes(ns)?, || {
            let mut node = Node {
                namespace: ns.clone(),
                height,
                start,
                digest: hash,
                checksum: [0; 32],
            };
            let capacity = (8 + "delivery-prefix-node.v1".len() + 1 + 8 + 32usize)
                .checked_add(namespace_capacity(ns)?)
                .ok_or_else(invalid)?;
            let bytes = prepare_encoder_owned(db, capacity, |e| {
                e.string("delivery-prefix-node.v1");
                node.namespace.encode(e)?;
                e.u8(node.height);
                e.u64(node.start);
                e.raw(&node.digest);
                Ok(())
            })?;
            node.checksum = bytes.with_value(|bytes| *blake3::hash(bytes).as_bytes());
            Ok(Record::CheckpointNode(node))
        })?;
        let node_key = namespace_key.with_value(|key| {
            prepare_string_owned(
                db,
                format_args!("delivery_prefix_node.v1.{}.{height:02x}.{start:016x}", key),
            )
        })?;
        record.with_value(|record| {
            node_key.with_value(|key| stage_checkpoint_record_owned(db, guard, record, key))
        })?;
        if old_count & (1u64 << height) == 0 {
            peaks[height as usize] = Some(hash);
            break;
        }
        let left = peaks[height as usize].take().ok_or_else(invalid)?;
        let branch =
            prepare_encoder_owned(db, 8 + "delivery-prefix-branch.v1".len() + 1 + 64, |e| {
                e.string("delivery-prefix-branch.v1");
                e.u8(height);
                e.raw(&left);
                e.raw(&hash);
                Ok(())
            })?;
        hash = branch.with_value(|bytes| *blake3::hash(bytes).as_bytes());
        start -= 1u64 << height;
    }
    let digest = prefix_digest_owned(db, count, &peaks)?;
    let native = namespace_copy_bytes(ns)?
        .checked_add(preparation_array_bytes::<Option<Digest>>(64)?)
        .and_then(|bytes| bytes.checked_add(64))
        .ok_or_else(invalid)?;
    let record = OwnedPreparationValue::try_produce(db, native, || {
        let mut checkpoint = Checkpoint {
            namespace: ns.clone(),
            count,
            digest,
            signature: Vec::new(),
        };
        let bytes = checkpoint_bytes_owned(db, &checkpoint)?;
        // FabricIdentity::sign_lineage constructs exactly one 64-byte vector.
        checkpoint.signature = bytes.with_value(|bytes| signer(bytes))?;
        if checkpoint.signature.len() != 64 || checkpoint.signature.capacity() != 64 {
            return Err(invalid());
        }
        let mut retained_peaks = Vec::with_capacity(64);
        if retained_peaks.capacity() != 64 {
            return Err(invalid());
        }
        retained_peaks.extend_from_slice(&peaks);
        Ok(Record::CheckpointFrontier(Frontier {
            checkpoint,
            peaks: retained_peaks,
        }))
    })?;
    record.with_value(|record| {
        key.with_value(|key| stage_checkpoint_record_owned(db, guard, record, key))
    })
}

pub(crate) fn held(
    db: &Database,
    checkpoint: &Checkpoint,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<bool> {
    let Some(current) = current(db, &checkpoint.namespace, read_memory.clone())? else {
        return Ok(false);
    };
    if checkpoint.count > current.count {
        return Ok(false);
    }
    if checkpoint.count == current.count {
        return Ok(checkpoint.digest == current.digest);
    }
    let mut peaks = vec![None; 64];
    let mut start = 0;
    for height in (0..64u8).rev() {
        if checkpoint.count & (1u64 << height) != 0 {
            match read(
                db,
                &node_key(&checkpoint.namespace, height, start)?,
                None,
                read_memory.clone(),
            )? {
                Some(Record::CheckpointNode(node)) => {
                    node.verify()?;
                    peaks[height as usize] = Some(node.digest);
                }
                None => return Ok(false),
                _ => return Err(invalid()),
            }
            start += 1u64 << height;
        }
    }
    Ok(prefix_digest(checkpoint.count, &peaks)? == checkpoint.digest)
}
pub(crate) fn edge_key(ns: &Namespace) -> Result<String> {
    Ok(format!("delivery_edge_checkpoint.v1.{}", ns.key()?))
}
pub(crate) fn admission(
    db: &Database,
    checkpoint: &Checkpoint,
    read_memory: std::sync::Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
) -> Result<Option<Record>> {
    checkpoint.verify()?;
    if let Some(Record::EdgeCheckpoint(old)) = read(
        db,
        &edge_key(&checkpoint.namespace)?,
        None,
        read_memory.clone(),
    )? {
        if old.count > checkpoint.count {
            return Ok(None);
        }
        if old.count == checkpoint.count {
            if old.digest != checkpoint.digest {
                return Err(invalid());
            }
            return Ok(None);
        }
    }
    Ok(Some(Record::EdgeCheckpoint(checkpoint.clone())))
}

impl Checkpoint {
    pub(crate) fn verify_owned(&self, db: &Database) -> Result<()> {
        if self.count == 0 {
            return Err(invalid());
        }
        let bytes = checkpoint_bytes_owned(db, self)?;
        bytes.with_value(|bytes| {
            crate::identity::FabricIdentity::verify_lineage_by_node_id(
                &self.namespace.hub_node,
                bytes,
                &self.signature,
            )
        })
    }
}
impl Frontier {
    pub(crate) fn verify_owned(&self, db: &Database) -> Result<()> {
        self.checkpoint.verify_owned(db)?;
        if prefix_digest_owned(db, self.checkpoint.count, &self.peaks)? != self.checkpoint.digest {
            return Err(invalid());
        }
        Ok(())
    }
}
impl Node {
    pub(crate) fn verify_owned(&self) -> Result<()> {
        if self.height >= 64 || self.start & ((1u64 << self.height) - 1) != 0 {
            return Err(invalid());
        }
        if finite_hash(|e| {
            e.string("delivery-prefix-node.v1");
            self.namespace.encode_finite(e)?;
            e.u8(self.height);
            e.u64(self.start);
            e.raw(&self.digest);
            Ok(())
        })? != self.checksum
        {
            return Err(invalid());
        }
        Ok(())
    }
}
