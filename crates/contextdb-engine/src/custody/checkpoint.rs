//! Statements 11/13/15/17: bounded restore evidence on the existing status lane.
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
fn read(db: &Database, key: &str, ws: Option<&contextdb_tx::WriteSet>) -> Result<Option<Record>> {
    if let Some((_, bytes)) =
        ws.and_then(|ws| ws.config_writes.iter().rev().find(|(k, _)| k == key))
    {
        return decode(bytes).map(Some);
    }
    Ok(db
        .custody_records_in(&[key.into()])?
        .into_iter()
        .find(|r| r.key().ok().as_deref() == Some(key)))
}
pub(crate) fn current(db: &Database, ns: &Namespace) -> Result<Option<Checkpoint>> {
    match read(db, &frontier_key(ns)?, None)? {
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
) -> Result<()> {
    let key = frontier_key(ns)?;
    let (old_count, mut peaks) = match read(db, &key, Some(ws))? {
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
pub(crate) fn held(db: &Database, checkpoint: &Checkpoint) -> Result<bool> {
    let Some(current) = current(db, &checkpoint.namespace)? else {
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
            match read(db, &node_key(&checkpoint.namespace, height, start)?, None)? {
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
pub(crate) fn admission(db: &Database, checkpoint: &Checkpoint) -> Result<Option<Record>> {
    checkpoint.verify()?;
    if let Some(Record::EdgeCheckpoint(old)) = read(db, &edge_key(&checkpoint.namespace)?, None)? {
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
