//! Exact private durable history bytes; public change-log bytes stay unchanged.
use super::*;

pub(super) const GRAPH_CONTRIBUTIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_contributions_v1");
pub(super) const GRAPH_OCCURRENCE_LOCATIONS: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_occurrence_locations_v1");
pub(super) const GRAPH_PREDECESSOR_LIFECYCLE: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("graph_predecessor_lifecycle_v1");
const VERSION: u8 = 1;

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct StoredChangeLogEntry {
    pub(super) entry: ChangeLogEntry,
    pub(super) tag: u8,
    pub(super) witness: Option<GraphDeletionWitness>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct GraphDeletionWitness {
    pub(super) id: u64,
    pub(super) predecessor: Vec<u8>,
    pub(super) deletion_tx: TxId,
}

impl content_identity::HistoryRecord for StoredChangeLogEntry {
    const KIND: content_identity::HistoryKind = content_identity::HistoryKind::ChangeLog;

    fn commitment_bytes(&self) -> Result<Vec<u8>> {
        let mut encoded = Vec::new();
        RedbPersistence::encode_stored_change_log_parts(
            &self.entry,
            self.tag,
            self.witness.as_ref(),
            &mut encoded,
        )?;
        Ok(encoded)
    }
}

fn malformed(reason: impl Into<String>) -> Error {
    Error::StoreCorrupted {
        path: "graph_history".into(),
        reason: reason.into(),
    }
}

/// The separator is at the fixed end offset, so embedded NULs in an edge
/// type do not change the physical identity or transaction interpretation.
pub(super) fn parse_physical_predecessor(p: &[u8]) -> Result<(NodeId, NodeId, String, TxId, Lsn)> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    if p.len() < 49 || p[p.len() - 17] != 0 {
        return Err(malformed("malformed physical graph predecessor key"));
    }
    let separator = p.len() - 17;
    let edge_type = std::str::from_utf8(&p[32..separator])
        .map_err(|_| malformed("physical graph predecessor type is not UTF-8"))?;
    let source = NodeId::from_slice(&p[..16])
        .map_err(|_| malformed("malformed graph predecessor source"))?;
    let target = NodeId::from_slice(&p[16..32])
        .map_err(|_| malformed("malformed graph predecessor target"))?;
    let created_tx = TxId(u64::from_be_bytes(
        p[separator + 1..separator + 9].try_into().unwrap(),
    ));
    let creation_lsn = Lsn(u64::from_be_bytes(p[separator + 9..].try_into().unwrap()));
    crate::read_image_memory::reserve(edge_type.len())?;
    Ok((
        source,
        target,
        edge_type.to_owned(),
        created_tx,
        creation_lsn,
    ))
}

fn validate_parts(
    entry: &ChangeLogEntry,
    tag: u8,
    witness: Option<&GraphDeletionWitness>,
) -> Result<()> {
    match (entry, tag, witness) {
        (ChangeLogEntry::EdgeDelete { .. }, 1, None) => Ok(()),
        (
            ChangeLogEntry::EdgeDelete {
                source,
                target,
                edge_type,
                lsn,
            },
            2,
            Some(witness),
        ) => {
            let (p_source, p_target, p_type, created_tx, creation_lsn) =
                parse_physical_predecessor(&witness.predecessor)?;
            if witness.id == 0
                || p_source != *source
                || p_target != *target
                || p_type != *edge_type
                || created_tx >= witness.deletion_tx
                || creation_lsn >= *lsn
            {
                return Err(malformed(
                    "graph deletion witness disagrees with its public record",
                ));
            }
            Ok(())
        }
        (ChangeLogEntry::EdgeDelete { .. }, _, _) => {
            Err(malformed("invalid private graph deletion tag or witness"))
        }
        (_, 0, None) => Ok(()),
        _ => Err(malformed(
            "private history tag disagrees with its public variant",
        )),
    }
}

/// Measures the unchanged public codec without allocating a growing buffer.
struct PublicLength(usize);
impl std::io::Write for PublicLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_add(bytes.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "public history length overflow",
            )
        })?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl RedbPersistence {
    pub(super) fn decode_stored_change_log(bytes: &[u8]) -> Result<StoredChangeLogEntry> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
#[cfg(any(test, feature = "test-seams"))]
crate::read_work_probe::note_graph("history", "private_history_decode", &[], None, None);

        if bytes.len() < 6 || bytes[0] != VERSION {
            return Err(malformed("invalid private history version or length"));
        }
        let public_len = usize::try_from(u32::from_be_bytes(bytes[1..5].try_into().unwrap()))
            .map_err(|_| malformed("public history length exceeds address space"))?;
        let public_end = 5usize
            .checked_add(public_len)
            .ok_or_else(|| malformed("public history length overflow"))?;
        let extension_start = public_end
            .checked_add(1)
            .ok_or_else(|| malformed("private history length overflow"))?;
        if extension_start > bytes.len() {
            return Err(malformed("truncated public history bytes"));
        }
        let (entry, consumed): (ChangeLogEntry, usize) = crate::read_image_memory::decode(
            &bytes[5..public_end],
        )
        .map_err(|error| match error {
            Error::Other(reason) => malformed(reason),
            other => other,
        })?;
        if consumed != public_len {
            return Err(malformed("public history bytes have trailing data"));
        }
        let tag = bytes[public_end];
        let extension = &bytes[extension_start..];
        let witness = match tag {
            0 | 1 if extension.is_empty() => None,
            2 => {
                if extension.len() < 20 {
                    return Err(malformed("truncated graph deletion witness"));
                }
                let id = u64::from_be_bytes(extension[..8].try_into().unwrap());
                let p_len =
                    usize::try_from(u32::from_be_bytes(extension[8..12].try_into().unwrap()))
                        .map_err(|_| malformed("predecessor length exceeds address space"))?;
                let p_end = 12usize
                    .checked_add(p_len)
                    .ok_or_else(|| malformed("predecessor length overflow"))?;
                let expected = p_end
                    .checked_add(8)
                    .ok_or_else(|| malformed("private witness length overflow"))?;
                if extension.len() != expected {
                    return Err(malformed("graph deletion witness length mismatch"));
                }
                crate::read_image_memory::reserve(p_len)?;
                Some(GraphDeletionWitness {
                    id,
                    predecessor: extension[12..p_end].to_vec(),
                    deletion_tx: TxId(u64::from_be_bytes(extension[p_end..].try_into().unwrap())),
                })
            }
            _ => return Err(malformed("invalid private history tag or trailing bytes")),
        };
        validate_parts(&entry, tag, witness.as_ref())?;
        Ok(StoredChangeLogEntry {
            entry,
            tag,
            witness,
        })
    }

    pub(super) fn encode_stored_change_log_parts(
        entry: &ChangeLogEntry,
        tag: u8,
        witness: Option<&GraphDeletionWitness>,
        encoded: &mut Vec<u8>,
    ) -> Result<()> {
        validate_parts(entry, tag, witness)?;
        let mut public_length = PublicLength(0);
        bincode::serde::encode_into_std_write(
            entry,
            &mut public_length,
            bincode::config::standard(),
        )
        .map_err(|error| malformed(format!("cannot measure public history bytes: {error}")))?;
        let public_len = u32::try_from(public_length.0)
            .map_err(|_| malformed("public history bytes exceed u32 length"))?;
        let predecessor_len = witness
            .map(|w| u32::try_from(w.predecessor.len()))
            .transpose()
            .map_err(|_| malformed("predecessor bytes exceed u32 length"))?;
        let extension_len = match witness {
            Some(w) => w
                .predecessor
                .len()
                .checked_add(20)
                .ok_or_else(|| malformed("private witness length overflow"))?,
            None => 0,
        };
        let total_len = public_length
            .0
            .checked_add(6)
            .and_then(|n| n.checked_add(extension_len))
            .ok_or_else(|| malformed("private history length overflow"))?;
        if encoded.capacity() < total_len {
            crate::read_image_memory::reserve(total_len)?;
        }
        encoded.clear();
        encoded.try_reserve_exact(total_len).map_err(|error| {
            malformed(format!("cannot allocate private history bytes: {error}"))
        })?;
        encoded.push(VERSION);
        encoded.extend_from_slice(&public_len.to_be_bytes());
        bincode::serde::encode_into_std_write(entry, &mut *encoded, bincode::config::standard())
            .map_err(|error| malformed(format!("cannot encode public history bytes: {error}")))?;
        encoded.push(tag);
        if let Some(witness) = witness {
            encoded.extend_from_slice(&witness.id.to_be_bytes());
            encoded.extend_from_slice(
                &predecessor_len
                    .expect("witness length measured")
                    .to_be_bytes(),
            );
            encoded.extend_from_slice(&witness.predecessor);
            encoded.extend_from_slice(&witness.deletion_tx.0.to_be_bytes());
        }
        if encoded.len() != total_len {
            return Err(malformed(
                "public history encoding changed its measured length",
            ));
        }
        Ok(())
    }

    pub(super) fn stored_change_log_mutation(
        key: &str,
        bytes: &[u8],
    ) -> Result<content_identity::ContentIdentityMutation> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        Self::decode_stored_change_log(bytes)?;
        content_identity::private_history_mutation(key, bytes)
    }

    pub(super) fn stored_change_log_value_hash(bytes: &[u8]) -> Result<[u8; 32]> {
        Self::decode_stored_change_log(bytes)?;
        Ok(content_identity::private_history_value_hash(bytes))
    }

    pub(super) fn stored_change_log_mutation_in_write(
        write: &redb::WriteTransaction,
        key: &str,
    ) -> Result<content_identity::ContentIdentityMutation> {
        let table = write
            .open_table(writer_history::WRITER_CHANGE_LOG_TABLE)
            .map_err(Self::storage_error)?;
        let position = Self::numeric_change_key(key)?;
        let value = table
            .get(observed_graph_key!("history", "get", position.as_slice()))
            .map_err(Self::storage_error)?
            .ok_or_else(|| malformed("new history occurrence is absent from its selector"))?;
        Self::stored_change_log_mutation(key, value.value())
    }
}

pub(super) fn creation_contribution_key(p: &[u8]) -> Result<Vec<u8>> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    let (_, _, _, tx, lsn) = parse_physical_predecessor(p)?;
    let p_len = u32::try_from(p.len())
        .map_err(|_| malformed("creation predecessor bytes exceed u32 length"))?;
    let len = p
        .len()
        .checked_add(22)
        .ok_or_else(|| malformed("creation contribution key length overflow"))?;
    crate::read_image_memory::reserve(len)?;
    let mut key = Vec::with_capacity(len);
    key.push(VERSION);
    key.extend_from_slice(&lsn.0.to_be_bytes());
    key.extend_from_slice(&tx.0.to_be_bytes());
    key.push(0);
    key.extend_from_slice(&p_len.to_be_bytes());
    key.extend_from_slice(p);
    Ok(key)
}

pub(super) fn deletion_contribution_key(
    stored_lsn: Lsn,
    witness: &GraphDeletionWitness,
) -> [u8; 26] {
    let mut key = [0; 26];
    key[0] = VERSION;
    key[1..9].copy_from_slice(&stored_lsn.0.to_be_bytes());
    key[9..17].copy_from_slice(&witness.deletion_tx.0.to_be_bytes());
    key[17] = 1;
    key[18..].copy_from_slice(&witness.id.to_be_bytes());
    key
}

pub(super) fn location_key(id: u64) -> [u8; 9] {
    let mut key = [0; 9];
    key[0] = VERSION;
    key[1..].copy_from_slice(&id.to_be_bytes());
    key
}

pub(super) fn lifecycle_key(p: &[u8]) -> Result<Vec<u8>> {
    let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
    parse_physical_predecessor(p)?;
    let p_len = u32::try_from(p.len())
        .map_err(|_| malformed("lifecycle predecessor bytes exceed u32 length"))?;
    let len = p
        .len()
        .checked_add(5)
        .ok_or_else(|| malformed("lifecycle key length overflow"))?;
    crate::read_image_memory::reserve(len)?;
    let mut key = Vec::with_capacity(len);
    key.push(VERSION);
    key.extend_from_slice(&p_len.to_be_bytes());
    key.extend_from_slice(p);
    Ok(key)
}

pub(super) fn contribution_digest(creation: bool, key: &[u8], value: &[u8]) -> Result<[u8; 32]> {
    let key_len = u32::try_from(key.len())
        .map_err(|_| malformed("contribution digest key exceeds u32 length"))?;
    let value_len = u64::try_from(value.len())
        .map_err(|_| malformed("contribution digest value exceeds u64 length"))?;
    if creation {
        parse_physical_predecessor(key)?;
    } else {
        if key.len() != 8 {
            return Err(malformed("deletion digest key is not a BE64 occurrence id"));
        }
        let record = RedbPersistence::decode_stored_change_log(value)?;
        if record.tag != 2
            || record
                .witness
                .as_ref()
                .is_none_or(|w| w.id.to_be_bytes() != key)
        {
            return Err(malformed(
                "deletion digest source disagrees with its occurrence id",
            ));
        }
    }
    let mut hash = blake3::Hasher::new();
    hash.update(if creation {
        b"contextdb.graph.creation.v1"
    } else {
        b"contextdb.graph.deletion.v1"
    });
    hash.update(&key_len.to_be_bytes());
    hash.update(key);
    hash.update(&value_len.to_be_bytes());
    hash.update(value);
    Ok(*hash.finalize().as_bytes())
}
