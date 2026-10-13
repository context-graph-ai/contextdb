//! A private ordered witness for each retained physical relational version.
use super::*;
use contextdb_core::IndexDecl;

const FORMAT: u8 = 1;
const FIXED_KEY_BYTES: usize = 72;

#[derive(Clone, Copy)]
pub(in super::super) struct VersionManifest<'a> {
    pub(in super::super) table: &'a str,
    pub(in super::super) identity: [u8; 24],
    pub(in super::super) deleted_tx: Option<TxId>,
    pub(in super::super) digest: [u8; 32],
}

fn bad(reason: &str) -> Error {
    Error::Other(format!("relational version manifest: {reason}"))
}

pub(in super::super) fn manifest_prefix(table: &str) -> Result<Vec<u8>> {
    let len = u32::try_from(table.len()).map_err(|_| bad("table length overflow"))?;
    let capacity = (FIXED_KEY_BYTES - 2)
        .checked_add(table.len())
        .ok_or_else(|| bad("key length overflow"))?;
    let mut key = Vec::with_capacity(capacity);
    key.push(FORMAT);
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(table.as_bytes());
    Ok(key)
}

pub(in super::super) fn parse_manifest(coordinate: &[u8]) -> Result<VersionManifest<'_>> {
    if coordinate.len() < FIXED_KEY_BYTES - 2 || coordinate[0] != FORMAT {
        return Err(bad("invalid format or truncated key"));
    }
    let length = u32::from_be_bytes(coordinate[1..5].try_into().expect("length")) as usize;
    if length.checked_add(FIXED_KEY_BYTES - 2) != Some(coordinate.len()) {
        return Err(bad("invalid table length or trailing fields"));
    }
    let table =
        std::str::from_utf8(&coordinate[5..5 + length]).map_err(|_| bad("table is not UTF-8"))?;
    let p = 5 + length;
    let identity = coordinate[p..p + 24].try_into().expect("identity");
    let closure = u64::from_be_bytes(coordinate[p + 25..p + 33].try_into().expect("closure"));
    let deleted_tx = match coordinate[p + 24] {
        0 if closure == 0 => None,
        1 => Some(TxId(closure)),
        _ => return Err(bad("invalid closure marker")),
    };
    let digest = coordinate[p + 33..].try_into().expect("digest");
    Ok(VersionManifest {
        table,
        identity,
        deleted_tx,
        digest,
    })
}

pub(in super::super) fn parse_manifest_with_checkpoint<'a>(
    coordinate: &'a [u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<VersionManifest<'a>> {
    if coordinate.len() < FIXED_KEY_BYTES - 2 || coordinate[0] != FORMAT {
        return Err(bad("invalid format or truncated key"));
    }
    let length = u32::from_be_bytes(coordinate[1..5].try_into().expect("length")) as usize;
    if length.checked_add(FIXED_KEY_BYTES - 2) != Some(coordinate.len()) {
        return Err(bad("invalid table length or trailing fields"));
    }
    let raw = &coordinate[5..5 + length];
    let mut at = 0;
    while at < raw.len() {
        checkpoint()?;
        let mut end = raw.len().min(at.saturating_add(64 * 1024));
        if end < raw.len() {
            while end > at && raw[end] & 0xc0 == 0x80 {
                end -= 1;
            }
        }
        if end == at {
            return Err(bad("table is not UTF-8"));
        }
        std::str::from_utf8(&raw[at..end]).map_err(|_| bad("table is not UTF-8"))?;
        at = end;
    }
    // Every span was validated above; avoid a second whole-table scan.
    // SAFETY: the entire slice is the concatenation of the valid UTF-8
    // windows checked above, each ending before a complete character.
    let table = unsafe { std::str::from_utf8_unchecked(raw) };
    let p = 5 + length;
    let identity = coordinate[p..p + 24].try_into().expect("identity");
    let closure = u64::from_be_bytes(coordinate[p + 25..p + 33].try_into().expect("closure"));
    let deleted_tx = match coordinate[p + 24] {
        0 if closure == 0 => None,
        1 => Some(TxId(closure)),
        _ => return Err(bad("invalid closure marker")),
    };
    Ok(VersionManifest {
        table,
        identity,
        deleted_tx,
        digest: coordinate[p + 33..].try_into().expect("digest"),
    })
}
pub(in super::super) fn prefix_with_checkpoint(
    table: &str,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    let len = u32::try_from(table.len()).map_err(|_| bad("table length overflow"))?;
    let capacity = (FIXED_KEY_BYTES - 2)
        .checked_add(table.len())
        .ok_or_else(|| bad("key length overflow"))?;
    let mut key = Vec::with_capacity(capacity);
    key.push(FORMAT);
    key.extend_from_slice(&len.to_be_bytes());
    for window in table.as_bytes().chunks(64 * 1024) {
        checkpoint()?;
        key.extend_from_slice(window);
    }
    Ok(key)
}
pub(in super::super) fn copy_with_checkpoint(
    bytes: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    let mut copy = Vec::with_capacity(bytes.len());
    for window in bytes.chunks(64 * 1024) {
        checkpoint()?;
        copy.extend_from_slice(window);
    }
    Ok(copy)
}
pub(in super::super) fn starts_with_checkpoint(
    bytes: &[u8],
    prefix: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<bool> {
    if bytes.len() < prefix.len() {
        return Ok(false);
    }
    Ok(bytes_cmp_checkpoint(&bytes[..prefix.len()], prefix, checkpoint)? == Ordering::Equal)
}
pub(in super::super) fn equal_with_checkpoint(
    left: &[u8],
    right: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<bool> {
    Ok(bytes_cmp_checkpoint(left, right, checkpoint)? == Ordering::Equal)
}

/// Four offsets per named value; no decoded map or canonical body copy.
#[derive(Clone, Copy)]
struct Span {
    name: usize,
    name_end: usize,
    value: usize,
    value_end: usize,
}
struct Encoded<'a, 'c> {
    bytes: &'a [u8],
    position: usize,
    checkpoint: &'c mut dyn FnMut() -> Result<()>,
}
impl<'a> Encoded<'a, '_> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| bad("body offset overflow"))?;
        let raw = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| bad("truncated body"))?;
        for _ in raw.chunks(64 * 1024) {
            (self.checkpoint)()?;
        }
        self.position = end;
        Ok(raw)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn integer(&mut self) -> Result<u64> {
        let tag = self.byte()?;
        let (value, minimum) = match tag {
            0..=250 => return Ok(tag as u64),
            251 => (
                u16::from_le_bytes(self.take(2)?.try_into().expect("u16")) as u64,
                251,
            ),
            252 => (
                u32::from_le_bytes(self.take(4)?.try_into().expect("u32")) as u64,
                65536,
            ),
            253 => (
                u64::from_le_bytes(self.take(8)?.try_into().expect("u64")),
                4294967296,
            ),
            _ => return Err(bad("invalid integer encoding")),
        };
        if value < minimum {
            return Err(bad("noncanonical integer encoding"));
        }
        Ok(value)
    }
    fn length(&mut self) -> Result<usize> {
        usize::try_from(self.integer()?).map_err(|_| bad("length overflow"))
    }
    fn text(&mut self) -> Result<(usize, usize)> {
        let len = self.length()?;
        let start = self.position;
        let raw = self.take(len)?;
        let mut at = 0;
        while at < raw.len() {
            (self.checkpoint)()?;
            let mut end = raw.len().min(at + 64 * 1024);
            if end < raw.len() {
                // A UTF-8 codepoint uses at most four bytes. Validate complete
                // windows while preserving the trailing codepoint for the next.
                while end > at && raw[end] & 0xc0 == 0x80 {
                    end -= 1;
                }
            }
            if end == at {
                return Err(bad("body text is not UTF-8"));
            }
            std::str::from_utf8(&raw[at..end]).map_err(|_| bad("body text is not UTF-8"))?;
            at = end;
        }
        Ok((start, self.position))
    }
    fn option_integer(&mut self) -> Result<Option<u64>> {
        match self.byte()? {
            0 => Ok(None),
            1 => Ok(Some(self.integer()?)),
            _ => Err(bad("invalid option marker")),
        }
    }
    fn floats(&mut self) -> Result<()> {
        let bytes = self
            .length()?
            .checked_mul(4)
            .ok_or_else(|| bad("vector size overflow"))?;
        self.take(bytes)?;
        Ok(())
    }
    fn scalar_value(&mut self) -> Result<()> {
        match self.integer()? {
            0 => (),
            1 => {
                if self.byte()? > 1 {
                    return Err(bad("invalid boolean"));
                }
            }
            2 | 6 | 9 => {
                self.integer()?;
            }
            3 => {
                self.take(8)?;
            }
            4 | 7 => {
                self.text()?;
            }
            5 => {
                let len = self.length()?;
                if len != 16 {
                    return Err(bad("invalid UUID length"));
                }
                self.take(len)?;
            }
            8 => self.floats()?,
            _ => return Err(bad("foreign scalar type")),
        }
        Ok(())
    }
    fn value(&mut self) -> Result<()> {
        match self.integer()? {
            0 => self.scalar_value()?,
            1 => match self.integer()? {
                0 => self.floats()?,
                kind @ (1 | 2) => {
                    self.take(8)?;
                    let dimension = self.integer()?;
                    if dimension > u32::MAX as u64 {
                        return Err(bad("vector dimension overflow"));
                    }
                    let len = self.length()?;
                    let expected = if kind == 1 {
                        dimension
                    } else {
                        dimension.div_ceil(2)
                    };
                    if len as u64 != expected {
                        return Err(bad("quantized payload length disagrees with dimension"));
                    }
                    self.take(len)?;
                }
                _ => return Err(bad("foreign vector representation")),
            },
            _ => return Err(bad("foreign persisted value")),
        }
        Ok(())
    }
}

/// Build only the declared scalar posting key, borrowing every payload from
/// the authenticated body. Quantized vector index keys retain their existing
/// admitted decoding path; ordinary wide scalar keys need just one buffer.
pub(in super::super) struct PrunedScalarPosting {
    pub(in super::super) key: Vec<u8>,
    pub(in super::super) suffix: usize,
    pub(in super::super) row_id: RowId,
    pub(in super::super) created: TxId,
    pub(in super::super) deleted: Option<TxId>,
}
pub(in super::super) fn has_indexed_vector(
    body: &[u8],
    declarations: &[IndexDecl],
) -> Result<bool> {
    let mut checkpoint = || Ok(());
    let mut input = Encoded {
        bytes: body,
        position: 0,
        checkpoint: &mut checkpoint,
    };
    input.integer()?;
    let count = input.length()?;
    let mut found = false;
    for _ in 0..count {
        let (name, name_end) = input.text()?;
        let start = input.position;
        input.value()?;
        if body[start] != 0
            && declarations.iter().any(|declaration| {
                declaration
                    .columns
                    .iter()
                    .any(|(column, _)| column.as_bytes() == &body[name..name_end])
            })
        {
            found = true;
        }
    }
    Ok(found)
}
pub(in super::super) fn scalar_posting_for_body(
    body: &[u8],
    declaration: &IndexDecl,
) -> Result<Option<PrunedScalarPosting>> {
    let controls = declaration
        .columns
        .len()
        .checked_mul(std::mem::size_of::<Option<&[u8]>>())
        .and_then(|n| n.checked_add(256))
        .ok_or_else(|| bad("posting descriptor overflow"))?;
    crate::read_image_memory::reserve(controls)?;
    let mut values = vec![None; declaration.columns.len()];
    let mut checkpoint = || Ok(());
    let mut input = Encoded {
        bytes: body,
        position: 0,
        checkpoint: &mut checkpoint,
    };
    let row_id = RowId(input.integer()?);
    let count = input.length()?;
    let mut vector_key = false;
    for _ in 0..count {
        let (name, name_end) = input.text()?;
        let start = input.position;
        input.value()?;
        for (position, (column, _)) in declaration.columns.iter().enumerate() {
            if column.as_bytes() == &body[name..name_end] {
                if values[position].is_some() {
                    return Err(bad("duplicate indexed value"));
                }
                if body[start] != 0 {
                    vector_key = true;
                }
                values[position] = Some(&body[start + 1..input.position]);
            }
        }
    }
    let created = TxId(input.integer()?);
    let deleted = input.option_integer()?.map(TxId);
    input.integer()?; // lsn, authenticated by the manifest mutation
    input.option_integer()?;
    if input.position != body.len() {
        return Err(bad("trailing body fields"));
    }
    if vector_key {
        return Ok(None);
    }
    let capacity = values.iter().try_fold(32usize, |n, value| {
        n.checked_add(1 + value.map_or(1, <[u8]>::len))
            .ok_or_else(|| bad("posting key overflow"))
    })?;
    crate::read_image_memory::reserve(capacity)?;
    let mut key = Vec::with_capacity(capacity);
    bincode::serde::encode_into_std_write(
        declaration.columns.len() as u64,
        &mut key,
        bincode::config::standard(),
    )
    .map_err(|e| bad(&format!("posting length: {e}")))?;
    for ((_, direction), value) in declaration.columns.iter().zip(values) {
        key.push(u8::from(*direction == contextdb_core::SortDirection::Desc));
        key.extend_from_slice(value.unwrap_or(&[0])); // Value::Null
    }
    let suffix = key.len();
    bincode::serde::encode_into_std_write((row_id, created), &mut key, bincode::config::standard())
        .map_err(|e| bad(&format!("posting identity: {e}")))?;
    Ok(Some(PrunedScalarPosting {
        key,
        suffix,
        row_id,
        created,
        deleted,
    }))
}

pub(in super::super) fn scalar_posting_identity(key: &[u8]) -> Result<(usize, RowId, TxId)> {
    let mut checkpoint = || Ok(());
    let mut input = Encoded {
        bytes: key,
        position: 0,
        checkpoint: &mut checkpoint,
    };
    let count = input.length()?;
    for _ in 0..count {
        if input.byte()? > 1 {
            return Err(bad("invalid index direction"));
        }
        input.scalar_value()?;
    }
    let suffix = input.position;
    let row = RowId(input.integer()?);
    let created = TxId(input.integer()?);
    if input.position != key.len() {
        return Err(bad("trailing posting fields"));
    }
    Ok((suffix, row, created))
}

/// Parse and hash the exact persisted representation before schema projection.
/// Scratch is admitted before the descriptor vector is allocated and refunded
/// after the hash, including every parse/admission failure.
pub(in super::super) fn manifest_for_body(
    table: &str,
    physical: &[u8],
    body: &[u8],
    admission: &mut ProofAdmission,
) -> Result<Vec<u8>> {
    manifest_for_body_with_checkpoint(table, physical, body, admission, &mut || Ok(()))
}

pub(in super::super) fn manifest_for_body_with_checkpoint(
    table: &str,
    physical: &[u8],
    body: &[u8],
    admission: &mut ProofAdmission,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<Vec<u8>> {
    checkpoint()?;
    if physical.len() != 24 {
        return Err(bad("physical key width"));
    }
    let mut input = Encoded {
        bytes: body,
        position: 0,
        checkpoint,
    };
    let row_id = input.integer()?;
    let count = input.length()?;
    if count > body.len() / 3 {
        return Err(bad("impossible value count"));
    }
    let descriptor_bytes = std::alloc::Layout::array::<Span>(count)
        .map_err(|_| bad("descriptor layout overflow"))?
        .size();
    let key_bytes = FIXED_KEY_BYTES
        .checked_add(table.len())
        .ok_or_else(|| bad("key length overflow"))?;
    let scratch = descriptor_bytes
        .checked_add(std::mem::size_of::<blake3::Hasher>())
        .and_then(|n| n.checked_add(32))
        .ok_or_else(|| bad("scratch overflow"))?;
    admission.admit(scratch)?;
    let result = (|| {
        let mut spans = Vec::with_capacity(count);
        for _ in 0..count {
            (input.checkpoint)()?;
            let (name, name_end) = input.text()?;
            let value = input.position;
            input.value()?;
            spans.push(Span {
                name,
                name_end,
                value,
                value_end: input.position,
            });
        }
        let created = input.integer()?;
        let deleted = input.option_integer()?;
        let lsn = input.integer()?;
        let created_at = input.option_integer()?;
        if input.position != body.len() {
            return Err(bad("trailing body fields"));
        }
        let mut identity = [0u8; 24];
        for (offset, value) in [(0, row_id), (8, created), (16, lsn)] {
            identity[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
        }
        if identity != physical {
            return Err(bad("body disagrees with physical identity"));
        }
        let checkpoint = input.checkpoint;
        // Unstable sorting is in-place and allocates no population-sized work.
        fn compare_names(
            body: &[u8],
            a: Span,
            b: Span,
            checkpoint: &mut dyn FnMut() -> Result<()>,
        ) -> Result<Ordering> {
            let a = &body[a.name..a.name_end];
            let b = &body[b.name..b.name_end];
            for (left, right) in a.chunks(64 * 1024).zip(b.chunks(64 * 1024)) {
                checkpoint()?;
                let order = left.cmp(right);
                if order != Ordering::Equal {
                    return Ok(order);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }
        fn sift(
            spans: &mut [Span],
            body: &[u8],
            mut root: usize,
            end: usize,
            checkpoint: &mut dyn FnMut() -> Result<()>,
        ) -> Result<()> {
            loop {
                checkpoint()?;
                let Some(mut child) = root
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(1))
                    .filter(|n| *n < end)
                else {
                    return Ok(());
                };
                if child + 1 < end
                    && compare_names(body, spans[child], spans[child + 1], checkpoint)?
                        == Ordering::Less
                {
                    child += 1;
                }
                if compare_names(body, spans[root], spans[child], checkpoint)? != Ordering::Less {
                    return Ok(());
                }
                spans.swap(root, child);
                root = child;
            }
        }
        for root in (0..count / 2).rev() {
            sift(&mut spans, body, root, count, checkpoint)?;
        }
        for end in (1..count).rev() {
            spans.swap(0, end);
            sift(&mut spans, body, 0, end, checkpoint)?;
        }
        for pair in spans.windows(2) {
            if compare_names(body, pair[0], pair[1], checkpoint)? == Ordering::Equal {
                return Err(bad("duplicate named value"));
            }
        }
        let mut hash = blake3::Hasher::new();
        let mut hash_bytes = 0usize;
        let mut update = |bytes: &[u8]| -> Result<()> {
            for window in bytes.chunks(64 * 1024) {
                checkpoint()?;
                hash.update(window);
                hash_bytes = hash_bytes
                    .checked_add(window.len())
                    .ok_or_else(|| bad("hash byte count overflow"))?;
            }
            Ok(())
        };
        update(b"contextdb.relational.version.v1")?;
        update(
            &u32::try_from(table.len())
                .map_err(|_| bad("table length overflow"))?
                .to_be_bytes(),
        )?;
        update(table.as_bytes())?;
        update(&identity)?;
        for value in [created_at, deleted] {
            update(&[u8::from(value.is_some())])?;
            update(&value.unwrap_or(0).to_be_bytes())?;
        }
        update(&(count as u64).to_be_bytes())?;
        for span in &spans {
            update(
                &u32::try_from(span.name_end - span.name)
                    .map_err(|_| bad("name length overflow"))?
                    .to_be_bytes(),
            )?;
            update(&body[span.name..span.name_end])?;
            update(&body[span.value..span.value + 1])?;
            update(&((span.value_end - span.value - 1) as u64).to_be_bytes())?;
            update(&body[span.value + 1..span.value_end])?;
        }
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_hashed_bytes(hash_bytes);
        let digest = *hash.finalize().as_bytes();
        admission.admit(key_bytes)?;
        let mut coordinate = Vec::with_capacity(key_bytes - 2);
        coordinate.push(FORMAT);
        coordinate.extend_from_slice(&(table.len() as u32).to_be_bytes());
        for window in table.as_bytes().chunks(64 * 1024) {
            checkpoint()?;
            coordinate.extend_from_slice(window);
        }
        coordinate.extend_from_slice(&identity);
        coordinate.push(u8::from(deleted.is_some()));
        coordinate.extend_from_slice(&deleted.unwrap_or(0).to_be_bytes());
        coordinate.extend_from_slice(&digest);
        Ok(coordinate)
    })();
    admission.release(scratch);
    result
}

pub(in super::super) fn manifest_mutation(
    coordinate: &[u8],
    present: bool,
) -> Result<ContentIdentityMutation> {
    parse_manifest(coordinate)?;
    let key = history_logical_key(HistoryKind::RelationalVersion, coordinate);
    Ok(if present {
        ContentIdentityMutation::put(LogicalFamily::ChangeAndDdl, key, [1])
    } else {
        ContentIdentityMutation::remove(LogicalFamily::ChangeAndDdl, key)
    })
}

/// An authenticated successor under the current writer transaction's root.
/// Only a path and one resolved separator exist at a time.
pub(in super::super) fn writer_manifest_next(
    write: &redb::WriteTransaction,
    start: &[u8],
    exclusive: bool,
    admission: &mut ProofAdmission,
) -> Result<Option<Vec<u8>>> {
    let family = LogicalFamily::ChangeAndDdl;
    let roots = write
        .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let encoded = roots
        .get(family as u8)
        .map_err(RedbPersistence::storage_error)?
        .ok_or_else(|| bad("missing family root"))?;
    let root = decode_published_root(family, encoded.value())?.identity;
    drop(encoded);
    drop(roots);
    let nodes = write
        .open_table(CONTENT_IDENTITY_NODES_TABLE)
        .map_err(RedbPersistence::storage_error)?;
    let target_bytes = start
        .len()
        .checked_add(2)
        .ok_or_else(|| bad("seek layout overflow"))?;
    admission.admit(target_bytes)?;
    let target = history_tree_key(HistoryKind::RelationalVersion, start);
    fn descend(
        nodes: &impl ReadableTable<&'static [u8], &'static [u8]>,
        id: Option<[u8; 32]>,
        expected: [u8; 32],
        target: &[u8],
        exclusive: bool,
        depth: usize,
        admission: &mut ProofAdmission,
    ) -> Result<Option<Vec<u8>>> {
        let Some(id) = id else { return Ok(None) };
        if depth > 64 {
            return Err(bad("proof path exceeds maximum height"));
        }
        let family = LogicalFamily::ChangeAndDdl;
        let (node, charged) = read_node_charged(nodes, family, id, admission)?;
        let result = (|| {
            if node.subtree_hash != expected {
                return Err(bad("writer proof root mismatch"));
            }
            for entry in &node.entries {
                let (key, key_charge) = resolve_key_charged(nodes, family, &entry.key, admission)?;
                let ordering = identity_key_cmp(&key, target)?;
                if ordering == Ordering::Less
                    || (node.level == 0 && exclusive && ordering == Ordering::Equal)
                {
                    admission.release(key_charge);
                    continue;
                }
                if node.level == 0 {
                    if key.starts_with(&[
                        ORDERED_HISTORY_KEY_TAG,
                        HistoryKind::RelationalVersion as u8,
                    ]) {
                        parse_manifest(&key[2..])?;
                        if entry.value_hash != value_hash(family, &[1]) {
                            return Err(bad("marker hash mismatch"));
                        }
                        admission.admit(key.len() - 2)?;
                        let coordinate = key[2..].to_vec();
                        admission.release(key_charge);
                        return Ok(Some(coordinate));
                    }
                    admission.release(key_charge);
                    return Ok(None);
                }
                let child = chunk_id(node.level - 1, &entry.key);
                admission.release(key_charge);
                if let Some(found) = descend(
                    nodes,
                    Some(child),
                    entry.value_hash,
                    target,
                    exclusive,
                    depth + 1,
                    admission,
                )? {
                    return Ok(Some(found));
                }
            }
            Ok(None)
        })();
        drop(node);
        admission.release(charged);
        result
    }
    let result = descend(
        &nodes, root.root, root.hash, &target, exclusive, 0, admission,
    );
    drop(target);
    admission.release(target_bytes);
    result
}

pub(in super::super) fn manifest_marker_hash() -> [u8; 32] {
    value_hash(LogicalFamily::ChangeAndDdl, &[1])
}

impl ContentIdentityReadVerifier {
    fn verify_manifest_projection_hash(
        &self,
        family: LogicalFamily,
        key: Vec<u8>,
        expected: Option<[u8; 32]>,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        let mutation = ContentIdentityMutation::remove(family, key);
        let mut admission = ProofAdmission::for_reader(memory);
        let result =
            self.verify_reader_projection(&mutation, &mut admission, Some(expected), checkpoint);
        if let Some(error) = admission.take_reader_refusal() {
            return Err(error);
        }
        result.map(|_| ()).map_err(|e| self.typed(e))
    }

    /// The established current scalar/vector projections are checked by
    /// streaming their unchanged canonical encoding into its existing hash.
    /// This creates no canonical value buffer or cloned value population.
    pub(in super::super) fn verify_current_row_projection(
        &self,
        table: &str,
        meta: &TableMeta,
        row: &VersionedRow,
        memory: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
        checkpoint: &mut dyn FnMut() -> Result<()>,
    ) -> Result<()> {
        checkpoint()?;
        let count = row.values.len();
        let descriptor_bytes = std::alloc::Layout::array::<(&String, &Value)>(count)
            .map_err(|_| bad("projection descriptor overflow"))?
            .size();
        let mut credit = contextdb_core::read_memory::ReadCredit::try_new(
            memory.clone(),
            descriptor_bytes
                .checked_add(std::mem::size_of::<blake3::Hasher>())
                .and_then(|n| n.checked_add(table.len()))
                .and_then(|n| n.checked_add(24))
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<(&String, &Value)>>()))
                .and_then(|n| n.checked_add(std::mem::size_of::<JsonSink>()))
                .and_then(|n| n.checked_add(std::mem::size_of::<ContentIdentityMutation>()))
                .and_then(|n| n.checked_add(std::mem::size_of::<ProofAdmission>()))
                .and_then(|n| n.checked_add(std::mem::size_of::<Vec<u8>>()))
                .and_then(|n| {
                    n.checked_add(2 * std::mem::size_of::<EmissionWriter<'_, JsonSink<'_>>>())
                })
                .and_then(|n| {
                    n.checked_add(
                        2 * std::mem::size_of::<
                            serde_json::Serializer<
                                &mut EmissionWriter<'_, JsonSink<'_>>,
                                StringFragmentFormatter,
                            >,
                        >(),
                    )
                })
                .ok_or_else(|| bad("projection controls overflow"))?,
            "reader",
            "current_projection_hash",
            "Raise the declared reader memory limit.",
        )?;
        let mut values = Vec::with_capacity(count);
        for (name, value) in &row.values {
            if matches!(value, Value::Vector(_)) {
                continue;
            }
            let mut vector_column = false;
            for column in &meta.columns {
                checkpoint()?;
                if matches!(column.column_type, contextdb_core::ColumnType::Vector(_))
                    && bytes_cmp_checkpoint(column.name.as_bytes(), name.as_bytes(), checkpoint)?
                        == Ordering::Equal
                {
                    vector_column = true;
                    break;
                }
            }
            if !vector_column {
                values.push((name, value));
            }
        }
        fn compare(
            a: &str,
            b: &str,
            checkpoint: &mut dyn FnMut() -> Result<()>,
        ) -> Result<Ordering> {
            for (left, right) in a
                .as_bytes()
                .chunks(64 * 1024)
                .zip(b.as_bytes().chunks(64 * 1024))
            {
                checkpoint()?;
                let order = left.cmp(right);
                if order != Ordering::Equal {
                    return Ok(order);
                }
            }
            Ok(a.len().cmp(&b.len()))
        }
        fn sift(
            values: &mut [(&String, &Value)],
            mut root: usize,
            end: usize,
            checkpoint: &mut dyn FnMut() -> Result<()>,
        ) -> Result<()> {
            loop {
                checkpoint()?;
                let Some(mut child) = root
                    .checked_mul(2)
                    .and_then(|n| n.checked_add(1))
                    .filter(|n| *n < end)
                else {
                    return Ok(());
                };
                if child + 1 < end
                    && compare(values[child].0, values[child + 1].0, checkpoint)? == Ordering::Less
                {
                    child += 1;
                }
                if compare(values[root].0, values[child].0, checkpoint)? != Ordering::Less {
                    return Ok(());
                }
                values.swap(root, child);
                root = child;
            }
        }
        let len = values.len();
        for root in (0..len / 2).rev() {
            sift(&mut values, root, len, checkpoint)?;
        }
        for end in (1..len).rev() {
            values.swap(0, end);
            sift(&mut values, 0, end, checkpoint)?;
        }
        fn projection_key(
            parts: &[&[u8]],
            checkpoint: &mut dyn FnMut() -> Result<()>,
        ) -> Result<Vec<u8>> {
            let bytes = parts.iter().try_fold(0usize, |n, part| {
                n.checked_add(8)
                    .and_then(|n| n.checked_add(part.len()))
                    .ok_or_else(|| bad("projection key overflow"))
            })?;
            let mut key = Vec::with_capacity(bytes);
            for part in parts {
                key.extend_from_slice(&(part.len() as u64).to_le_bytes());
                for window in part.chunks(64 * 1024) {
                    checkpoint()?;
                    key.extend_from_slice(window);
                }
            }
            Ok(key)
        }
        // The private encoder preserves the externally tagged Value bytes while
        // bounding serde_json's string scan at UTF-8 character boundaries.
        struct JsonSink<'a> {
            hash: Option<&'a mut blake3::Hasher>,
            bytes: usize,
        }
        impl std::io::Write for JsonSink<'_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                if let Some(hash) = &mut self.hash {
                    hash.update(bytes);
                }
                self.bytes = self
                    .bytes
                    .checked_add(bytes.len())
                    .ok_or_else(|| std::io::Error::other("JSON length overflow"))?;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut measured = JsonSink {
            hash: None,
            bytes: 0,
        };
        emit_projection(&mut measured, &values, checkpoint)?;
        let measured = measured.bytes;
        let family = LogicalFamily::Relational;
        let mut hash = blake3::Hasher::new();
        checkpoint()?;
        hash.update(b"contextdb-logical-record-value-v1");
        hash.update(&1u64.to_le_bytes());
        hash.update(&[family as u8]);
        hash.update(&(measured as u64).to_le_bytes());
        let mut sink = JsonSink {
            hash: Some(&mut hash),
            bytes: 0,
        };
        emit_projection(&mut sink, &values, checkpoint)?;
        drop(sink);
        #[cfg(feature = "test-seams")]
        crate::read_work_probe::note_hashed_bytes(
            b"contextdb-logical-record-value-v1".len() + 17 + measured,
        );
        let expected = *hash.finalize().as_bytes();
        let row_id = row.row_id.0.to_le_bytes();
        self.verify_manifest_projection_hash(
            family,
            projection_key(&[table.as_bytes(), &row_id], checkpoint)?,
            Some(expected),
            memory.clone(),
            checkpoint,
        )?;
        for column in &meta.columns {
            checkpoint()?;
            let contextdb_core::ColumnType::Vector(dimension) = column.column_type else {
                continue;
            };
            if column.quantization != VectorQuantization::F32 {
                continue;
            }
            let key_bytes = table
                .len()
                .checked_add(column.name.len())
                .and_then(|n| n.checked_add(32))
                .ok_or_else(|| bad("vector key overflow"))?;
            credit.try_grow_for(
                key_bytes,
                "reader",
                "current_vector_projection_key",
                "Raise the declared reader memory limit.",
            )?;
            let key = projection_key(
                &[
                    table.as_bytes(),
                    column.name.as_bytes(),
                    &row.row_id.0.to_le_bytes(),
                ],
                checkpoint,
            )?;
            let expected = match row.values.get(&column.name) {
                Some(Value::Vector(vector)) if vector.len() == dimension => {
                    let bytes = vector
                        .len()
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(8))
                        .ok_or_else(|| bad("vector hash length overflow"))?;
                    let mut hash = blake3::Hasher::new();
                    hash.update(b"contextdb-logical-record-value-v1");
                    hash.update(&1u64.to_le_bytes());
                    hash.update(&[LogicalFamily::F32Vectors as u8]);
                    hash.update(&(bytes as u64).to_le_bytes());
                    hash.update(&(vector.len() as u64).to_le_bytes());
                    for window in vector.chunks((64 * 1024) / 4) {
                        checkpoint()?;
                        for value in window {
                            hash.update(&value.to_bits().to_le_bytes());
                        }
                    }
                    #[cfg(feature = "test-seams")]
                    crate::read_work_probe::note_hashed_bytes(
                        b"contextdb-logical-record-value-v1".len() + 17 + bytes,
                    );
                    Some(*hash.finalize().as_bytes())
                }
                None | Some(Value::Null) if column.nullable => None,
                _ => {
                    return Err(self.corrupt(
                        "current vector representation disagrees with its declared column",
                    ));
                }
            };
            self.verify_manifest_projection_hash(
                LogicalFamily::F32Vectors,
                key,
                expected,
                memory.clone(),
                checkpoint,
            )?;
            credit.try_shrink(key_bytes)?;
        }
        Ok(())
    }
}

/// Default serde_json escaping, omitting only each bounded string's quotes.
struct StringFragmentFormatter;
impl serde_json::ser::Formatter for StringFragmentFormatter {
    fn begin_string<W: ?Sized + std::io::Write>(&mut self, _: &mut W) -> std::io::Result<()> {
        Ok(())
    }
    fn end_string<W: ?Sized + std::io::Write>(&mut self, _: &mut W) -> std::io::Result<()> {
        Ok(())
    }
}

struct EmissionWriter<'a, W> {
    writer: &'a mut W,
    checkpoint: &'a mut dyn FnMut() -> Result<()>,
    escaped: bool,
    failure: Option<Error>,
}
impl<W: std::io::Write> std::io::Write for EmissionWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = if self.escaped {
            std::str::from_utf8(bytes)
                .map_err(|_| bad("JSON fragment is not UTF-8"))
                .and_then(|text| emit_string_content(self.writer, text, self.checkpoint, false))
        } else {
            emit_bytes(self.writer, bytes, self.checkpoint)
        };
        if let Err(error) = result {
            self.failure = Some(error);
            return Err(std::io::Error::other("reader JSON emission refused"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn emit_bytes<W: std::io::Write>(
    writer: &mut W,
    bytes: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    for window in bytes.chunks(64 * 1024) {
        checkpoint()?;
        writer
            .write_all(window)
            .map_err(|error| bad(&error.to_string()))?;
    }
    Ok(())
}
fn emit_serialized<W: std::io::Write, T: Serialize + ?Sized>(
    writer: &mut W,
    value: &T,
    checkpoint: &mut dyn FnMut() -> Result<()>,
    escaped: bool,
) -> Result<()> {
    checkpoint()?;
    let mut output = EmissionWriter {
        writer,
        checkpoint,
        escaped,
        failure: None,
    };
    let encoded = serde_json::to_writer(&mut output, value);
    if let Some(error) = output.failure.take() {
        return Err(error);
    }
    encoded.map_err(|error| bad(&error.to_string()))
}
fn emit_string_content<W: std::io::Write>(
    writer: &mut W,
    text: &str,
    checkpoint: &mut dyn FnMut() -> Result<()>,
    escaped: bool,
) -> Result<()> {
    let mut position = 0;
    while position < text.len() {
        checkpoint()?;
        let mut end = position.saturating_add(64 * 1024).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let mut output = EmissionWriter {
            writer: &mut *writer,
            checkpoint: &mut *checkpoint,
            escaped,
            failure: None,
        };
        let encoded = text[position..end].serialize(&mut serde_json::Serializer::with_formatter(
            &mut output,
            StringFragmentFormatter,
        ));
        if let Some(error) = output.failure.take() {
            return Err(error);
        }
        encoded.map_err(|error| bad(&error.to_string()))?;
        position = end;
    }
    Ok(())
}
fn emit_raw<W: std::io::Write>(
    writer: &mut W,
    bytes: &[u8],
    checkpoint: &mut dyn FnMut() -> Result<()>,
    escaped: bool,
) -> Result<()> {
    if escaped {
        emit_string_content(
            writer,
            std::str::from_utf8(bytes).map_err(|_| bad("JSON token is not UTF-8"))?,
            checkpoint,
            false,
        )
    } else {
        emit_bytes(writer, bytes, checkpoint)
    }
}
fn emit_string<W: std::io::Write>(
    writer: &mut W,
    text: &str,
    checkpoint: &mut dyn FnMut() -> Result<()>,
    escaped: bool,
) -> Result<()> {
    emit_raw(writer, b"\"", checkpoint, escaped)?;
    emit_string_content(writer, text, checkpoint, escaped)?;
    emit_raw(writer, b"\"", checkpoint, escaped)
}
fn emit_json<W: std::io::Write>(
    writer: &mut W,
    value: &serde_json::Value,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    checkpoint()?;
    match value {
        serde_json::Value::Null => emit_raw(writer, b"null", checkpoint, true),
        serde_json::Value::Bool(value) => emit_serialized(writer, value, checkpoint, true),
        serde_json::Value::Number(value) => emit_serialized(writer, value, checkpoint, true),
        serde_json::Value::String(value) => emit_string(writer, value, checkpoint, true),
        serde_json::Value::Array(values) => {
            emit_raw(writer, b"[", checkpoint, true)?;
            for (position, value) in values.iter().enumerate() {
                if position != 0 {
                    emit_raw(writer, b",", checkpoint, true)?;
                }
                emit_json(writer, value, checkpoint)?;
            }
            emit_raw(writer, b"]", checkpoint, true)
        }
        serde_json::Value::Object(values) => {
            emit_raw(writer, b"{", checkpoint, true)?;
            for (position, (name, value)) in values.iter().enumerate() {
                if position != 0 {
                    emit_raw(writer, b",", checkpoint, true)?;
                }
                emit_string(writer, name, checkpoint, true)?;
                emit_raw(writer, b":", checkpoint, true)?;
                emit_json(writer, value, checkpoint)?;
            }
            emit_raw(writer, b"}", checkpoint, true)
        }
    }
}
pub(in super::super) fn emit_value<W: std::io::Write>(
    writer: &mut W,
    value: &Value,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    let name = match value {
        Value::Null => return emit_bytes(writer, b"\"Null\"", checkpoint),
        Value::Bool(_) => "Bool",
        Value::Int64(_) => "Int64",
        Value::Float64(_) => "Float64",
        Value::Text(_) => "Text",
        Value::Uuid(_) => "Uuid",
        Value::Timestamp(_) => "Timestamp",
        Value::Json(_) => "Json",
        Value::Vector(_) => "Vector",
        Value::TxId(_) => "TxId",
    };
    emit_bytes(writer, b"{", checkpoint)?;
    emit_string(writer, name, checkpoint, false)?;
    emit_bytes(writer, b":", checkpoint)?;
    match value {
        Value::Bool(value) => emit_serialized(writer, value, checkpoint, false)?,
        Value::Int64(value) | Value::Timestamp(value) => {
            emit_serialized(writer, value, checkpoint, false)?
        }
        Value::Float64(value) => emit_serialized(writer, value, checkpoint, false)?,
        Value::Text(value) => emit_string(writer, value, checkpoint, false)?,
        Value::Uuid(value) => emit_serialized(writer, value, checkpoint, false)?,
        Value::TxId(value) => emit_serialized(writer, value, checkpoint, false)?,
        Value::Json(value) => {
            emit_bytes(writer, b"\"", checkpoint)?;
            emit_json(writer, value, checkpoint)?;
            emit_bytes(writer, b"\"", checkpoint)?;
        }
        Value::Vector(values) => {
            emit_bytes(writer, b"[", checkpoint)?;
            for (position, value) in values.iter().enumerate() {
                if position != 0 {
                    emit_bytes(writer, b",", checkpoint)?;
                }
                emit_serialized(writer, value, checkpoint, false)?;
            }
            emit_bytes(writer, b"]", checkpoint)?;
        }
        Value::Null => unreachable!(),
    }
    emit_bytes(writer, b"}", checkpoint)
}
fn emit_projection<W: std::io::Write>(
    writer: &mut W,
    values: &[(&String, &Value)],
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<()> {
    emit_bytes(writer, b"{", checkpoint)?;
    for (position, (name, value)) in values.iter().enumerate() {
        if position != 0 {
            emit_bytes(writer, b",", checkpoint)?;
        }
        emit_string(writer, name, checkpoint, false)?;
        emit_bytes(writer, b":", checkpoint)?;
        emit_value(writer, value, checkpoint)?;
    }
    emit_bytes(writer, b"}", checkpoint)
}

pub(in super::super) fn relation_name_with_checkpoint(
    table: &str,
    checkpoint: &mut dyn FnMut() -> Result<()>,
) -> Result<String> {
    let capacity = table
        .len()
        .checked_add(4)
        .ok_or_else(|| bad("relation name overflow"))?;
    let mut name = String::with_capacity(capacity);
    name.push_str("rel_");
    let mut position = 0;
    while position < table.len() {
        checkpoint()?;
        let mut end = position.saturating_add(64 * 1024).min(table.len());
        while !table.is_char_boundary(end) {
            end -= 1;
        }
        name.push_str(&table[position..end]);
        position = end;
    }
    Ok(name)
}

/// The admitted but unpublished manifest deltas a writer overlays on its
/// unchanged authenticated root. Only the latest delta at a coordinate counts.
pub(in super::super) trait ManifestOverlay {
    /// `Some(true)` when the latest delta at `coordinate` adds it,
    /// `Some(false)` when it removes it, `None` when there is no delta.
    fn latest_present(&self, coordinate: &[u8]) -> Option<bool>;
    /// The smallest coordinate whose latest delta adds it, at or after `start`
    /// (strictly after when `exclusive`).
    fn first_present_from(&self, start: &[u8], exclusive: bool) -> Option<&[u8]>;
}

fn overlay_coordinate(mutation: &ContentIdentityMutation) -> &[u8] {
    &mutation.logical_key[ORDERED_HISTORY_MAGIC.len() + 1..]
}

/// Unindexed deltas in publication order: each query rescans the list.
impl ManifestOverlay for [ContentIdentityMutation] {
    fn latest_present(&self, coordinate: &[u8]) -> Option<bool> {
        latest_in_slice(self, coordinate).map(|mutation| mutation.logical_value.is_some())
    }

    fn first_present_from(&self, start: &[u8], exclusive: bool) -> Option<&[u8]> {
        let mut selected: Option<&[u8]> = None;
        for mutation in self {
            #[cfg(any(test, feature = "test-seams"))]
            commit_row_work::count_overlay_entries(1);
            let key = overlay_coordinate(mutation);
            if mutation.logical_value.is_none()
                || !std::ptr::eq(latest_in_slice(self, key).expect("overlay member"), mutation)
                || key < start
                || (exclusive && key == start)
                || selected.is_some_and(|selected| key >= selected)
            {
                continue;
            }
            selected = Some(key);
        }
        selected
    }
}

fn latest_in_slice<'a>(
    pending: &'a [ContentIdentityMutation],
    coordinate: &[u8],
) -> Option<&'a ContentIdentityMutation> {
    pending.iter().rev().find(|mutation| {
        #[cfg(any(test, feature = "test-seams"))]
        commit_row_work::count_overlay_entries(1);
        overlay_coordinate(mutation) == coordinate
    })
}

/// Overlay admitted unpublished deltas on the unchanged authenticated root.
/// Rooted seeks still authenticate every stored candidate, including removed
/// predecessors; the overlay cannot make a damaged stored member disappear.
pub(in super::super) fn writer_manifest_next_pending<O: ManifestOverlay + ?Sized>(
    write: &redb::WriteTransaction,
    start: &[u8],
    exclusive: bool,
    admission: &mut ProofAdmission,
    pending: &O,
) -> Result<Option<Vec<u8>>> {
    let mut stored = writer_manifest_next(write, start, exclusive, admission)?;
    while stored
        .as_deref()
        .is_some_and(|key| pending.latest_present(key) == Some(false))
    {
        let key = stored.take().expect("selected predecessor");
        stored = writer_manifest_next(write, &key, true, admission)?;
        let bytes = key.capacity();
        drop(key);
        admission.release(bytes);
    }
    let selected = pending
        .first_present_from(start, exclusive)
        .filter(|key| stored.as_deref().is_none_or(|stored| *key < stored));
    if let Some(key) = selected {
        admission.admit(key.len())?;
        let selected = key.to_vec();
        if let Some(stored) = stored {
            let bytes = stored.capacity();
            drop(stored);
            admission.release(bytes);
        }
        Ok(Some(selected))
    } else {
        Ok(stored)
    }
}

/// Work counters for one writer's commit, read by the row-scaling tests.
#[cfg(any(test, feature = "test-seams"))]
pub(in super::super) mod commit_row_work {
    use std::cell::Cell;

    thread_local! {
        static OVERLAY_ENTRIES: Cell<u64> = const { Cell::new(0) };
        static PROTECTED_ROOT_EVENTS: Cell<u64> = const { Cell::new(0) };
    }

    /// One open, close, delete or relocation of a historical-root-protected
    /// table. Each one makes redb finalize the checksums of every page the
    /// write transaction has dirtied in the tables closed since the last one.
    pub(in super::super::super) fn count_protected_root_event() {
        PROTECTED_ROOT_EVENTS.with(|count| count.set(count.get().saturating_add(1)));
    }

    pub(in super::super::super) fn take_protected_root_events() -> u64 {
        PROTECTED_ROOT_EVENTS.with(|count| count.replace(0))
    }

    /// Pending manifest deltas examined to answer one overlay query.
    pub(in super::super::super) fn count_overlay_entries(entries: usize) {
        OVERLAY_ENTRIES.with(|count| count.set(count.get().saturating_add(entries as u64)));
    }

    pub(in super::super::super) fn take_overlay_entries() -> u64 {
        OVERLAY_ENTRIES.with(|count| count.replace(0))
    }
}

pub(in super::super) fn manifest_mutation_bytes(coordinate: &[u8], present: bool) -> Result<usize> {
    coordinate
        .len()
        .checked_add(ORDERED_HISTORY_MAGIC.len() + 1)
        .and_then(|bytes| bytes.checked_add(usize::from(present)))
        .ok_or_else(|| bad("mutation layout overflow"))
}
