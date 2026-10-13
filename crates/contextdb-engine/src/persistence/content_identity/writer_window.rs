//! Invocation-local proof planning over borrowed originals and disposable working storage.
use super::*;
use std::cell::{Cell, RefCell};

const NONE: u64 = u64::MAX;
const RECORD_BUFFER_BYTES: usize = Parent::BYTES + 32;
const STREAM_BLOCK_BYTES: usize = MAX_NODE_BYTES;
const STREAM_HEADER_BYTES: usize = 16;

impl Window<'_, '_, '_> {
    /// A bounded authenticated writer descent, retaining only its proof path.
    fn history_search<T>(
        &self,
        address: ([u8; 32], [u8; 32]),
        level: Option<u32>,
        bounds: (&[u8], &[u8], bool),
        reverse: bool,
        take: &mut impl FnMut(&[u8], [u8; 32]) -> Result<T>,
    ) -> Result<Option<T>> {
        let original = self.original(address.0, address.1)?;
        if original.level > MAX_CHUNK_LEVEL
            || level.is_some_and(|parent| original.level.checked_add(1) != Some(parent))
        {
            return Err(history_control_damage(
                "history writer proof depth mismatch",
            ));
        }
        self.validate_order(&original)?;
        let (low, high, exact) = bounds;
        let count = original.entries.entries.len();
        for step in 0..count {
            let index = if reverse { count - 1 - step } else { step };
            let entry = &original.entries.entries[index];
            let key = &original.guard.value()
                [entry.start as usize..entry.start as usize + entry.len as usize];
            let lower_ok =
                self.with_referenced(key, |raw| Ok(identity_key_cmp(raw, low)? != Ordering::Less))?;
            if !lower_ok {
                continue;
            }
            if original.level == 0 {
                let in_range = self.with_referenced(key, |raw| {
                    let order = identity_key_cmp(raw, high)?;
                    Ok(if exact {
                        order == Ordering::Equal
                    } else {
                        order == Ordering::Less
                    })
                })?;
                if in_range {
                    return self
                        .with_referenced(key, |raw| take(raw, entry.hash))
                        .map(Some);
                }
                continue;
            }
            if index > 0 {
                let previous = &original.entries.entries[index - 1];
                let key = &original.guard.value()
                    [previous.start as usize..previous.start as usize + previous.len as usize];
                let too_high = self.with_referenced(key, |raw| {
                    Ok(identity_key_cmp(raw, high)? != Ordering::Less)
                })?;
                if too_high {
                    continue;
                }
            }
            let child = chunk_id(original.level - 1, key);
            if let Some(value) = self.history_search(
                (child, entry.hash),
                Some(original.level),
                bounds,
                reverse,
                take,
            )? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

fn with_history_window<T>(
    table: &redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    root: &FamilyRoot,
    bounds: (&[u8], &[u8], bool),
    reverse: bool,
    mut take: impl FnMut(&[u8], [u8; 32]) -> Result<T>,
) -> Result<Option<T>> {
    let Some(id) = root.root else {
        return Ok(None);
    };
    let scratch = Scratch::open(admission)?;
    let base = admission
        .held
        .checked_add(scratch._charge.bytes)
        .ok_or_else(overflow)?;
    let ledger = Ledger {
        live: Cell::new(0),
        base,
        caller_peak: &admission.peak,
    };
    let workspace = Workspace {
        admission: RefCell::new(ProofAdmission {
            reader_budget: admission.reader_budget.clone(),
            reader_refusal: admission.reader_refusal.clone(),
            accountant: admission
                .accountant
                .clone()
                .or_else(|| admission.scratch_accountant.clone()),
            reclaim: admission.reclaim.clone(),
            charge_fixed_refusal_control: admission.charge_fixed_refusal_control,
            scratch_owner: admission.scratch_owner.clone(),
            scratch_accountant: admission.scratch_accountant.clone(),
            held: 0,
            peak: Cell::new(0),
        }),
        ledger: &ledger,
    };
    Window::open(
        table,
        LogicalFamily::ChangeAndDdl,
        Vec::new(),
        scratch,
        &workspace,
    )
    .history_search((id, root.hash), None, bounds, reverse, &mut take)
}

pub(super) fn history_member(
    table: &redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    root: &FamilyRoot,
    key: &[u8],
) -> Result<Option<[u8; 32]>> {
    with_history_window(
        table,
        admission,
        root,
        (key, key, true),
        false,
        |_, hash| Ok(hash),
    )
}
pub(super) fn history_commit_extreme(
    table: &redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    root: &FamilyRoot,
    reverse: bool,
) -> Result<Option<(Lsn, [u8; 32])>> {
    with_history_window(
        table,
        admission,
        root,
        (
            &[ORDERED_HISTORY_KEY_TAG, 0],
            &[ORDERED_HISTORY_KEY_TAG, 1],
            false,
        ),
        reverse,
        |key, hash| {
            let raw: [u8; 8] = key[2..]
                .try_into()
                .map_err(|_| history_control_damage("invalid committed LSN coordinate"))?;
            Ok((Lsn(u64::from_be_bytes(raw)), hash))
        },
    )
}
pub(super) fn history_ddl_extreme(
    table: &redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    root: &FamilyRoot,
) -> Result<Option<(u64, Lsn, [u8; 32], [u8; 32])>> {
    with_history_window(
        table,
        admission,
        root,
        (
            &[ORDERED_HISTORY_KEY_TAG, 2],
            &[ORDERED_HISTORY_KEY_TAG, 3],
            false,
        ),
        true,
        |key, value| {
            let coordinate = &key[2..];
            let text = std::str::from_utf8(coordinate)
                .map_err(|_| history_control_damage("invalid DDL coordinate encoding"))?;
            validate_history_key(text)?;
            let lsn = text
                .split_once(':')
                .map_or(text, |(lsn, _)| lsn)
                .parse()
                .map_err(|_| history_control_damage("invalid DDL coordinate LSN"))?;
            Ok((
                u64::try_from(coordinate.len()).map_err(|_| overflow())?,
                Lsn(lsn),
                history_open_control::ddl_coordinate_digest(coordinate, None)?,
                value,
            ))
        },
    )
}

fn overflow() -> Error {
    Error::Other("proof scratch offset overflow".into())
}
fn scratch_error(operation: &str, error: std::io::Error) -> Error {
    Error::ScratchWorkingStorageUnavailable {
        operation: operation.into(),
        io_kind: error.kind(),
    }
}
fn position(value: u64) -> Result<usize> {
    usize::try_from(value).map_err(|_| overflow())
}

/// No buffered IO owner exists beside the admitted backend. All variable
/// bodies and directories live in its immediately unlinked file.
struct Scratch {
    backend: Box<dyn redb::ScratchBackend>,
    end: Cell<u64>,
    _charge: AdmittedBytes,
}
impl Scratch {
    fn open(admission: &mut ProofAdmission) -> Result<Self> {
        let (backend, charge) = admission.open_scratch()?;
        Ok(Self {
            backend,
            end: Cell::new(0),
            _charge: charge,
        })
    }
    fn reserve(&self, bytes: usize) -> Result<u64> {
        let start = self.end.get();
        let end = start
            .checked_add(u64::try_from(bytes).map_err(|_| overflow())?)
            .ok_or_else(overflow)?;
        self.backend
            .set_len(end)
            .map_err(|e| scratch_error("extend_content_identity_scratch", e))?;
        self.end.set(end);
        Ok(start)
    }
    fn read(&self, offset: u64, bytes: &mut [u8]) -> Result<()> {
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|end| end > self.end.get())
        {
            return Err(scratch_error(
                "read_content_identity_scratch",
                std::io::Error::from(std::io::ErrorKind::UnexpectedEof),
            ));
        }
        self.backend
            .read(offset, bytes)
            .map_err(|e| scratch_error("read_content_identity_scratch", e))
    }
    fn write(&self, offset: u64, bytes: &[u8]) -> Result<()> {
        if offset
            .checked_add(bytes.len() as u64)
            .is_none_or(|end| end > self.end.get())
        {
            return Err(overflow());
        }
        self.backend
            .write(offset, bytes)
            .map_err(|e| scratch_error("write_content_identity_scratch", e))
    }
    fn append(&self, bytes: &[u8]) -> Result<u64> {
        let offset = self.reserve(bytes.len())?;
        self.write(offset, bytes)?;
        Ok(offset)
    }
    fn get<T: DiskRecord>(&self, offset: u64) -> Result<T> {
        let mut bytes = [0; RECORD_BUFFER_BYTES];
        self.read(offset, &mut bytes[..T::BYTES + 32])?;
        if blake3::hash(&bytes[..T::BYTES]).as_bytes() != &bytes[T::BYTES..T::BYTES + 32] {
            return Err(scratch_error(
                "authenticate_content_identity_scratch_record",
                std::io::Error::from(std::io::ErrorKind::InvalidData),
            ));
        }
        T::decode(&bytes[..T::BYTES])
    }
    fn put<T: DiskRecord>(&self, offset: u64, record: T) -> Result<()> {
        let mut bytes = [0; RECORD_BUFFER_BYTES];
        record.encode(&mut bytes[..T::BYTES]);
        let hash = *blake3::hash(&bytes[..T::BYTES]).as_bytes();
        bytes[T::BYTES..T::BYTES + 32].copy_from_slice(&hash);
        self.write(offset, &bytes[..T::BYTES + 32])
    }
    fn push<T: DiskRecord>(&self, record: T) -> Result<u64> {
        let offset = self.reserve(T::BYTES + 32)?;
        self.put(offset, record)?;
        Ok(offset)
    }
}

trait DiskRecord: Copy {
    const BYTES: usize;
    fn encode(self, bytes: &mut [u8]);
    fn decode(bytes: &[u8]) -> Result<Self>;
}
struct Encoder<'a> {
    bytes: &'a mut [u8],
    at: usize,
}
impl Encoder<'_> {
    fn bytes(&mut self, value: &[u8]) {
        self.bytes[self.at..self.at + value.len()].copy_from_slice(value);
        self.at += value.len();
    }
    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }
    fn hash(&mut self, value: Option<[u8; 32]>) {
        self.bytes(&[u8::from(value.is_some())]);
        self.bytes(&value.unwrap_or([0; 32]));
    }
    fn locator(&mut self, loc: Locator) {
        self.u64(loc.kind);
        self.bytes(&loc.id);
        self.bytes(&loc.hash);
        self.u64(loc.start);
        self.u64(loc.len);
    }
    fn key(&mut self, key: Key) {
        self.locator(key.encoded);
        self.locator(key.raw);
        self.bytes(&key.inline);
    }
}
struct Decoder<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl Decoder<'_> {
    fn array<const N: usize>(&mut self) -> [u8; N] {
        let value = self.bytes[self.at..self.at + N]
            .try_into()
            .expect("fixed scratch field");
        self.at += N;
        value
    }
    fn u64(&mut self) -> u64 {
        u64::from_be_bytes(self.array())
    }
    fn hash(&mut self) -> Result<Option<[u8; 32]>> {
        let flag = self.array::<1>()[0];
        let hash = self.array();
        match flag {
            0 => Ok(None),
            1 => Ok(Some(hash)),
            _ => Err(Error::Other("invalid scratch optional hash".into())),
        }
    }
    fn locator(&mut self) -> Result<Locator> {
        let loc = Locator {
            kind: self.u64(),
            id: self.array(),
            hash: self.array(),
            start: self.u64(),
            len: self.u64(),
        };
        if loc.kind > 2 {
            return Err(Error::Other("invalid scratch key locator".into()));
        }
        Ok(loc)
    }
    fn key(&mut self) -> Result<Key> {
        Ok(Key {
            encoded: self.locator()?,
            raw: self.locator()?,
            inline: self.array(),
        })
    }
}

/// Raw provenance points to a complete caller key, authenticated original,
/// or a byte-identical resident inline copy. Wide separators keep
/// their complete original slice, including accepted equal encodings.
#[derive(Clone, Copy)]
struct Locator {
    kind: u64,
    id: [u8; 32],
    hash: [u8; 32],
    start: u64,
    len: u64,
}
impl Locator {
    fn input(index: usize, len: usize) -> Self {
        Self {
            kind: 0,
            id: [0; 32],
            hash: [0; 32],
            start: index as u64,
            len: len as u64,
        }
    }
    fn inline(bytes: &[u8]) -> Self {
        Self {
            kind: 2,
            id: [0; 32],
            hash: [0; 32],
            start: 0,
            len: bytes.len() as u64,
        }
    }
}
#[derive(Clone, Copy)]
struct Key {
    encoded: Locator,
    raw: Locator,
    inline: [u8; INLINE_SEPARATOR_BYTES],
}

#[derive(Clone, Copy)]
struct Edit {
    key: Key,
    value: Option<[u8; 32]>,
}
impl DiskRecord for Edit {
    const BYTES: usize = 176 + INLINE_SEPARATOR_BYTES + 33;
    fn encode(self, bytes: &mut [u8]) {
        let mut e = Encoder { bytes, at: 0 };
        e.key(self.key);
        e.hash(self.value);
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut d = Decoder { bytes, at: 0 };
        Ok(Self {
            key: d.key()?,
            value: d.hash()?,
        })
    }
}

/// Parent submissions preserve the old map's operation order, independently
/// of semantic key order. A tombstone precedes processing its source chunk;
/// every real finish receives its later ordinal, including an equal raw key.
#[derive(Clone, Copy)]
struct Parent {
    edit: Edit,
    sequence: u64,
}
impl DiskRecord for Parent {
    const BYTES: usize = Edit::BYTES + 8;
    fn encode(self, bytes: &mut [u8]) {
        self.edit.encode(&mut bytes[..Edit::BYTES]);
        bytes[Edit::BYTES..].copy_from_slice(&self.sequence.to_be_bytes());
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(Self {
            edit: Edit::decode(&bytes[..Edit::BYTES])?,
            sequence: u64::from_be_bytes(bytes[Edit::BYTES..].try_into().expect("parent ordinal")),
        })
    }
}

/// Operations remain distinct until sorted by (physical key, operation ordinal).
/// A seen-only operation never changes a staged body, and a body never invents
/// original authority. This is the former directory's replacement order.
#[derive(Clone, Copy)]
struct Write {
    key: [u8; 33],
    original: Option<[u8; 32]>,
    hash: Option<[u8; 32]>,
    body: u64,
    len: u64,
    remove: bool,
    sequence: u64,
    operation: u8,
}
impl Write {
    fn seen(key: [u8; 33], hash: [u8; 32]) -> Self {
        Self {
            key,
            original: Some(hash),
            hash: None,
            body: NONE,
            len: 0,
            remove: false,
            sequence: 0,
            operation: 0,
        }
    }
    fn removal(key: [u8; 33]) -> Self {
        Self {
            key,
            original: None,
            hash: None,
            body: NONE,
            len: 0,
            remove: true,
            sequence: 0,
            operation: 1,
        }
    }
    fn finished(key: [u8; 33], hash: [u8; 32], body: u64, len: usize) -> Self {
        Self {
            key,
            original: None,
            hash: Some(hash),
            body,
            len: len as u64,
            remove: false,
            sequence: 0,
            operation: 2,
        }
    }
    fn fold(&mut self, later: Self) -> Result<()> {
        if self.key != later.key {
            return Err(Error::Other("proof write fold key mismatch".into()));
        }
        if let Some(hash) = later.original {
            if self.original.is_some_and(|original| original != hash) {
                return Err(Error::Other(
                    "logical identity original hash changed".into(),
                ));
            }
            self.original = Some(hash);
        }
        if later.operation != 0 {
            self.hash = later.hash;
            self.body = later.body;
            self.len = later.len;
            self.remove = later.remove;
            self.operation = later.operation;
        }
        self.sequence = later.sequence;
        Ok(())
    }
    fn order(&self, other: &Self) -> Ordering {
        self.key
            .cmp(&other.key)
            .then(self.sequence.cmp(&other.sequence))
    }
}
impl DiskRecord for Write {
    const BYTES: usize = 33 + 66 + 16 + 1 + 8 + 1;
    fn encode(self, bytes: &mut [u8]) {
        let mut e = Encoder { bytes, at: 0 };
        e.bytes(&self.key);
        e.hash(self.original);
        e.hash(self.hash);
        e.u64(self.body);
        e.u64(self.len);
        e.bytes(&[u8::from(self.remove)]);
        e.u64(self.sequence);
        e.bytes(&[self.operation]);
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut d = Decoder { bytes, at: 0 };
        let key = d.array();
        let original = d.hash()?;
        let hash = d.hash()?;
        let body = d.u64();
        let len = d.u64();
        let flag = d.array::<1>()[0];
        let sequence = d.u64();
        let operation = d.array::<1>()[0];
        if flag > 1 || operation > 2 {
            return Err(Error::Other("invalid proof write operation".into()));
        }
        Ok(Self {
            key,
            original,
            hash,
            body,
            len,
            remove: flag != 0,
            sequence,
            operation,
        })
    }
}

/// Bounded-block sequential streams share the one admitted unlinked backend.
/// Bodies may be interleaved in that file. Only a block boundary has a link;
/// records have no links, record checksums, trees or random access directory.
#[derive(Clone, Copy)]
struct Stream {
    first: u64,
    tail: u64,
    tail_count: usize,
    count: u64,
}
impl Stream {
    fn new() -> Self {
        Self {
            first: NONE,
            tail: NONE,
            tail_count: 0,
            count: 0,
        }
    }
    fn record_offset<T: DiskRecord>(block: u64, index: usize) -> Result<u64> {
        let offset = index.checked_mul(T::BYTES).ok_or_else(overflow)?;
        block
            .checked_add(STREAM_HEADER_BYTES as u64)
            .and_then(|start| start.checked_add(offset as u64))
            .ok_or_else(overflow)
    }
    fn push<T: DiskRecord>(&mut self, scratch: &Scratch, record: T) -> Result<()> {
        let slots = (STREAM_BLOCK_BYTES - STREAM_HEADER_BYTES) / T::BYTES;
        if slots == 0 {
            return Err(overflow());
        }
        if self.tail == NONE || self.tail_count == slots {
            let block = scratch.reserve(STREAM_BLOCK_BYTES)?;
            let mut header = [0; STREAM_HEADER_BYTES];
            header[..8].copy_from_slice(&NONE.to_be_bytes());
            header[8..].copy_from_slice(&(slots as u64).to_be_bytes());
            scratch.write(block, &header)?;
            if self.tail == NONE {
                self.first = block;
            } else {
                scratch.write(self.tail, &block.to_be_bytes())?;
            }
            self.tail = block;
            self.tail_count = 0;
        }
        let mut bytes = [0; RECORD_BUFFER_BYTES];
        record.encode(&mut bytes[..T::BYTES]);
        scratch.write(
            Self::record_offset::<T>(self.tail, self.tail_count)?,
            &bytes[..T::BYTES],
        )?;
        self.tail_count += 1;
        self.count = self.count.checked_add(1).ok_or_else(overflow)?;
        Ok(())
    }
    fn reader(self) -> StreamReader {
        StreamReader {
            block: self.first,
            next: NONE,
            index: 0,
            count: 0,
            remaining: self.count,
        }
    }
}
#[derive(Clone, Copy)]
struct StreamReader {
    block: u64,
    next: u64,
    index: usize,
    count: usize,
    remaining: u64,
}
impl StreamReader {
    fn read<T: DiskRecord>(&mut self, scratch: &Scratch) -> Result<Option<T>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        if self.index == self.count {
            if self.count != 0 {
                self.block = self.next;
            }
            if self.block == NONE {
                return Err(overflow());
            }
            let mut header = [0; STREAM_HEADER_BYTES];
            scratch.read(self.block, &mut header)?;
            self.next = u64::from_be_bytes(header[..8].try_into().expect("stream next"));
            self.count = position(u64::from_be_bytes(
                header[8..].try_into().expect("stream count"),
            ))?;
            self.index = 0;
            if self.count == 0 || self.count > (STREAM_BLOCK_BYTES - STREAM_HEADER_BYTES) / T::BYTES
            {
                return Err(Error::Other("invalid proof stream block".into()));
            }
            self.count = self
                .count
                .min(position(self.remaining.min(usize::MAX as u64))?);
        }
        let mut bytes = [0; RECORD_BUFFER_BYTES];
        scratch.read(
            Stream::record_offset::<T>(self.block, self.index)?,
            &mut bytes[..T::BYTES],
        )?;
        self.index += 1;
        self.remaining -= 1;
        Ok(Some(T::decode(&bytes[..T::BYTES])?))
    }
}

/// A stack ledger covers every detached window lease concurrently with all
/// remaining grouped families. Its base is sampled before detaching scratch;
/// dynamic allocation overlap updates the same caller peak before allocation.
struct Ledger<'a> {
    live: Cell<usize>,
    base: usize,
    caller_peak: &'a Cell<usize>,
}
impl Ledger<'_> {
    fn add(&self, bytes: usize) -> Result<()> {
        let live = self.live.get().checked_add(bytes).ok_or_else(overflow)?;
        let total = self.base.checked_add(live).ok_or_else(overflow)?;
        self.live.set(live);
        self.caller_peak.set(self.caller_peak.get().max(total));
        Ok(())
    }
}
struct Lease<'a> {
    charge: AdmittedBytes,
    ledger: &'a Ledger<'a>,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        // The owning payload/guard is an earlier field and has already dropped.
        self.ledger
            .live
            .set(self.ledger.live.get() - self.charge.bytes);
    }
}
struct Workspace<'a> {
    admission: RefCell<ProofAdmission>,
    ledger: &'a Ledger<'a>,
}
impl<'a> Workspace<'a> {
    fn charge(&self, bytes: usize) -> Result<Lease<'a>> {
        let mut admission = self.admission.borrow_mut();
        admission.admit(bytes)?;
        let charge = admission.detach(bytes);
        self.ledger
            .add(bytes)
            .map_err(|_| proof_allocation_overflow(&admission))?;
        Ok(Lease {
            charge,
            ledger: self.ledger,
        })
    }
    fn array<T>(
        &self,
        count: usize,
        mut init: impl FnMut(usize) -> T,
    ) -> Result<OwnedArray<'a, T>> {
        let layout = std::alloc::Layout::array::<T>(count)
            .map_err(|_| proof_allocation_overflow(&self.admission.borrow()))?;
        let charge = self.charge(layout.size())?;
        let mut entries = Box::<[T]>::new_uninit_slice(count);
        for (index, entry) in entries.iter_mut().enumerate() {
            entry.write(init(index));
        }
        // SAFETY: every element above is initialized, and there is no fallible
        // work between allocation, initialization and conversion.
        let entries = unsafe { entries.assume_init() };
        Ok(OwnedArray {
            entries,
            _charge: charge,
        })
    }
}
struct OwnedArray<'a, T> {
    entries: Box<[T]>,
    _charge: Lease<'a>,
}
#[repr(C)]
#[derive(Clone, Copy)]
struct DecodedEntry {
    start: u32,
    len: u32,
    hash: [u8; 32],
}
struct VerifiedOriginal<'a, 'mem> {
    id: [u8; 32],
    hash: [u8; 32],
    guard: redb::AccessGuard<'a, &'static [u8]>,
    entries: OwnedArray<'mem, DecodedEntry>,
    level: u32,
    seen: Cell<bool>,
    order_verified: Cell<bool>,
    _page_charge: Lease<'mem>,
}
#[derive(Clone, Copy)]
struct Frame {
    slot: usize,
    index: usize,
}

enum Edits {
    Inputs { next: usize },
    Stream(StreamReader),
}
#[derive(Clone, Copy)]
struct Action {
    key: [u8; 33],
    hash: [u8; 32],
    body: u64,
    len: u64,
    kind: u64,
    next: u64,
}
impl DiskRecord for Action {
    const BYTES: usize = 97;
    fn encode(self, bytes: &mut [u8]) {
        let mut e = Encoder { bytes, at: 0 };
        e.bytes(&self.key);
        e.bytes(&self.hash);
        e.u64(self.body);
        e.u64(self.len);
        e.u64(self.kind);
        e.u64(self.next);
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        let mut d = Decoder { bytes, at: 0 };
        let a = Self {
            key: d.array(),
            hash: d.array(),
            body: d.u64(),
            len: d.u64(),
            kind: d.u64(),
            next: d.u64(),
        };
        if a.kind > 2 {
            return Err(Error::Other("invalid scratch action".into()));
        }
        Ok(a)
    }
}
struct Journal {
    first: u64,
    last: u64,
}
impl Journal {
    fn new() -> Self {
        Self {
            first: NONE,
            last: NONE,
        }
    }
    fn push(&mut self, s: &Scratch, mut action: Action) -> Result<()> {
        action.next = NONE;
        let at = s.push(action)?;
        if self.last == NONE {
            self.first = at;
        } else {
            let mut prior = s.get::<Action>(self.last)?;
            prior.next = at;
            s.put(self.last, prior)?;
        }
        self.last = at;
        Ok(())
    }
}

struct OperandBorrow<'a> {
    count: &'a Cell<usize>,
}
impl<'a> OperandBorrow<'a> {
    fn enter(count: &'a Cell<usize>) -> Result<Self> {
        if count.get() >= 2 {
            return Err(Error::Other(
                "proof comparison exhausted its operand guards".into(),
            ));
        }
        count.set(count.get() + 1);
        Ok(Self { count })
    }
}
impl Drop for OperandBorrow<'_> {
    fn drop(&mut self) {
        self.count.set(self.count.get() - 1);
    }
}

struct Window<'a, 'txn, 'mem> {
    table: &'a redb::Table<'txn, &'static [u8], &'static [u8]>,
    family: LogicalFamily,
    inputs: EntryEdits,
    scratch: Scratch,
    workspace: &'mem Workspace<'mem>,
    slots: Option<OwnedArray<'mem, RefCell<Option<VerifiedOriginal<'a, 'mem>>>>>,
    cache_next: Cell<usize>,
    operand_borrows: Cell<usize>,
    wide_borrows: Cell<usize>,
    current: Cell<Option<usize>>,
    path: Option<OwnedArray<'mem, Frame>>,
    path_len: usize,
    edits: Edits,
    next_edit: Option<Edit>,
    parents: Stream,
    writes: Stream,
    journal: Journal,
    retirements: Journal,
    builder: Option<OwnedArray<'mem, u8>>,
    build_len: usize,
    build_count: usize,
    build_last: Option<Key>,
    build_hash: blake3::Hasher,
}
impl<'a, 'txn, 'mem> Window<'a, 'txn, 'mem> {
    fn open(
        table: &'a redb::Table<'txn, &'static [u8], &'static [u8]>,
        family: LogicalFamily,
        inputs: EntryEdits,
        scratch: Scratch,
        workspace: &'mem Workspace<'mem>,
    ) -> Self {
        Self {
            table,
            family,
            inputs,
            scratch,
            workspace,
            slots: None,
            cache_next: Cell::new(0),
            operand_borrows: Cell::new(0),
            wide_borrows: Cell::new(0),
            current: Cell::new(None),
            path: None,
            path_len: 0,
            edits: Edits::Inputs { next: 0 },
            next_edit: None,
            parents: Stream::new(),
            writes: Stream::new(),
            journal: Journal::new(),
            retirements: Journal::new(),
            builder: None,
            build_len: 9,
            build_count: 0,
            build_last: None,
            build_hash: blake3::Hasher::new(),
        }
    }
    fn with_referenced<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        if key.first() != Some(&KEY_REFERENCE_TAG) {
            return f(key);
        }
        let _operand = OperandBorrow::enter(&self.wide_borrows)?;
        referenced_key_live_bytes(key)?;
        let expected = u32::from_be_bytes(key[1..5].try_into().expect("length")) as usize;
        let id = key_hash(key);
        let stored = node_storage_key(self.family, &id);
        let guard = self
            .table
            .get(stored.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| Error::Other("authenticated separator key is missing".into()))?;
        #[cfg(any(test, feature = "test-seams"))]
        observe_node_read(guard.value().len());
        // Acquire only a transient storage guard before this admission. Its
        // retained backing pages remain charged through both nested operands.
        let charge = self.workspace.charge(guard.retained_page_bytes())?;
        struct Wide<'a, 'mem> {
            guard: redb::AccessGuard<'a, &'static [u8]>,
            _charge: Lease<'mem>,
        }
        let wide = Wide {
            guard,
            _charge: charge,
        };
        let expected = expected.checked_add(1).ok_or_else(overflow)?;
        if wide.guard.value().len() != expected
            || wide.guard.value().first() != Some(&0)
            || key_hash(&wide.guard.value()[1..]) != id
        {
            return Err(Error::Other(
                "authenticated separator key is damaged".into(),
            ));
        }
        f(&wide.guard.value()[1..])
    }
    fn compare_encoded(&self, left: &[u8], right: &[u8]) -> Result<Ordering> {
        self.with_referenced(left, |left| {
            self.with_referenced(right, |right| identity_key_cmp(left, right))
        })
    }
    fn original(&self, id: [u8; 32], expected: [u8; 32]) -> Result<VerifiedOriginal<'a, 'mem>> {
        let stored = node_storage_key(self.family, &id);
        let guard = self
            .table
            .get(stored.as_slice())
            .map_err(RedbPersistence::storage_error)?
            .ok_or_else(|| {
                Error::Other("logical identity tree references a missing chunk".into())
            })?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::read_work_probe::note_proof_node_read();
            observe_node_read(guard.value().len());
        }
        let page_charge = self.workspace.charge(guard.retained_page_bytes())?;
        // Construct an owner before authentication or decoding, so every
        // refusal drops the borrowed page before its detached credit.
        struct Page<'a, 'mem> {
            guard: redb::AccessGuard<'a, &'static [u8]>,
            charge: Lease<'mem>,
        }
        let page = Page {
            guard,
            charge: page_charge,
        };
        let node = EncodedNode::authenticate(self.family, page.guard.value())?;
        if node.subtree_hash != expected {
            return Err(Error::Other(
                "logical identity chunk disagrees with committed parent".into(),
            ));
        }
        let mut entries = self.workspace.array(node.count, |_| DecodedEntry {
            start: 0,
            len: 0,
            hash: [0; 32],
        })?;
        let mut cursor = 9;
        let mut last = &[][..];
        for entry in &mut entries.entries {
            let value = node.entry_at(&mut cursor)?;
            let start = value.key.as_ptr() as usize - node.bytes.as_ptr() as usize;
            *entry = DecodedEntry {
                start: u32::try_from(start).map_err(|_| overflow())?,
                len: u32::try_from(value.key.len()).map_err(|_| overflow())?,
                hash: value.value_hash,
            };
            last = value.key;
        }
        if chunk_id(node.level, last) != id {
            return Err(Error::Other(
                "logical identity chunk id disagrees with its separator".into(),
            ));
        }
        let level = node.level;
        Ok(VerifiedOriginal {
            id,
            hash: expected,
            guard: page.guard,
            entries,
            level,
            seen: Cell::new(false),
            order_verified: Cell::new(false),
            _page_charge: page.charge,
        })
    }
    fn validate_order(&self, original: &VerifiedOriginal<'_, '_>) -> Result<()> {
        if original.order_verified.get() {
            return Ok(());
        }
        let mut prior: Option<&[u8]> = None;
        for entry in &original.entries.entries {
            let start = entry.start as usize;
            let key = &original.guard.value()[start..start + entry.len as usize];
            if let Some(prior) = prior
                && (prior.first() == Some(&KEY_REFERENCE_TAG)
                    || key.first() == Some(&KEY_REFERENCE_TAG))
                && self.compare_encoded(prior, key)? != Ordering::Less
            {
                return Err(Error::Other("internal separator order is invalid".into()));
            }
            prior = Some(key);
        }
        original.order_verified.set(true);
        Ok(())
    }
    fn initialize(&mut self, root: &FamilyRoot) -> Result<u32> {
        let original = root
            .root
            .map(|id| self.original(id, root.hash))
            .transpose()?;
        if let Some(original) = original.as_ref() {
            self.observe_residency_with_extra(
                original.guard.value().len(),
                original.entries._charge.charge.bytes,
            );
        }
        let height = original.as_ref().map_or(0, |node| node.level);
        if height > MAX_CHUNK_LEVEL {
            return Err(Error::Other(
                "logical identity internal chunks exceed permitted depth".into(),
            ));
        }
        self.slots = Some(
            self.workspace
                .array(height as usize + 3, |_| RefCell::new(None))?,
        );
        self.path = Some(
            self.workspace
                .array(height as usize + 1, |_| Frame { slot: 0, index: 0 })?,
        );
        if let Some(original) = original {
            self.validate_order(&original)?;
            self.writes.push(
                &self.scratch,
                Write::seen(node_storage_key(self.family, &original.id), original.hash),
            )?;
            original.seen.set(true);
            *self.slots.as_ref().expect("slots").entries[0].borrow_mut() = Some(original);
            self.current.set(Some(0));
            self.cache_next.set(1);
        }
        self.observe_residency();
        Ok(height)
    }
    fn pinned(&self, index: usize) -> bool {
        self.current.get() == Some(index)
            || self.path.as_ref().is_some_and(|path| {
                path.entries[..self.path_len]
                    .iter()
                    .any(|frame| frame.slot == index)
            })
    }
    fn cached(&self, id: [u8; 32], expected: [u8; 32]) -> Result<usize> {
        let slots = &self.slots.as_ref().expect("initialized slots").entries;
        for (index, slot) in slots.iter().enumerate() {
            let Ok(entry) = slot.try_borrow() else {
                continue;
            };
            if entry
                .as_ref()
                .is_some_and(|node| node.id == id && node.hash == expected)
            {
                #[cfg(any(test, feature = "test-seams"))]
                observe_cache_hit();
                self.cache_next.set((index + 1) % slots.len());
                return Ok(index);
            }
        }
        for offset in 0..slots.len() {
            let index = (self.cache_next.get() + offset) % slots.len();
            if self.pinned(index) {
                continue;
            }
            let Ok(mut entry) = slots[index].try_borrow_mut() else {
                continue;
            };
            *entry = None;
            let original = self.original(id, expected)?;
            self.observe_residency_with_extra(
                original.guard.value().len(),
                original.entries._charge.charge.bytes,
            );
            *entry = Some(original);
            self.cache_next.set((index + 1) % slots.len());
            drop(entry);
            self.observe_residency();
            return Ok(index);
        }
        Err(Error::Other(
            "proof comparison exhausted its operand guards".into(),
        ))
    }
    fn load(&mut self, id: [u8; 32], expected: [u8; 32]) -> Result<()> {
        let prior = self.current.replace(None);
        let result = self.cached(id, expected);
        if result.is_err() {
            self.current.set(prior);
        }
        let index = result?;
        self.current.set(Some(index));
        // Seen authority is recorded independently even if a comparison had
        // loaded this node into the cache before the cursor first visited it.
        let already_seen = {
            let entry = self.slots.as_ref().expect("slots").entries[index].borrow();
            let original = entry.as_ref().expect("authenticated original");
            self.validate_order(original)?;
            original.seen.replace(true)
        };
        if !already_seen {
            let mut write = Write::seen(node_storage_key(self.family, &id), expected);
            write.sequence = self.writes.count;
            self.writes.push(&self.scratch, write)?;
        }
        Ok(())
    }
    fn with_locator<R>(&self, loc: Locator, f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        let len = position(loc.len)?;
        match loc.kind {
            0 => {
                let bytes = &self
                    .inputs
                    .get(position(loc.start)?)
                    .ok_or_else(|| Error::Other("invalid caller key locator".into()))?
                    .0;
                if bytes.len() != len {
                    return Err(Error::Other("caller key locator length mismatch".into()));
                }
                f(bytes)
            }
            1 => {
                let _operand = OperandBorrow::enter(&self.operand_borrows)?;
                let start = position(loc.start)?;
                let end = start.checked_add(len).ok_or_else(overflow)?;
                let index = self.cached(loc.id, loc.hash)?;
                let original = self.slots.as_ref().expect("slots").entries[index].borrow();
                f(original
                    .as_ref()
                    .expect("authenticated original")
                    .guard
                    .value()
                    .get(start..end)
                    .ok_or_else(|| Error::Other("original key locator out of bounds".into()))?)
            }
            _ => Err(Error::Other("invalid proof key locator".into())),
        }
    }
    fn with_key<R>(&self, key: Key, raw: bool, f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        let loc = if raw { key.raw } else { key.encoded };
        if loc.kind == 2 {
            let len = position(loc.len)?;
            f(key
                .inline
                .get(..len)
                .ok_or_else(|| Error::Other("proof separator exceeds inline bound".into()))?)
        } else {
            self.with_locator(loc, f)
        }
    }
    fn with_encoded<R>(&self, key: Key, f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        self.with_key(key, false, f)
    }
    fn with_semantic<R>(&self, key: Key, f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        self.with_key(key, true, |bytes| self.with_referenced(bytes, f))
    }
    fn compare_keys(&self, left: Key, right: Key) -> Result<Ordering> {
        self.with_semantic(left, |left| {
            self.with_semantic(right, |right| identity_key_cmp(left, right))
        })
    }
    fn current_meta(&self) -> (usize, u32, usize, [u8; 32], [u8; 32]) {
        let slot = self.current.get().expect("current original");
        let original = self.slots.as_ref().expect("slots").entries[slot].borrow();
        let node = original.as_ref().expect("current original");
        (
            slot,
            node.level,
            node.entries.entries.len(),
            node.id,
            node.hash,
        )
    }
    fn entry(&self, index: usize) -> Result<(Key, [u8; 32])> {
        let slot = self.current.get().expect("current original");
        let original = self.slots.as_ref().expect("slots").entries[slot].borrow();
        let node = original.as_ref().expect("current original");
        let entry = node
            .entries
            .entries
            .get(index)
            .ok_or_else(|| Error::Other("proof source entry out of bounds".into()))?;
        let loc = Locator {
            kind: 1,
            id: node.id,
            hash: node.hash,
            start: entry.start as u64,
            len: entry.len as u64,
        };
        Ok((
            Key {
                encoded: loc,
                raw: loc,
                inline: [0; INLINE_SEPARATOR_BYTES],
            },
            entry.hash,
        ))
    }
    fn push_frame(&mut self, slot: usize, index: usize) -> Result<()> {
        let path = self.path.as_mut().expect("path");
        let frame = path.entries.get_mut(self.path_len).ok_or_else(|| {
            Error::Other("logical identity cursor exceeds authenticated height".into())
        })?;
        *frame = Frame { slot, index };
        self.path_len += 1;
        Ok(())
    }
    fn seek(&mut self, root: &FamilyRoot, target: u32, edit: Edit) -> Result<bool> {
        self.path_len = 0;
        let Some(id) = root.root else {
            self.current.set(None);
            return Ok(false);
        };
        self.load(id, root.hash)?;
        while self.current_meta().1 > target {
            let (slot, level, count, _, _) = self.current_meta();
            let mut left = 0;
            let mut right = count;
            while left < right {
                let mid = (left + right) / 2;
                match self.compare_keys(self.entry(mid)?.0, edit.key)? {
                    Ordering::Less => left = mid + 1,
                    Ordering::Greater => right = mid,
                    Ordering::Equal => {
                        left = mid;
                        break;
                    }
                }
            }
            let index = left.min(count - 1);
            let (key, hash) = self.entry(index)?;
            let child = self.with_encoded(key, |key| Ok(chunk_id(level - 1, key)))?;
            self.push_frame(slot, index)?;
            self.load(child, hash)?;
            if self.current_meta().1 + 1 != level {
                return Err(Error::Other("logical identity child level mismatch".into()));
            }
        }
        if self.current_meta().1 != target {
            return Err(Error::Other("logical identity child level mismatch".into()));
        }
        Ok(true)
    }
    fn has_next(&self) -> bool {
        self.path.as_ref().expect("path").entries[..self.path_len]
            .iter()
            .any(|frame| {
                let node = self.slots.as_ref().expect("slots").entries[frame.slot].borrow();
                frame.index + 1 < node.as_ref().expect("pinned parent").entries.entries.len()
            })
    }
    fn advance(&mut self, target: u32) -> Result<bool> {
        while self.path_len != 0 {
            self.path_len -= 1;
            let mut frame = self.path.as_ref().expect("path").entries[self.path_len];
            self.current.set(Some(frame.slot));
            frame.index += 1;
            let (_, level, count, _, _) = self.current_meta();
            if frame.index == count {
                continue;
            }
            let (key, hash) = self.entry(frame.index)?;
            let id = self.with_encoded(key, |key| Ok(chunk_id(level - 1, key)))?;
            self.push_frame(frame.slot, frame.index)?;
            self.load(id, hash)?;
            if self.current_meta().1 + 1 != level {
                return Err(Error::Other("logical identity child level mismatch".into()));
            }
            while self.current_meta().1 > target {
                let (slot, level, _, _, _) = self.current_meta();
                let (key, hash) = self.entry(0)?;
                let id = self.with_encoded(key, |key| Ok(chunk_id(level - 1, key)))?;
                self.push_frame(slot, 0)?;
                self.load(id, hash)?;
                if self.current_meta().1 + 1 != level {
                    return Err(Error::Other("logical identity child level mismatch".into()));
                }
            }
            return Ok(true);
        }
        self.current.set(None);
        Ok(false)
    }
    fn separator(&self, key: Key) -> Result<Key> {
        self.with_encoded(key, |bytes| {
            let mut reference = [0; KEY_REFERENCE_BYTES];
            let encoded = encoded_key_in(1, bytes, &mut reference);
            if key.raw.kind == 1 {
                self.with_referenced(encoded, |_| Ok(()))?;
            }
            let inline_raw = encoded.len() <= INLINE_SEPARATOR_BYTES
                && encoded.first() != Some(&KEY_REFERENCE_TAG)
                && self.with_key(key, true, |raw| Ok(raw == encoded))?;
            if encoded.len() > INLINE_SEPARATOR_BYTES {
                return Err(Error::Other("proof separator exceeds inline bound".into()));
            }
            let copied = Locator::inline(encoded);
            let mut inline = [0; INLINE_SEPARATOR_BYTES];
            inline[..encoded.len()].copy_from_slice(encoded);
            Ok(Key {
                encoded: copied,
                raw: if inline_raw { copied } else { key.raw },
                inline,
            })
        })
    }
    fn parent(&mut self, edit: Edit) -> Result<()> {
        let sequence = self.parents.count;
        self.parents.push(&self.scratch, Parent { edit, sequence })
    }
    fn parent_order(&self, left: &Parent, right: &Parent) -> Result<Ordering> {
        Ok(self
            .compare_keys(left.edit.key, right.edit.key)?
            .then(left.sequence.cmp(&right.sequence)))
    }
    fn fold_parent(
        &self,
        output: &mut Stream,
        pending: &mut Option<Parent>,
        parent: Parent,
    ) -> Result<()> {
        if let Some(prior) = *pending {
            match self.compare_keys(prior.edit.key, parent.edit.key)? {
                Ordering::Greater => {
                    return Err(Error::Other(
                        "generated parent stream is not ordered".into(),
                    ));
                }
                Ordering::Equal => {
                    if prior.sequence >= parent.sequence {
                        return Err(Error::Other(
                            "generated parent ordinal is not ordered".into(),
                        ));
                    }
                }
                Ordering::Less => output.push(&self.scratch, prior.edit)?,
            }
        }
        // The complete later key, including raw locator and inline bytes,
        // replaces the prior key along with its value, exactly as the old map.
        *pending = Some(parent);
        Ok(())
    }
    fn end_parents(&mut self) -> Result<Stream> {
        let count = self.parents.count;
        if count == 0 {
            return Ok(self.parents);
        }
        let capacity = (MAX_NODE_BYTES / std::mem::size_of::<Parent>())
            .min(position(count.min(usize::MAX as u64))?);
        if capacity == 0 {
            return Err(overflow());
        }
        let mut reader = self.parents.reader();
        let first = reader.read::<Parent>(&self.scratch)?.ok_or_else(overflow)?;
        let mut buffer = self.workspace.array(capacity, |_| first)?;
        self.observe_residency_with_extra(0, buffer._charge.charge.bytes);
        let mut output = Stream::new();
        let mut pending = None;
        if count <= capacity as u64 {
            buffer.entries[0] = first;
            for record in &mut buffer.entries[1..] {
                *record = reader.read(&self.scratch)?.ok_or_else(overflow)?;
            }
            heapsort(&mut buffer.entries, |a, b| self.parent_order(a, b))?;
            for parent in buffer.entries.iter().copied() {
                self.fold_parent(&mut output, &mut pending, parent)?;
            }
        } else {
            // Fixed-sized sorted runs need no resident run directory. The
            // operation stream may decrease after arbitrarily many deletions,
            // or after a seek revisits accepted semantically equal encodings.
            reader = self.parents.reader();
            let mut runs = Stream::new();
            while reader.remaining != 0 {
                let len = capacity.min(position(reader.remaining.min(usize::MAX as u64))?);
                for record in &mut buffer.entries[..len] {
                    *record = reader.read(&self.scratch)?.ok_or_else(overflow)?;
                }
                heapsort(&mut buffer.entries[..len], |a, b| self.parent_order(a, b))?;
                for parent in &buffer.entries[..len] {
                    runs.push(&self.scratch, *parent)?;
                }
            }
            // Comparisons can retain charged original/wide guards, but only
            // two current parent records and constant cursors remain here.
            drop(buffer);
            let mut width = capacity as u64;
            while width < count {
                let mut input = runs.reader();
                let mut merged = Stream::new();
                while input.remaining != 0 {
                    let left_count = width.min(input.remaining);
                    let right_count = width.min(input.remaining - left_count);
                    let mut left = input;
                    let mut right = input;
                    right.skip::<Parent>(left_count, &self.scratch)?;
                    input = right;
                    input.skip::<Parent>(right_count, &self.scratch)?;
                    let mut left_remaining = left_count;
                    let mut right_remaining = right_count;
                    let mut a = left.read::<Parent>(&self.scratch)?;
                    let mut b = if right_remaining == 0 {
                        None
                    } else {
                        right.read::<Parent>(&self.scratch)?
                    };
                    while a.is_some() || b.is_some() {
                        let take_left = match (a, b) {
                            (Some(a), Some(b)) => self.parent_order(&a, &b)? != Ordering::Greater,
                            (Some(_), None) => true,
                            _ => false,
                        };
                        if take_left {
                            merged.push(&self.scratch, a.ok_or_else(overflow)?)?;
                            left_remaining -= 1;
                            a = if left_remaining == 0 {
                                None
                            } else {
                                left.read(&self.scratch)?
                            };
                        } else {
                            merged.push(&self.scratch, b.ok_or_else(overflow)?)?;
                            right_remaining -= 1;
                            b = if right_remaining == 0 {
                                None
                            } else {
                                right.read(&self.scratch)?
                            };
                        }
                    }
                    if left_remaining != 0 || right_remaining != 0 {
                        return Err(overflow());
                    }
                }
                if merged.count != count {
                    return Err(overflow());
                }
                runs = merged;
                width = width.checked_mul(2).ok_or_else(overflow)?;
            }
            let mut reader = runs.reader();
            while let Some(parent) = reader.read::<Parent>(&self.scratch)? {
                self.fold_parent(&mut output, &mut pending, parent)?;
            }
        }
        if let Some(parent) = pending {
            output.push(&self.scratch, parent.edit)?;
        }
        self.parents = output;
        Ok(output)
    }
    fn begin(&mut self, level: u32) {
        self.build_len = 9;
        self.build_count = 0;
        self.build_last = None;
        self.build_hash = blake3::Hasher::new();
        self.build_hash.update(b"contextdb-prolly-chunk");
        self.build_hash.update(&[self.family as u8]);
        self.build_hash.update(&level.to_be_bytes());
    }
    fn grow_builder(&mut self, needed: usize) -> Result<()> {
        let capacity = self
            .builder
            .as_ref()
            .map_or(0, |builder| builder.entries.len());
        if needed <= capacity {
            return Ok(());
        }
        let capacity = needed
            .checked_next_power_of_two()
            .ok_or_else(|| proof_allocation_overflow(&self.workspace.admission.borrow()))?
            .min(MAX_NODE_BYTES);
        if capacity < needed {
            return Err(overflow());
        }
        let mut next = self.workspace.array(capacity, |_| 0u8)?;
        if let Some(prior) = self.builder.as_ref() {
            let copied = self.build_len.min(prior.entries.len());
            next.entries[..copied].copy_from_slice(&prior.entries[..copied]);
        }
        // Include the real old-plus-new body peak before freeing the old buffer.
        self.observe_residency_with_extra(next.entries.len(), 0);
        // New capacity is wholly admitted while the old allocation is alive.
        self.builder = Some(next);
        self.observe_residency();
        Ok(())
    }
    fn push(&mut self, level: u32, key: Key, value: [u8; 32]) -> Result<()> {
        let bytes = self.with_encoded(key, |key| Ok(36 + encoded_key_len(level, key)))?;
        if NODE_HEADER_BYTES + bytes > MAX_NODE_BYTES {
            return Err(Error::Other(
                "logical identity key exceeds authenticated chunk capacity".into(),
            ));
        }
        if self.build_len + 32 + bytes > MAX_NODE_BYTES {
            self.finish(level)?;
        }
        let start = self.build_len + 32;
        let end = start + bytes;
        let cut = self.with_encoded(key, |key| {
            Ok(chunk_cut(level, key, start, end, self.build_count + 1))
        })?;
        self.grow_builder(end)?;
        let mut builder = self.builder.take().expect("builder capacity");
        let mut hash = std::mem::replace(&mut self.build_hash, blake3::Hasher::new());
        let written = self.with_encoded(key, |key| {
            let mut reference = [0; KEY_REFERENCE_BYTES];
            let key = encoded_key_in(level, key, &mut reference);
            let at = self.build_len;
            builder.entries[at..at + 4].copy_from_slice(&(key.len() as u32).to_be_bytes());
            builder.entries[at + 4..at + 4 + key.len()].copy_from_slice(key);
            builder.entries[at + 4 + key.len()..at + bytes].copy_from_slice(&value);
            hash.update(&(key.len() as u64).to_be_bytes());
            hash.update(key);
            hash.update(&value);
            Ok(())
        });
        self.builder = Some(builder);
        self.build_hash = hash;
        written?;
        self.build_len += bytes;
        self.build_count += 1;
        self.build_last = Some(key);
        if cut {
            self.finish(level)?;
        }
        Ok(())
    }
    fn finish(&mut self, level: u32) -> Result<()> {
        if self.build_count == 0 {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-seams"))]
        let started = std::time::Instant::now();
        let hash = *self.build_hash.finalize().as_bytes();
        let len = self.build_len + 32;
        let builder = self.builder.as_mut().expect("nonempty builder");
        builder.entries[0] = NODE_VERSION;
        builder.entries[1..5].copy_from_slice(&level.to_be_bytes());
        builder.entries[5..9].copy_from_slice(&(self.build_count as u32).to_be_bytes());
        builder.entries[self.build_len..len].copy_from_slice(&hash);
        let body = self.scratch.append(&builder.entries[..len])?;
        let last = self.build_last.expect("nonempty builder");
        let separator = self.separator(last)?;
        let id = self.with_encoded(last, |key| Ok(chunk_id(level, key)))?;
        let mut write = Write::finished(node_storage_key(self.family, &id), hash, body, len);
        write.sequence = self.writes.count;
        self.writes.push(&self.scratch, write)?;
        // Only a real finished chunk creates a live parent separator. Internal
        // chunk_cut still refuses cuts with fewer than two entries.
        self.parent(Edit {
            key: separator,
            value: Some(hash),
        })?;
        self.begin(level);
        #[cfg(any(test, feature = "test-seams"))]
        observe_identity_span(IdentitySpan::BuildAndHash, started);
        Ok(())
    }
    fn read_edit(&mut self) -> Result<Option<Edit>> {
        match &mut self.edits {
            Edits::Inputs { next } => {
                let Some((key, value)) = self.inputs.get(*next) else {
                    return Ok(None);
                };
                let loc = Locator::input(*next, key.len());
                *next += 1;
                Ok(Some(Edit {
                    key: Key {
                        encoded: loc,
                        raw: loc,
                        inline: [0; INLINE_SEPARATOR_BYTES],
                    },
                    value: *value,
                }))
            }
            Edits::Stream(reader) => reader.read(&self.scratch),
        }
    }
    fn step_edit(&mut self) -> Result<()> {
        self.next_edit = self.read_edit()?;
        Ok(())
    }
    fn edit_level(&mut self, root: &FamilyRoot, level: u32) -> Result<()> {
        self.parents = Stream::new();
        self.step_edit()?;
        while let Some(edit) = self.next_edit {
            let mut current = self.seek(root, level, edit)?;
            self.begin(level);
            loop {
                if current {
                    let (_, _, count, id, _) = self.current_meta();
                    let mut removal = Write::removal(node_storage_key(self.family, &id));
                    removal.sequence = self.writes.count;
                    self.writes.push(&self.scratch, removal)?;
                    let last = self.entry(count - 1)?.0;
                    self.with_encoded(last, |key| self.with_referenced(key, |_| Ok(())))?;
                    let tombstone = self.separator(last)?;
                    self.parent(Edit {
                        key: tombstone,
                        value: None,
                    })?;
                    for source_index in 0..count {
                        let source = self.entry(source_index)?;
                        let mut replaced = false;
                        while let Some(edit) = self.next_edit {
                            let order = self.compare_keys(edit.key, source.0)?;
                            if order == Ordering::Greater {
                                break;
                            }
                            self.step_edit()?;
                            if let Some(value) = edit.value {
                                self.push(level, edit.key, value)?;
                            }
                            if order == Ordering::Equal {
                                replaced = true;
                                break;
                            }
                        }
                        if !replaced {
                            self.push(level, source.0, source.1)?;
                        }
                    }
                    // Preserve the synchronized-boundary stop exactly. A
                    // partially filled builder must continue through successors.
                    if self.build_count == 0 && self.has_next() {
                        break;
                    }
                    current = self.advance(level)?;
                    if current {
                        continue;
                    }
                }
                while let Some(edit) = self.next_edit {
                    self.step_edit()?;
                    if let Some(value) = edit.value {
                        self.push(level, edit.key, value)?;
                    }
                }
                self.finish(level)?;
                break;
            }
        }
        let parents = self.end_parents()?;
        self.edits = Edits::Stream(parents.reader());
        self.next_edit = None;
        Ok(())
    }
    fn observe_residency(&self) {
        self.observe_residency_with_extra(0, 0);
    }
    #[cfg(feature = "test-seams")]
    fn observe_residency_with_extra(&self, extra_bodies: usize, extra_records: usize) {
        let mut bodies = self
            .builder
            .as_ref()
            .map_or(0, |builder| builder.entries.len());
        let mut records = self
            .path
            .as_ref()
            .map_or(0, |path| path._charge.charge.bytes);
        if let Some(slots) = &self.slots {
            records += slots._charge.charge.bytes;
            for slot in &slots.entries {
                let Ok(slot) = slot.try_borrow() else {
                    continue;
                };
                if let Some(node) = slot.as_ref() {
                    bodies += node.guard.value().len();
                    records += node.entries._charge.charge.bytes;
                }
            }
        }
        observe_proof_memory(bodies + extra_bodies, records + extra_records);
    }
    #[cfg(not(feature = "test-seams"))]
    fn observe_residency_with_extra(&self, _extra_bodies: usize, _extra_records: usize) {}
}

impl StreamReader {
    /// Skip to a run boundary by whole blocks and offsets, never by allocating
    /// a directory or reading each record being skipped.
    fn skip<T: DiskRecord>(&mut self, mut count: u64, scratch: &Scratch) -> Result<()> {
        if count > self.remaining {
            return Err(overflow());
        }
        while count != 0 {
            if self.index == self.count {
                if self.count != 0 {
                    self.block = self.next;
                }
                if self.block == NONE {
                    return Err(overflow());
                }
                let mut header = [0; STREAM_HEADER_BYTES];
                scratch.read(self.block, &mut header)?;
                self.next = u64::from_be_bytes(header[..8].try_into().expect("stream next"));
                self.count = position(u64::from_be_bytes(
                    header[8..].try_into().expect("stream count"),
                ))?;
                self.index = 0;
                if self.count == 0
                    || self.count > (STREAM_BLOCK_BYTES - STREAM_HEADER_BYTES) / T::BYTES
                {
                    return Err(Error::Other("invalid proof stream block".into()));
                }
                self.count = self
                    .count
                    .min(position(self.remaining.min(usize::MAX as u64))?);
            }
            let step = count.min((self.count - self.index) as u64);
            self.index += position(step)?;
            self.remaining -= step;
            count -= step;
        }
        Ok(())
    }
}

/// Sort inside the exact admitted record array. No recursion, stable-sort
/// workspace, auxiliary heap, or platform-dependent equal-key ordering.
fn heapsort<T>(
    records: &mut [T],
    mut compare: impl FnMut(&T, &T) -> Result<Ordering>,
) -> Result<()> {
    fn sift<T>(
        records: &mut [T],
        mut root: usize,
        len: usize,
        compare: &mut impl FnMut(&T, &T) -> Result<Ordering>,
    ) -> Result<()> {
        loop {
            let Some(mut child) = root.checked_mul(2).and_then(|v| v.checked_add(1)) else {
                return Ok(());
            };
            if child >= len {
                return Ok(());
            }
            if child + 1 < len && compare(&records[child], &records[child + 1])? == Ordering::Less {
                child += 1;
            }
            if compare(&records[root], &records[child])? != Ordering::Less {
                return Ok(());
            }
            records.swap(root, child);
            root = child;
        }
    }
    for root in (0..records.len() / 2).rev() {
        sift(records, root, records.len(), &mut compare)?;
    }
    for end in (1..records.len()).rev() {
        records.swap(0, end);
        sift(records, 0, end, &mut compare)?;
    }
    Ok(())
}

impl Window<'_, '_, '_> {
    fn planned_write(&self, key: [u8; 33]) -> Result<Option<Write>> {
        let mut reader = self.writes.reader();
        let mut result: Option<Write> = None;
        while let Some(write) = reader.read::<Write>(&self.scratch)? {
            if write.key != key {
                continue;
            }
            if let Some(prior) = result.as_mut() {
                prior.fold(write)?;
            } else {
                result = Some(write);
            }
        }
        Ok(result)
    }
    fn emit_write(&mut self, write: Write) -> Result<()> {
        if write.body != NONE {
            if write.original != write.hash {
                self.journal.push(
                    &self.scratch,
                    Action {
                        key: write.key,
                        hash: write.hash.expect("planned body hash"),
                        body: write.body,
                        len: write.len,
                        kind: 0,
                        next: NONE,
                    },
                )?;
            }
        } else if write.remove && write.original.is_some() {
            self.journal.push(
                &self.scratch,
                Action {
                    key: write.key,
                    hash: [0; 32],
                    body: NONE,
                    len: 0,
                    kind: 2,
                    next: NONE,
                },
            )?;
        }
        Ok(())
    }
    fn fold_sorted(&mut self, records: impl IntoIterator<Item = Write>) -> Result<()> {
        let mut pending: Option<Write> = None;
        for write in records {
            if let Some(prior) = pending.as_mut() {
                if prior.key == write.key {
                    prior.fold(write)?;
                    continue;
                }
                if prior.key > write.key {
                    return Err(Error::Other("proof directory order is invalid".into()));
                }
                self.emit_write(*prior)?;
            }
            pending = Some(write);
        }
        if let Some(write) = pending {
            self.emit_write(write)?;
        }
        Ok(())
    }
    fn finalize_directory(&mut self) -> Result<()> {
        let count = self.writes.count;
        if count == 0 {
            return Ok(());
        }
        let capacity = (MAX_NODE_BYTES / std::mem::size_of::<Write>())
            .min(position(count.min(usize::MAX as u64))?);
        if capacity == 0 {
            return Err(overflow());
        }
        let mut buffer = self
            .workspace
            .array(capacity, |_| Write::removal([0; 33]))?;
        #[cfg(any(test, feature = "test-seams"))]
        observe_proof_memory(0, buffer._charge.charge.bytes);
        let mut reader = self.writes.reader();
        if count <= capacity as u64 {
            for record in &mut buffer.entries {
                *record = reader.read(&self.scratch)?.ok_or_else(overflow)?;
            }
            heapsort(&mut buffer.entries, |a, b| Ok(a.order(b)))?;
            return self.fold_sorted(buffer.entries.iter().copied());
        }
        let mut runs = Stream::new();
        while reader.remaining != 0 {
            let len = capacity.min(position(reader.remaining.min(usize::MAX as u64))?);
            for record in &mut buffer.entries[..len] {
                *record = reader.read(&self.scratch)?.ok_or_else(overflow)?;
            }
            heapsort(&mut buffer.entries[..len], |a, b| Ok(a.order(b)))?;
            for record in &buffer.entries[..len] {
                runs.push(&self.scratch, *record)?;
            }
        }
        // The array is no longer needed. Merging holds exactly two current
        // records and block cursors on the stack; no run directory grows with Q.
        drop(buffer);
        let mut width = capacity as u64;
        while width < count {
            let mut input = runs.reader();
            let mut output = Stream::new();
            while input.remaining != 0 {
                let left_count = width.min(input.remaining);
                let right_count = width.min(input.remaining - left_count);
                let mut left = input;
                let mut right = input;
                right.skip::<Write>(left_count, &self.scratch)?;
                input = right;
                input.skip::<Write>(right_count, &self.scratch)?;
                let mut left_remaining = left_count;
                let mut right_remaining = right_count;
                let mut a = if left_remaining == 0 {
                    None
                } else {
                    left.read::<Write>(&self.scratch)?
                };
                let mut b = if right_remaining == 0 {
                    None
                } else {
                    right.read::<Write>(&self.scratch)?
                };
                while a.is_some() || b.is_some() {
                    let take_left = match (a, b) {
                        (Some(a), Some(b)) => a.order(&b) != Ordering::Greater,
                        (Some(_), None) => true,
                        _ => false,
                    };
                    if take_left {
                        output.push(&self.scratch, a.ok_or_else(overflow)?)?;
                        left_remaining -= 1;
                        a = if left_remaining == 0 {
                            None
                        } else {
                            left.read(&self.scratch)?
                        };
                    } else {
                        output.push(&self.scratch, b.ok_or_else(overflow)?)?;
                        right_remaining -= 1;
                        b = if right_remaining == 0 {
                            None
                        } else {
                            right.read(&self.scratch)?
                        };
                    }
                }
                if left_remaining != 0 || right_remaining != 0 {
                    return Err(overflow());
                }
            }
            if output.count != count {
                return Err(overflow());
            }
            runs = output;
            width = width.checked_mul(2).ok_or_else(overflow)?;
        }
        let mut reader = runs.reader();
        let mut pending: Option<Write> = None;
        while let Some(write) = reader.read::<Write>(&self.scratch)? {
            if let Some(prior) = pending.as_mut() {
                if prior.key == write.key {
                    prior.fold(write)?;
                    continue;
                }
                if prior.key > write.key {
                    return Err(Error::Other("proof directory order is invalid".into()));
                }
                self.emit_write(*prior)?;
            }
            pending = Some(write);
        }
        if let Some(write) = pending {
            self.emit_write(write)?;
        }
        Ok(())
    }
    fn plan(&mut self, root: &FamilyRoot) -> Result<FamilyRoot> {
        // The full admitted input keys remain borrowed; wide bodies are
        // sequentially spooled before any durable table mutation is allowed.
        for (key, value) in &self.inputs {
            if key.len() <= INLINE_SEPARATOR_BYTES {
                continue;
            }
            let hash = key_hash(key);
            let stored = node_storage_key(self.family, &hash);
            if value.is_some() {
                let len = key.len().checked_add(1).ok_or_else(overflow)?;
                let body = self.scratch.reserve(len)?;
                self.scratch.write(body, &[0])?;
                self.scratch.write(body + 1, key)?;
                self.journal.push(
                    &self.scratch,
                    Action {
                        key: stored,
                        hash,
                        body,
                        len: len as u64,
                        kind: 1,
                        next: NONE,
                    },
                )?;
            } else {
                self.retirements.push(
                    &self.scratch,
                    Action {
                        key: stored,
                        hash: [0; 32],
                        body: NONE,
                        len: 0,
                        kind: 2,
                        next: NONE,
                    },
                )?;
            }
        }
        let height = self.initialize(root)?;
        #[cfg(any(test, feature = "test-seams"))]
        observe_tree_height(height);
        for level in 0..=height {
            self.edit_level(root, level)?;
        }
        let mut level = height;
        let mut final_stream = self.parents;
        let mut count = 0u64;
        let mut first = None;
        let mut reader = final_stream.reader();
        while let Some(edit) = reader.read::<Edit>(&self.scratch)? {
            if edit.value.is_none() {
                continue;
            }
            count = count.checked_add(1).ok_or_else(overflow)?;
            if first.is_none() {
                first = Some(edit);
            }
        }
        while count > 1 {
            level += 1;
            if level > MAX_CHUNK_LEVEL {
                return Err(Error::Other(
                    "logical identity internal chunks exceed permitted depth".into(),
                ));
            }
            self.parents = Stream::new();
            self.begin(level);
            let mut reader = final_stream.reader();
            while let Some(edit) = reader.read::<Edit>(&self.scratch)? {
                if let Some(value) = edit.value {
                    self.push(level, edit.key, value)?;
                }
            }
            self.finish(level)?;
            final_stream = self.end_parents()?;
            if final_stream.count >= count {
                return Err(Error::Other(
                    "logical identity internal chunks cannot reduce oversized separators".into(),
                ));
            }
            count = final_stream.count;
            first = final_stream.reader().read::<Edit>(&self.scratch)?;
        }
        let mut result = if let Some(edit) = first {
            FamilyRoot {
                root: Some(self.with_encoded(edit.key, |key| Ok(chunk_id(level, key)))?),
                hash: edit.value.expect("live final separator"),
                open_config_root: root.open_config_root,
            }
        } else {
            empty_root(self.family)
        };
        while level > 0 {
            let Some(id) = result.root else {
                break;
            };
            let stored = node_storage_key(self.family, &id);
            let staged = self.planned_write(stored)?;
            let child = if let Some(write) = staged.filter(|write| write.body != NONE) {
                let mut header = [0; 9];
                self.scratch.read(write.body, &mut header)?;
                if header[0] != NODE_VERSION
                    || u32::from_be_bytes(header[1..5].try_into().expect("level")) != level
                {
                    return Err(Error::Other("scratch root level mismatch".into()));
                }
                if u32::from_be_bytes(header[5..9].try_into().expect("count")) != 1 {
                    break;
                }
                let len = position(write.len)?;
                const SINGLETON_BYTES: usize = 9 + 4 + INLINE_SEPARATOR_BYTES + 32 + 32;
                if len > SINGLETON_BYTES {
                    return Err(Error::Other(
                        "scratch singleton root exceeds separator bound".into(),
                    ));
                }
                let mut bytes = [0; SINGLETON_BYTES];
                self.scratch.read(write.body, &mut bytes[..len])?;
                let node = EncodedNode::authenticate(self.family, &bytes[..len])?;
                if node.subtree_hash != result.hash {
                    return Err(Error::Other("scratch root hash mismatch".into()));
                }
                let mut cursor = 9;
                let entry = node.entry_at(&mut cursor)?;
                (chunk_id(level - 1, entry.key), entry.value_hash)
            } else {
                self.load(id, result.hash)?;
                if self.current_meta().2 != 1 {
                    break;
                }
                let (key, hash) = self.entry(0)?;
                self.with_encoded(key, |key| {
                    self.with_referenced(key, |_| Ok((chunk_id(level - 1, key), hash)))
                })?
            };
            let mut write = Write::removal(stored);
            write.sequence = self.writes.count;
            self.writes.push(&self.scratch, write)?;
            level -= 1;
            result.root = Some(child.0);
            result.hash = child.1;
        }
        // Original guards, descriptor arrays, the path and the chunk builder
        // stop being resident before the separate bounded directory sort phase.
        self.current.set(None);
        self.path_len = 0;
        self.slots = None;
        self.path = None;
        self.builder = None;
        self.finalize_directory()?;
        let mut at = self.retirements.first;
        while at != NONE {
            let action = self.scratch.get::<Action>(at)?;
            self.journal.push(&self.scratch, action)?;
            at = action.next;
        }
        Ok(result)
    }
    fn plan_for_replay(mut self, root: &FamilyRoot) -> Result<(FamilyRoot, Scratch, u64)> {
        let result = self.plan(root)?;
        let Self {
            scratch,
            inputs,
            journal,
            ..
        } = self;
        // Everything borrowing the original table drops before scratch escapes.
        drop(inputs);
        Ok((result, scratch, journal.first))
    }
}
fn replay(
    scratch: &Scratch,
    first: u64,
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    family: LogicalFamily,
) -> Result<()> {
    let mut at = first;
    while at != NONE {
        let action = scratch.get::<Action>(at)?;
        if action.kind == 2 {
            table
                .remove(action.key.as_slice())
                .map_err(RedbPersistence::storage_error)?;
            #[cfg(any(test, feature = "test-seams"))]
            observe_node_remove();
        } else {
            let len = position(action.len)?;
            #[cfg(any(test, feature = "test-seams"))]
            let before = redb::commit_work::freed_pages_so_far();
            // The zeroed guard owns the destination pages. Reading directly
            // into them avoids a full-body zero Vec and a duplicate IO buffer.
            // Any error after reservation propagates to the owning transaction;
            // dropping this guard alone is deliberately not a rollback.
            let mut destination = table
                .insert_zeroed_reserve(action.key.as_slice(), len)
                .map_err(RedbPersistence::storage_error)?;
            scratch.read(action.body, destination.as_mut())?;
            let bytes = destination.as_mut();
            if action.kind == 1 {
                if bytes.first() != Some(&0) || key_hash(&bytes[1..]) != action.key[1..] {
                    return Err(Error::Other(
                        "scratch separator blob does not authenticate".into(),
                    ));
                }
            } else {
                let node = EncodedNode::authenticate(family, bytes)?;
                if node.subtree_hash != action.hash {
                    return Err(Error::Other("scratch planned chunk hash mismatch".into()));
                }
                let mut cursor = 9;
                let mut last = &[][..];
                for _ in 0..node.count {
                    last = node.entry_at(&mut cursor)?.key;
                }
                if node_storage_key(family, &chunk_id(node.level, last)) != action.key {
                    return Err(Error::Other(
                        "scratch chunk id disagrees with its separator".into(),
                    ));
                }
            }
            drop(destination);
            #[cfg(any(test, feature = "test-seams"))]
            {
                observe_node_write(len);
                observe_written_key(&action.key, action.kind == 1);
                if action.kind == 0 {
                    let copied = redb::commit_work::freed_pages_so_far() - before;
                    WRITES_BY_LEVEL.with(|levels| {
                        levels.borrow_mut().entry(action.key[1]).or_default().1 += copied
                    });
                }
            }
        }
        at = action.next;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn update_family(
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    family: LogicalFamily,
    root: &FamilyRoot,
    edits: EntryEdits,
    list_frames: usize,
) -> Result<FamilyRoot> {
    let mut remaining = edits.iter().map(|(key, _)| key.len()).sum();
    update_family_consuming_keys(
        table,
        admission,
        family,
        root,
        edits,
        list_frames,
        &mut remaining,
    )
}

pub(super) fn update_family_consuming_keys(
    table: &mut redb::Table<'_, &'static [u8], &'static [u8]>,
    admission: &mut ProofAdmission,
    family: LogicalFamily,
    root: &FamilyRoot,
    edits: EntryEdits,
    list_frames: usize,
    coalesced_key_bytes: &mut usize,
) -> Result<FamilyRoot> {
    let input_bytes = edits.iter().try_fold(0usize, |sum, (key, _)| {
        sum.checked_add(key.capacity())
            .ok_or_else(|| proof_allocation_overflow(admission))
    })?;
    let scratch = Scratch::open(admission)?;
    let base = admission
        .held
        .checked_add(scratch._charge.bytes)
        .ok_or_else(|| proof_allocation_overflow(admission))?;
    let ledger = Ledger {
        live: Cell::new(0),
        base,
        caller_peak: &admission.peak,
    };
    let workspace = Workspace {
        admission: RefCell::new(ProofAdmission {
            reader_budget: admission.reader_budget.clone(),
            reader_refusal: admission.reader_refusal.clone(),
            accountant: admission
                .accountant
                .clone()
                .or_else(|| admission.scratch_accountant.clone()),
            reclaim: admission.reclaim.clone(),
            charge_fixed_refusal_control: admission.charge_fixed_refusal_control,
            scratch_owner: admission.scratch_owner.clone(),
            scratch_accountant: admission.scratch_accountant.clone(),
            held: 0,
            peak: Cell::new(0),
        }),
        ledger: &ledger,
    };
    let (result, scratch, first) =
        Window::open(table, family, edits, scratch, &workspace).plan_for_replay(root)?;
    drop(workspace);
    debug_assert_eq!(ledger.live.get(), 0);
    drop(ledger);
    admission.release(input_bytes);
    *coalesced_key_bytes = coalesced_key_bytes.saturating_sub(input_bytes);
    admission.release(list_frames);
    replay(&scratch, first, table, family)?;
    #[cfg(any(test, feature = "test-seams"))]
    observe_proof_memory(scratch._charge.bytes, 0);
    Ok(result)
}

#[cfg(test)]
#[test]
fn cached_original_guards_preserve_admission_and_authenticate_fresh_loads() {
    // This private row observes the real owners; stack handles have no heap quote.
    fn with_window<R>(
        table: &redb::Table<'_, &'static [u8], &'static [u8]>,
        family: LogicalFamily,
        inputs: EntryEdits,
        admission: &mut ProofAdmission,
        f: impl FnOnce(&mut Window<'_, '_, '_>) -> Result<R>,
    ) -> Result<R> {
        let scratch = Scratch::open(admission)?;
        let ledger = Ledger {
            live: Cell::new(0),
            base: admission.held.checked_add(scratch._charge.bytes).unwrap(),
            caller_peak: &admission.peak,
        };
        let workspace = Workspace {
            admission: RefCell::new(ProofAdmission {
                accountant: admission
                    .accountant
                    .clone()
                    .or_else(|| admission.scratch_accountant.clone()),
                reclaim: admission.reclaim.clone(),
                charge_fixed_refusal_control: admission.charge_fixed_refusal_control,
                scratch_owner: admission.scratch_owner.clone(),
                scratch_accountant: admission.scratch_accountant.clone(),
                reader_budget: admission.reader_budget.clone(),
                reader_refusal: admission.reader_refusal.clone(),
                held: 0,
                peak: Cell::new(0),
            }),
            ledger: &ledger,
        };
        let mut window = Window::open(table, family, inputs, scratch, &workspace);
        let result = f(&mut window);
        drop(window);
        assert_eq!(
            ledger.live.get(),
            0,
            "every detached workspace lease releases at teardown"
        );
        result
    }
    fn array_bytes<T>(array: &OwnedArray<'_, T>) -> usize {
        let bytes = std::alloc::Layout::array::<T>(array.entries.len())
            .unwrap()
            .size();
        assert_eq!(
            array._charge.charge.bytes, bytes,
            "each real array has its exact Layout lease"
        );
        bytes
    }
    fn resident(
        window: &Window<'_, '_, '_>,
        accountant: &crate::memory_accounting::MemoryAccountant,
        baseline: usize,
    ) -> usize {
        let mut bytes = 0;
        if let Some(slots) = &window.slots {
            bytes += array_bytes(slots);
            for slot in &slots.entries {
                if let Some(node) = slot.borrow().as_ref() {
                    assert!(!node.guard.value().is_empty());
                    let pages = node.guard.retained_page_bytes();
                    assert_eq!(
                        node._page_charge.charge.bytes, pages,
                        "the AccessGuard owns its actual backing pages"
                    );
                    bytes += pages + array_bytes(&node.entries);
                }
            }
        }
        if let Some(path) = &window.path {
            bytes += array_bytes(path);
        }
        if let Some(builder) = &window.builder {
            bytes += array_bytes(builder);
        }
        assert_eq!(
            window.workspace.ledger.live.get(),
            bytes,
            "live ledger equals simultaneous detached heap/page leases"
        );
        assert_eq!(
            accountant.usage().used - baseline,
            window.scratch._charge.bytes + bytes,
            "scratch plus live owners account for all funded bytes, with no stack charge"
        );
        assert!(window.workspace.ledger.caller_peak.get() >= window.workspace.ledger.base + bytes);
        bytes
    }
    fn locator(id: [u8; 32], hash: [u8; 32]) -> Locator {
        Locator {
            kind: 1,
            id,
            hash,
            start: 0,
            len: 1,
        }
    }
    fn auth_counts(scope: &crate::read_work_probe::ReadWorkObservationScope) -> (usize, usize) {
        let counters = scope.counters();
        (
            counters.proof_nodes_read as usize,
            counters.proof_nodes_hashed as usize,
        )
    }
    let directory = tempfile::tempdir().unwrap();
    let database = ProofFixtureDatabase::create(directory.path().join("guard-modes.redb")).unwrap();
    let family = LogicalFamily::Configuration;
    let mutations = (0..4)
        .map(|id| {
            ContentIdentityMutation::put(
                family,
                [
                    vec![HASHED_KEY_TAG],
                    format!("{id:02}{}", "k".repeat(70 * 1024)).into_bytes(),
                ]
                .concat(),
                b"initial".to_vec(),
            )
        })
        .collect::<Vec<_>>();
    let write = database.begin_write().unwrap();
    RedbPersistence::initialize_content_identity_in_write(&write).unwrap();
    // Encode the durable fixture before any Window admission is exercised.
    // The ordinary node/root encoders keep the decisive mutant in the row's own lease oracle.
    let fixture_entries = mutations
        .iter()
        .map(|mutation| PageEntry {
            key: mutation.logical_key.clone(),
            value_hash: *blake3::hash(mutation.logical_value.as_ref().unwrap()).as_bytes(),
        })
        .collect::<Vec<_>>();
    let fixture_hash = node_hash(family, 0, &fixture_entries);
    let fixture_id = chunk_id(0, &fixture_entries.last().unwrap().key);
    let fixture_bytes = encode_node(&Node {
        level: 0,
        entries: fixture_entries,
        subtree_hash: fixture_hash,
    });
    let other_entries = vec![PageEntry {
        key: [vec![HASHED_KEY_TAG], b"guard replacement".to_vec()].concat(),
        value_hash: [7; 32],
    }];
    let other_id = chunk_id(0, &other_entries[0].key);
    let other_hash = node_hash(family, 0, &other_entries);
    let other_bytes = encode_node(&Node {
        level: 0,
        entries: other_entries,
        subtree_hash: other_hash,
    });
    assert_ne!(
        other_id, fixture_id,
        "replacement uses a genuinely different original"
    );
    {
        let mut table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        for mutation in &mutations {
            let reference = encoded_key(0, &mutation.logical_key);
            let stored = node_storage_key(family, &key_hash(&reference));
            let mut blob = vec![0];
            blob.extend_from_slice(&mutation.logical_key);
            table.insert(stored.as_slice(), blob.as_slice()).unwrap();
        }
        let stored = node_storage_key(family, &fixture_id);
        table
            .insert(stored.as_slice(), fixture_bytes.as_slice())
            .unwrap();
        let stored = node_storage_key(family, &other_id);
        table
            .insert(stored.as_slice(), other_bytes.as_slice())
            .unwrap();
    }
    {
        let mut roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
        let mut published = {
            let guard = roots.get(family as u8).unwrap().unwrap();
            decode_published_root(family, guard.value()).unwrap()
        };
        published.identity.root = Some(fixture_id);
        published.identity.hash = fixture_hash;
        let bytes = encode_published_root(&published);
        roots.insert(family as u8, bytes.as_slice()).unwrap();
    }
    write.commit().unwrap();

    // N/2N inputs share the same authenticated durable height and node/page shape.
    // Only the workspace controls are population-independent; inputs keep their own charges.
    let mut controls = Vec::new();
    for count in [2_usize, 4] {
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            256 * 1024 * 1024,
        ));
        let mut admission = database.admission(Some(Arc::clone(&accountant).into()));
        let inputs: EntryEdits = mutations
            .iter()
            .take(count)
            .map(|mutation| {
                (
                    mutation.logical_key.clone(),
                    Some(*blake3::hash(mutation.logical_value.as_ref().unwrap()).as_bytes()),
                )
            })
            .collect();
        let input_bytes =
            std::alloc::Layout::array::<(Vec<u8>, Option<[u8; 32]>)>(inputs.capacity())
                .unwrap()
                .size()
                + inputs.iter().map(|(key, _)| key.capacity()).sum::<usize>();
        admission.admit(input_bytes).unwrap();
        let baseline = accountant.usage().used;
        let write = database.begin_write().unwrap();
        let root = {
            let roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
            decode_root(family, roots.get(family as u8).unwrap().unwrap().value()).unwrap()
        };
        let table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
        with_window(&table, family, inputs, &mut admission, |window| {
            assert_eq!(window.workspace.ledger.live.get(), 0);
            assert_eq!(
                accountant.usage().used - baseline,
                window.scratch._charge.bytes,
                "opening stack controls allocates no window heap"
            );
            let height = window.initialize(&root)?;
            let slots = window.slots.as_ref().unwrap();
            let path = window.path.as_ref().unwrap();
            assert_eq!(slots.entries.len(), height as usize + 3);
            assert_eq!(path.entries.len(), height as usize + 1);
            controls.push((height, array_bytes(slots), array_bytes(path)));
            resident(window, &accountant, baseline);
            window.grow_builder(1000)?;
            let prior = window.builder.as_ref().unwrap().entries.len();
            let before = window.workspace.ledger.live.get();
            window.grow_builder(prior + 1)?;
            let next = window.builder.as_ref().unwrap().entries.len();
            assert!(
                window.workspace.ledger.caller_peak.get()
                    >= window.workspace.ledger.base + before + next,
                "builder growth admits the entire new buffer while the old buffer remains live"
            );
            resident(window, &accountant, baseline);
            Ok(())
        })
        .unwrap();
        assert_eq!(accountant.usage().used, baseline);
        admission.release(input_bytes);
        assert_eq!(accountant.usage().used, 0);
    }
    assert_eq!(
        controls[0], controls[1],
        "at fixed authenticated H, N/2N use identical exact slot/path layouts"
    );

    for mode in 0..3 {
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            if mode == 2 { 1 } else { 256 * 1024 * 1024 },
        ));
        if mode == 2 {
            accountant.charge_retained(1);
        }
        let baseline = accountant.usage().used;
        let mut admission = match mode {
            0 => database.admission(Some(Arc::clone(&accountant).into())),
            1 => {
                let mut admission = database.admission(None);
                admission.scratch_accountant = Some(Arc::clone(&accountant).into());
                admission
            }
            _ => ProofAdmission::fixed_refusal_control(Some(Arc::clone(&accountant).into()))
                .with_scratch_owner(Some(Arc::downgrade(&database.owner))),
        };
        let write = database.begin_write().unwrap();
        let root = {
            let roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
            decode_root(family, roots.get(family as u8).unwrap().unwrap().value()).unwrap()
        };
        let id = root.root.unwrap();
        {
            let table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
            with_window(&table, family, Vec::new(), &mut admission, |window| {
                let scope = crate::read_work_probe::ReadWorkObservationScope::enter(crate::read_work_probe::ReadWorkPhase::Commit);
                window.initialize(&root)?;
                resident(window, &accountant, baseline);
                let before = auth_counts(&scope);
                let used = accountant.usage().used;
                window.with_locator(locator(id, root.hash), |bytes| {
                    assert_eq!(bytes, &[NODE_VERSION]);
                    assert!(window.slots.as_ref().unwrap().entries[0].try_borrow_mut().is_err(), "callback physically borrows its AccessGuard owner");
                    assert_eq!(accountant.usage().used, used);
                    Ok(())
                })?;
                window.with_locator(locator(id, root.hash), |_| Ok(()))?;
                assert_eq!(auth_counts(&scope), before, "same id+expected cache hits perform no read or authentication");
                assert_eq!(accountant.usage().used, used);

                // Borrowed operands survive the real cached replacement path.
                window.current.set(None);
                window.path_len = 0;
                window.cache_next.set(0);
                let held = window.slots.as_ref().unwrap().entries[0].borrow();
                let replacement = window.cached(other_id, other_hash)?;
                assert_ne!(replacement, 0, "cached cannot evict the actively borrowed guard");
                let retained = held.as_ref().unwrap();
                assert_eq!((retained.id, retained.hash), (id, root.hash), "replacement preserves the retained slot's original authority");
                assert!(!retained.guard.value().is_empty());
                let loaded = window.slots.as_ref().unwrap().entries[replacement].borrow();
                assert_eq!((loaded.as_ref().unwrap().id, loaded.as_ref().unwrap().hash), (other_id, other_hash));
                drop(loaded);
                drop(held);
                resident(window, &accountant, baseline);
                let release = { let slot = window.slots.as_ref().unwrap().entries[0].borrow();
                    let node = slot.as_ref().unwrap(); node.guard.retained_page_bytes() + array_bytes(&node.entries) };
                let used = accountant.usage().used;
                let removed = window.slots.as_ref().unwrap().entries[0].borrow_mut().take();
                drop(removed);
                assert_eq!(used - accountant.usage().used, release, "dropping concrete guard plus descriptors releases exactly their leases");
                resident(window, &accountant, baseline);

                // Clear the distinct replacement, then reacquire through production cached.
                let removed = window.slots.as_ref().unwrap().entries[replacement].borrow_mut().take();
                drop(removed);
                let before = auth_counts(&scope);
                window.cache_next.set(0);
                window.with_locator(locator(id, root.hash), |_| Ok(()))?;
                assert_eq!(auth_counts(&scope), (before.0 + 1, before.1 + 1));
                resident(window, &accountant, baseline);
                let mut wrong = root.hash; wrong[0] ^= 1;
                window.cache_next.set(0);
                let before = auth_counts(&scope);
                let error = window.with_locator(locator(id, wrong), |_| Ok(())).unwrap_err();
                assert!(matches!(error, Error::Other(ref message) if message.contains("disagrees with committed parent")));
                assert_eq!(auth_counts(&scope), (before.0 + 1, before.1 + 1), "wrong expected reloads and authenticates, never reuses good cache authority");
                assert!(window.slots.as_ref().unwrap().entries[0].borrow().is_none(), "real eviction releases before failed replacement");
                resident(window, &accountant, baseline);

                // Authoritative wide bytes are owned and charged inside the callback.
                let reference = encoded_key(0, &mutations[0].logical_key);
                let before = accountant.usage().used;
                let stored = node_storage_key(family, &key_hash(&reference));
                let pages = { let guard = table.get(stored.as_slice()).unwrap().unwrap(); guard.retained_page_bytes() };
                window.with_referenced(&reference, |bytes| {
                    assert_eq!(bytes, mutations[0].logical_key.as_slice());
                    assert_eq!(accountant.usage().used - before, pages);
                    window.with_referenced(&reference, |_| {
                        assert_eq!(accountant.usage().used - before, 2 * pages);
                        Ok(())
                    })
                })?;
                assert_eq!(accountant.usage().used, before);
                resident(window, &accountant, baseline);
                Ok(())
            }).unwrap();
        }
        assert_eq!(
            accountant.usage().used,
            baseline,
            "normal, cleanup, and fixed-refusal owners all release scratch and workspace"
        );
        if mode == 2 {
            let value = b"refused";
            write
                .open_table(super::super::CONFIG_TABLE)
                .unwrap()
                .insert("guard_refusal", value.as_slice())
                .unwrap();
            RedbPersistence::apply_content_identity_mutations_in_write(
                &write,
                &config_mutations("guard_refusal", Some(value)),
                &mut admission,
            )
            .unwrap();
            drop(admission);
            write.commit().unwrap();
            let read = database.begin_read().unwrap();
            let config = read.open_table(super::super::CONFIG_TABLE).unwrap();
            assert_eq!(
                config.get("guard_refusal").unwrap().unwrap().value(),
                value.as_slice()
            );
            assert_eq!(accountant.usage().used, baseline);
            accountant.release(1);
        } else {
            drop(admission);
            drop(write);
        }
        assert_eq!(accountant.usage().used, 0);
    }

    // The production family commit retains the same receipt and releases its owners.
    {
        let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
            256 * 1024 * 1024,
        ));
        let mut admission = database.admission(Some(Arc::clone(&accountant).into()));
        let write = database.begin_write().unwrap();
        let changes = mutations
            .iter()
            .map(|mutation| {
                ContentIdentityMutation::put(
                    family,
                    mutation.logical_key.clone(),
                    b"updated".to_vec(),
                )
            })
            .collect::<Vec<_>>();
        RedbPersistence::apply_content_identity_mutations_in_write(
            &write,
            &changes,
            &mut admission,
        )
        .unwrap();
        assert!(admission.peak() > 0);
        drop(admission);
        assert_eq!(accountant.usage().used, 0);
        write.commit().unwrap();
    }

    // No guard or verification result survives a Window lifetime/table mutation.
    let write = database.begin_write().unwrap();
    let root = {
        let roots = write.open_table(CONTENT_IDENTITY_ROOTS_TABLE).unwrap();
        decode_root(family, roots.get(family as u8).unwrap().unwrap().value()).unwrap()
    };
    let id = root.root.unwrap();
    let stored = node_storage_key(family, &id);
    let accountant = Arc::new(crate::memory_accounting::MemoryAccountant::with_budget(
        256 * 1024 * 1024,
    ));
    let mut admission = database.admission(Some(Arc::clone(&accountant).into()));
    let mut table = write.open_table(CONTENT_IDENTITY_NODES_TABLE).unwrap();
    let original = table
        .get(stored.as_slice())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&root)?;
        window.with_locator(locator(id, root.hash), |_| Ok(()))
    })
    .unwrap();
    assert_eq!(accountant.usage().used, 0);
    let mut damaged = original.clone();
    let at = damaged.len() - 33;
    damaged[at] ^= 1;
    table.insert(stored.as_slice(), damaged.as_slice()).unwrap();
    let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&root)
    })
    .unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message.contains("does not authenticate its contents"))
    );
    assert_eq!(accountant.usage().used, 0);

    let mut replacement = decode_node(family, &original).unwrap();
    replacement.entries[0].value_hash[0] ^= 1;
    replacement.subtree_hash = node_hash(family, replacement.level, &replacement.entries);
    let bytes = encode_node(&replacement);
    table.insert(stored.as_slice(), bytes.as_slice()).unwrap();
    let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&root)
    })
    .unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message.contains("disagrees with committed parent"))
    );
    let changed_root = FamilyRoot {
        hash: replacement.subtree_hash,
        ..root.clone()
    };
    with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&changed_root)?;
        resident(window, &accountant, 0);
        Ok(())
    })
    .unwrap();
    assert_eq!(accountant.usage().used, 0);
    let mut wrong_id = id;
    wrong_id[1] ^= 1;
    let wrong_stored = node_storage_key(family, &wrong_id);
    table
        .insert(wrong_stored.as_slice(), bytes.as_slice())
        .unwrap();
    let wrong_root = FamilyRoot {
        root: Some(wrong_id),
        ..changed_root.clone()
    };
    let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&wrong_root)
    })
    .unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message.contains("id disagrees with its separator"))
    );
    assert_eq!(accountant.usage().used, 0);

    // Correctly hashed malformed order/cut fixtures reach canonical checks.
    for entries in [
        vec![
            PageEntry {
                key: vec![HASHED_KEY_TAG, b'b'],
                value_hash: [1; 32],
            },
            PageEntry {
                key: vec![HASHED_KEY_TAG, b'a'],
                value_hash: [2; 32],
            },
        ],
        vec![
            PageEntry {
                key: [
                    vec![HASHED_KEY_TAG],
                    vec![b'a'; TARGET_NODE_ENTRY_BYTES * 2],
                ]
                .concat(),
                value_hash: [1; 32],
            },
            PageEntry {
                key: vec![HASHED_KEY_TAG, b'z'],
                value_hash: [2; 32],
            },
        ],
    ] {
        let hash = node_hash(family, 0, &entries);
        let fixture_id = chunk_id(0, &entries.last().unwrap().key);
        let bytes = encode_node(&Node {
            level: 0,
            entries,
            subtree_hash: hash,
        });
        let stored = node_storage_key(family, &fixture_id);
        table.insert(stored.as_slice(), bytes.as_slice()).unwrap();
        let fixture_root = FamilyRoot {
            root: Some(fixture_id),
            hash,
            open_config_root: None,
        };
        let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
            window.initialize(&fixture_root)
        })
        .unwrap_err();
        assert!(
            matches!(error, Error::Other(ref message) if message.contains("not strictly ordered") || message.contains("crosses a canonical boundary"))
        );
        assert_eq!(accountant.usage().used, 0);
    }
    // Reference-key order needs authoritative wide comparisons after body hashing.
    let mut reversed = decode_node(family, &fixture_bytes).unwrap();
    reversed.entries.reverse();
    reversed.subtree_hash = node_hash(family, reversed.level, &reversed.entries);
    let reversed_id = chunk_id(reversed.level, &reversed.entries.last().unwrap().key);
    let reversed_stored = node_storage_key(family, &reversed_id);
    let reversed_bytes = encode_node(&reversed);
    table
        .insert(reversed_stored.as_slice(), reversed_bytes.as_slice())
        .unwrap();
    let reversed_root = FamilyRoot {
        root: Some(reversed_id),
        hash: reversed.subtree_hash,
        open_config_root: None,
    };
    let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
        window.initialize(&reversed_root)
    })
    .unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message.contains("internal separator order is invalid"))
    );
    assert_eq!(accountant.usage().used, 0);
    // Changed, absent, and wrong-hash wide blobs refuse under fresh physical owners.
    table
        .insert(stored.as_slice(), original.as_slice())
        .unwrap();
    let reference = encoded_key(0, &mutations[0].logical_key);
    let wide_stored = node_storage_key(family, &key_hash(&reference));
    let wide = table
        .get(wide_stored.as_slice())
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    for variant in 0..3 {
        if variant == 1 {
            table.remove(wide_stored.as_slice()).unwrap();
        } else {
            let mut damaged = wide.clone();
            if variant == 0 {
                damaged[0] ^= 1;
            } else {
                damaged[1] ^= 1;
            }
            table
                .insert(wide_stored.as_slice(), damaged.as_slice())
                .unwrap();
        }
        let error = with_window(&table, family, Vec::new(), &mut admission, |window| {
            window.initialize(&root)?;
            window.with_referenced::<()>(&reference, |_| {
                panic!("invalid authoritative wide bytes must never reach the callback")
            })
        })
        .unwrap_err();
        assert!(
            matches!(error, Error::Other(ref message) if message.contains("separator key is damaged") || message.contains("separator key is missing"))
        );
        assert_eq!(accountant.usage().used, 0);
    }
}
