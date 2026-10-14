//! Canonical walks over typed borrowed fields; no Legacy Record is constructed.
use super::{native_decode::*, native_records::*, native_selection::*};
use contextdb_core::read_memory::ReadMemoryOwner;
use serde::Serialize;
use std::io::Write;

// Borrowed field walkers feed the same finite count/hash/exact-slice sink
// used by the signed packet producer. This callback is stack storage only.
pub(crate) struct NativeEncoder<'a> {
    sink: &'a mut dyn FnMut(&[u8]),
    failed: bool,
}
pub(crate) trait ByteSource {
    fn byte_len(&self) -> usize;
    fn emit(&self, e: &mut NativeEncoder<'_>);
}
impl ByteSource for [u8] {
    fn byte_len(&self) -> usize {
        self.len()
    }
    fn emit(&self, e: &mut NativeEncoder<'_>) {
        e.raw(self)
    }
}
impl<const N: usize> ByteSource for [u8; N] {
    fn byte_len(&self) -> usize {
        N
    }
    fn emit(&self, e: &mut NativeEncoder<'_>) {
        e.raw(self)
    }
}
impl ByteSource for Bytes<'_> {
    fn byte_len(&self) -> usize {
        self.len()
    }
    fn emit(&self, e: &mut NativeEncoder<'_>) {
        match self.iter() {
            Ok(iter) => {
                for b in iter {
                    match b {
                        Ok(b) => e.u8(b),
                        Err(_) => e.failed = true,
                    }
                }
            }
            Err(_) => e.failed = true,
        }
    }
}
impl NativeEncoder<'_> {
    pub(crate) fn raw(&mut self, b: &[u8]) {
        (self.sink)(b)
    }
    pub(crate) fn u8(&mut self, v: u8) {
        self.raw(&[v])
    }
    pub(crate) fn u32(&mut self, v: u32) {
        self.raw(&v.to_be_bytes())
    }
    pub(crate) fn u64(&mut self, v: u64) {
        self.raw(&v.to_be_bytes())
    }
    pub(crate) fn bytes<B: ByteSource + ?Sized>(&mut self, b: &B) {
        self.u64(b.byte_len() as u64);
        b.emit(self)
    }
    pub(crate) fn string(&mut self, s: &str) {
        self.bytes(s.as_bytes())
    }
    pub(crate) fn node(&mut self, s: &str) -> Decoded<()> {
        self.raw(&node_bytes(s)?);
        Ok(())
    }
    pub(crate) fn option<T>(&mut self, v: Option<&T>, emit: impl FnOnce(&mut Self, &T)) {
        match v {
            None => self.u8(0),
            Some(v) => {
                self.u8(1);
                emit(self, v)
            }
        }
    }
    pub(crate) fn key(&mut self, key: &NaturalKey<'_>) {
        if key.encode(self).is_err() {
            self.failed = true
        }
    }
    pub(crate) fn reference(&mut self, table: &str, key: &NaturalKey<'_>) {
        self.string(table);
        self.key(key)
    }
    pub(crate) fn policy(&mut self, p: &ApplicationTablePolicy<'_>) -> Decoded<()> {
        use contextdb_core::{EdgeDiscardMode as E, HistoryPolicy as H, SyncDirection as D};
        self.u8(match p.sync_direction {
            D::None => 0,
            D::Push => 1,
            D::Pull => 2,
            D::Both => 3,
        });
        self.u8(match p.sync_conflict {
            Resolution::InsertIfNotExists => 0,
            Resolution::LatestWins => 1,
            _ => return Err(Invalid::Canonical),
        });
        self.u8(u8::from(p.immutable));
        self.option(p.retain.as_ref(), |e, r| {
            e.u64(r.seconds);
            e.u8(u8::from(r.sync_safe))
        });
        self.u8(match p.history {
            H::All => 0,
            H::CurrentOnly => 1,
        });
        match p.manifest_tables {
            None => self.u8(0),
            Some(t) => {
                self.u8(1);
                self.u64(t.len() as u64);
                for s in t.iter()? {
                    self.string(s?)
                }
            }
        }
        self.u8(match p.edge_discard {
            E::Never => 0,
            E::AfterOutcome => 1,
            E::Always => 2,
        });
        Ok(())
    }
}
pub(crate) fn node_bytes(s: &str) -> Decoded<[u8; 32]> {
    if s.len() != 64 {
        return Err(Invalid::Canonical);
    }
    let mut out = [0; 32];
    for (i, p) in s.as_bytes().chunks_exact(2).enumerate() {
        let digit = |b| match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            _ => Err(Invalid::Canonical),
        };
        out[i] = (digit(p[0])? << 4) | digit(p[1])?;
    }
    Ok(out)
}
fn shared_walk(
    shared: &mut super::canonical::FiniteEncoder<'_>,
    walk: impl FnOnce(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Decoded<()> {
    let mut raw = |b: &[u8]| shared.raw(b);
    let mut encoder = NativeEncoder {
        sink: &mut raw,
        failed: false,
    };
    walk(&mut encoder)?;
    if encoder.failed {
        Err(Invalid::Canonical)
    } else {
        Ok(())
    }
}
pub(crate) fn finite_len(
    walk: impl FnOnce(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Decoded<usize> {
    super::canonical::finite_len_with_error(|e| shared_walk(e, walk), || Invalid::Canonical)
}
pub(crate) fn finite_hash(
    walk: impl FnOnce(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Decoded<Digest> {
    super::canonical::finite_hash_with_error(|e| shared_walk(e, walk), || Invalid::Canonical)
}
pub(crate) fn finite_bytes(
    owner: &ReadMemoryOwner,
    walk: impl Fn(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Selected<NativeBuffer> {
    let n = finite_len(|e| walk(e))?;
    let mut b = NativeBuffer::allocate(owner, n)?;
    super::canonical::finite_emit_with_error(
        b.bytes_mut(),
        |e| shared_walk(e, |e| walk(e)),
        || Invalid::Canonical,
    )?;
    Ok(b)
}
pub(crate) fn verify_signature(
    owner: &ReadMemoryOwner,
    node: &str,
    signature: Bytes<'_>,
    walk: impl Fn(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Selected<()> {
    use ed25519_dalek::Verifier;
    let public = node_bytes(node)?;
    let key = ed25519_dalek::VerifyingKey::from_bytes(&public)
        .map_err(|_| Failure::Invalid(Invalid::Signature))?;
    let mut sig = [0; 64];
    signature.copy_into(&mut sig)?;
    let sig = ed25519_dalek::Signature::from_bytes(&sig);
    let bytes = finite_bytes(owner, walk)?;
    key.verify(bytes.bytes(), &sig)
        .map_err(|_| Failure::Invalid(Invalid::Signature))
}
// Exact named-codec equality, including field order, omitted default fields,
// minimal integer tags, UTF-8, UUID binary encoding and trailing-byte rejection.
struct Compare<'a> {
    expected: &'a [u8],
    offset: usize,
}
impl Write for Compare<'_> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let end = self
            .offset
            .checked_add(b.len())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
        if self.expected.get(self.offset..end) != Some(b) {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        self.offset = end;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn decode_record(raw: &[u8]) -> Decoded<RecordView<'_>> {
    let n = NodeView::parse(raw)?;
    // Pass one visits all nested real typed fields without construction of
    // owned strings, arrays, JSON trees or serde Record clones.
    RecordView::decode(n)?;
    // Pass two constructs fixed borrowed fields over the original loan.
    let record = RecordView::decode(n)?;
    let mut w = Compare {
        expected: raw,
        offset: 0,
    };
    record
        .serialize(&mut rmp_serde::Serializer::new(&mut w).with_struct_map())
        .map_err(|_| Invalid::Canonical)?;
    if w.offset != raw.len() {
        return Err(Invalid::Canonical);
    }
    Ok(record)
}
impl NaturalKey<'_> {
    pub(crate) fn encode(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.u64(1 + self.rest.len() as u64);
        e.string(self.column);
        self.value.encode(e)?;
        for pair in self.rest.iter()? {
            let (c, v) = pair?;
            e.string(c);
            v.encode(e)?;
        }
        Ok(())
    }
}
impl Value<'_> {
    fn encode(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        match self {
            Self::Null => e.u8(0),
            Self::Bool(b) => {
                e.u8(1);
                e.u8(u8::from(*b))
            }
            Self::Int64(n) => {
                e.u8(2);
                e.raw(&n.to_be_bytes())
            }
            Self::Float64(n) => {
                e.u8(3);
                e.u64(n.to_bits())
            }
            Self::Text(s) => {
                e.u8(4);
                e.string(s)
            }
            Self::Uuid(u) => {
                e.u8(5);
                e.raw(u.as_bytes())
            }
            Self::Timestamp(n) => {
                e.u8(6);
                e.raw(&n.to_be_bytes())
            }
            Self::Json(s) => {
                e.u8(7);
                super::native_json::encode(s, e)?
            }
            Self::Vector(v) => {
                e.u8(8);
                e.u64(v.len() as u64);
                for f in v.iter()? {
                    e.u32(f?.to_bits())
                }
            }
            Self::TxId(t) => {
                e.u8(9);
                e.u64(*t)
            }
        }
        Ok(())
    }
}
pub(crate) fn diagnostic_resolution(r: Resolution) -> u8 {
    match r {
        Resolution::InsertIfNotExists => 0,
        Resolution::ServerWins => 1,
        Resolution::EdgeWins => 2,
        Resolution::LatestWins => 3,
    }
}

// Checked private canonical walks; original Legacy methods remain exact.
impl RowRef<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) {
        e.reference(&self.table, &self.key);
    }
}
impl LocalLifeAnchor<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) {
        e.string(&self.original_table);
        e.u64(self.table_generation);
        e.u64(self.local_row_id);
        e.u64(self.creation_lsn.0);
    }
}
impl StateRef<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes());
        e.raw(&self.token);
    }
}
impl Namespace<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string(self.tenant);
        e.node(&self.hub_node)?;
        e.raw(&self.hub_incarnation.0.to_be_bytes());
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        Ok(())
    }
}
impl RowEvidence<'_> {
    pub(crate) fn encode_creator_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.node(&self.creator.author_node_id)?;
        e.raw(&self.creator.author_database_incarnation.0.to_be_bytes());
        e.u64(self.creator.author_local_mutation_position.0);
        e.u64(self.creator.table_generation);
        e.string(&self.creator.lineage_root);
        e.bytes(&self.creator.attestation);
        Ok(())
    }
    pub(crate) fn encode_life_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        self.reference.encode_finite(e);
        self.encode_creator_finite(e)?;
        e.u8(0);
        e.u64(0);
        Ok(())
    }
    pub(crate) fn encode_projection_finite(
        &self,
        tenant: &str,
        e: &mut NativeEncoder<'_>,
    ) -> Decoded<()> {
        self.reference.encode_finite(e);
        e.u64(self.authored_source.0.0);
        e.u32(self.authored_source.1);
        e.raw(&self.origin_digest_finite(tenant)?);
        e.raw(&self.row_digest);
        Ok(())
    }
}
impl Seal<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) {
        self.root.encode_finite(e);
        e.raw(&self.root_digest);
        e.raw(&self.origin_life_digest);
        e.raw(self.membership_revision.as_bytes());
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.u64(self.registered_count);
        e.raw(&self.unit_digest);
        e.option(self.last_complete.as_ref(), |e, d| e.raw(d));
        e.raw(&self.projection_digest);
        e.raw(&self.authority_digest);
    }
}
impl PolicyEvidence<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string(&self.table);
        e.policy(&self.installed)?;
        e.u8(self.authority);
        if self.authority != 0 {
            let p = self.bound.as_ref().ok_or(Invalid::Canonical)?;
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        e.u8(u8::from(!self.agreement));
        Ok(())
    }
}
impl SignedBinding<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string("delivery-policy-binding.v1");
        e.u32(self.format);
        self.namespace.encode_finite(e)?;
        e.u64(self.tables.len() as u64);
        for p in self.tables.iter()? {
            let p = p?;
            e.string(&p.table);
            e.u64(p.version);
            e.raw(&p.digest);
            e.policy(&p.policy)?;
        }
        self.issuance_state.encode_finite(e);
        e.u64(self.issuance_source.0.0);
        e.u32(self.issuance_source.1);
        Ok(())
    }
}
impl TerminalRecord<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string("delivery-terminal-core.v1");
        e.u32(1);
        self.namespace.encode_finite(e)?;
        self.root.encode_finite(e);
        e.raw(&self.origin_life_digest);
        e.u8(0);
        e.raw(self.submission.as_bytes());
        e.raw(&self.seal_digest);
        e.u8(1);
        e.raw(&self.unit_digest);
        e.u8(1);
        e.u8(0);
        e.u64(self.source.0.0);
        e.u32(self.source.1);
        e.u8(self.kind);
        e.option(self.cause.as_ref(), |e, c| e.string(c));
        e.u64(self.position.0.0);
        e.u32(self.position.1);
        e.raw(&self.state.token);
        e.u8(0);
        e.u64(self.diagnostics.subject_count);
        e.u64(self.diagnostics.conflict_count);
        e.u64(self.diagnostics.canonical_bytes);
        e.raw(&self.diagnostics.commitment);
        Ok(())
    }
}
impl ManifestRecord<'_> {
    pub(crate) fn signed_bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string("delivery-submission.v1");
        e.string(self.tenant);
        e.node(&self.edge_node)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        e.u8(0);
        e.raw(self.id.as_bytes());
        self.seal.encode_finite(e);
        e.raw(&self.seal.projection_digest);
        e.raw(&self.seal.authority_digest);
        Ok(())
    }
}
impl DiagnosticBlock<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.raw(&self.salt);
        e.u64(self.subjects.len() as u64);
        for subject in self.subjects.iter()? {
            let (r, m) = subject?;
            r.encode_finite(e);
            e.u8(m);
        }
        e.u64(self.conflicts.len() as u64);
        for c in self.conflicts.iter()? {
            let c = c?;
            e.key(&c.natural_key);
            e.u8(diagnostic_resolution(c.resolution));
            e.option(c.reason.as_ref(), |e, v| e.string(v));
            e.option(c.table.as_ref(), |e, v| e.string(v));
            e.option(c.mutation_kind.as_ref(), |e, v| e.string(v));
            e.option(c.winning_author_node_id.as_ref(), |e, v| e.string(v));
            e.option(c.hub_acceptance_position.as_ref(), |e, v| e.u64(v.0));
            e.option(c.refusal_cause.as_ref(), |e, v| {
                e.string(&v.table);
                e.key(&v.natural_key);
            });
        }
        Ok(())
    }
}

impl RowEvidence<'_> {
    pub(crate) fn origin_digest_finite(&self, tenant: &str) -> Decoded<Digest> {
        finite_hash(|e| {
            e.string("delivery-origin-life.v1");
            e.string(tenant);
            self.reference.encode_finite(e);
            self.encode_creator_finite(e)
        })
    }
}
impl Seal<'_> {
    pub(crate) fn digest_finite(&self) -> Decoded<Digest> {
        finite_hash(|e| {
            e.string("delivery-submission-seal.v1");
            self.encode_finite(e);
            Ok(())
        })
    }
}
impl SchemaDependency<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string("delivery-schema-dependency.v1");
        e.string(self.tenant);
        e.node(&self.author_node)?;
        e.raw(&self.author_incarnation.0.to_be_bytes());
        e.u64(self.source_lsn.0);
        e.u32(self.ordinal);
        e.u32(self.unit_statements);
        e.string(&self.table);
        e.u64(self.table_generation);
        e.bytes(&self.ddl_bytes);
        e.bytes(&self.ddl_digest);
        Ok(())
    }
}
impl BirthRef<'_> {
    pub(crate) fn encode_finite(&self, e: &mut NativeEncoder<'_>) {
        e.raw(self.logical_store_id.as_bytes());
        e.raw(&self.incarnation.0.to_be_bytes());
        e.raw(self.birth_id.as_bytes())
    }
}
impl Birth<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        self.identity.encode_finite(e);
        e.option(self.predecessor.as_ref(), |e, s| s.encode_finite(e));
        e.option(self.selected_image.as_ref(), |e, s| s.encode_finite(e));
        e.u64(self.triggers.len() as u64);
        for d in self.triggers.iter()? {
            e.raw(&d?)
        }
        e.u64(self.retired.len() as u64);
        for b in self.retired.iter()? {
            b?.encode_finite(e)
        }
        Ok(())
    }
    pub(crate) fn digest_finite(&self) -> Decoded<Digest> {
        finite_hash(|e| {
            e.string("delivery-incarnation-birth.v1");
            self.bytes_finite(e)
        })
    }
}
impl StateRecord<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) {
        self.state.encode_finite(e);
        e.option(self.parent.as_ref(), |e, p| e.raw(p));
        e.u64(self.sequence);
        e.raw(&self.retirement_root)
    }
}
pub(crate) fn birth_eq(a: &BirthRef<'_>, b: &BirthRef<'_>) -> bool {
    a.logical_store_id == b.logical_store_id
        && a.incarnation == b.incarnation
        && a.birth_id == b.birth_id
}
pub(crate) fn state_birth_eq(a: &StateRef<'_>, b: &BirthRef<'_>) -> bool {
    a.logical_store_id == b.logical_store_id
        && a.incarnation == b.incarnation
        && a.birth_id == b.birth_id
}
pub(crate) fn state_eq(a: &StateRef<'_>, b: &StateRef<'_>) -> bool {
    a.logical_store_id == b.logical_store_id
        && a.incarnation == b.incarnation
        && a.birth_id == b.birth_id
        && a.token == b.token
}
pub(crate) fn namespace_eq(a: &Namespace<'_>, b: &Namespace<'_>) -> bool {
    a.tenant == b.tenant
        && a.hub_node == b.hub_node
        && a.hub_incarnation == b.hub_incarnation
        && a.edge_node == b.edge_node
        && a.edge_incarnation == b.edge_incarnation
}
fn birth_cmp(a: &BirthRef<'_>, b: &BirthRef<'_>) -> std::cmp::Ordering {
    a.logical_store_id
        .as_bytes()
        .cmp(b.logical_store_id.as_bytes())
        .then_with(|| a.incarnation.0.cmp(&b.incarnation.0))
        .then_with(|| a.birth_id.as_bytes().cmp(b.birth_id.as_bytes()))
}
pub(crate) trait BirthDirectory<'a>: Copy {
    fn len(self) -> usize;
    fn get(self, index: usize) -> Decoded<Birth<'a>>;
    fn iter(self) -> Decoded<impl Iterator<Item = Decoded<Birth<'a>>>> {
        Ok((0..self.len()).map(move |i| self.get(i)))
    }
}
impl<'a> BirthDirectory<'a> for Seq<'a, Birth<'a>> {
    fn len(self) -> usize {
        Seq::len(self)
    }
    fn get(self, index: usize) -> Decoded<Birth<'a>> {
        Seq::get(self, index)
    }
}
impl<'a> BirthDirectory<'a> for &[Birth<'a>] {
    fn len(self) -> usize {
        <[Birth<'a>]>::len(self)
    }
    fn get(self, index: usize) -> Decoded<Birth<'a>> {
        <[Birth<'a>]>::get(self, index)
            .copied()
            .ok_or(Invalid::Companions)
    }
}
pub(crate) fn retirement_root<'a>(births: impl BirthDirectory<'a>) -> Decoded<Digest> {
    if births.len() == 0 {
        return finite_hash(|e| {
            e.string("delivery-history-index.v1");
            e.u64(0);
            Ok(())
        });
    }
    let mut previous = None;
    for b in births.iter()? {
        let b = b?;
        if previous.is_some_and(|p| birth_cmp(&p, &b.identity) != std::cmp::Ordering::Less) {
            return Err(Invalid::Companions);
        }
        previous = Some(b.identity)
    }
    finite_hash(|e| {
        e.string("delivery-retirement-index.v1");
        e.u64(births.len() as u64);
        for b in births.iter()? {
            let b = b?;
            e.u64(finite_len(|e| b.bytes_finite(e))? as u64);
            b.bytes_finite(e)?
        }
        Ok(())
    })
}
pub(crate) fn retirement_closure<'a>(
    owner: &ReadMemoryOwner,
    active: &Birth<'a>,
    retired: impl BirthDirectory<'a>,
) -> Selected<()> {
    retirement_closure_with_checkpoint(owner, active, retired, || Ok(()))
}
pub(crate) fn retirement_closure_with_checkpoint<'a>(
    owner: &ReadMemoryOwner,
    active: &Birth<'a>,
    retired: impl BirthDirectory<'a>,
    checkpoint: impl Fn() -> Selected<()>,
) -> Selected<()> {
    checkpoint()?;
    if active.retired.len() != retired.len() {
        return Err(Invalid::Companions.into());
    }
    for i in 0..retired.len() {
        checkpoint()?;
        if !birth_eq(&active.retired.get(i)?, &retired.get(i)?.identity) {
            return Err(Invalid::Companions.into());
        }
    }
    for i in 0..=retired.len() {
        checkpoint()?;
        let b = if i == retired.len() {
            *active
        } else {
            retired.get(i)?
        };
        if b.identity.logical_store_id != active.identity.logical_store_id {
            return Err(Invalid::Companions.into());
        }
        let mut prev = None;
        for id in b.triggers.iter()? {
            let id = id?;
            if prev.is_some_and(|p| p >= id) {
                return Err(Invalid::Companions.into());
            }
            prev = Some(id)
        }
        let mut prev = None;
        for id in b.retired.iter()? {
            let id = id?;
            if birth_eq(&id, &b.identity)
                || prev.is_some_and(|p| birth_cmp(&p, &id) != std::cmp::Ordering::Less)
            {
                return Err(Invalid::Companions.into());
            }
            prev = Some(id);
            if !retired
                .iter()?
                .any(|t| t.is_ok_and(|t| birth_eq(&t.identity, &id)))
            {
                return Err(Invalid::Companions.into());
            }
        }
        for state in [b.predecessor, b.selected_image].into_iter().flatten() {
            if !b
                .retired
                .iter()?
                .any(|x| x.is_ok_and(|x| state_birth_eq(&state, &x)))
            {
                return Err(Invalid::Companions.into());
            }
        }
    }
    // The original check forbids reachability cycles, not a stronger subset
    // policy. Fixed prepaid reachability marks replace its pending/visited maps.
    let mut seen = NativeSelectionHold::try_new(owner, retired.len())?;
    for _ in 0..retired.len() {
        seen.retain(false)?
    }
    for origin in 0..=retired.len() {
        checkpoint()?;
        let o = if origin == retired.len() {
            *active
        } else {
            retired.get(origin)?
        };
        seen.as_mut_slice().fill(false);
        for id in o.retired.iter()? {
            let id = id?;
            let ix = (0..retired.len())
                .find(|i| retired.get(*i).is_ok_and(|b| birth_eq(&b.identity, &id)))
                .ok_or(Invalid::Companions)?;
            seen.as_mut_slice()[ix] = true;
        }
        loop {
            checkpoint()?;
            let mut changed = false;
            for i in 0..retired.len() {
                checkpoint()?;
                if !seen.as_slice()[i] {
                    continue;
                }
                let b = retired.get(i)?;
                if birth_eq(&b.identity, &o.identity) {
                    return Err(Invalid::Companions.into());
                }
                for id in b.retired.iter()? {
                    let id = id?;
                    if birth_eq(&id, &o.identity) {
                        return Err(Invalid::Companions.into());
                    }
                    let ix = (0..retired.len())
                        .find(|i| retired.get(*i).is_ok_and(|b| birth_eq(&b.identity, &id)))
                        .ok_or(Invalid::Companions)?;
                    if !seen.as_slice()[ix] {
                        seen.as_mut_slice()[ix] = true;
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }
    }
    Ok(())
}

pub(crate) fn certificate_fields_finite(
    e: &mut NativeEncoder<'_>,
    format: u32,
    tenant: &str,
    hub_node: &str,
    state: &StateRecord<'_>,
    birth: &Birth<'_>,
    retirement_digest: &Digest,
) -> Decoded<()> {
    e.string("delivery-committed-state.v1");
    e.u32(format);
    e.string(tenant);
    e.node(hub_node)?;
    e.raw(state.state.logical_store_id.as_bytes());
    e.raw(&state.state.incarnation.0.to_be_bytes());
    e.raw(state.state.birth_id.as_bytes());
    state.bytes_finite(e);
    e.raw(&birth.digest_finite()?);
    e.raw(retirement_digest);
    Ok(())
}
impl Certificate<'_> {
    pub(crate) fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        certificate_fields_finite(
            e,
            self.format,
            self.tenant,
            self.hub_node,
            &self.state,
            &self.birth,
            &self.retirement_digest,
        )
    }
    pub(crate) fn id_finite(&self) -> Decoded<Digest> {
        finite_hash(|e| {
            e.string("delivery-state-certificate-id.v1");
            e.u64(finite_len(|e| self.bytes_finite(e))? as u64);
            self.bytes_finite(e)?;
            e.bytes(&self.signature);
            Ok(())
        })
    }
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        if self.format != 1
            || !state_birth_eq(&self.state.state, &self.birth.identity)
            || self.state.retirement_root != self.retirement_digest
            || retirement_root(self.retired_births)? != self.retirement_digest
        {
            return Err(Invalid::Companions.into());
        }
        retirement_closure(owner, &self.birth, self.retired_births)?;
        verify_signature(owner, self.hub_node, self.signature, |e| {
            self.bytes_finite(e)
        })
    }
}
impl EdgeAuthority<'_> {
    fn encode_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.u64(self.destination_epoch);
        e.string(self.namespace.tenant);
        e.node(self.namespace.hub_node)?;
        self.birth.encode_finite(e);
        e.node(self.namespace.edge_node)?;
        e.raw(&self.namespace.edge_incarnation.0.to_be_bytes());
        e.u64(self.validity_revision);
        Ok(())
    }
}
impl Checkpoint<'_> {
    fn bytes_finite(&self, e: &mut NativeEncoder<'_>) -> Decoded<()> {
        e.string("delivery-prefix-checkpoint.v1");
        self.namespace.encode_finite(e)?;
        e.u64(self.count);
        e.raw(&self.digest);
        Ok(())
    }
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        if self.count == 0 {
            return Err(Invalid::Canonical.into());
        }
        verify_signature(owner, self.namespace.hub_node, self.signature, |e| {
            self.bytes_finite(e)
        })
    }
}
impl Frontier<'_> {
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        self.checkpoint.verify_native(owner)?;
        if self.peaks.len() != 64 {
            return Err(Invalid::Canonical.into());
        }
        let digest = finite_hash(|e| {
            e.string("delivery-prefix-root.v1");
            e.u64(self.checkpoint.count);
            for height in 0..64 {
                let peak = self.peaks.get(height)?;
                if peak.is_some() != (self.checkpoint.count & (1u64 << height) != 0) {
                    return Err(Invalid::Canonical);
                }
                if let Some(hash) = peak {
                    e.u8(height as u8);
                    e.raw(&hash)
                }
            }
            Ok(())
        })?;
        if digest != self.checkpoint.digest {
            return Err(Invalid::Canonical.into());
        }
        Ok(())
    }
}
impl Node<'_> {
    fn verify_native(&self) -> Decoded<()> {
        if self.height >= 64 || self.start & ((1u64 << self.height) - 1) != 0 {
            return Err(Invalid::Canonical);
        }
        let hash = finite_hash(|e| {
            e.string("delivery-prefix-node.v1");
            self.namespace.encode_finite(e)?;
            e.u8(self.height);
            e.u64(self.start);
            e.raw(&self.digest);
            Ok(())
        })?;
        if hash != self.checksum {
            return Err(Invalid::Canonical);
        }
        Ok(())
    }
}
impl SignedBinding<'_> {
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        if self.format != 1 || self.namespace.hub_incarnation != self.issuance_state.incarnation {
            return Err(Invalid::Canonical.into());
        }
        let mut prev = None;
        for p in self.tables.iter()? {
            let p = p?;
            if prev.is_some_and(|s: &str| s >= p.table)
                || p.version == 0
                || policy_digest(p.table, &p.policy)? != p.digest
            {
                return Err(Invalid::Canonical.into());
            }
            prev = Some(p.table)
        }
        verify_signature(owner, self.namespace.hub_node, self.signature, |e| {
            self.bytes_finite(e)
        })
    }
}
fn policy_digest(table: &str, p: &ApplicationTablePolicy<'_>) -> Decoded<Digest> {
    finite_hash(|e| {
        e.string("tenant-table-policy.v1");
        e.string(table);
        e.policy(p)
    })
}
impl TerminalRecord<'_> {
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        if self.kind > 2 || self.namespace.hub_incarnation != self.state.incarnation {
            return Err(Invalid::Canonical.into());
        }
        verify_signature(owner, self.namespace.hub_node, self.signature, |e| {
            self.bytes_finite(e)
        })
    }
}
struct EncoderWriter<'a, 'b>(&'a mut NativeEncoder<'b>);
impl Write for EncoderWriter<'_, '_> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.raw(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn mp<T: Serialize + ?Sized>(e: &mut NativeEncoder<'_>, v: &T) -> Decoded<()> {
    v.serialize(&mut rmp_serde::Serializer::new(EncoderWriter(e)))
        .map_err(|_| Invalid::Canonical)
}
struct KeyPairs<'a>(NaturalKey<'a>);
impl Serialize for KeyPairs<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(1 + self.0.rest.len()))?;
        seq.serialize_element(&(self.0.column, self.0.value))?;
        for p in self
            .0
            .rest
            .iter()
            .map_err(|_| serde::ser::Error::custom("invalid key pairs"))?
        {
            seq.serialize_element(&p.map_err(|_| serde::ser::Error::custom("invalid key pair"))?)?;
        }
        seq.end()
    }
}
fn lineage_verify(owner: &ReadMemoryOwner, tenant: &str, row: &RowEvidence<'_>) -> Selected<()> {
    // Match the actual compact MessagePack attestation tuple and its lowercase
    // incarnation text without a temporary String.
    let incarnation = row.creator.author_database_incarnation.0.to_be_bytes();
    let mut text = [0u8; 32];
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (i, b) in incarnation.iter().enumerate() {
        text[2 * i] = HEX[(b >> 4) as usize];
        text[2 * i + 1] = HEX[(b & 15) as usize];
    }
    let incarnation = std::str::from_utf8(&text).map_err(|_| Invalid::Canonical)?;
    verify_signature(
        owner,
        row.creator.author_node_id,
        row.creator.attestation,
        |e| {
            mp(
                e,
                &(
                    "contextdb.row-lineage-attestation.v1",
                    tenant,
                    row.reference.table,
                    KeyPairs(row.reference.key),
                    row.creator.table_generation,
                    row.creator.author_node_id,
                    incarnation,
                    row.creator.author_local_mutation_position.0,
                    row.creator.lineage_root,
                ),
            )
        },
    )
}
// Validate the real positional DDL codec before borrowing its untouched bytes
// into the exact provenance tuple. The enum does not use a named struct map.
fn ddl_codec(n: NodeView<'_>) -> Decoded<()> {
    let (tag, body) = n.variant()?;
    let a = body.array()?;
    let count = a.len();
    let get = |i| body.array()?.nth(i).ok_or(Invalid::Codec)?;
    let text = |i| -> Decoded<()> {
        get(i)?.string()?;
        Ok(())
    };
    let texts = |i| -> Decoded<()> {
        Seq::<&str>::decode(get(i)?)?;
        Ok(())
    };
    let pairs = |i| -> Decoded<()> {
        Seq::<(&str, &str)>::decode(get(i)?)?;
        Ok(())
    };
    match (tag, count) {
        ("CreateTable" | "AlterTable", 6) => {
            text(0)?;
            pairs(1)?;
            texts(2)?;
            // Single/composite foreign keys are tuple-encoded real core types.
            for f in get(3)?.array()? {
                let f = f?;
                let mut a = f.array()?;
                if a.len() != 3 {
                    return Err(Invalid::Codec);
                }
                for _ in 0..3 {
                    a.next().ok_or(Invalid::Codec)??.string()?;
                }
            }
            for f in get(4)?.array()? {
                let f = f?;
                let mut a = f.array()?;
                if a.len() != 3 {
                    return Err(Invalid::Codec);
                }
                Seq::<&str>::decode(a.next().ok_or(Invalid::Codec)??)?;
                a.next().ok_or(Invalid::Codec)??.string()?;
                Seq::<&str>::decode(a.next().ok_or(Invalid::Codec)??)?;
            }
            Seq::<Seq<'_, &str>>::decode(get(5)?)?;
        }
        ("DropTable" | "DropTrigger", 1) => text(0)?,
        ("CreateIndex", 3) => {
            text(0)?;
            text(1)?;
            pairs(2)?
        }
        ("DropIndex" | "DropRoute", 2) => {
            text(0)?;
            text(1)?
        }
        ("CreateTrigger" | "CreateTriggerIncludingSync", 3) => {
            text(0)?;
            text(1)?;
            texts(2)?
        }
        ("CreateEventType", 3) => {
            text(0)?;
            text(1)?;
            text(2)?
        }
        ("CreateSink", 3) => {
            text(0)?;
            text(1)?;
            Option::<&str>::decode(get(2)?)?;
        }
        ("CreateRoute", 5) => {
            for i in 0..4 {
                text(i)?
            }
            Option::<(&str, Seq<'_, &str>)>::decode(get(4)?)?;
        }
        _ => return Err(Invalid::Codec),
    }
    Ok(())
}
impl SchemaDependency<'_> {
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        let mut ddl = NativeBuffer::allocate(owner, self.ddl_bytes.len())?;
        self.ddl_bytes.copy_into(ddl.bytes_mut())?;
        let n = NodeView::parse(ddl.bytes())?;
        ddl_codec(n)?;
        let mut compare = Compare {
            expected: ddl.bytes(),
            offset: 0,
        };
        n.serialize(&mut rmp_serde::Serializer::new(&mut compare))
            .map_err(|_| Invalid::Canonical)?;
        if compare.offset != ddl.bytes().len() {
            return Err(Invalid::Canonical.into());
        }
        let digest = finite_hash(|e| {
            e.raw(&[0x96]);
            mp(e, "contextdb.sync-ddl-provenance.v6")?;
            e.raw(ddl.bytes());
            mp(e, &self.source_lsn.0)?;
            mp(e, &self.ordinal)?;
            mp(e, &Some(self.table))?;
            mp(e, &Some(self.table_generation))
        })?;
        let mut stored = [0; 32];
        self.ddl_digest.copy_into(&mut stored)?;
        if digest != stored {
            return Err(Invalid::Canonical.into());
        }
        verify_signature(owner, self.author_node, self.signature, |e| {
            self.bytes_finite(e)
        })
    }
}
fn policy_eq(a: &ApplicationTablePolicy<'_>, b: &ApplicationTablePolicy<'_>) -> Decoded<bool> {
    // Canonical policy identity deliberately ignores declared retain units;
    // structural equality additionally includes that persisted user spelling.
    if finite_hash(|e| e.policy(a))? != finite_hash(|e| e.policy(b))? {
        return Ok(false);
    }
    Ok(match (a.retain, b.retain) {
        (Some(a), Some(b)) => a.declared_unit == b.declared_unit,
        (None, None) => true,
        _ => false,
    })
}
fn value_eq(a: &Value<'_>, b: &Value<'_>) -> Decoded<bool> {
    Ok(match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::Int64(a), Value::Int64(b)) => a == b,
        (Value::Float64(a), Value::Float64(b)) => a == b,
        (Value::Text(a), Value::Text(b)) => a == b,
        (Value::Uuid(a), Value::Uuid(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::TxId(a), Value::TxId(b)) => a == b,
        (Value::Json(a), Value::Json(b)) => super::native_json::semantic_eq(a, b)?,
        (Value::Vector(a), Value::Vector(b)) => {
            if a.len() != b.len() {
                false
            } else {
                let mut same = true;
                for (x, y) in a.iter()?.zip(b.iter()?) {
                    if x? != y? {
                        same = false;
                        break;
                    }
                }
                same
            }
        }
        _ => false,
    })
}
fn key_eq(a: &NaturalKey<'_>, b: &NaturalKey<'_>) -> Decoded<bool> {
    if a.column != b.column || !value_eq(&a.value, &b.value)? || a.rest.len() != b.rest.len() {
        return Ok(false);
    }
    for (x, y) in a.rest.iter()?.zip(b.rest.iter()?) {
        let (x, y) = (x?, y?);
        if x.0 != y.0 || !value_eq(&x.1, &y.1)? {
            return Ok(false);
        }
    }
    Ok(true)
}
fn reference_eq(a: &RowRef<'_>, b: &RowRef<'_>) -> Decoded<bool> {
    Ok(a.table == b.table && key_eq(&a.key, &b.key)?)
}

impl ManifestRecord<'_> {
    fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        if let Some(b) = self.binding {
            b.verify_native(owner)?;
            if b.namespace.edge_node != self.edge_node
                || b.namespace.edge_incarnation != self.edge_incarnation
                || b.namespace.tenant != self.tenant
            {
                return Err(Invalid::Canonical.into());
            }
        }
        let mut previous = None;
        for p in self.policies.iter()? {
            let p = p?;
            if previous.is_some_and(|v: &str| v >= p.table) {
                return Err(Invalid::Canonical.into());
            }
            previous = Some(p.table);
            let required = match p.authority {
                0 if p.bound.is_none() => None,
                1 if self.binding.is_none() => p.bound,
                2 if self.binding.is_some() => p.bound,
                _ => return Err(Invalid::Canonical.into()),
            };
            if p.authority != 0 && required.is_none() {
                return Err(Invalid::Canonical.into());
            }
            if let Some(r) = required {
                if r.table != p.table
                    || r.version == 0
                    || policy_digest(r.table, &r.policy)? != r.digest
                    || p.agreement != policy_eq(&p.installed, &r.policy)?
                {
                    return Err(Invalid::Canonical.into());
                }
                if let Some(b) = self.binding {
                    let mut found = false;
                    for x in b.tables.iter()? {
                        let x = x?;
                        if x.table == r.table
                            && x.version == r.version
                            && x.digest == r.digest
                            && policy_eq(&x.policy, &r.policy)?
                        {
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        return Err(Invalid::Canonical.into());
                    }
                }
            }
        }
        let mut root_policy = false;
        for p in self.policies.iter()? {
            let p = p?;
            if p.table == self.seal.root.table
                && (p.authority != 0
                    || p.installed.sync_direction == contextdb_core::SyncDirection::None)
                && p.installed.manifest_tables.is_some()
            {
                root_policy = true
            }
            if !self
                .rows
                .iter()?
                .any(|r| r.is_ok_and(|r| r.reference.table == p.table))
            {
                return Err(Invalid::Canonical.into());
            }
        }
        if !root_policy {
            return Err(Invalid::Canonical.into());
        }
        for r in self.rows.iter()? {
            let r = r?;
            if !self
                .policies
                .iter()?
                .any(|p| p.is_ok_and(|p| p.table == r.reference.table))
            {
                return Err(Invalid::Canonical.into());
            }
        }
        let mut previous = None;
        for s in self.schemas.iter()? {
            let s = s?;
            s.verify_native(owner)?;
            let pos = (s.source_lsn, s.ordinal);
            if previous.is_some_and(|p| p >= pos)
                || s.tenant != self.tenant
                || s.author_node != self.edge_node
                || s.author_incarnation != self.edge_incarnation
            {
                return Err(Invalid::Canonical.into());
            }
            previous = Some(pos)
        }
        for r in self.rows.iter()? {
            let r = r?;
            if !self.schemas.iter()?.any(|s| {
                s.is_ok_and(|s| {
                    s.table == r.reference.table && s.table_generation == r.creator.table_generation
                })
            }) {
                return Err(Invalid::Canonical.into());
            }
        }
        if self.rows.len() == 0
            || self.seal.kind != 0
            || self.seal.last_complete.is_some()
            || self.seal.registered_count != self.member_lives.len() as u64
            || self.rows.len()
                != self
                    .member_lives
                    .len()
                    .checked_add(1)
                    .ok_or(Invalid::Canonical)?
        {
            return Err(Invalid::Canonical.into());
        }
        let root = self.rows.get(0)?;
        if !reference_eq(&root.reference, &self.seal.root)?
            || root.row_digest != self.seal.root_digest
            || root.origin_digest_finite(self.tenant)? != self.seal.origin_life_digest
        {
            return Err(Invalid::Canonical.into());
        }
        let mut rows = NativeSelectionHold::try_new(owner, self.rows.len())?;
        for (i, r) in self.rows.iter()?.enumerate() {
            let r = r?;
            let key = finite_bytes(owner, |e| r.reference.key.encode(e))?;
            rows.retain((r, key, i))?;
        }
        rows.as_mut_slice().sort_unstable_by(|a, b| {
            a.0.reference
                .table
                .cmp(b.0.reference.table)
                .then_with(|| a.1.bytes().cmp(b.1.bytes()))
                .then_with(|| a.2.cmp(&b.2))
        });
        let projection = finite_hash(|e| {
            e.string("delivery-materialization-projection.v1");
            e.u8(0);
            e.u64(self.seal.source.0.0);
            e.u32(self.seal.source.1);
            e.u64(self.schemas.len() as u64);
            for s in self.schemas.iter()? {
                let s = s?;
                e.u64(finite_len(|e| s.bytes_finite(e))? as u64);
                s.bytes_finite(e)?;
                e.bytes(&s.signature)
            }
            e.u64(rows.as_slice().len() as u64);
            for (r, _, _) in rows.as_slice() {
                r.encode_projection_finite(self.tenant, e)?;
            }
            Ok(())
        })?;
        let authority = finite_hash(|e| {
            e.string("delivery-authority-evidence.v1");
            e.u64(self.policies.len() as u64);
            for p in self.policies.iter()? {
                p?.encode_finite(e)?
            }
            e.u8(0);
            e.u64(rows.as_slice().len() as u64);
            for (r, _, _) in rows.as_slice() {
                r.encode_life_finite(e)?
            }
            e.u64(0);
            Ok(())
        })?;
        if projection != self.seal.projection_digest || authority != self.seal.authority_digest {
            return Err(Invalid::Canonical.into());
        }
        for r in self.rows.iter()? {
            lineage_verify(owner, self.tenant, &r?)?
        }
        verify_signature(owner, self.edge_node, self.signature, |e| {
            self.signed_bytes_finite(e)
        })
    }
}
impl RecordView<'_> {
    pub(crate) fn verify_native(&self, owner: &ReadMemoryOwner) -> Selected<()> {
        match self {
            Self::CheckpointFrontier(f) => f.verify_native(owner),
            Self::CheckpointNode(n) => Ok(n.verify_native()?),
            Self::EdgeCheckpoint(c) => c.verify_native(owner),
            Self::Policy(p) => {
                if p.version == 0 || policy_digest(p.table, &p.policy)? != p.digest {
                    return Err(Invalid::Canonical.into());
                }
                Ok(())
            }
            Self::Binding(b) => b.verify_native(owner),
            Self::Manifest(m) => m.verify_native(owner),
            Self::Terminal { record, .. } => record.verify_native(owner),
            Self::Certificate(c) => c.verify_native(owner),
            _ => Ok(()),
        }
    }
}
use super::preparation::BorrowedHex;
impl RecordView<'_> {
    pub(crate) fn key_native(&self, owner: &ReadMemoryOwner) -> Selected<NativeBuffer> {
        let namespace = |n: &Namespace<'_>| finite_bytes(owner, |e| n.encode_finite(e));
        let life = |l: &LocalLifeAnchor<'_>| {
            finite_bytes(owner, |e| {
                l.encode_finite(e);
                Ok(())
            })
        };
        match self {
            Self::CheckpointFrontier(f) => {
                let n = namespace(&f.checkpoint.namespace)?;
                NativeBuffer::format(
                    owner,
                    format_args!("delivery_prefix_frontier.v1.{}", BorrowedHex(n.bytes())),
                )
            }
            Self::CheckpointNode(n) => {
                let ns = namespace(&n.namespace)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_prefix_node.v1.{}.{:02x}.{:016x}",
                        BorrowedHex(ns.bytes()),
                        n.height,
                        n.start
                    ),
                )
            }
            Self::EdgeCheckpoint(c) => {
                let n = namespace(&c.namespace)?;
                NativeBuffer::format(
                    owner,
                    format_args!("delivery_edge_checkpoint.v1.{}", BorrowedHex(n.bytes())),
                )
            }
            Self::MaterializedOwner(o) => {
                let l = life(&o.incumbent)?;
                let n = namespace(&o.namespace)?;
                let r = finite_bytes(owner, |e| {
                    o.source.reference.encode_finite(e);
                    Ok(())
                })?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_materialization_owner.v1.{}.{}.{}.{}",
                        BorrowedHex(l.bytes()),
                        BorrowedHex(n.bytes()),
                        o.submission.simple(),
                        BorrowedHex(r.bytes())
                    ),
                )
            }
            Self::Policy(p) => NativeBuffer::format(
                owner,
                format_args!("tenant_table_policy.v1.{}", BorrowedHex(p.table.as_bytes())),
            ),
            Self::Binding(b) => {
                let n = namespace(&b.namespace)?;
                NativeBuffer::format(
                    owner,
                    format_args!("tenant_policy_binding.v1.{}", BorrowedHex(n.bytes())),
                )
            }
            Self::State(s) => NativeBuffer::format(
                owner,
                format_args!("delivery_state_history.v1.{}", BorrowedHex(&s.state.token)),
            ),
            Self::Root { life: l, .. } => {
                let l = life(l)?;
                NativeBuffer::format(
                    owner,
                    format_args!("delivery_root.v1.{}", BorrowedHex(l.bytes())),
                )
            }
            Self::Manifest(m) => NativeBuffer::format(
                owner,
                format_args!("delivery_manifest.v1.{}", m.id.simple()),
            ),
            Self::MemberOwner {
                member,
                root,
                submission,
            } => {
                let m = life(member)?;
                let r = life(root)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_member_owner.v1.{}.{}.{}",
                        BorrowedHex(m.bytes()),
                        BorrowedHex(r.bytes()),
                        submission.simple()
                    ),
                )
            }
            Self::Terminal { edge, record: r } => {
                let n = namespace(&r.namespace)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_{}_outcome.v1.{}.{}",
                        if *edge { "edge" } else { "hub" },
                        BorrowedHex(n.bytes()),
                        r.submission.simple()
                    ),
                )
            }
            Self::Diagnostic {
                namespace: n,
                submission,
                ..
            } => {
                let n = namespace(n)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_diagnostic.v1.{}.{}",
                        BorrowedHex(n.bytes()),
                        submission.simple()
                    ),
                )
            }
            Self::DiagnosticOwner {
                namespace: n,
                submission,
                life: l,
            } => {
                let n = namespace(n)?;
                let l = life(l)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_diagnostic_owner.v1.{}.{}.{}",
                        BorrowedHex(n.bytes()),
                        submission.simple(),
                        BorrowedHex(l.bytes())
                    ),
                )
            }
            Self::SourceHistory {
                root, submission, ..
            } => {
                let r = life(root)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_source_history.v1.{}.{}",
                        BorrowedHex(r.bytes()),
                        submission.simple()
                    ),
                )
            }
            Self::PolicyProfile {
                life: l,
                submission,
                ..
            } => {
                let l = life(l)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_policy_profile.v1.{}.{}",
                        BorrowedHex(l.bytes()),
                        submission.simple()
                    ),
                )
            }
            Self::Order {
                namespace: n,
                position,
                ..
            } => {
                let n = namespace(n)?;
                NativeBuffer::format(
                    owner,
                    format_args!(
                        "delivery_hub_order.v1.{}.{:016x}.{:08x}",
                        BorrowedHex(n.bytes()),
                        position.0.0,
                        position.1
                    ),
                )
            }
            Self::ObjectAdmission(a) => {
                let b = finite_bytes(owner, |e| {
                    e.raw(&a.object_digest);
                    a.authority.encode_finite(e)
                })?;
                NativeBuffer::format(
                    owner,
                    format_args!("delivery_object_admission.v1.{}", BorrowedHex(b.bytes())),
                )
            }
            Self::Control(_) => NativeBuffer::format(owner, format_args!("delivery_hub_state.v1")),
            Self::Birth(b) => NativeBuffer::format(
                owner,
                format_args!(
                    "delivery_incarnation_birth.v1.{}",
                    b.identity.birth_id.simple()
                ),
            ),
            Self::Certificate(c) => NativeBuffer::format(
                owner,
                format_args!("delivery_peer_evidence.v1.{}", BorrowedHex(&c.id_finite()?)),
            ),
            Self::Destination(_) => {
                NativeBuffer::format(owner, format_args!("delivery_destination.v1"))
            }
        }
    }
    pub(crate) fn submission(&self) -> Option<uuid::Uuid> {
        match self {
            Self::Manifest(m) => Some(m.id),
            Self::Terminal { record: t, .. } => Some(t.submission),
            Self::MaterializedOwner(o) => Some(o.submission),
            Self::Root { submission, .. }
            | Self::MemberOwner { submission, .. }
            | Self::Diagnostic { submission, .. }
            | Self::DiagnosticOwner { submission, .. }
            | Self::SourceHistory { submission, .. }
            | Self::PolicyProfile { submission, .. }
            | Self::Order { submission, .. } => Some(*submission),
            _ => None,
        }
    }
    pub(crate) fn group_matches(&self, _owner: &ReadMemoryOwner, group: &str) -> Selected<bool> {
        let mut found = false;
        visit_groups(self, &mut |walk| {
            let expected = group.as_bytes();
            let mut position = 0usize;
            let mut equal = true;
            let mut emit = |bytes: &[u8]| match position.checked_add(bytes.len()) {
                Some(end) => {
                    equal &= expected.get(position..end) == Some(bytes);
                    position = end;
                }
                None => equal = false,
            };
            let valid = {
                let mut e = NativeEncoder {
                    sink: &mut emit,
                    failed: false,
                };
                walk(&mut e)?;
                !e.failed
            };
            found |= equal && position == expected.len() && valid;
            Ok(())
        })?;
        Ok(found)
    }
}
fn object_digest(
    tag: u8,
    walk: impl Fn(&mut NativeEncoder<'_>) -> Decoded<()>,
    signature: Bytes<'_>,
) -> Decoded<Digest> {
    finite_hash(|e| {
        e.string("delivery-authority-object.v1");
        e.u8(tag);
        e.u64(finite_len(|e| walk(e))? as u64);
        walk(e)?;
        e.bytes(&signature);
        Ok(())
    })
}
fn life_eq(a: &LocalLifeAnchor<'_>, b: &LocalLifeAnchor<'_>) -> bool {
    a.original_table == b.original_table
        && a.table_generation == b.table_generation
        && a.local_row_id == b.local_row_id
        && a.creation_lsn == b.creation_lsn
}
fn bytes_eq(a: Bytes<'_>, b: Bytes<'_>) -> Decoded<bool> {
    if a.len() != b.len() {
        return Ok(false);
    }
    for (x, y) in a.iter()?.zip(b.iter()?) {
        if x? != y? {
            return Ok(false);
        }
    }
    Ok(true)
}
fn evidence_eq(a: &RowEvidence<'_>, b: &RowEvidence<'_>) -> Decoded<bool> {
    Ok(reference_eq(&a.reference, &b.reference)?
        && a.authored_source == b.authored_source
        && a.row_digest == b.row_digest
        && a.creator.author_node_id == b.creator.author_node_id
        && a.creator.author_database_incarnation == b.creator.author_database_incarnation
        && a.creator.author_local_mutation_position == b.creator.author_local_mutation_position
        && a.creator.table_generation == b.creator.table_generation
        && a.creator.lineage_root == b.creator.lineage_root
        && bytes_eq(a.creator.attestation, b.creator.attestation)?)
}
/// Companions are source-selected loans with their own prepaid raw windows.
/// No Legacy resident admission fact or owned Record is used as evidence.
#[cfg(feature = "sync-orchestration")]
pub(crate) fn verify_companions(
    record: &RecordView<'_>,
    views: &[super::store::NativeCustodyView<'_>],
    owner: &ReadMemoryOwner,
) -> Selected<()> {
    match record {
        RecordView::Terminal { edge, record: t } => {
            let mut block = None;
            let mut order = false;
            let mut admission = false;
            let object = object_digest(1, |e| t.bytes_finite(e), t.signature)?;
            for view in views {
                match view.record()? {
                    RecordView::Diagnostic {
                        namespace,
                        submission,
                        block: b,
                    } if namespace_eq(&namespace, &t.namespace) && submission == t.submission => {
                        block = Some(b)
                    }
                    RecordView::Order {
                        namespace,
                        position,
                        submission,
                    } if namespace_eq(&namespace, &t.namespace)
                        && position == t.position
                        && submission == t.submission =>
                    {
                        order = true
                    }
                    RecordView::ObjectAdmission(a) if a.object_digest == object => admission = true,
                    _ => {}
                }
            }
            let b = block.ok_or(Invalid::Companions)?;
            if t.diagnostics.subject_count != b.subjects.len() as u64
                || t.diagnostics.conflict_count != b.conflicts.len() as u64
                || t.diagnostics.canonical_bytes != finite_len(|e| b.bytes_finite(e))? as u64
                || t.diagnostics.commitment
                    != finite_hash(|e| {
                        e.string("delivery-diagnostics.v1");
                        b.bytes_finite(e)
                    })?
                || (*edge && !admission)
                || (!*edge && !order)
            {
                return Err(Invalid::Companions.into());
            }
        }
        RecordView::Root { life, submission } => {
            let mut manifest = None;
            for view in views {
                if let RecordView::Manifest(m) = view.record()? {
                    if m.id == *submission && life_eq(&m.root_life, life) {
                        manifest = Some(m);
                        break;
                    }
                }
            }
            let m = manifest.ok_or(Invalid::Companions)?;
            let seal = m.seal.digest_finite()?;
            let mut history = false;
            for view in views {
                if let RecordView::SourceHistory {
                    root,
                    submission,
                    seal_digest,
                    ..
                } = view.record()?
                {
                    if life_eq(&root, life) && submission == m.id && seal_digest == seal {
                        history = true
                    }
                }
            }
            if !history {
                return Err(Invalid::Companions.into());
            }
            for member in m.member_lives.iter()? {
                let member = member?;
                let mut found = false;
                for view in views {
                    if let RecordView::MemberOwner {
                        member: o,
                        root,
                        submission,
                    } = view.record()?
                    {
                        if life_eq(&member, &o) && life_eq(&root, life) && submission == m.id {
                            found = true;
                            break;
                        }
                    }
                }
                if !found {
                    return Err(Invalid::Companions.into());
                }
            }
        }
        RecordView::MaterializedOwner(o) => {
            let mut manifest = false;
            let mut terminal = false;
            for view in views {
                match view.record()? {
                    RecordView::Manifest(m)
                        if m.id == o.submission
                            && m.tenant == o.namespace.tenant
                            && m.edge_node == o.namespace.edge_node
                            && m.edge_incarnation == o.namespace.edge_incarnation =>
                    {
                        for row in m.rows.iter()? {
                            if evidence_eq(&row?, &o.source)? {
                                manifest = true;
                                break;
                            }
                        }
                    }
                    RecordView::Terminal {
                        edge: false,
                        record: t,
                    } if t.submission == o.submission
                        && namespace_eq(&t.namespace, &o.namespace)
                        && t.kind != 2 =>
                    {
                        terminal = true
                    }
                    _ => {}
                }
            }
            if !manifest || !terminal {
                return Err(Invalid::Companions.into());
            }
        }
        RecordView::ObjectAdmission(a) => {
            let mut certificate = None;
            let mut object = false;
            for view in views {
                match view.record()? {
                    RecordView::Certificate(c) if c.id_finite()? == a.certificate => {
                        certificate = Some(c)
                    }
                    RecordView::Binding(b)
                        if object_digest(0, |e| b.bytes_finite(e), b.signature)?
                            == a.object_digest =>
                    {
                        object = true
                    }
                    RecordView::Terminal { record: t, .. }
                        if object_digest(1, |e| t.bytes_finite(e), t.signature)?
                            == a.object_digest =>
                    {
                        object = true
                    }
                    _ => {}
                }
            }
            let c = certificate.ok_or(Invalid::Companions)?;
            if !object
                || !state_eq(&c.state.state, &a.owning_state)
                || c.tenant != a.authority.namespace.tenant
                || c.hub_node != a.authority.namespace.hub_node
                || !state_birth_eq(&a.owning_state, &a.authority.birth)
            {
                return Err(Invalid::Companions.into());
            }
        }
        RecordView::Control(c) => {
            if c.format != 1 {
                return Err(Invalid::Companions.into());
            }
            let mut count = 0usize;
            let mut active = None;
            for view in views {
                match view.record()? {
                    RecordView::State(s) if state_birth_eq(&s.state, &c.identity) => {
                        count = count.checked_add(1).ok_or(Invalid::Codec)?
                    }
                    RecordView::Birth(b) if birth_eq(&b.identity, &c.identity) => active = Some(b),
                    _ => {}
                }
            }
            let birth = active.ok_or(Invalid::Companions)?;
            let mut states = NativeSelectionHold::try_new(owner, count)?;
            for view in views {
                if let RecordView::State(s) = view.record()? {
                    if state_birth_eq(&s.state, &c.identity) {
                        states.retain(s)?
                    }
                }
            }
            states
                .as_mut_slice()
                .sort_unstable_by_key(|s| s.state.token);
            let history = finite_hash(|e| {
                e.string("delivery-history-index.v1");
                e.u64(states.as_slice().len() as u64);
                for s in states.as_slice() {
                    e.raw(&s.state.token);
                    e.raw(&finite_hash(|e| {
                        e.string("delivery-state-record.v1");
                        s.bytes_finite(e);
                        Ok(())
                    })?);
                }
                Ok(())
            })?;
            if history != c.active_history_root {
                return Err(Invalid::Companions.into());
            }
            let mut retired = NativeSelectionHold::try_new(owner, birth.retired.len())?;
            for id in birth.retired.iter()? {
                let id = id?;
                let mut found = None;
                for view in views {
                    if let RecordView::Birth(b) = view.record()? {
                        if birth_eq(&b.identity, &id) {
                            found = Some(b);
                            break;
                        }
                    }
                }
                retired.retain(found.ok_or(Invalid::Companions)?)?;
            }
            retirement_closure(owner, &birth, retired.as_slice())?;
            if retirement_root(retired.as_slice())? != c.retired_history_root
                || states
                    .as_slice()
                    .iter()
                    .any(|s| s.retirement_root != c.retired_history_root)
            {
                return Err(Invalid::Companions.into());
            }
            for b in std::iter::once(&birth).chain(retired.as_slice()) {
                for trigger in b.triggers.iter()? {
                    let trigger = trigger?;
                    let mut found = false;
                    for view in views {
                        if let RecordView::Certificate(cert) = view.record()? {
                            if cert.id_finite()? == trigger {
                                found = true;
                                break;
                            }
                        }
                    }
                    if !found {
                        return Err(Invalid::Companions.into());
                    }
                }
            }
            let mut seen = NativeSelectionHold::try_new(owner, count)?;
            for _ in 0..count {
                seen.retain(false)?
            }
            let mut token = Some(c.head);
            let mut child_sequence = None;
            let mut used = 0usize;
            while let Some(t) = token {
                let ix = states
                    .as_slice()
                    .iter()
                    .position(|s| s.state.token == t)
                    .ok_or(Invalid::Companions)?;
                if seen.as_slice()[ix] {
                    return Err(Invalid::Companions.into());
                }
                seen.as_mut_slice()[ix] = true;
                used += 1;
                let s = states.as_slice()[ix];
                if child_sequence.is_some_and(|seq| s.sequence.checked_add(1) != Some(seq)) {
                    return Err(Invalid::Companions.into());
                }
                child_sequence = Some(s.sequence);
                token = s.parent;
            }
            if used != count || child_sequence != Some(0) {
                return Err(Invalid::Companions.into());
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn terminal_object(t: &TerminalRecord<'_>) -> Decoded<Digest> {
    object_digest(1, |e| t.bytes_finite(e), t.signature)
}

/// Borrowed group projection shared by maintenance and membership checks.
/// Each callback walks one UTF-8 group without allocating a decoded Record.
pub(crate) fn visit_groups(
    record: &RecordView<'_>,
    visit: &mut dyn FnMut(&dyn Fn(&mut NativeEncoder<'_>) -> Decoded<()>) -> Decoded<()>,
) -> Decoded<()> {
    fn reference(e: &mut NativeEncoder<'_>, tag: &[u8], r: &RowRef<'_>) -> Decoded<()> {
        e.raw(tag);
        let mut emit = |bytes: &[u8]| {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            for byte in bytes {
                e.raw(&[HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]);
            }
        };
        let mut hex = NativeEncoder {
            sink: &mut emit,
            failed: false,
        };
        r.encode_finite(&mut hex);
        if hex.failed {
            return Err(Invalid::Canonical);
        }
        Ok(())
    }
    fn digest(e: &mut NativeEncoder<'_>, tag: &[u8], bytes: &Digest) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        e.raw(tag);
        for byte in bytes {
            e.raw(&[HEX[(byte >> 4) as usize], HEX[(byte & 15) as usize]]);
        }
    }
    match record {
        RecordView::Manifest(m) => {
            visit(&|e| reference(e, b"root:", &m.seal.root))?;
            for row in m.rows.iter()? {
                let row = row?;
                visit(&|e| reference(e, b"row:", &row.reference))?;
            }
        }
        RecordView::Terminal { record: t, .. } => {
            visit(&|e| reference(e, b"root:", &t.root))?;
            let object = terminal_object(t)?;
            visit(&|e| {
                digest(e, b"object:", &object);
                Ok(())
            })?;
        }
        RecordView::MaterializedOwner(o) => {
            visit(&|e| reference(e, b"root:", &o.source.reference))?
        }
        RecordView::Binding(b) => {
            visit(&|e| {
                e.raw(b"authority");
                Ok(())
            })?;
            let object = object_digest(0, |e| b.bytes_finite(e), b.signature)?;
            visit(&|e| {
                digest(e, b"object:", &object);
                Ok(())
            })?;
        }
        RecordView::ObjectAdmission(a) => {
            visit(&|e| {
                digest(e, b"object:", &a.object_digest);
                Ok(())
            })?;
            visit(&|e| {
                digest(e, b"certificate:", &a.certificate);
                Ok(())
            })?;
        }
        RecordView::Policy(_) => visit(&|e| {
            e.raw(b"policies");
            Ok(())
        })?,
        RecordView::Control(_)
        | RecordView::State(_)
        | RecordView::Birth(_)
        | RecordView::Certificate(_)
        | RecordView::Destination(_) => visit(&|e| {
            e.raw(b"authority");
            Ok(())
        })?,
        _ => {}
    }
    if let Some(id) = record.submission() {
        let mut text = [0; 36];
        let text = id.hyphenated().encode_lower(&mut text);
        visit(&|e| {
            e.raw(b"submission:");
            e.raw(text.as_bytes());
            Ok(())
        })?;
    }
    Ok(())
}
/// Write a counted canonical walk into caller-prepaid storage, with no growth.
pub(crate) fn finite_into(
    bytes: &mut [u8],
    walk: impl FnOnce(&mut NativeEncoder<'_>) -> Decoded<()>,
) -> Decoded<()> {
    super::canonical::finite_emit_with_error(bytes, |e| shared_walk(e, walk), || Invalid::Canonical)
}
