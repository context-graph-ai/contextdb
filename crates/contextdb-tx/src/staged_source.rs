//! Private persistent sources. Credits follow native ownership rather than a
//! transaction's lifetime; no data Arc or Weak is exported from this module.
use contextdb_core::{Error, Result};
use std::alloc::Layout;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Original writer registration, carried unchanged through debit transfer.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct SourceLease {
    pub identity: usize,
    pub epoch: usize,
}

/// The row change passed alongside a sidecar copy (unchanged tuple shape).
type SidecarChange<'a> = (
    &'a str,
    contextdb_core::RowId,
    Option<(contextdb_core::Lsn, u8)>,
    Option<usize>,
);

/// The engine implements this on its existing accountant. No separately
/// allocated trait adapter or reader budget is introduced at this boundary.
#[doc(hidden)]
pub trait SourceFunding: Send + Sync {
    /// Observation of the exact supplied receiver and already-held lease.
    #[doc(hidden)]
    fn matches_original_source_origin(
        &self,
        _origin: NativeSourceOrigin,
        _lease: SourceLease,
    ) -> Option<bool> {
        None
    }
    /// Unknown control inventory is distinct from an absent allocation.
    #[doc(hidden)]
    fn retained_control_bytes(&self) -> Option<usize> {
        None
    }
    #[doc(hidden)]
    fn visit_retained_controls<'a>(
        &'a self,
        _visit: &mut dyn FnMut(RetainedControl<'a>),
    ) -> Option<()> {
        None
    }
    fn admit_source(&self, bytes: usize) -> Result<SourceLease>;
    fn retain_source_lease(&self, lease: SourceLease);
    fn release_source(&self, bytes: usize, lease: SourceLease);
    fn copy_row(self: Arc<Self>, row: &contextdb_core::VersionedRow) -> Result<AdmittedSourceRow>;
    fn copy_eager_row(
        self: Arc<Self>,
        row: &contextdb_core::VersionedRow,
    ) -> Result<AdmittedEagerSourceRow>;
    fn copy_vector(
        self: Arc<Self>,
        entry: &contextdb_core::VectorEntry,
    ) -> Result<AdmittedSourceVector>;
    /// Providers must supply a genuine original reservation, not a source debit.
    #[doc(hidden)]
    fn copy_eager_vector(
        self: Arc<Self>,
        _entry: &contextdb_core::VectorEntry,
    ) -> Result<AdmittedEagerSourceVector> {
        Err(Error::Other(
            "provider has no original eager vector copy".into(),
        ))
    }
    fn copy_key(self: Arc<Self>, key: &contextdb_core::IndexKey) -> Result<AdmittedSourceKey>;
    #[doc(hidden)]
    fn copy_aux(self: Arc<Self>, _bodies: AuxBorrow<'_>) -> Result<AdmittedAuxBatch> {
        Err(Error::Other(
            "provider has no auxiliary native copy capability".into(),
        ))
    }
    #[doc(hidden)]
    fn copy_sidecars(
        self: Arc<Self>,
        _maps: SidecarsBorrow<'_>,
        _change: Option<SidecarChange<'_>>,
    ) -> Result<AdmittedSidecars> {
        Err(Error::Other(
            "provider has no sidecar native copy capability".into(),
        ))
    }
}

/// Fixed observation of the actual native accountant and pinned writer use.
/// This is not a refund capability; the native constructor checks its owner.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct NativeSourceOrigin {
    accountant: usize,
    lease: SourceLease,
}
impl NativeSourceOrigin {
    pub fn same_origin(&self, other: &Self) -> bool {
        self.matches(other.accountant, other.lease)
    }
    pub fn new(accountant: usize, lease: SourceLease) -> Self {
        Self { accountant, lease }
    }
    fn matches(self, accountant: usize, lease: SourceLease) -> bool {
        self.accountant == accountant
            && self.lease.identity == lease.identity
            && self.lease.epoch == lease.epoch
    }
}
#[derive(Clone)]
enum FundingReceiver {
    Host(Arc<dyn SourceFunding>),
    Native {
        receiver: Arc<dyn SourceFunding>,
        finish: fn(Arc<dyn SourceFunding>),
        origin: NativeSourceOrigin,
    },
}
/// Opaque provider custody. Every permanent native count has a typed finalizer.
#[doc(hidden)]
pub struct SourceFunder {
    receiver: Option<FundingReceiver>,
}
impl SourceFunder {
    /// Compare only the retained receiver and its native provenance.  This is
    /// deliberately crate-private: source ownership must not become a public
    /// provider or policy capability.
    pub(crate) fn same_receiver(&self, other: &Self) -> bool {
        if !Arc::ptr_eq(self.receiver(), other.receiver()) {
            return false;
        }
        match (self.receiver.as_ref(), other.receiver.as_ref()) {
            (
                Some(FundingReceiver::Native { origin: left, .. }),
                Some(FundingReceiver::Native { origin: right, .. }),
            ) => left.same_origin(right),
            (Some(FundingReceiver::Host(_)), Some(FundingReceiver::Host(_))) => true,
            _ => false,
        }
    }
    pub fn from_host(receiver: Arc<dyn SourceFunding>) -> Self {
        Self {
            receiver: Some(FundingReceiver::Host(receiver)),
        }
    }
    /// Allocate a preadmitted concrete provider and pair its sole strong count.
    ///
    /// # Safety
    /// The actual global Arc<T> header must be preadmitted; origin must match
    /// its held accountant/key/epoch. No explicit Weak or raw ownership alias
    /// may exist, and every permanent count must use this paired protocol.
    /// Consuming copy methods may hold plain Arc<Self> only synchronously under
    /// the dispatch guard; they must pair every escaping owner and never retain
    /// a temporary receiver in a callback, future, global, or returned carrier.
    pub unsafe fn from_prepared_native<T: SourceFunding + 'static>(
        value: T,
        origin: NativeSourceOrigin,
    ) -> Self {
        unsafe { Self::adopt_native_receiver(Arc::new(value), origin) }
    }
    /// Pair one exact concrete native receiver count.
    ///
    /// # Safety
    /// All conditions of from_prepared_native apply. The input must be the
    /// exact sized T/global Arc<T> allocation, never a wrapper allocation.
    pub unsafe fn adopt_native_receiver<T: SourceFunding + 'static>(
        receiver: Arc<T>,
        origin: NativeSourceOrigin,
    ) -> Self {
        fn finish<T: SourceFunding + 'static>(receiver: Arc<dyn SourceFunding>) {
            // SAFETY: the constructor erases exactly T; consuming the erased
            // count and restoring its data pointer transfers exactly one owner.
            let raw = Arc::into_raw(receiver) as *const T;
            let typed = unsafe { Arc::from_raw(raw) };
            // With no explicit Weak, the final ArcInner is freed before T's
            // refund-bearing destructor runs. All permanent counts use this.
            drop(Arc::into_inner(typed));
        }
        Self {
            receiver: Some(FundingReceiver::Native {
                receiver,
                finish: finish::<T>,
                origin,
            }),
        }
    }
    fn receiver(&self) -> &Arc<dyn SourceFunding> {
        match self.receiver.as_ref().expect("live source funder") {
            FundingReceiver::Host(receiver) | FundingReceiver::Native { receiver, .. } => receiver,
        }
    }
    pub fn matches_native_origin(&self, accountant: usize, lease: SourceLease) -> bool {
        match self.receiver.as_ref().expect("live source funder") {
            FundingReceiver::Host(_) => false,
            FundingReceiver::Native { origin, .. } => origin.matches(accountant, lease),
        }
    }
    fn matches_original_source_origin(
        &self,
        origin: NativeSourceOrigin,
        lease: SourceLease,
    ) -> bool {
        match self.receiver.as_ref().expect("live source funder") {
            FundingReceiver::Native { origin: actual, .. } => {
                actual.same_origin(&origin) && actual.matches(actual.accountant, lease)
            }
            FundingReceiver::Host(receiver) => {
                receiver.matches_original_source_origin(origin, lease) == Some(true)
            }
        }
    }
    pub fn admit_source(&self, bytes: usize) -> Result<SourceLease> {
        self.receiver().admit_source(bytes)
    }
    pub fn retain_source_lease(&self, lease: SourceLease) {
        self.receiver().retain_source_lease(lease);
    }
    pub fn release_source(&self, bytes: usize, lease: SourceLease) {
        self.receiver().release_source(bytes, lease);
    }
    pub fn copy_aux(&self, bodies: AuxBorrow<'_>) -> Result<AdmittedAuxBatch> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_aux(bodies)?;
        result.validate(&guard)?;
        Ok(result)
    }
    pub fn copy_sidecars(
        &self,
        maps: SidecarsBorrow<'_>,
        change: Option<SidecarChange<'_>>,
    ) -> Result<AdmittedSidecars> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_sidecars(maps, change)?;
        result.validate(&guard)?;
        Ok(result)
    }
    pub fn copy_row(&self, row: &contextdb_core::VersionedRow) -> Result<AdmittedSourceRow> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_row(row);
        drop(guard);
        result
    }
    pub fn copy_eager_row(
        &self,
        row: &contextdb_core::VersionedRow,
    ) -> Result<AdmittedEagerSourceRow> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_eager_row(row);
        drop(guard);
        result
    }
    pub fn copy_vector(&self, entry: &contextdb_core::VectorEntry) -> Result<AdmittedSourceVector> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_vector(entry);
        drop(guard);
        result
    }
    pub fn copy_eager_vector(
        &self,
        entry: &contextdb_core::VectorEntry,
    ) -> Result<AdmittedEagerSourceVector> {
        let guard = self.clone();
        let result = guard
            .receiver()
            .clone()
            .copy_eager_vector(entry)
            .and_then(|copied| {
                copied.validate_original_pairing(&guard)?;
                Ok(copied)
            });
        drop(guard);
        result
    }
    pub fn retained_control_bytes(&self) -> Option<usize> {
        self.receiver().retained_control_bytes()
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        visit(RetainedDebit {
            owner: DebitOwner::Provider(self),
            bytes: self.retained_control_bytes(),
        });
        if self
            .receiver()
            .visit_retained_controls(&mut |control| {
                visit(RetainedDebit {
                    bytes: Some(control.bytes),
                    owner: DebitOwner::Lower(control),
                });
            })
            .is_none()
        {
            visit(RetainedDebit {
                owner: DebitOwner::ProviderLower(self),
                bytes: None,
            });
        }
    }
    pub fn copy_key(&self, key: &contextdb_core::IndexKey) -> Result<AdmittedSourceKey> {
        let guard = self.clone();
        let result = guard.receiver().clone().copy_key(key);
        drop(guard);
        result
    }
}
impl Clone for SourceFunder {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}
impl fmt::Debug for SourceFunder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceFunder").finish_non_exhaustive()
    }
}
impl Drop for SourceFunder {
    fn drop(&mut self) {
        match self.receiver.take() {
            Some(FundingReceiver::Native {
                receiver, finish, ..
            }) => finish(receiver),
            Some(FundingReceiver::Host(receiver)) => drop(receiver),
            None => (),
        }
    }
}
#[derive(Clone)]
enum GrantReceiver {
    Host(Arc<dyn crate::StagedPostingCharge>),
    Native {
        receiver: Arc<dyn crate::StagedPostingCharge>,
        finish: fn(Arc<dyn crate::StagedPostingCharge>),
        origin: NativeSourceOrigin,
    },
}
/// Opaque genuine original-copy reservation. No native receiver/Weak escapes.
#[doc(hidden)]
pub struct OriginalCopyGrant {
    receiver: Option<GrantReceiver>,
}
impl OriginalCopyGrant {
    pub fn from_host(receiver: Arc<dyn crate::StagedPostingCharge>) -> Self {
        Self {
            receiver: Some(GrantReceiver::Host(receiver)),
        }
    }
    /// # Safety
    /// The input is the exact sized T/global Arc<T> allocation with its actual
    /// control preadmitted and origin matching its accountant/key/epoch. Every
    /// permanent strong count must use this protocol; no explicit Weak, raw
    /// alias or wrapper allocation may exist. No plain receiver may escape.
    pub unsafe fn from_native<T: crate::StagedPostingCharge + 'static>(
        receiver: Arc<T>,
        origin: NativeSourceOrigin,
    ) -> Self {
        fn finish<T: crate::StagedPostingCharge + 'static>(
            receiver: Arc<dyn crate::StagedPostingCharge>,
        ) {
            let raw = Arc::into_raw(receiver) as *const T;
            // SAFETY: the erased count was constructed from exactly Arc<T>.
            let typed = unsafe { Arc::from_raw(raw) };
            drop(Arc::into_inner(typed));
        }
        Self {
            receiver: Some(GrantReceiver::Native {
                receiver,
                finish: finish::<T>,
                origin,
            }),
        }
    }
    fn receiver(&self) -> &Arc<dyn crate::StagedPostingCharge> {
        match self.receiver.as_ref().expect("live original grant") {
            GrantReceiver::Host(receiver) | GrantReceiver::Native { receiver, .. } => receiver,
        }
    }
    pub fn retained_bytes(&self) -> usize {
        self.receiver().retained_bytes()
    }
    pub fn retained_control_bytes(&self) -> Option<usize> {
        self.receiver().retained_control_bytes()
    }
    /// A private successor retains the opaque original owner, never its std shell.
    pub fn reserve_private_original_copy(&self, bytes: usize) -> Result<Self> {
        self.receiver().reserve_private_original_copy(bytes)
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        visit(RetainedDebit {
            owner: DebitOwner::GrantPayload(self),
            bytes: Some(self.retained_bytes()),
        });
        visit(RetainedDebit {
            owner: DebitOwner::GrantControl(self),
            bytes: self.retained_control_bytes(),
        });
        if self
            .receiver()
            .visit_retained_controls(&mut |control| {
                visit(RetainedDebit {
                    bytes: Some(control.bytes),
                    owner: DebitOwner::Lower(control),
                });
            })
            .is_none()
        {
            visit(RetainedDebit {
                owner: DebitOwner::GrantLower(self),
                bytes: None,
            });
        }
    }
    /// Dispatch only at a real terminal public-output boundary.
    pub fn reserve_terminal_copy(
        &self,
        bytes: usize,
    ) -> Result<Arc<dyn crate::StagedPostingCharge>> {
        match self.receiver.as_ref().expect("live original grant") {
            GrantReceiver::Host(receiver) => receiver.reserve_copy(bytes),
            GrantReceiver::Native { receiver, .. } => receiver.reserve_terminal_copy(bytes),
        }
    }
}
impl Clone for OriginalCopyGrant {
    fn clone(&self) -> Self {
        Self {
            receiver: self.receiver.clone(),
        }
    }
}
impl fmt::Debug for OriginalCopyGrant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut value = f.debug_struct("OriginalCopyGrant");
        value.field("bytes", &self.retained_bytes());
        if let Some(GrantReceiver::Native { origin, .. }) = &self.receiver {
            value.field("origin", origin);
        }
        value.finish_non_exhaustive()
    }
}
impl Drop for OriginalCopyGrant {
    fn drop(&mut self) {
        match self.receiver.take() {
            Some(GrantReceiver::Native {
                receiver, finish, ..
            }) => finish(receiver),
            Some(GrantReceiver::Host(receiver)) => drop(receiver),
            None => (),
        }
    }
}

/// The native layout of std's two counters followed by the aligned value.
/// Qualification must witness this on the effective gate toolchain.
#[doc(hidden)]
pub fn source_arc_layout<T>() -> Layout {
    Layout::new::<[usize; 2]>()
        .extend(Layout::new::<T>())
        .expect("source Arc layout fits address space")
        .0
        .pad_to_align()
}

struct SourceInvoice {
    funder: SourceFunder,
    lease: SourceLease,
    bytes: usize,
}
impl Drop for SourceInvoice {
    fn drop(&mut self) {
        self.funder.release_source(self.bytes, self.lease);
    }
}

/// Every clone includes an invoice holder. Arc::into_inner elects exactly one
/// final holder, even when final holders drop concurrently, and frees the
/// invoice's Arc allocation before returning its inline refund obligation.
#[doc(hidden)]
pub struct SourceCharge(Option<Arc<SourceInvoice>>);
impl SourceCharge {
    pub fn admit(funder: Arc<dyn SourceFunding>, bytes: usize) -> Result<Self> {
        Self::admit_funded(SourceFunder::from_host(funder), bytes)
    }
    pub fn adopt(funder: Arc<dyn SourceFunding>, bytes: usize, lease: SourceLease) -> Result<Self> {
        Self::adopt_funded(SourceFunder::from_host(funder), bytes, lease)
    }
    pub fn admit_funded(funder: SourceFunder, bytes: usize) -> Result<Self> {
        let total = bytes
            .checked_add(source_arc_layout::<SourceInvoice>().size())
            .ok_or_else(|| Error::Other("source allocation size overflow".into()))?;
        let lease = funder.admit_source(total)?;
        Ok(Self(Some(Arc::new(SourceInvoice {
            funder,
            lease,
            bytes: total,
        }))))
    }

    /// The payload debit is already held. Only the invoice's actual control
    /// allocation is newly admitted. On failure the caller still owns that
    /// debit and lease; on success it must disarm them without refunding.
    pub fn adopt_funded(funder: SourceFunder, bytes: usize, lease: SourceLease) -> Result<Self> {
        let total = bytes
            .checked_add(source_arc_layout::<SourceInvoice>().size())
            .ok_or_else(|| Error::Other("source allocation size overflow".into()))?;
        let control_lease = funder.admit_source(source_arc_layout::<SourceInvoice>().size())?;
        funder.retain_source_lease(lease);
        let invoice = Arc::new(SourceInvoice {
            funder: funder.clone(),
            lease,
            bytes: total,
        });
        // Merge only the new control debit; the original lease now owns both
        // debits. No byte is refunded when the temporary registration ends.
        funder.release_source(0, control_lease);
        Ok(Self(Some(invoice)))
    }

    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        let invoice = self.0.as_ref().expect("live source charge");
        visit(RetainedDebit {
            owner: DebitOwner::Source(self),
            bytes: Some(invoice.bytes),
        });
        invoice.funder.visit_retained_debits(visit);
    }
    pub(crate) fn matches_funder(&self, other: &SourceFunder) -> bool {
        let invoice = self.0.as_ref().expect("live source charge");
        if !Arc::ptr_eq(invoice.funder.receiver(), other.receiver()) {
            return false;
        }
        match (invoice.funder.receiver.as_ref(), other.receiver.as_ref()) {
            (
                Some(FundingReceiver::Native { origin: actual, .. }),
                Some(FundingReceiver::Native {
                    origin: expected, ..
                }),
            ) => {
                actual.same_origin(expected) && expected.matches(expected.accountant, invoice.lease)
            }
            (Some(FundingReceiver::Host(_)), Some(FundingReceiver::Host(_))) => true,
            _ => false,
        }
    }
    pub(crate) fn bytes(&self) -> usize {
        self.0.as_ref().expect("live source charge").bytes
    }
}
impl Clone for SourceCharge {
    fn clone(&self) -> Self {
        Self(Some(self.0.as_ref().expect("live source charge").clone()))
    }
}
impl fmt::Debug for SourceCharge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceCharge")
            .field("bytes", &self.bytes())
            .finish()
    }
}
impl Drop for SourceCharge {
    fn drop(&mut self) {
        if let Some(invoice) = self.0.take().and_then(Arc::into_inner) {
            drop(invoice);
        }
    }
}

/// Data and its invoice are inseparable. Private data ownership is destroyed
/// before the last charge wrapper can refund either native control or body.
pub(crate) struct FundedArc<T> {
    data: Option<Arc<T>>,
    charge: Option<SourceCharge>,
}
impl<T> FundedArc<T> {
    pub(crate) fn prepare(funder: SourceFunder) -> Result<SourceCharge> {
        SourceCharge::admit_funded(funder, source_arc_layout::<T>().size())
    }
    pub(crate) fn from_prepared(value: T, charge: SourceCharge) -> Self {
        Self {
            data: Some(Arc::new(value)),
            charge: Some(charge),
        }
    }
    pub(crate) fn try_new(value: T, funder: SourceFunder) -> Result<Self> {
        let charge = Self::prepare(funder)?;
        Ok(Self::from_prepared(value, charge))
    }
    pub(crate) fn allocation_key(&self) -> usize {
        Arc::as_ptr(self.data.as_ref().expect("live funded source")) as usize
    }
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.data.as_ref().unwrap(), other.data.as_ref().unwrap())
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.charge
            .as_ref()
            .expect("live funded source charge")
            .visit_retained_debits(visit);
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.charge
            .as_ref()
            .expect("live funded source charge")
            .bytes()
    }
}
impl<T> Clone for FundedArc<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            charge: self.charge.clone(),
        }
    }
}
impl<T> Deref for FundedArc<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.data.as_ref().expect("live funded source")
    }
}
impl<T: fmt::Debug> fmt::Debug for FundedArc<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(f)
    }
}
impl<T> Drop for FundedArc<T> {
    fn drop(&mut self) {
        drop(self.data.take().and_then(Arc::into_inner));
        drop(self.charge.take());
    }
}

pub(crate) type Link<K, V> = Option<FundedArc<Node<K, V>>>;
#[derive(Debug)]
pub(crate) struct Entry<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
}
#[derive(Debug)]
pub(crate) struct Node<K, V> {
    entry: FundedArc<Entry<K, V>>,
    left: Link<K, V>,
    right: Link<K, V>,
    height: u16,
    len: usize,
}
/// Charges for structural nodes/entries in this tree, excluding K/V custody.
pub(crate) fn retained_link_bytes<K, V>(root: &Link<K, V>) -> usize {
    root.as_ref().map_or(0, |node| {
        node.retained_bytes()
            .checked_add(node.entry.retained_bytes())
            .and_then(|bytes| bytes.checked_add(retained_link_bytes(&node.left)))
            .and_then(|bytes| bytes.checked_add(retained_link_bytes(&node.right)))
            .expect("live tree liabilities fit address space")
    })
}
/// Whole-union callers replay this walk for each root and visit K/V separately.
pub(crate) fn visit_link_debits<'a, K, V>(
    root: &'a Link<K, V>,
    visit: &mut dyn FnMut(RetainedDebit<'a>),
) {
    if let Some(node) = root {
        node.visit_retained_debits(visit);
        node.entry.visit_retained_debits(visit);
        visit_link_debits(&node.left, visit);
        visit_link_debits(&node.right, visit);
    }
}
/// Borrow entries without copying keys, values, or allocating traversal storage.
// retained for the native route; remove when a caller lands
#[allow(dead_code)]
pub(crate) fn for_each_entry<'a, K, V>(root: &'a Link<K, V>, visit: &mut impl FnMut(&'a K, &'a V)) {
    if let Some(node) = root {
        for_each_entry(&node.left, visit);
        visit(&node.entry.key, &node.entry.value);
        for_each_entry(&node.right, visit);
    }
}
fn height<K, V>(root: &Link<K, V>) -> u16 {
    root.as_ref().map_or(0, |n| n.height)
}
pub(crate) fn len<K, V>(root: &Link<K, V>) -> usize {
    root.as_ref().map_or(0, |n| n.len)
}
fn node<K, V>(
    entry: FundedArc<Entry<K, V>>,
    left: Link<K, V>,
    right: Link<K, V>,
    funder: &SourceFunder,
) -> Result<FundedArc<Node<K, V>>> {
    let charge = FundedArc::<Node<K, V>>::prepare(funder.clone())?;
    Ok(FundedArc::from_prepared(
        Node {
            entry,
            height: 1 + height(&left).max(height(&right)),
            len: 1 + len(&left) + len(&right),
            left,
            right,
        },
        charge,
    ))
}
fn balance<K, V>(
    entry: FundedArc<Entry<K, V>>,
    left: Link<K, V>,
    right: Link<K, V>,
    funder: &SourceFunder,
) -> Result<FundedArc<Node<K, V>>> {
    if height(&left) > height(&right) + 1 {
        let l = left.as_ref().expect("left-heavy source");
        if height(&l.left) >= height(&l.right) {
            let r = node(entry, l.right.clone(), right, funder)?;
            return node(l.entry.clone(), l.left.clone(), Some(r), funder);
        }
        let m = l.right.as_ref().expect("left-right source");
        let a = node(l.entry.clone(), l.left.clone(), m.left.clone(), funder)?;
        let b = node(entry, m.right.clone(), right, funder)?;
        return node(m.entry.clone(), Some(a), Some(b), funder);
    }
    if height(&right) > height(&left) + 1 {
        let r = right.as_ref().expect("right-heavy source");
        if height(&r.right) >= height(&r.left) {
            let l = node(entry, left, r.left.clone(), funder)?;
            return node(r.entry.clone(), Some(l), r.right.clone(), funder);
        }
        let m = r.left.as_ref().expect("right-left source");
        let a = node(entry, left, m.left.clone(), funder)?;
        let b = node(r.entry.clone(), m.right.clone(), r.right.clone(), funder)?;
        return node(m.entry.clone(), Some(a), Some(b), funder);
    }
    node(entry, left, right, funder)
}
pub(crate) fn insert<K: Ord, V>(
    root: &Link<K, V>,
    entry: FundedArc<Entry<K, V>>,
    funder: &SourceFunder,
) -> Result<Link<K, V>> {
    let Some(n) = root else {
        return node(entry, None, None, funder).map(Some);
    };
    match entry.key.cmp(&n.entry.key) {
        Ordering::Less => balance(
            n.entry.clone(),
            insert(&n.left, entry, funder)?,
            n.right.clone(),
            funder,
        )
        .map(Some),
        Ordering::Greater => balance(
            n.entry.clone(),
            n.left.clone(),
            insert(&n.right, entry, funder)?,
            funder,
        )
        .map(Some),
        Ordering::Equal => node(entry, n.left.clone(), n.right.clone(), funder).map(Some),
    }
}
pub(crate) fn get_by<K, V>(root: &Link<K, V>, cmp: impl Fn(&K) -> Ordering) -> Option<&V> {
    let mut cursor = root.as_deref();
    while let Some(n) = cursor {
        match cmp(&n.entry.key) {
            Ordering::Less => cursor = n.left.as_deref(),
            Ordering::Greater => cursor = n.right.as_deref(),
            Ordering::Equal => return Some(&n.entry.value),
        }
    }
    None
}
pub(crate) fn remove_by<K, V>(
    root: &Link<K, V>,
    cmp: &impl Fn(&K) -> Ordering,
    funder: &SourceFunder,
) -> Result<Link<K, V>> {
    let Some(n) = root else { return Ok(None) };
    match cmp(&n.entry.key) {
        Ordering::Less => balance(
            n.entry.clone(),
            remove_by(&n.left, cmp, funder)?,
            n.right.clone(),
            funder,
        )
        .map(Some),
        Ordering::Greater => balance(
            n.entry.clone(),
            n.left.clone(),
            remove_by(&n.right, cmp, funder)?,
            funder,
        )
        .map(Some),
        Ordering::Equal => {
            if n.left.is_none() {
                return Ok(n.right.clone());
            }
            if n.right.is_none() {
                return Ok(n.left.clone());
            }
            let mut successor = n.right.as_deref().unwrap();
            while let Some(left) = successor.left.as_deref() {
                successor = left;
            }
            let entry = successor.entry.clone();
            let right = remove_first(&n.right, funder)?;
            balance(entry, n.left.clone(), right, funder).map(Some)
        }
    }
}
fn remove_first<K, V>(root: &Link<K, V>, funder: &SourceFunder) -> Result<Link<K, V>> {
    let n = root.as_ref().expect("nonempty source");
    if n.left.is_none() {
        return Ok(n.right.clone());
    }
    balance(
        n.entry.clone(),
        remove_first(&n.left, funder)?,
        n.right.clone(),
        funder,
    )
    .map(Some)
}
/// Seek using borrowed comparisons. A comparator returns key-vs-bound order;
/// included/excluded endpoints and prefix ranks are supplied by the caller.
pub(crate) fn seek_by<K, V>(
    root: &Link<K, V>,
    reverse: bool,
    allowed: impl Fn(&K) -> Ordering,
) -> Option<(&K, &V)> {
    let mut cursor = root.as_deref();
    let mut candidate = None;
    while let Some(n) = cursor {
        match allowed(&n.entry.key) {
            Ordering::Less => cursor = n.right.as_deref(),
            Ordering::Greater => cursor = n.left.as_deref(),
            Ordering::Equal => {
                candidate = Some((&n.entry.key, &n.entry.value));
                cursor = if reverse {
                    n.right.as_deref()
                } else {
                    n.left.as_deref()
                };
            }
        }
    }
    candidate
}

/// The original payload debit, never an informational byte sidecar. Only the
/// engine's admission transfer constructs one; cells move it with the body.
#[doc(hidden)]
pub struct OriginalRowInvoice {
    charge: SourceCharge,
}
impl fmt::Debug for OriginalRowInvoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalRowInvoice")
            .field("charge", &self.charge)
            .finish()
    }
}
impl OriginalRowInvoice {
    pub fn from_charge(charge: SourceCharge) -> Self {
        Self { charge }
    }
    pub(crate) fn matches(&self, funder: &SourceFunder) -> bool {
        self.charge.matches_funder(funder)
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.charge.bytes()
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.charge.visit_retained_debits(visit);
    }
}
#[doc(hidden)]
pub struct OriginalVectorInvoice {
    charge: SourceCharge,
}
impl fmt::Debug for OriginalVectorInvoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalVectorInvoice")
            .field("charge", &self.charge)
            .finish()
    }
}
impl OriginalVectorInvoice {
    pub fn from_charge(charge: SourceCharge) -> Self {
        Self { charge }
    }
    pub(crate) fn matches(&self, funder: &SourceFunder) -> bool {
        self.charge.matches_funder(funder)
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.charge.bytes()
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.charge.visit_retained_debits(visit);
    }
}

/// Exact-capacity native buffer with its credit outside the allocation. Growth
/// prepares a separate replacement while this buffer and its credit remain.
#[derive(Debug)]
pub(crate) struct SourceBuffer<T> {
    values: Vec<T>,
    _charge: Option<SourceCharge>,
}
impl<T> SourceBuffer<T> {
    pub(crate) fn empty() -> Self {
        Self {
            values: Vec::new(),
            _charge: None,
        }
    }
    pub(crate) fn with_capacity(capacity: usize, funder: &SourceFunder) -> Result<Self> {
        let layout = Layout::array::<T>(capacity)
            .map_err(|_| Error::Other("source buffer size overflow".into()))?;
        let charge = if layout.size() == 0 {
            None
        } else {
            Some(SourceCharge::admit_funded(funder.clone(), layout.size())?)
        };
        let values = Vec::with_capacity(capacity);
        if std::mem::size_of::<T>() != 0 && values.capacity() != capacity {
            drop(values);
            return Err(Error::Other("source native capacity mismatch".into()));
        }
        Ok(Self {
            values,
            _charge: charge,
        })
    }
    pub(crate) fn push(&mut self, value: T) {
        assert!(
            self.values.len() < self.values.capacity(),
            "source buffer must be prepared before growth"
        );
        self.values.push(value);
    }
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }
    // retained for the native route; remove when a caller lands
    #[allow(dead_code)]
    pub(crate) fn clear(&mut self) {
        self.values.clear();
    }
    pub(crate) fn insert(&mut self, position: usize, value: T) {
        assert!(
            self.values.len() < self.values.capacity(),
            "source inverse requires preserved capacity"
        );
        self.values.insert(position, value);
    }
    pub(crate) fn as_slice(&self) -> &[T] {
        &self.values
    }
    pub(crate) fn values_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
    pub(crate) fn replace_at(&mut self, position: usize, value: T) {
        self.values[position] = value;
    }
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }
    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self._charge.as_ref().map_or(0, SourceCharge::bytes)
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        if let Some(charge) = &self._charge {
            charge.visit_retained_debits(visit);
        }
    }
    pub(crate) fn into_parts(self) -> (Vec<T>, Option<SourceCharge>) {
        (self.values, self._charge)
    }
    pub(crate) fn remove(&mut self, index: usize) -> T {
        self.values.remove(index)
    }
    pub(crate) fn append_into(&mut self, replacement: &mut Self) {
        assert!(replacement.values.capacity() >= replacement.values.len() + self.values.len());
        replacement.values.append(&mut self.values);
    }
}
#[derive(Debug)]
struct NameAtom {
    text: Box<str>,
    _charge: Option<SourceCharge>,
}
#[derive(Debug, Clone)]
pub(crate) struct SourceName(FundedArc<NameAtom>);
impl SourceName {
    pub(crate) fn new(text: &str, funder: &SourceFunder) -> Result<Self> {
        let control = FundedArc::<NameAtom>::prepare(funder.clone())?;
        let buffer = if text.is_empty() {
            None
        } else {
            Some(SourceCharge::admit_funded(funder.clone(), text.len())?)
        };
        Ok(Self(FundedArc::from_prepared(
            NameAtom {
                text: text.into(),
                _charge: buffer,
            },
            control,
        )))
    }
    pub(crate) fn as_str(&self) -> &str {
        &self.0.text
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.0.visit_retained_debits(visit);
        if let Some(charge) = &self.0._charge {
            charge.visit_retained_debits(visit);
        }
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.0
            .retained_bytes()
            .saturating_add(self.0._charge.as_ref().map_or(0, SourceCharge::bytes))
    }
}
impl PartialEq for SourceName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for SourceName {}
impl PartialOrd for SourceName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SourceName {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

#[derive(Debug)]
pub(crate) struct SourceIndexDecl {
    name: SourceName,
    columns: SourceBuffer<(SourceName, contextdb_core::SortDirection)>,
    kind: contextdb_core::IndexKind,
}
#[derive(Debug)]
pub(crate) struct SourceSchema {
    layout: SourceBuffer<(SourceName, contextdb_core::ColumnType)>,
    indexes: SourceBuffer<SourceIndexDecl>,
}
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct SchemaHandle(FundedArc<SourceSchema>);
impl SchemaHandle {
    pub(crate) fn layout(&self) -> &[(SourceName, contextdb_core::ColumnType)] {
        self.0.layout.as_slice()
    }
    pub(crate) fn for_each_index(
        &self,
        mut visit: impl FnMut(
            &str,
            contextdb_core::IndexKind,
            &[(SourceName, contextdb_core::SortDirection)],
        ),
    ) {
        for index in self.0.indexes.as_slice() {
            visit(index.name.as_str(), index.kind, index.columns.as_slice());
        }
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.0.visit_retained_debits(visit);
        self.0.layout.visit_retained_debits(visit);
        for (name, _) in self.0.layout.as_slice() {
            name.visit_retained_debits(visit);
        }
        self.0.indexes.visit_retained_debits(visit);
        for index in self.0.indexes.as_slice() {
            index.name.visit_retained_debits(visit);
            index.columns.visit_retained_debits(visit);
            for (name, _) in index.columns.as_slice() {
                name.visit_retained_debits(visit);
            }
        }
    }
    pub(crate) fn matches_index(&self, other: &Self, name: &str) -> bool {
        self.0.matches_index(&other.0, name)
    }
    pub(crate) fn matches_meta(&self, meta: &contextdb_core::TableMeta, name: &str) -> bool {
        self.0.matches_meta(meta, name)
    }
    pub(crate) fn matches_all_meta(&self, meta: &contextdb_core::TableMeta) -> bool {
        self.0.indexes.as_slice().len() == meta.indexes.len()
            && self.0.layout.as_slice().len() == meta.columns.len()
            && self
                .0
                .layout
                .as_slice()
                .iter()
                .zip(&meta.columns)
                .all(|((name, kind), column)| {
                    name.as_str() == column.name && *kind == column.column_type
                })
            && self
                .0
                .indexes
                .as_slice()
                .iter()
                .all(|index| self.matches_meta(meta, index.name.as_str()))
    }
    pub(crate) fn index_name(&self, name: &str) -> Option<SourceName> {
        self.0.index(name).map(|index| index.name.clone())
    }
    pub(crate) fn ptr_eq(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        let mut bytes = self.0.retained_bytes();
        bytes = bytes.saturating_add(self.0.layout.retained_bytes());
        for (name, _) in self.0.layout.as_slice() {
            bytes = bytes.saturating_add(name.retained_bytes());
        }
        bytes = bytes.saturating_add(self.0.indexes.retained_bytes());
        for index in self.0.indexes.as_slice() {
            bytes = bytes.saturating_add(index.name.retained_bytes());
            bytes = bytes.saturating_add(index.columns.retained_bytes());
            for (name, _) in index.columns.as_slice() {
                bytes = bytes.saturating_add(name.retained_bytes());
            }
        }
        bytes
    }
}
impl SourceSchema {
    pub(crate) fn from_meta(
        meta: &contextdb_core::TableMeta,
        funder: &SourceFunder,
    ) -> Result<SchemaHandle> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let mut layout = SourceBuffer::with_capacity(meta.columns.len(), funder)?;
        for column in &meta.columns {
            layout.push((
                SourceName::new(&column.name, funder)?,
                column.column_type.clone(),
            ));
        }
        let mut indexes = SourceBuffer::with_capacity(meta.indexes.len(), funder)?;
        for index in &meta.indexes {
            let name = SourceName::new(&index.name, funder)?;
            let mut columns = SourceBuffer::with_capacity(index.columns.len(), funder)?;
            for (column, direction) in &index.columns {
                columns.push((SourceName::new(column, funder)?, *direction));
            }
            indexes.push(SourceIndexDecl {
                name,
                columns,
                kind: index.kind,
            });
        }
        Ok(SchemaHandle(FundedArc::from_prepared(
            Self { layout, indexes },
            control,
        )))
    }
    fn index(&self, name: &str) -> Option<&SourceIndexDecl> {
        self.indexes
            .as_slice()
            .iter()
            .find(|i| i.name.as_str() == name)
    }
    pub(crate) fn matches_index(&self, other: &Self, name: &str) -> bool {
        self.layout.as_slice() == other.layout.as_slice()
            && match (self.index(name), other.index(name)) {
                (None, None) => true,
                (Some(a), Some(b)) => {
                    a.kind == b.kind && a.columns.as_slice() == b.columns.as_slice()
                }
                _ => false,
            }
    }
    pub(crate) fn matches_meta(&self, meta: &contextdb_core::TableMeta, name: &str) -> bool {
        self.layout.as_slice().len() == meta.columns.len()
            && self
                .layout
                .as_slice()
                .iter()
                .zip(&meta.columns)
                .all(|((name, kind), column)| {
                    name.as_str() == column.name && *kind == column.column_type
                })
            && match (
                self.index(name),
                meta.indexes.iter().find(|i| i.name == name),
            ) {
                (None, None) => true,
                (Some(a), Some(b)) => {
                    a.kind == b.kind
                        && a.columns.as_slice().len() == b.columns.len()
                        && a.columns.as_slice().iter().zip(&b.columns).all(
                            |((name, direction), (column, sort))| {
                                name.as_str() == column && direction == sort
                            },
                        )
                }
                _ => false,
            }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RowIdentity {
    pub(crate) tx: contextdb_core::TxId,
    pub(crate) table: SourceName,
    pub(crate) row: contextdb_core::RowId,
    pub(crate) created_tx: contextdb_core::TxId,
    pub(crate) deleted_tx: Option<contextdb_core::TxId>,
    pub(crate) lsn: contextdb_core::Lsn,
    pub(crate) created_at: Option<contextdb_core::Wallclock>,
    pub(crate) sequence: usize,
    pub(crate) generation: u64,
    pub(crate) base: crate::PublishedRead,
    pub(crate) schema: SchemaHandle,
}
impl RowIdentity {
    pub(crate) fn matches_row(&self, row: &contextdb_core::VersionedRow) -> bool {
        self.row == row.row_id
            && self.created_tx == row.created_tx
            && self.deleted_tx == row.deleted_tx
            && self.lsn == row.lsn
            && self.created_at == row.created_at
    }
}
#[derive(Debug)]
pub(crate) enum RowCustody {
    Sealed {
        row: contextdb_core::VersionedRow,
        _invoice: OriginalRowInvoice,
    },
}
#[derive(Debug)]
pub(crate) struct RowCell {
    pub(crate) identity: RowIdentity,
    pub(crate) keys: SourceBuffer<(SourceName, PostingHandle)>,
    pub(crate) custody: parking_lot::Mutex<Option<RowCustody>>,
}
pub(crate) type RowCellHandle = FundedArc<RowCell>;

#[derive(Debug)]
pub(crate) struct PostingAtom {
    pub(crate) position: (contextdb_core::IndexKey, contextdb_core::RowId),
    // This debit owns only this exact key buffer and nested copied values.
    // Bundle and index hold the same atom, never independently cloned keys.
    pub(crate) _payload: SourceCharge,
}
pub(crate) type PostingHandle = FundedArc<PostingAtom>;
impl PartialEq for PostingHandle {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position
    }
}
impl Eq for PostingHandle {}
impl PartialOrd for PostingHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PostingHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        self.position.cmp(&other.position)
    }
}
#[derive(Debug)]
pub(crate) struct IndexDomain {
    pub(crate) schema: SchemaHandle,
    pub(crate) postings: Link<PostingHandle, contextdb_core::RowId>,
}
#[derive(Debug, Default)]
pub(crate) struct TableRoot {
    pub(crate) by_id: Link<contextdb_core::RowId, RowCellHandle>,
    pub(crate) by_sequence: Link<usize, contextdb_core::RowId>,
    pub(crate) indexes: Link<SourceName, FundedArc<IndexDomain>>,
    pub(crate) next_sequence: usize,
}
#[derive(Debug, Default)]
pub(crate) struct SourceRoot {
    pub(crate) tables: Link<SourceName, FundedArc<TableRoot>>,
    pub(crate) row_count: usize,
    pub(crate) vectors: Link<VectorName, FundedArc<VectorRoot>>,
}

/// Resolver authority belongs to a captured descriptor, never a retained root.
/// Implementations take the host guard before a cell guard and authenticate the
/// full immutable header before calling the trusted copy/score.
pub(crate) trait SourceResolver: Send + Sync {
    fn with_cell(
        &self,
        source: &CapturedIdentity,
        cell: &RowCellHandle,
        f: &mut dyn FnMut(&contextdb_core::VersionedRow) -> Result<()>,
    ) -> Result<()>;
    fn with_vector_cell(
        &self,
        source: &CapturedIdentity,
        cell: &VectorCellHandle,
        f: &mut dyn FnMut(&contextdb_core::VectorEntry) -> Result<()>,
    ) -> Result<()>;
}
/// A captured source either retains its legacy host resolver or resolves
/// immutable sealed cells directly from the descriptor's existing custody.
/// The sealed form is inline so capture admission covers the descriptor
/// allocation itself without creating a second resolver allocation.
pub(crate) enum CaptureResolver {
    Host(Arc<dyn SourceResolver>),
    Sealed,
}
impl CaptureResolver {
    fn with_cell(
        &self,
        source: &CapturedIdentity,
        cell: &RowCellHandle,
        f: &mut dyn FnMut(&contextdb_core::VersionedRow) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Host(resolver) => resolver.with_cell(source, cell, f),
            Self::Sealed => with_sealed_cell(source, cell, f),
        }
    }

    fn with_vector_cell(
        &self,
        source: &CapturedIdentity,
        cell: &VectorCellHandle,
        f: &mut dyn FnMut(&contextdb_core::VectorEntry) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Host(resolver) => resolver.with_vector_cell(source, cell, f),
            Self::Sealed => with_sealed_vector_cell(source, cell, f),
        }
    }
}
#[derive(Debug)]
pub(crate) struct CapturedIdentity {
    pub(crate) tx: contextdb_core::TxId,
    pub(crate) generation: u64,
    pub(crate) base: crate::PublishedRead,
}
struct CaptureDescriptor {
    root: Option<FundedArc<TableRoot>>,
    identity: CapturedIdentity,
    resolver: CaptureResolver,
}
struct CaptureCredit(Option<Arc<contextdb_core::read_memory::ReadCredit>>);
impl Clone for CaptureCredit {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl Drop for CaptureCredit {
    fn drop(&mut self) {
        drop(self.0.take().and_then(Arc::into_inner));
    }
}
/// An authenticated immutable statement image. Borrowed callbacks may copy or
/// score one admitted selection; they must finish before predicates/user code.
#[doc(hidden)]
pub struct CanonicalStatementRows {
    descriptor: Option<Arc<CaptureDescriptor>>,
    credit: CaptureCredit,
}
impl Clone for CanonicalStatementRows {
    fn clone(&self) -> Self {
        Self {
            descriptor: self.descriptor.clone(),
            credit: self.credit.clone(),
        }
    }
}
impl Drop for CanonicalStatementRows {
    fn drop(&mut self) {
        drop(self.descriptor.take().and_then(Arc::into_inner));
    }
}
impl fmt::Debug for CanonicalStatementRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CanonicalStatementRows")
            .field("len", &self.len())
            .finish()
    }
}
impl CanonicalStatementRows {
    pub(crate) fn capture(
        root: Option<FundedArc<TableRoot>>,
        identity: CapturedIdentity,
        resolver: CaptureResolver,
        reader: Arc<dyn contextdb_core::read_memory::ReadMemoryBudget>,
    ) -> Result<Self> {
        let bytes = source_arc_layout::<CaptureDescriptor>()
            .size()
            .checked_add(source_arc_layout::<contextdb_core::read_memory::ReadCredit>().size())
            .ok_or_else(|| Error::Other("source descriptor size overflow".into()))?;
        let credit = contextdb_core::read_memory::ReadCredit::try_new(
            reader,
            bytes,
            "writer_source",
            "statement_capture",
            "Release statement captures or raise READ_MEMORY_LIMIT.",
        )?;
        Ok(Self {
            descriptor: Some(Arc::new(CaptureDescriptor {
                root,
                identity,
                resolver,
            })),
            credit: CaptureCredit(Some(Arc::new(credit))),
        })
    }
    fn descriptor(&self) -> &CaptureDescriptor {
        self.descriptor.as_ref().expect("live source descriptor")
    }
    pub fn len(&self) -> usize {
        self.descriptor()
            .root
            .as_ref()
            .map_or(0, |root| len(&root.by_id))
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn contains(&self, id: &contextdb_core::RowId) -> bool {
        self.descriptor()
            .root
            .as_ref()
            .is_some_and(|root| get_by(&root.by_id, |key| id.cmp(key)).is_some())
    }
    pub fn with_row<R>(
        &self,
        id: contextdb_core::RowId,
        f: impl for<'r> FnOnce(Option<&'r contextdb_core::VersionedRow>) -> Result<R>,
    ) -> Result<R> {
        let descriptor = self.descriptor();
        let Some(root) = descriptor.root.as_ref() else {
            return f(None);
        };
        let Some(cell) = get_by(&root.by_id, |key| id.cmp(key)) else {
            return f(None);
        };
        let mut callback = Some(f);
        let mut result = None;
        descriptor
            .resolver
            .with_cell(&descriptor.identity, cell, &mut |row| {
                result = Some(callback
                    .take()
                    .expect("source resolver calls at most once")(
                    Some(row)
                )?);
                Ok(())
            })?;
        result.ok_or_else(|| Error::Other("captured staged source identity mismatch".into()))
    }
    pub fn next_unordered(&self, after: Option<usize>) -> Option<(usize, contextdb_core::RowId)> {
        seek_by(
            &self.descriptor().root.as_ref()?.by_sequence,
            false,
            |sequence| {
                if after.is_some_and(|after| *sequence <= after) {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            },
        )
        .map(|(sequence, id)| (*sequence, *id))
    }
}

impl TableRoot {
    /// Prepare only changed paths; all original cells are already immutable.
    pub(crate) fn prepare_insert(
        &self,
        cell: RowCellHandle,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| Error::Other("staged sequence overflow".into()))?;
        assert_eq!(cell.identity.sequence, self.next_sequence);
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let id = cell.identity.row;
        let mut sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(old) = get_by(&self.by_id, |key| id.cmp(key)) {
            sequence = remove_by(&sequence, &|key| old.identity.sequence.cmp(key), funder)?;
            indexes = remove_cell_postings(indexes, old, funder)?;
        }
        for (name, posting) in cell.keys.as_slice() {
            let prior = get_by(&indexes, |key| name.cmp(key));
            let root = prior
                .filter(|domain| {
                    domain
                        .schema
                        .matches_index(&cell.identity.schema, name.as_str())
                })
                .and_then(|domain| domain.postings.clone());
            let postings = insert(
                &root,
                FundedArc::try_new(
                    Entry {
                        key: posting.clone(),
                        value: id,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let domain = FundedArc::try_new(
                IndexDomain {
                    schema: cell.identity.schema.clone(),
                    postings,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: domain,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: id,
                    value: cell.clone(),
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let by_sequence = insert(
            &sequence,
            FundedArc::try_new(
                Entry {
                    key: cell.identity.sequence,
                    value: id,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence,
            },
            control,
        ))
    }
    pub(crate) fn prepare_replace(
        &self,
        cell: RowCellHandle,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let old = get_by(&self.by_id, |id| cell.identity.row.cmp(id))
            .ok_or_else(source_identity_error)?;
        if old.identity.sequence != cell.identity.sequence {
            return Err(source_identity_error());
        }
        let next_sequence = self.next_sequence;
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let id = cell.identity.row;
        let mut sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(old) = get_by(&self.by_id, |key| id.cmp(key)) {
            sequence = remove_by(&sequence, &|key| old.identity.sequence.cmp(key), funder)?;
            indexes = remove_cell_postings(indexes, old, funder)?;
        }
        for (name, posting) in cell.keys.as_slice() {
            let prior = get_by(&indexes, |key| name.cmp(key));
            let root = prior
                .filter(|domain| {
                    domain
                        .schema
                        .matches_index(&cell.identity.schema, name.as_str())
                })
                .and_then(|domain| domain.postings.clone());
            let postings = insert(
                &root,
                FundedArc::try_new(
                    Entry {
                        key: posting.clone(),
                        value: id,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let domain = FundedArc::try_new(
                IndexDomain {
                    schema: cell.identity.schema.clone(),
                    postings,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: domain,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: id,
                    value: cell.clone(),
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let by_sequence = insert(
            &sequence,
            FundedArc::try_new(
                Entry {
                    key: cell.identity.sequence,
                    value: id,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence,
            },
            control,
        ))
    }
    pub(crate) fn prepare_restore_slot(
        &self,
        cell: RowCellHandle,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let next_sequence = self.next_sequence;
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let id = cell.identity.row;
        let mut sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(old) = get_by(&self.by_id, |key| id.cmp(key)) {
            sequence = remove_by(&sequence, &|key| old.identity.sequence.cmp(key), funder)?;
            indexes = remove_cell_postings(indexes, old, funder)?;
        }
        for (name, posting) in cell.keys.as_slice() {
            let prior = get_by(&indexes, |key| name.cmp(key));
            let root = prior
                .filter(|domain| {
                    domain
                        .schema
                        .matches_index(&cell.identity.schema, name.as_str())
                })
                .and_then(|domain| domain.postings.clone());
            let postings = insert(
                &root,
                FundedArc::try_new(
                    Entry {
                        key: posting.clone(),
                        value: id,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
            let domain = FundedArc::try_new(
                IndexDomain {
                    schema: cell.identity.schema.clone(),
                    postings,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: domain,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: id,
                    value: cell.clone(),
                },
                funder.clone(),
            )?,
            funder,
        )?;
        let by_sequence = insert(
            &sequence,
            FundedArc::try_new(
                Entry {
                    key: cell.identity.sequence,
                    value: id,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence,
            },
            control,
        ))
    }
    pub(crate) fn prepare_remove(
        &self,
        id: contextdb_core::RowId,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let mut by_sequence = self.by_sequence.clone();
        let mut indexes = self.indexes.clone();
        if let Some(old) = get_by(&self.by_id, |key| id.cmp(key)) {
            by_sequence = remove_by(&by_sequence, &|key| old.identity.sequence.cmp(key), funder)?;
            indexes = remove_cell_postings(indexes, old, funder)?;
        }
        let by_id = remove_by(&self.by_id, &|key| id.cmp(key), funder)?;
        Ok(FundedArc::from_prepared(
            Self {
                by_id,
                by_sequence,
                indexes,
                next_sequence: self.next_sequence,
            },
            control,
        ))
    }
}
fn remove_cell_postings(
    mut indexes: Link<SourceName, FundedArc<IndexDomain>>,
    cell: &RowCellHandle,
    funder: &SourceFunder,
) -> Result<Link<SourceName, FundedArc<IndexDomain>>> {
    for (name, posting) in cell.keys.as_slice() {
        let Some(domain) = get_by(&indexes, |key| name.cmp(key)) else {
            continue;
        };
        if !domain
            .schema
            .matches_index(&cell.identity.schema, name.as_str())
        {
            continue;
        }
        let postings = remove_by(&domain.postings, &|key| posting.cmp(key), funder)?;
        if postings.is_none() {
            indexes = remove_by(&indexes, &|key| name.cmp(key), funder)?;
        } else {
            let next = FundedArc::try_new(
                IndexDomain {
                    schema: domain.schema.clone(),
                    postings,
                },
                funder.clone(),
            )?;
            indexes = insert(
                &indexes,
                FundedArc::try_new(
                    Entry {
                        key: name.clone(),
                        value: next,
                    },
                    funder.clone(),
                )?,
                funder,
            )?;
        }
    }
    Ok(indexes)
}
impl SourceRoot {
    pub(crate) fn table(&self, table: &str) -> Option<&FundedArc<TableRoot>> {
        get_by(&self.tables, |name| table.cmp(name.as_str()))
    }
    pub(crate) fn prepare_table(
        &self,
        table: SourceName,
        next: FundedArc<TableRoot>,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let old_count = self
            .table(table.as_str())
            .map_or(0, |root| len(&root.by_id));
        let row_count = self
            .row_count
            .checked_sub(old_count)
            .and_then(|count| count.checked_add(len(&next.by_id)))
            .ok_or_else(|| Error::Other("source row count overflow".into()))?;
        let tables = insert(
            &self.tables,
            FundedArc::try_new(
                Entry {
                    key: table,
                    value: next,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                tables,
                row_count,
                vectors: self.vectors.clone(),
            },
            control,
        ))
    }
}

/// Shared prefix comparator. Rank zero starts before a matching logical
/// prefix, rank two ends after it, and rank one breaks full-key ties by RowId.
pub(crate) fn compare_statement_keys(
    left: &[contextdb_core::DirectedValue],
    left_rank: u8,
    left_row: Option<contextdb_core::RowId>,
    right: &[contextdb_core::DirectedValue],
    right_rank: u8,
    right_row: Option<contextdb_core::RowId>,
) -> Ordering {
    let common = left.len().min(right.len());
    let order = left[..common].cmp(&right[..common]);
    if !order.is_eq() {
        return order;
    }
    match left.len().cmp(&right.len()) {
        Ordering::Less => {
            if left_rank == 2 {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        Ordering::Greater => {
            if right_rank == 2 {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        Ordering::Equal => left_rank.cmp(&right_rank).then_with(|| {
            if left_rank == 1 && right_rank == 1 {
                left_row.cmp(&right_row)
            } else {
                Ordering::Equal
            }
        }),
    }
}
impl CanonicalStatementRows {
    pub fn has_index_for(&self, index: &str, meta: &contextdb_core::TableMeta) -> bool {
        self.descriptor().root.as_ref().is_some_and(|root| {
            get_by(&root.indexes, |name| index.cmp(name.as_str())).is_some_and(|domain| {
                len(&domain.postings) == self.len() && domain.schema.matches_meta(meta, index)
            })
        })
    }
    #[allow(clippy::too_many_arguments)]
    pub fn with_next_ordered<R>(
        &self,
        index: &str,
        reverse: bool,
        after: Option<&(contextdb_core::IndexKey, contextdb_core::RowId)>,
        start: &std::ops::Bound<contextdb_core::IndexKey>,
        end: &std::ops::Bound<contextdb_core::IndexKey>,
        prefix_len: usize,
        f: impl for<'k> FnOnce(
            Option<(
                &'k (contextdb_core::IndexKey, contextdb_core::RowId),
                contextdb_core::RowId,
            )>,
        ) -> Result<R>,
    ) -> Result<R> {
        use std::ops::Bound;
        let Some(root) = self.descriptor().root.as_ref() else {
            return f(None);
        };
        let Some(domain) = get_by(&root.indexes, |name| index.cmp(name.as_str())) else {
            return f(None);
        };
        let compare_bound = |position: &(contextdb_core::IndexKey, contextdb_core::RowId),
                             bound: &Bound<contextdb_core::IndexKey>,
                             lower: bool| {
            match bound {
                Bound::Unbounded => None,
                Bound::Included(key) | Bound::Excluded(key) => {
                    let before = matches!(bound, Bound::Included(_)) == lower;
                    Some(compare_statement_keys(
                        &position.0,
                        1,
                        Some(position.1),
                        &key[..prefix_len.min(key.len())],
                        if before { 0 } else { 2 },
                        None,
                    ))
                }
            }
        };
        let selected = seek_by(&domain.postings, reverse, |posting| {
            let position = &posting.position;
            // Continuation replaces the corresponding endpoint, exactly as
            // the legacy StatementRows run cursor does.
            let lower = match (reverse, after) {
                (false, Some(after)) => Some(position.cmp(after)),
                _ => compare_bound(position, start, true),
            };
            if lower.is_some_and(|order| order != Ordering::Greater) {
                return Ordering::Less;
            }
            let upper = match (reverse, after) {
                (true, Some(after)) => Some(position.cmp(after)),
                _ => compare_bound(position, end, false),
            };
            if upper.is_some_and(|order| order != Ordering::Less) {
                return Ordering::Greater;
            }
            Ordering::Equal
        });
        f(selected.map(|(posting, id)| (&posting.position, *id)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct VectorName {
    pub(crate) table: SourceName,
    pub(crate) column: SourceName,
}
#[derive(Debug, Clone)]
pub(crate) struct VectorIdentity {
    pub(crate) tx: contextdb_core::TxId,
    pub(crate) index: VectorName,
    pub(crate) row: contextdb_core::RowId,
    pub(crate) created_tx: contextdb_core::TxId,
    pub(crate) deleted_tx: Option<contextdb_core::TxId>,
    pub(crate) lsn: contextdb_core::Lsn,
    pub(crate) generation: u64,
    pub(crate) base: crate::PublishedRead,
}
impl VectorIdentity {
    pub(crate) fn matches_entry(&self, entry: &contextdb_core::VectorEntry) -> bool {
        self.index.table.as_str() == entry.index.table
            && self.index.column.as_str() == entry.index.column
            && self.row == entry.row_id
            && self.created_tx == entry.created_tx
            && self.deleted_tx == entry.deleted_tx
            && self.lsn == entry.lsn
    }
}
#[derive(Debug)]
pub(crate) enum VectorCustody {
    Sealed {
        entry: contextdb_core::VectorEntry,
        _invoice: OriginalVectorInvoice,
    },
}
#[derive(Debug)]
pub(crate) struct VectorCell {
    pub(crate) identity: VectorIdentity,
    pub(crate) custody: parking_lot::Mutex<Option<VectorCustody>>,
}
pub(crate) type VectorCellHandle = FundedArc<VectorCell>;

#[derive(Debug, Clone)]
enum VectorReference {
    Scalar {
        row: RowCellHandle,
        column: SourceName,
    },
    Owned(VectorCellHandle),
}
/// Immutable identity selection. Scalar and owned cells are selected at
/// preparation by actual ownership, never by comparing equal f32 contents.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct CanonicalVectorRef {
    row: contextdb_core::RowId,
    pub(crate) owner: VectorCellHandle,
    reference: VectorReference,
}
impl CanonicalVectorRef {
    pub fn row_id(&self) -> contextdb_core::RowId {
        self.row
    }
    pub(crate) fn scalar(
        row: contextdb_core::RowId,
        owner: VectorCellHandle,
        source: RowCellHandle,
        column: SourceName,
    ) -> Self {
        Self {
            row,
            owner,
            reference: VectorReference::Scalar {
                row: source,
                column,
            },
        }
    }
    pub(crate) fn owned(row: contextdb_core::RowId, source: VectorCellHandle) -> Self {
        Self {
            row,
            owner: source.clone(),
            reference: VectorReference::Owned(source),
        }
    }
}
#[derive(Debug, Default)]
pub(crate) struct VectorRoot {
    pub(crate) rows: Link<contextdb_core::RowId, CanonicalVectorRef>,
}
impl VectorRoot {
    pub(crate) fn prepare_insert(
        &self,
        source: CanonicalVectorRef,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let rows = insert(
            &self.rows,
            FundedArc::try_new(
                Entry {
                    key: source.row_id(),
                    value: source,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(Self { rows }, control))
    }
    pub(crate) fn prepare_remove(
        &self,
        row: contextdb_core::RowId,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let rows = remove_by(&self.rows, &|key| row.cmp(key), funder)?;
        Ok(FundedArc::from_prepared(Self { rows }, control))
    }
}

impl SourceRoot {
    pub(crate) fn vector(&self, table: &str, column: &str) -> Option<&FundedArc<VectorRoot>> {
        get_by(&self.vectors, |name| {
            (table, column).cmp(&(name.table.as_str(), name.column.as_str()))
        })
    }
    pub(crate) fn prepare_vector(
        &self,
        name: VectorName,
        next: FundedArc<VectorRoot>,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let vectors = insert(
            &self.vectors,
            FundedArc::try_new(
                Entry {
                    key: name,
                    value: next,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                tables: self.tables.clone(),
                row_count: self.row_count,
                vectors,
            },
            control,
        ))
    }
}
/// A sealed selection in the transaction's original vector-slot order.
#[doc(hidden)]
pub struct CanonicalStatementVectors {
    pub(crate) vectors: Vec<CanonicalVectorRef>,
    pub(crate) source: CanonicalStatementRows,
    pub(crate) _credit: contextdb_core::read_memory::ReadCredit,
}
impl CanonicalStatementVectors {
    pub fn len(&self) -> usize {
        self.vectors.len()
    }
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
    pub fn with_vector<R>(
        &self,
        position: usize,
        f: impl for<'v> FnOnce(Option<(contextdb_core::RowId, &'v [f32])>) -> Result<R>,
    ) -> Result<R> {
        match self.vectors.get(position) {
            Some(vector) => self.source.with_vector(vector, f),
            None => f(None),
        }
    }
}

impl CanonicalStatementRows {
    pub fn with_vector<R>(
        &self,
        vector: &CanonicalVectorRef,
        f: impl for<'v> FnOnce(Option<(contextdb_core::RowId, &'v [f32])>) -> Result<R>,
    ) -> Result<R> {
        let descriptor = self.descriptor();
        let mut callback = Some(f);
        let mut result = None;
        match &vector.reference {
            VectorReference::Scalar { row, column } => {
                descriptor
                    .resolver
                    .with_cell(&descriptor.identity, row, &mut |body| {
                        let Some(contextdb_core::Value::Vector(values)) =
                            body.values.get(column.as_str())
                        else {
                            return Err(Error::Other(
                                "captured staged source identity mismatch".into(),
                            ));
                        };
                        result = Some(callback
                            .take()
                            .expect("source resolver calls at most once")(
                            Some((vector.row, values)),
                        )?);
                        Ok(())
                    })?;
            }
            VectorReference::Owned(cell) => {
                descriptor
                    .resolver
                    .with_vector_cell(&descriptor.identity, cell, &mut |entry| {
                        result = Some(callback
                            .take()
                            .expect("source resolver calls at most once")(
                            Some((vector.row, &entry.vector)),
                        )?);
                        Ok(())
                    })?;
            }
        }
        result.ok_or_else(|| Error::Other("captured staged source identity mismatch".into()))
    }
}

pub(crate) fn with_sealed_cell(
    source: &CapturedIdentity,
    cell: &RowCellHandle,
    f: &mut dyn FnMut(&contextdb_core::VersionedRow) -> Result<()>,
) -> Result<()> {
    let custody = cell.custody.lock();
    if cell.identity.tx != source.tx
        || cell.identity.generation != source.generation
        || !same_published_source(&cell.identity.base, &source.base)
    {
        return Err(source_identity_error());
    }
    match custody.as_ref() {
        Some(RowCustody::Sealed { row, .. }) if cell.identity.matches_row(row) => f(row),
        _ => Err(source_identity_error()),
    }
}

pub(crate) fn with_sealed_vector_cell(
    source: &CapturedIdentity,
    cell: &VectorCellHandle,
    f: &mut dyn FnMut(&contextdb_core::VectorEntry) -> Result<()>,
) -> Result<()> {
    let custody = cell.custody.lock();
    if cell.identity.tx != source.tx
        || cell.identity.generation != source.generation
        || !same_published_source(&cell.identity.base, &source.base)
    {
        return Err(source_identity_error());
    }
    match custody.as_ref() {
        Some(VectorCustody::Sealed { entry, .. }) if cell.identity.matches_entry(entry) => f(entry),
        _ => Err(source_identity_error()),
    }
}

fn same_published_source(left: &crate::PublishedRead, right: &crate::PublishedRead) -> bool {
    left.snapshot == right.snapshot
        && left.lsn == right.lsn
        && match (&left.handle, &right.handle) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
}

fn source_identity_error() -> Error {
    Error::Other("captured staged source identity mismatch".into())
}

/// Active hosts are mutable, but their node allocation has the same explicit
/// free-before-refund pair. No HashMap directory or unadmitted capacity grows.
pub(crate) struct FundedBox<T> {
    // Field order frees the value and actual node allocation before credit.
    data: Box<T>,
    charge: SourceCharge,
}
impl<T> FundedBox<T> {
    pub(crate) fn try_new(value: T, funder: &SourceFunder) -> Result<Self> {
        let charge = SourceCharge::admit_funded(funder.clone(), Layout::new::<T>().size())?;
        Ok(Self {
            data: Box::new(value),
            charge,
        })
    }
    pub(crate) fn get(&self) -> &T {
        &self.data
    }
    pub(crate) fn get_mut(&mut self) -> &mut T {
        &mut self.data
    }
    fn into_inner(self) -> T {
        let Self { data, charge } = self;
        let value = *data;
        drop(charge);
        value
    }
}
struct MutableNode<K, V> {
    key: K,
    value: V,
    left: MutableLink<K, V>,
    right: MutableLink<K, V>,
    height: u16,
}
type MutableLink<K, V> = Option<FundedBox<MutableNode<K, V>>>;
pub(crate) struct PreparedMapEntry<K, V>(FundedBox<MutableNode<K, V>>);
impl<K, V> PreparedMapEntry<K, V> {
    pub(crate) fn key(&self) -> &K {
        &self.0.get().key
    }
    pub(crate) fn value(&self) -> &V {
        &self.0.get().value
    }
    pub(crate) fn value_mut(&mut self) -> &mut V {
        &mut self.0.get_mut().value
    }
}
pub(crate) struct MutableSourceMap<K, V> {
    root: MutableLink<K, V>,
    len: usize,
}
impl<K, V> Default for MutableSourceMap<K, V> {
    fn default() -> Self {
        Self { root: None, len: 0 }
    }
}
fn mutable_height<K, V>(link: &MutableLink<K, V>) -> u16 {
    link.as_ref().map_or(0, |n| n.get().height)
}
fn repair_height<K, V>(node: &mut FundedBox<MutableNode<K, V>>) {
    let n = node.get_mut();
    n.height = 1 + mutable_height(&n.left).max(mutable_height(&n.right));
}
fn rotate_left<K, V>(mut node: FundedBox<MutableNode<K, V>>) -> FundedBox<MutableNode<K, V>> {
    let mut next = node.get_mut().right.take().expect("right rotation child");
    node.get_mut().right = next.get_mut().left.take();
    repair_height(&mut node);
    next.get_mut().left = Some(node);
    repair_height(&mut next);
    next
}
fn rotate_right<K, V>(mut node: FundedBox<MutableNode<K, V>>) -> FundedBox<MutableNode<K, V>> {
    let mut next = node.get_mut().left.take().expect("left rotation child");
    node.get_mut().left = next.get_mut().right.take();
    repair_height(&mut node);
    next.get_mut().right = Some(node);
    repair_height(&mut next);
    next
}
fn mutable_balance<K, V>(mut node: FundedBox<MutableNode<K, V>>) -> FundedBox<MutableNode<K, V>> {
    repair_height(&mut node);
    if mutable_height(&node.get().left) > mutable_height(&node.get().right) + 1 {
        let left = node.get().left.as_ref().unwrap().get();
        if mutable_height(&left.right) > mutable_height(&left.left) {
            let left = node.get_mut().left.take().unwrap();
            node.get_mut().left = Some(rotate_left(left));
        }
        return rotate_right(node);
    }
    if mutable_height(&node.get().right) > mutable_height(&node.get().left) + 1 {
        let right = node.get().right.as_ref().unwrap().get();
        if mutable_height(&right.left) > mutable_height(&right.right) {
            let right = node.get_mut().right.take().unwrap();
            node.get_mut().right = Some(rotate_right(right));
        }
        return rotate_left(node);
    }
    node
}
// Terminal extraction retains every original funded node. These total helpers
// contain no payload destruction, admission or asserting rotation. Height
// saturation is defensive; an installed AVL has logarithmic height.
fn repair_height_nonfallible<K, V>(node: &mut FundedBox<MutableNode<K, V>>) {
    let n = node.get_mut();
    n.height = mutable_height(&n.left)
        .max(mutable_height(&n.right))
        .saturating_add(1);
}
fn rotate_left_nonfallible<K, V>(
    mut node: FundedBox<MutableNode<K, V>>,
) -> FundedBox<MutableNode<K, V>> {
    let Some(mut next) = node.get_mut().right.take() else {
        return node;
    };
    node.get_mut().right = next.get_mut().left.take();
    repair_height_nonfallible(&mut node);
    next.get_mut().left = Some(node);
    repair_height_nonfallible(&mut next);
    next
}
fn rotate_right_nonfallible<K, V>(
    mut node: FundedBox<MutableNode<K, V>>,
) -> FundedBox<MutableNode<K, V>> {
    let Some(mut next) = node.get_mut().left.take() else {
        return node;
    };
    node.get_mut().left = next.get_mut().right.take();
    repair_height_nonfallible(&mut node);
    next.get_mut().right = Some(node);
    repair_height_nonfallible(&mut next);
    next
}
fn mutable_balance_nonfallible<K, V>(
    mut node: FundedBox<MutableNode<K, V>>,
) -> FundedBox<MutableNode<K, V>> {
    repair_height_nonfallible(&mut node);
    let left_height = u32::from(mutable_height(&node.get().left));
    let right_height = u32::from(mutable_height(&node.get().right));
    if left_height > right_height + 1 {
        let needs_rotation = node.get().left.as_ref().is_some_and(|left| {
            mutable_height(&left.get().right) > mutable_height(&left.get().left)
        });
        if needs_rotation && let Some(left) = node.get_mut().left.take() {
            node.get_mut().left = Some(rotate_left_nonfallible(left));
        }
        return rotate_right_nonfallible(node);
    }
    if right_height > left_height + 1 {
        let needs_rotation = node.get().right.as_ref().is_some_and(|right| {
            mutable_height(&right.get().left) > mutable_height(&right.get().right)
        });
        if needs_rotation && let Some(right) = node.get_mut().right.take() {
            node.get_mut().right = Some(rotate_right_nonfallible(right));
        }
        return rotate_left_nonfallible(node);
    }
    node
}

// A return ticket borrows the current map, so unrelated begins before this
// validation are included and no directory mutation can follow validation.
pub(crate) struct ValidatedVacantSourceSlot<'a, V> {
    map: &'a mut MutableSourceMap<contextdb_core::TxId, V>,
    directions: [bool; 2 * usize::BITS as usize],
    depth: usize,
    new_len: usize,
}

pub(crate) fn validate_original_singleton<V>(
    entry: &PreparedMapEntry<contextdb_core::TxId, V>,
) -> Result<()> {
    let node = entry.0.get();
    if entry.0.charge.0.is_none() || node.left.is_some() || node.right.is_some() || node.height != 1
    {
        return Err(source_identity_error());
    }
    Ok(())
}

pub(crate) fn prepare_original_reinstall<V>(
    map: &mut MutableSourceMap<contextdb_core::TxId, V>,
    key: contextdb_core::TxId,
) -> Result<ValidatedVacantSourceSlot<'_, V>> {
    fn validate_node<V>(
        node: &MutableNode<contextdb_core::TxId, V>,
        lower: Option<contextdb_core::TxId>,
        upper: Option<contextdb_core::TxId>,
    ) -> Result<()> {
        let left = u32::from(mutable_height(&node.left));
        let right = u32::from(mutable_height(&node.right));
        if lower.is_some_and(|bound| node.key <= bound)
            || upper.is_some_and(|bound| node.key >= bound)
            || left.abs_diff(right) > 1
            || u32::from(node.height) != 1 + left.max(right)
            || usize::from(node.height) >= 2 * usize::BITS as usize
            || node.left.as_ref().is_some_and(|child| {
                child.get().key >= node.key || lower.is_some_and(|bound| child.get().key <= bound)
            })
            || node.right.as_ref().is_some_and(|child| {
                child.get().key <= node.key || upper.is_some_and(|bound| child.get().key >= bound)
            })
        {
            return Err(source_identity_error());
        }
        Ok(())
    }
    let new_len = map.len.checked_add(1).ok_or_else(source_identity_error)?;
    if map.root.is_none() != (map.len == 0) {
        return Err(source_identity_error());
    }
    let mut directions = [false; 2 * usize::BITS as usize];
    let mut depth = 0;
    let mut lower = None;
    let mut upper = None;
    let mut cursor = map.root.as_ref();
    while let Some(node) = cursor {
        let node = node.get();
        validate_node(node, lower, upper)?;
        // These children include every neighborhood used by a single/double
        // total AVL rotation on this spine after the leaf is installed.
        if let Some(left) = node.left.as_ref() {
            validate_node(left.get(), lower, Some(node.key))?;
        }
        if let Some(right) = node.right.as_ref() {
            validate_node(right.get(), Some(node.key), upper)?;
        }
        let right = match key.cmp(&node.key) {
            Ordering::Equal => return Err(source_identity_error()),
            Ordering::Less => false,
            Ordering::Greater => true,
        };
        *directions
            .get_mut(depth)
            .ok_or_else(source_identity_error)? = right;
        depth += 1;
        if depth > map.len {
            return Err(source_identity_error());
        }
        if right {
            lower = Some(node.key);
            cursor = node.right.as_ref();
        } else {
            upper = Some(node.key);
            cursor = node.left.as_ref();
        }
    }
    Ok(ValidatedVacantSourceSlot {
        map,
        directions,
        depth,
        new_len,
    })
}

impl<V> ValidatedVacantSourceSlot<'_, V> {
    pub(crate) fn install_nonfallible(self, entry: PreparedMapEntry<contextdb_core::TxId, V>) {
        fn install<V>(
            link: &mut MutableLink<contextdb_core::TxId, V>,
            directions: &mut std::slice::Iter<'_, bool>,
            entry: FundedBox<MutableNode<contextdb_core::TxId, V>>,
        ) {
            // Borrow the unchanged validated links before moving any ancestor.
            // An exclusive ticket proves exactly this path and its vacant leaf.
            if let Some(node) = link.as_mut() {
                let right = directions.next().is_some_and(|right| *right);
                let node = node.get_mut();
                let child = if right {
                    &mut node.right
                } else {
                    &mut node.left
                };
                install(child, directions, entry);
            } else {
                // Assignment replaces only None, never an installed payload.
                *link = Some(entry);
                return;
            }
            if let Some(node) = link.take() {
                *link = Some(mutable_balance_nonfallible(node));
            }
        }
        let Self {
            map,
            directions,
            depth,
            new_len,
        } = self;
        // Build the bounded iterator before moving the original entry.
        let mut path = directions[..depth].iter();
        install(&mut map.root, &mut path, entry.0);
        map.len = new_len;
    }
}

impl<K: Ord, V> MutableSourceMap<K, V> {
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn floor(&self, upper: std::ops::Bound<K>) -> Option<(K, V)>
    where
        K: Copy,
        V: Copy,
    {
        let mut cursor = self.root.as_ref();
        let mut candidate = None;
        while let Some(node) = cursor {
            let current = node.get();
            let allowed = match &upper {
                std::ops::Bound::Unbounded => true,
                std::ops::Bound::Included(key) => current.key <= *key,
                std::ops::Bound::Excluded(key) => current.key < *key,
            };
            if allowed {
                candidate = Some((current.key, current.value));
                cursor = current.right.as_ref();
            } else {
                cursor = current.left.as_ref();
            }
        }
        candidate
    }

    /// Return the first entry in the supplied bounds that follows `after`.
    /// The commit-directory cursor keeps only these scalar bounds; it does not
    /// duplicate history or retain an allocation-sized traversal stack.
    pub(crate) fn next_in_range(
        &self,
        lower: std::ops::Bound<K>,
        upper: std::ops::Bound<K>,
        after: Option<K>,
    ) -> Option<(K, V)>
    where
        K: Copy,
        V: Copy,
    {
        let mut cursor = self.root.as_ref();
        let mut candidate = None;
        while let Some(node) = cursor {
            let node = node.get();
            let after_ok = after.is_none_or(|key| node.key > key);
            let lower_ok = match lower {
                std::ops::Bound::Unbounded => true,
                std::ops::Bound::Included(key) => node.key >= key,
                std::ops::Bound::Excluded(key) => node.key > key,
            };
            let upper_ok = match upper {
                std::ops::Bound::Unbounded => true,
                std::ops::Bound::Included(key) => node.key <= key,
                std::ops::Bound::Excluded(key) => node.key < key,
            };
            if after_ok && lower_ok && upper_ok {
                candidate = Some((node.key, node.value));
                cursor = node.left.as_ref();
            } else if after.is_some_and(|key| node.key <= key)
                || matches!(lower, std::ops::Bound::Included(key) if node.key < key)
                || matches!(lower, std::ops::Bound::Excluded(key) if node.key <= key)
            {
                cursor = node.right.as_ref();
            } else {
                cursor = node.left.as_ref();
            }
        }
        candidate
    }

    pub(crate) fn prepare(
        key: K,
        value: V,
        funder: &SourceFunder,
    ) -> Result<PreparedMapEntry<K, V>> {
        FundedBox::try_new(
            MutableNode {
                key,
                value,
                left: None,
                right: None,
                height: 1,
            },
            funder,
        )
        .map(PreparedMapEntry)
    }
    pub(crate) fn insert_prepared(&mut self, entry: PreparedMapEntry<K, V>) {
        fn put<K: Ord, V>(
            root: MutableLink<K, V>,
            entry: FundedBox<MutableNode<K, V>>,
        ) -> FundedBox<MutableNode<K, V>> {
            let Some(mut node) = root else { return entry };
            match entry.get().key.cmp(&node.get().key) {
                Ordering::Less => {
                    let left = node.get_mut().left.take();
                    node.get_mut().left = Some(put(left, entry));
                }
                Ordering::Greater => {
                    let right = node.get_mut().right.take();
                    node.get_mut().right = Some(put(right, entry));
                }
                Ordering::Equal => panic!("prepared source host already exists"),
            }
            mutable_balance(node)
        }
        self.root = Some(put(self.root.take(), entry.0));
        self.len += 1;
    }
    pub(crate) fn get(&self, key: &K) -> Option<&V> {
        let mut cursor = self.root.as_ref();
        while let Some(node) = cursor {
            let n = node.get();
            match key.cmp(&n.key) {
                Ordering::Less => cursor = n.left.as_ref(),
                Ordering::Greater => cursor = n.right.as_ref(),
                Ordering::Equal => return Some(&n.value),
            }
        }
        None
    }
    pub(crate) fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let mut cursor = self.root.as_mut();
        while let Some(node) = cursor {
            let n = node.get_mut();
            match key.cmp(&n.key) {
                Ordering::Less => cursor = n.left.as_mut(),
                Ordering::Greater => cursor = n.right.as_mut(),
                Ordering::Equal => return Some(&mut n.value),
            }
        }
        None
    }
    pub(crate) fn take_entry(&mut self, key: &K) -> Option<PreparedMapEntry<K, V>> {
        fn take_first<K, V>(
            mut node: FundedBox<MutableNode<K, V>>,
        ) -> (MutableLink<K, V>, FundedBox<MutableNode<K, V>>) {
            let Some(left) = node.get_mut().left.take() else {
                let right = node.get_mut().right.take();
                return (right, node);
            };
            let (next, first) = take_first(left);
            node.get_mut().left = next;
            (Some(mutable_balance(node)), first)
        }
        fn take<K: Ord, V>(
            root: MutableLink<K, V>,
            key: &K,
        ) -> (MutableLink<K, V>, Option<PreparedMapEntry<K, V>>) {
            let Some(mut node) = root else {
                return (None, None);
            };
            match key.cmp(&node.get().key) {
                Ordering::Less => {
                    let (next, value) = take(node.get_mut().left.take(), key);
                    node.get_mut().left = next;
                    (Some(mutable_balance(node)), value)
                }
                Ordering::Greater => {
                    let (next, value) = take(node.get_mut().right.take(), key);
                    node.get_mut().right = next;
                    (Some(mutable_balance(node)), value)
                }
                Ordering::Equal => {
                    let left = node.get_mut().left.take();
                    let right = node.get_mut().right.take();
                    let next = match (right, left) {
                        (None, left) => left,
                        (Some(right), left) => {
                            let (rest, mut first) = take_first(right);
                            first.get_mut().left = left;
                            first.get_mut().right = rest;
                            Some(mutable_balance(first))
                        }
                    };
                    node.get_mut().height = 1;
                    (next, Some(PreparedMapEntry(node)))
                }
            }
        }
        let (root, value) = take(self.root.take(), key);
        self.root = root;
        if value.is_some() {
            self.len -= 1
        };
        value
    }
    /// Total original-node extraction for the paired terminal path. The
    /// terminal caller uses TxId (integer comparison); arbitrary K::cmp is not
    /// covered by the no-panic claim. Every link moves into the map or result.
    pub(crate) fn take_entry_nonfallible(&mut self, key: &K) -> Option<PreparedMapEntry<K, V>> {
        fn take_first<K, V>(
            mut node: FundedBox<MutableNode<K, V>>,
        ) -> (MutableLink<K, V>, FundedBox<MutableNode<K, V>>) {
            let Some(left) = node.get_mut().left.take() else {
                let right = node.get_mut().right.take();
                return (right, node);
            };
            let (next, first) = take_first(left);
            node.get_mut().left = next;
            (Some(mutable_balance_nonfallible(node)), first)
        }
        fn take<K: Ord, V>(
            root: MutableLink<K, V>,
            key: &K,
        ) -> (MutableLink<K, V>, Option<PreparedMapEntry<K, V>>) {
            let Some(mut node) = root else {
                return (None, None);
            };
            match key.cmp(&node.get().key) {
                Ordering::Less => {
                    let (next, value) = take(node.get_mut().left.take(), key);
                    node.get_mut().left = next;
                    (Some(mutable_balance_nonfallible(node)), value)
                }
                Ordering::Greater => {
                    let (next, value) = take(node.get_mut().right.take(), key);
                    node.get_mut().right = next;
                    (Some(mutable_balance_nonfallible(node)), value)
                }
                Ordering::Equal => {
                    let left = node.get_mut().left.take();
                    let right = node.get_mut().right.take();
                    let next = match (right, left) {
                        (None, left) => left,
                        (Some(right), left) => {
                            let (rest, mut first) = take_first(right);
                            first.get_mut().left = left;
                            first.get_mut().right = rest;
                            Some(mutable_balance_nonfallible(first))
                        }
                    };
                    node.get_mut().height = 1;
                    (next, Some(PreparedMapEntry(node)))
                }
            }
        }
        let (root, value) = take(self.root.take(), key);
        self.root = root;
        if value.is_some() {
            self.len = self.len.saturating_sub(1)
        };
        value
    }
    pub(crate) fn remove(&mut self, key: &K) -> Option<V> {
        self.take_entry(key).map(|entry| entry.0.into_inner().value)
    }
    pub(crate) fn first_key(&self) -> Option<&K> {
        let mut cursor = self.root.as_ref()?;
        while let Some(left) = cursor.get().left.as_ref() {
            cursor = left
        }
        Some(&cursor.get().key)
    }
    pub(crate) fn visit_mut(&mut self, f: &mut impl FnMut(&K, &mut V) -> Result<()>) -> Result<()> {
        fn visit<K, V>(
            root: &mut MutableLink<K, V>,
            f: &mut impl FnMut(&K, &mut V) -> Result<()>,
        ) -> Result<()> {
            if let Some(node) = root {
                let n = node.get_mut();
                visit(&mut n.left, f)?;
                f(&n.key, &mut n.value)?;
                visit(&mut n.right, f)?
            }
            Ok(())
        }
        visit(&mut self.root, f)
    }
}

/// A genuinely copied payload and its receiver-bound admission. Field order
/// destroys every native payload allocation before refunding its invoice.
#[doc(hidden)]
#[derive(Debug)]
pub struct AdmittedSourceRow {
    pub(crate) row: contextdb_core::VersionedRow,
    pub(crate) invoice: OriginalRowInvoice,
}
impl AdmittedSourceRow {
    pub fn new(row: contextdb_core::VersionedRow, invoice: OriginalRowInvoice) -> Self {
        Self { row, invoice }
    }
}
#[doc(hidden)]
#[derive(Debug)]
pub struct AdmittedSourceVector {
    pub(crate) entry: contextdb_core::VectorEntry,
    pub(crate) invoice: OriginalVectorInvoice,
}
impl AdmittedSourceVector {
    pub fn new(entry: contextdb_core::VectorEntry, invoice: OriginalVectorInvoice) -> Self {
        Self { entry, invoice }
    }
}
#[doc(hidden)]
#[derive(Debug)]
pub struct AdmittedSourceKey {
    pub(crate) key: contextdb_core::IndexKey,
    pub(crate) charge: SourceCharge,
}
impl AdmittedSourceKey {
    pub fn new(key: contextdb_core::IndexKey, charge: SourceCharge) -> Self {
        Self { key, charge }
    }
}

pub(crate) fn entry_by<K, V>(
    root: &Link<K, V>,
    cmp: impl Fn(&K) -> Ordering,
) -> Option<&Entry<K, V>> {
    let mut cursor = root.as_deref();
    while let Some(node) = cursor {
        match cmp(&node.entry.key) {
            Ordering::Less => cursor = node.left.as_deref(),
            Ordering::Greater => cursor = node.right.as_deref(),
            Ordering::Equal => return Some(&node.entry),
        }
    }
    None
}
impl SourceRoot {
    pub(crate) fn table_name(&self, table: &str) -> Option<SourceName> {
        entry_by(&self.tables, |name| table.cmp(name.as_str())).map(|entry| entry.key.clone())
    }
}

impl CanonicalStatementRows {
    pub(crate) fn identity(&self) -> &CapturedIdentity {
        &self.descriptor().identity
    }
    pub(crate) fn cell(&self, row: contextdb_core::RowId) -> Option<&RowCellHandle> {
        get_by(&self.descriptor().root.as_ref()?.by_id, |key| row.cmp(key))
    }
}

impl TableRoot {
    pub(crate) fn prepare_identity(
        &self,
        cell: RowCellHandle,
        funder: &SourceFunder,
    ) -> Result<FundedArc<Self>> {
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        let by_id = insert(
            &self.by_id,
            FundedArc::try_new(
                Entry {
                    key: cell.identity.row,
                    value: cell,
                },
                funder.clone(),
            )?,
            funder,
        )?;
        Ok(FundedArc::from_prepared(
            Self {
                by_id,
                by_sequence: self.by_sequence.clone(),
                indexes: self.indexes.clone(),
                next_sequence: self.next_sequence,
            },
            control,
        ))
    }
}
impl CanonicalVectorRef {
    pub(crate) fn with_owner(&self, owner: VectorCellHandle) -> Self {
        let reference = match &self.reference {
            VectorReference::Owned(_) => VectorReference::Owned(owner.clone()),
            VectorReference::Scalar { row, column } => VectorReference::Scalar {
                row: row.clone(),
                column: column.clone(),
            },
        };
        Self {
            row: self.row,
            owner,
            reference,
        }
    }
}

/// Only the genuine original reservation implements this receiver operation.
/// Native control and its original writer registration are retained by the
/// opaque bridge; callers never receive its owner Arc or a Weak reference.
#[doc(hidden)]
pub trait OriginalCopyChargeSource: Send + Sync {
    #[doc(hidden)]
    fn original_source_origin(&self) -> Option<NativeSourceOrigin> {
        None
    }
    #[doc(hidden)]
    fn retained_control_bytes(&self) -> Option<usize> {
        None
    }
    #[doc(hidden)]
    fn visit_retained_controls<'a>(
        &'a self,
        _visit: &mut dyn FnMut(RetainedControl<'a>),
    ) -> Option<()> {
        None
    }
    /// Copy through the retained original owner. Legacy implementors opt out
    /// without allocating a default copy or constructing an error.
    #[doc(hidden)]
    fn copy_original_row_for(
        &self,
        row: &contextdb_core::VersionedRow,
        funder: &SourceFunder,
    ) -> Option<Result<AdmittedSourceRow>> {
        let _ = (row, funder);
        None
    }
    /// The selected original receiver admits the complete vector allocation.
    #[doc(hidden)]
    fn copy_original_vector_for(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &SourceFunder,
    ) -> Option<Result<AdmittedSourceVector>> {
        let _ = (entry, funder);
        None
    }
    /// Replacement content stays borrowed until the original receiver admits it.
    #[doc(hidden)]
    fn copy_original_vector_input_for(
        &self,
        input: BorrowedOriginalVectorCopyInput<'_>,
        funder: &SourceFunder,
    ) -> Option<Result<AdmittedSourceVector>> {
        let _ = (input, funder);
        None
    }
    fn reserve_private_original_copy(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.reserve_original_copy(bytes)
            .map(OriginalCopyGrant::from_host)
    }
    fn reserve_original_copy(&self, bytes: usize) -> Result<Arc<dyn crate::StagedPostingCharge>>;
}
#[doc(hidden)]
pub struct OriginalCopyBridge {
    data: Option<Arc<dyn OriginalCopyChargeSource>>,
    finish: fn(Arc<dyn OriginalCopyChargeSource>),
    charge: Option<SourceCharge>,
}
impl OriginalCopyBridge {
    pub fn retained_bytes(&self) -> usize {
        self.charge
            .as_ref()
            .expect("live original copy bridge charge")
            .bytes()
    }
    pub fn retained_original_control_bytes(&self) -> Option<usize> {
        self.data
            .as_ref()
            .expect("live original copy bridge")
            .retained_control_bytes()
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.charge
            .as_ref()
            .expect("live original copy bridge charge")
            .visit_retained_debits(visit);
        visit(RetainedDebit {
            owner: DebitOwner::BridgeOriginal(self),
            bytes: self.retained_original_control_bytes(),
        });
        if self
            .data
            .as_ref()
            .expect("live original copy bridge")
            .original_source_origin()
            .is_none()
        {
            visit(RetainedDebit {
                owner: DebitOwner::BridgeLower(self),
                bytes: None,
            });
        }
        if self
            .data
            .as_ref()
            .expect("live original copy bridge")
            .visit_retained_controls(&mut |control| {
                visit(RetainedDebit {
                    bytes: Some(control.bytes),
                    owner: DebitOwner::Lower(control),
                });
            })
            .is_none()
        {
            visit(RetainedDebit {
                owner: DebitOwner::BridgeLower(self),
                bytes: None,
            });
        }
    }
    pub fn prepare<T: OriginalCopyChargeSource + 'static>(
        owner: T,
        funder: Arc<dyn SourceFunding>,
    ) -> Result<Self> {
        Self::prepare_funded(owner, SourceFunder::from_host(funder))
    }
    pub fn reserve_private_original_copy(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.data
            .as_ref()
            .expect("live original copy bridge")
            .reserve_private_original_copy(bytes)
    }
    pub fn prepare_funded<T: OriginalCopyChargeSource + 'static>(
        owner: T,
        funder: SourceFunder,
    ) -> Result<Self> {
        fn finish<T: OriginalCopyChargeSource + 'static>(
            receiver: Arc<dyn OriginalCopyChargeSource>,
        ) {
            // SAFETY: prepare_funded creates exactly Arc<T>. Every private
            // count retains this concrete finalizer, with no Weak or raw alias.
            let raw = Arc::into_raw(receiver) as *const T;
            let typed = unsafe { Arc::from_raw(raw) };
            drop(Arc::into_inner(typed));
        }
        let charge = SourceCharge::admit_funded(funder, source_arc_layout::<T>().size())?;
        Ok(Self {
            data: Some(Arc::new(owner)),
            finish: finish::<T>,
            charge: Some(charge),
        })
    }
    /// Standard terminal output only. Retained native engine holders use the
    /// opaque reserve_private_original_copy companion instead.
    pub fn reserve_original_copy(
        &self,
        bytes: usize,
    ) -> Result<Arc<dyn crate::StagedPostingCharge>> {
        self.data
            .as_ref()
            .expect("live original copy bridge")
            .reserve_original_copy(bytes)
    }
}
impl fmt::Debug for OriginalCopyBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalCopyBridge").finish_non_exhaustive()
    }
}
impl Clone for OriginalCopyBridge {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            finish: self.finish,
            charge: self.charge.clone(),
        }
    }
}
impl Drop for OriginalCopyBridge {
    fn drop(&mut self) {
        if let Some(receiver) = self.data.take() {
            (self.finish)(receiver);
        }
        drop(self.charge.take());
    }
}
/// A genuine eager copy. Refusal/unwind destroys its native row before either
/// its transferred body debit or original reserve-copy metadata is released.
#[doc(hidden)]
pub struct AdmittedEagerSourceRow {
    pub(crate) row: contextdb_core::VersionedRow,
    pub(crate) invoice: OriginalRowInvoice,
    pub(crate) original_copy: OriginalCopyBridge,
}
impl AdmittedEagerSourceRow {
    pub(crate) fn copy_private_original_grant(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.original_copy.reserve_private_original_copy(bytes)
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.invoice.visit_retained_debits(visit);
        self.original_copy.visit_retained_debits(visit);
    }
    pub(crate) fn reserve_private_original_copy(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.copy_private_original_grant(bytes)
    }
    pub fn new(
        row: contextdb_core::VersionedRow,
        invoice: OriginalRowInvoice,
        original_copy: OriginalCopyBridge,
    ) -> Self {
        Self {
            row,
            invoice,
            original_copy,
        }
    }
    pub(crate) fn row(&self) -> &contextdb_core::VersionedRow {
        &self.row
    }
    pub(crate) fn retained_bytes(&self) -> usize {
        self.invoice.retained_bytes().saturating_add(
            self.original_copy
                .charge
                .as_ref()
                .expect("live original copy bridge charge")
                .bytes(),
        )
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn reserve_original_copy(
        &self,
        bytes: usize,
    ) -> Result<Arc<dyn crate::StagedPostingCharge>> {
        self.original_copy.reserve_original_copy(bytes)
    }
}
/// Genuine original-backed owned vector. Field order frees the payload first.
#[doc(hidden)]
#[derive(Debug)]
pub struct AdmittedEagerSourceVector {
    pub(crate) entry: contextdb_core::VectorEntry,
    pub(crate) invoice: OriginalVectorInvoice,
    pub(crate) original_copy: OriginalCopyBridge,
}
impl AdmittedEagerSourceVector {
    /// Check actual origin and debit price before any native key/root creation.
    pub(crate) fn validate_original_pairing(&self, funder: &SourceFunder) -> Result<()> {
        let invoice = self
            .invoice
            .charge
            .0
            .as_ref()
            .expect("live original vector invoice");
        let bridge_invoice = self
            .original_copy
            .charge
            .as_ref()
            .expect("live original bridge charge")
            .0
            .as_ref()
            .expect("live bridge invoice");
        let same_receiver = Arc::ptr_eq(invoice.funder.receiver(), funder.receiver())
            && Arc::ptr_eq(bridge_invoice.funder.receiver(), funder.receiver());
        let same_lease = invoice.lease.identity == bridge_invoice.lease.identity
            && invoice.lease.epoch == bridge_invoice.lease.epoch;
        let original = self
            .original_copy
            .data
            .as_ref()
            .expect("live original bridge")
            .original_source_origin();
        if !same_receiver
            || !same_lease
            || original
                .is_none_or(|origin| !funder.matches_original_source_origin(origin, invoice.lease))
        {
            return Err(Error::Other(
                "original eager vector funding mismatch".into(),
            ));
        }
        let entry = self.entry();
        if entry.index.table.capacity() != entry.index.table.len()
            || entry.index.column.capacity() != entry.index.column.len()
            || entry.vector.capacity() != entry.vector.len()
        {
            return Err(Error::Other("source native capacity mismatch".into()));
        }
        let vector = Layout::array::<f32>(entry.vector.len())
            .map_err(|_| Error::Other("source allocation size overflow".into()))?
            .size();
        let expected = entry
            .index
            .table
            .len()
            .checked_add(entry.index.column.len())
            .and_then(|bytes| bytes.checked_add(vector))
            .and_then(|bytes| bytes.checked_add(source_arc_layout::<SourceInvoice>().size()))
            .ok_or_else(|| Error::Other("source allocation size overflow".into()))?;
        if invoice.bytes != expected {
            return Err(Error::Other(
                "original eager vector debit price mismatch".into(),
            ));
        }
        Ok(())
    }
    pub fn new(
        entry: contextdb_core::VectorEntry,
        invoice: OriginalVectorInvoice,
        original_copy: OriginalCopyBridge,
    ) -> Self {
        Self {
            entry,
            invoice,
            original_copy,
        }
    }
    pub(crate) fn entry(&self) -> &contextdb_core::VectorEntry {
        &self.entry
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn reserve_private_original_copy(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.copy_private_original_grant(bytes)
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn copy_private_original_grant(&self, bytes: usize) -> Result<OriginalCopyGrant> {
        self.original_copy.reserve_private_original_copy(bytes)
    }
    // activated by bounded-storage S3/S4; remove allow at activation
    #[allow(dead_code)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.invoice
            .retained_bytes()
            .saturating_add(self.original_copy.retained_bytes())
    }
    pub(crate) fn visit_retained_debits<'a>(&'a self, visit: &mut dyn FnMut(RetainedDebit<'a>)) {
        self.invoice.visit_retained_debits(visit);
        self.original_copy.visit_retained_debits(visit);
    }
}
impl fmt::Debug for AdmittedEagerSourceRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdmittedEagerSourceRow")
            .field("row", &self.row)
            .field("invoice", &self.invoice)
            .field("original_copy", &self.original_copy)
            .finish()
    }
}

impl RowCell {
    /// The reviewed producer revision supplies an immutable original here;
    /// the ordinary WriteSet body must have its own independently held debit.
    pub(crate) fn sealed(
        identity: RowIdentity,
        keys: SourceBuffer<(SourceName, PostingHandle)>,
        original: AdmittedSourceRow,
        funder: &SourceFunder,
    ) -> Result<RowCellHandle> {
        if !identity.matches_row(&original.row) {
            return Err(Error::Other(
                "captured staged source identity mismatch".into(),
            ));
        }
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        Ok(FundedArc::from_prepared(
            Self {
                identity,
                keys,
                custody: parking_lot::Mutex::new(Some(RowCustody::Sealed {
                    row: original.row,
                    _invoice: original.invoice,
                })),
            },
            control,
        ))
    }
}
impl VectorCell {
    pub(crate) fn sealed(
        identity: VectorIdentity,
        original: AdmittedSourceVector,
        funder: &SourceFunder,
    ) -> Result<VectorCellHandle> {
        if !identity.matches_entry(&original.entry) {
            return Err(Error::Other(
                "captured staged source identity mismatch".into(),
            ));
        }
        let control = FundedArc::<Self>::prepare(funder.clone())?;
        Ok(FundedArc::from_prepared(
            Self {
                identity,
                custody: parking_lot::Mutex::new(Some(VectorCustody::Sealed {
                    entry: original.entry,
                    _invoice: original.invoice,
                })),
            },
            control,
        ))
    }
}

/// Actual shared lower allocations. Roles keep separate controls disjoint.
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RetainedControlKind {
    Accountant,
    Limit,
    Counter,
    WriterRegistration,
}
/// An opaque key from a real pointee. It is informational, never ownership.
#[doc(hidden)]
pub struct RetainedControlIdentity {
    pointer: *const (),
}
impl RetainedControlIdentity {
    pub fn from_borrowed<T: ?Sized>(owner: &T) -> Self {
        Self {
            pointer: std::ptr::from_ref(owner).cast::<()>(),
        }
    }
    fn same_owner(&self, other: &Self) -> bool {
        std::ptr::eq(self.pointer, other.pointer)
    }
}
/// The actual provider keeps typed allocation observation inside its crate.
#[doc(hidden)]
pub trait RetainedControlSource: Send + Sync {
    fn retained_control_identity(
        &self,
        kind: RetainedControlKind,
    ) -> Option<RetainedControlIdentity>;
}
/// Informational borrowed identity; no owner address or capability is exposed.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct RetainedControl<'a> {
    kind: RetainedControlKind,
    owner: &'a dyn RetainedControlSource,
    bytes: usize,
}
impl<'a> RetainedControl<'a> {
    /// Providers borrow existing typed observers, not movable handle addresses.
    pub fn from_borrowed(
        kind: RetainedControlKind,
        owner: &'a dyn RetainedControlSource,
        bytes: usize,
    ) -> Self {
        Self { kind, owner, bytes }
    }
    fn same_owner(self, other: Self) -> Option<bool> {
        if self.kind != other.kind {
            return Some(false);
        }
        let a = self.owner.retained_control_identity(self.kind)?;
        let b = other.owner.retained_control_identity(other.kind)?;
        Some(a.same_owner(&b))
    }
}
#[derive(Clone, Copy)]
enum DebitOwner<'a> {
    Source(&'a SourceCharge),
    GrantPayload(&'a OriginalCopyGrant),
    GrantControl(&'a OriginalCopyGrant),
    GrantLower(&'a OriginalCopyGrant),
    BridgeOriginal(&'a OriginalCopyBridge),
    BridgeLower(&'a OriginalCopyBridge),
    Provider(&'a SourceFunder),
    ProviderLower(&'a SourceFunder),
    Lower(RetainedControl<'a>),
}
#[derive(Clone, Copy)]
pub(crate) struct RetainedDebit<'a> {
    owner: DebitOwner<'a>,
    bytes: Option<usize>,
}
impl RetainedDebit<'_> {
    fn same_owner(self, other: Self) -> Option<bool> {
        Some(match (self.owner, other.owner) {
            (DebitOwner::Source(a), DebitOwner::Source(b)) => Arc::ptr_eq(
                a.0.as_ref().expect("live invoice"),
                b.0.as_ref().expect("live invoice"),
            ),
            (DebitOwner::GrantPayload(a), DebitOwner::GrantPayload(b))
            | (DebitOwner::GrantControl(a), DebitOwner::GrantControl(b))
            | (DebitOwner::GrantLower(a), DebitOwner::GrantLower(b)) => {
                Arc::ptr_eq(a.receiver(), b.receiver())
            }
            (DebitOwner::BridgeOriginal(a), DebitOwner::BridgeOriginal(b))
            | (DebitOwner::BridgeLower(a), DebitOwner::BridgeLower(b)) => Arc::ptr_eq(
                a.data.as_ref().expect("live bridge"),
                b.data.as_ref().expect("live bridge"),
            ),
            (DebitOwner::Provider(a), DebitOwner::Provider(b))
            | (DebitOwner::ProviderLower(a), DebitOwner::ProviderLower(b)) => {
                Arc::ptr_eq(a.receiver(), b.receiver())
            }
            (DebitOwner::Lower(a), DebitOwner::Lower(b)) => return a.same_owner(b),
            _ => false,
        })
    }
}
/// Replay a stable borrowed inventory to count each actual liability once.
/// Unknown/inconsistent observations or overflow yield no whole-census claim.
/// No scratch allocation or admission is needed, including on restore paths.
pub(crate) fn retained_union_bytes<'a>(
    walk: impl Fn(&mut dyn FnMut(RetainedDebit<'a>)),
) -> Option<usize> {
    let mut total = Some(0usize);
    let mut ordinal = 0usize;
    walk(&mut |debit| {
        if debit.same_owner(debit).is_none() {
            total = None;
        }
        let mut prior_ordinal = 0usize;
        let mut seen = false;
        walk(&mut |prior| {
            if prior_ordinal < ordinal {
                match prior.same_owner(debit) {
                    Some(true) => {
                        seen = true;
                        if prior.bytes != debit.bytes {
                            total = None;
                        }
                    }
                    None => total = None,
                    Some(false) => (),
                }
            }
            prior_ordinal += 1;
        });
        match debit.bytes {
            None => total = None,
            Some(bytes) if !seen => total = total.and_then(|total| total.checked_add(bytes)),
            Some(_) => (),
        }
        ordinal += 1;
    });
    total
}

/// Actual auxiliary payload admission; never a price-derived refund.
#[doc(hidden)]
pub struct OriginalAuxInvoice {
    charge: SourceCharge,
}
impl OriginalAuxInvoice {
    pub fn from_charge(charge: SourceCharge) -> Self {
        Self { charge }
    }
    pub(crate) fn matches(&self, funder: &SourceFunder) -> bool {
        self.charge.matches_funder(funder)
    }
}
#[doc(hidden)]
pub enum AuxBody {
    RelationalDelete(
        (
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::TxId,
        ),
    ),
    Predicate(crate::write_set::RelationalDeletePredicate),
    AdjInsert(contextdb_core::AdjEntry),
    AdjDelete(
        (
            contextdb_core::NodeId,
            contextdb_core::EdgeType,
            contextdb_core::NodeId,
            contextdb_core::TxId,
        ),
    ),
    VectorDelete(
        (
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::TxId,
        ),
    ),
    VectorMove(
        (
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::RowId,
            contextdb_core::TxId,
        ),
    ),
    Provenance(
        (
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::Lsn,
            u8,
        ),
    ),
    ConfigWrite((String, Vec<u8>)),
    ConfigDelete(String),
    ConfigMaxU64Key(String),
}
#[doc(hidden)]
pub enum AuxBodies {
    RelationalDeletes(
        Vec<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Predicates(Vec<crate::write_set::RelationalDeletePredicate>),
    AdjInserts(Vec<contextdb_core::AdjEntry>),
    AdjDeletes(
        Vec<(
            contextdb_core::NodeId,
            contextdb_core::EdgeType,
            contextdb_core::NodeId,
            contextdb_core::TxId,
        )>,
    ),
    VectorDeletes(
        Vec<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    VectorMoves(
        Vec<(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )>,
    ),
    Provenance(
        Vec<(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::Lsn,
            u8,
        )>,
    ),
    ConfigWrites(Vec<(String, Vec<u8>)>),
    ConfigDeletes(Vec<String>),
    ConfigMaxU64Keys(Vec<String>),
}
#[doc(hidden)]
#[derive(Clone, Copy)]
pub enum AuxBorrow<'a> {
    RelationalDeletes(
        &'a [(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )],
    ),
    Predicates(&'a [crate::write_set::RelationalDeletePredicate]),
    AdjInserts(&'a [contextdb_core::AdjEntry]),
    AdjDeletes(
        &'a [(
            contextdb_core::NodeId,
            contextdb_core::EdgeType,
            contextdb_core::NodeId,
            contextdb_core::TxId,
        )],
    ),
    VectorDeletes(
        &'a [(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )],
    ),
    VectorMoves(
        &'a [(
            contextdb_core::VectorIndexRef,
            contextdb_core::RowId,
            contextdb_core::RowId,
            contextdb_core::TxId,
        )],
    ),
    Provenance(
        &'a [(
            contextdb_core::TableName,
            contextdb_core::RowId,
            contextdb_core::Lsn,
            u8,
        )],
    ),
    ConfigWrites(&'a [(String, Vec<u8>)]),
    ConfigDeletes(&'a [String]),
    ConfigMaxU64Keys(&'a [String]),
}
impl AuxBodies {
    pub fn borrow(&self) -> AuxBorrow<'_> {
        match self {
            Self::RelationalDeletes(v) => AuxBorrow::RelationalDeletes(v),
            Self::Predicates(v) => AuxBorrow::Predicates(v),
            Self::AdjInserts(v) => AuxBorrow::AdjInserts(v),
            Self::AdjDeletes(v) => AuxBorrow::AdjDeletes(v),
            Self::VectorDeletes(v) => AuxBorrow::VectorDeletes(v),
            Self::VectorMoves(v) => AuxBorrow::VectorMoves(v),
            Self::Provenance(v) => AuxBorrow::Provenance(v),
            Self::ConfigWrites(v) => AuxBorrow::ConfigWrites(v),
            Self::ConfigDeletes(v) => AuxBorrow::ConfigDeletes(v),
            Self::ConfigMaxU64Keys(v) => AuxBorrow::ConfigMaxU64Keys(v),
        }
    }
    pub fn len(&self) -> usize {
        match self {
            Self::RelationalDeletes(v) => v.len(),
            Self::Predicates(v) => v.len(),
            Self::AdjInserts(v) => v.len(),
            Self::AdjDeletes(v) => v.len(),
            Self::VectorDeletes(v) => v.len(),
            Self::VectorMoves(v) => v.len(),
            Self::Provenance(v) => v.len(),
            Self::ConfigWrites(v) => v.len(),
            Self::ConfigDeletes(v) => v.len(),
            Self::ConfigMaxU64Keys(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
impl AuxBorrow<'_> {
    pub fn len(&self) -> usize {
        match self {
            Self::RelationalDeletes(v) => v.len(),
            Self::Predicates(v) => v.len(),
            Self::AdjInserts(v) => v.len(),
            Self::AdjDeletes(v) => v.len(),
            Self::VectorDeletes(v) => v.len(),
            Self::VectorMoves(v) => v.len(),
            Self::Provenance(v) => v.len(),
            Self::ConfigWrites(v) => v.len(),
            Self::ConfigDeletes(v) => v.len(),
            Self::ConfigMaxU64Keys(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
/// Fields destroy bodies before element invoices and native buffer credit.
#[doc(hidden)]
pub struct AdmittedAuxBatch {
    pub(crate) bodies: AuxBodies,
    pub(crate) invoices: SourceBuffer<OriginalAuxInvoice>,
    pub(crate) buffer_credit: Option<SourceCharge>,
}
impl AdmittedAuxBatch {
    /// Both Vec allocations and every native body are already admitted.
    pub fn from_parts(
        bodies: AuxBodies,
        invoices: Vec<OriginalAuxInvoice>,
        buffer_credit: SourceCharge,
    ) -> Self {
        Self {
            bodies,
            invoices: SourceBuffer {
                values: invoices,
                _charge: None,
            },
            buffer_credit: Some(buffer_credit),
        }
    }
    pub(crate) fn validate(&self, funder: &SourceFunder) -> Result<()> {
        if self.bodies.len() != self.invoices.len()
            || !self
                .invoices
                .as_slice()
                .iter()
                .all(|invoice| invoice.matches(funder))
            || !self
                .buffer_credit
                .as_ref()
                .is_some_and(|c| c.matches_funder(funder))
        {
            return Err(Error::Other("auxiliary source origin mismatch".into()));
        }
        Ok(())
    }
}
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct SidecarsBorrow<'a> {
    pub lsns: &'a std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, contextdb_core::Lsn>,
    >,
    pub kinds: &'a std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, u8>,
    >,
    pub admitted: &'a std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, usize>,
    >,
}
#[doc(hidden)]
pub struct AdmittedSidecars {
    pub(crate) lsns: std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, contextdb_core::Lsn>,
    >,
    pub(crate) kinds: std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, u8>,
    >,
    pub(crate) admitted: std::collections::HashMap<
        contextdb_core::TableName,
        std::collections::HashMap<contextdb_core::RowId, usize>,
    >,
    pub(crate) credit: Option<SourceCharge>,
}
impl AdmittedSidecars {
    pub fn from_parts(
        lsns: std::collections::HashMap<
            contextdb_core::TableName,
            std::collections::HashMap<contextdb_core::RowId, contextdb_core::Lsn>,
        >,
        kinds: std::collections::HashMap<
            contextdb_core::TableName,
            std::collections::HashMap<contextdb_core::RowId, u8>,
        >,
        admitted: std::collections::HashMap<
            contextdb_core::TableName,
            std::collections::HashMap<contextdb_core::RowId, usize>,
        >,
        credit: SourceCharge,
    ) -> Self {
        Self {
            lsns,
            kinds,
            admitted,
            credit: Some(credit),
        }
    }
    pub(crate) fn validate(&self, funder: &SourceFunder) -> Result<()> {
        if !self
            .credit
            .as_ref()
            .is_some_and(|c| c.matches_funder(funder))
        {
            return Err(Error::Other("sidecar source origin mismatch".into()));
        }
        Ok(())
    }
}

// Effective pinned standard HashMap bucket/control layout for lower native
// position maps. Every arithmetic step is checked before buffer allocation.
pub(crate) fn native_map_bytes<K, V>(members: usize) -> Result<usize> {
    if members == 0 {
        return Ok(0);
    }
    let overflow = || Error::Other("source allocation size overflow".into());
    let group = if cfg!(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64")
    )) {
        16usize
    } else if cfg!(any(
        target_pointer_width = "64",
        target_arch = "aarch64",
        target_arch = "wasm32"
    )) {
        8usize
    } else {
        4usize
    };
    let buckets = if members < 4 {
        4
    } else if members < 8 {
        8
    } else if members < 15 {
        16
    } else {
        members
            .checked_mul(8)
            .ok_or_else(overflow)?
            .checked_div(7)
            .unwrap()
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
    };
    let alignment = Layout::new::<(K, V)>().align().max(group);
    let data = Layout::array::<(K, V)>(buckets)
        .map_err(|_| overflow())?
        .size();
    let offset = data.checked_add(alignment - 1).ok_or_else(overflow)? & !(alignment - 1);
    let size = offset
        .checked_add(buckets)
        .and_then(|n| n.checked_add(group))
        .ok_or_else(overflow)?;
    Layout::from_size_align(size, alignment)
        .map(|l| l.size())
        .map_err(|_| overflow())
}

impl<T> FundedArc<T> {
    pub(crate) fn is_funded_by(&self, funder: &SourceFunder) -> bool {
        self.charge
            .as_ref()
            .expect("live native control charge")
            .matches_funder(funder)
    }
}
impl AdmittedEagerSourceRow {
    pub(crate) fn validate_exact_funder(&self, funder: &SourceFunder) -> Result<()> {
        if !self.invoice.matches(funder)
            || !self
                .original_copy
                .charge
                .as_ref()
                .expect("live original bridge charge")
                .matches_funder(funder)
        {
            return Err(Error::Other("native exact row funding mismatch".into()));
        }
        Ok(())
    }
}

impl OriginalRowInvoice {
    /// The actual payload and invoice control are both reserved by O.
    #[doc(hidden)]
    pub fn original_copy_allocation_bytes(payload_bytes: usize) -> Result<usize> {
        payload_bytes
            .checked_add(source_arc_layout::<SourceInvoice>().size())
            .ok_or_else(|| Error::Other("source allocation size overflow".into()))
    }

    /// Transfer a fully admitted original allocation, including invoice control.
    ///
    /// # Safety
    /// The authentic original reservation must hold `admitted_bytes`, including
    /// the measured SourceInvoice ArcInner, under exactly `origin` and `lease`.
    /// On success the caller must disarm that debit without refunding it. On
    /// refusal it must retain it. No second refund obligation may escape.
    #[doc(hidden)]
    pub unsafe fn from_original_copy_allocation(
        funder: &SourceFunder,
        admitted_bytes: usize,
        origin: NativeSourceOrigin,
        lease: SourceLease,
    ) -> Result<Self> {
        if admitted_bytes < source_arc_layout::<SourceInvoice>().size()
            || !funder.matches_original_source_origin(origin, lease)
        {
            return Err(Error::Other(
                "native source transfer origin mismatch".into(),
            ));
        }
        struct RetainedLease<'a> {
            funder: &'a SourceFunder,
            lease: SourceLease,
            armed: bool,
        }
        impl Drop for RetainedLease<'_> {
            fn drop(&mut self) {
                if self.armed {
                    self.funder.release_source(0, self.lease);
                }
            }
        }
        funder.retain_source_lease(lease);
        let mut retained = RetainedLease {
            funder,
            lease,
            armed: true,
        };
        let invoice_funder = funder.clone();
        // Keep the invoice uninitialized through allocation: on unwind only
        // the stack guard releases the lease, and the caller still owns bytes.
        let mut allocation = Arc::<SourceInvoice>::new_uninit();
        Arc::get_mut(&mut allocation)
            .expect("private original invoice has one owner")
            .write(SourceInvoice {
                funder: invoice_funder,
                lease,
                bytes: admitted_bytes,
            });
        // SAFETY: the unique allocation was initialized above. No fallible
        // operation separates initialization, installation and guard disarm.
        let invoice = Self::from_charge(SourceCharge(Some(unsafe { allocation.assume_init() })));
        retained.armed = false;
        Ok(invoice)
    }
}

impl OriginalCopyBridge {
    pub(crate) fn copy_original_row_for(
        &self,
        row: &contextdb_core::VersionedRow,
        funder: &SourceFunder,
    ) -> Result<AdmittedSourceRow> {
        let original = self.data.as_ref().expect("live original copy bridge");
        let charge = self
            .charge
            .as_ref()
            .expect("live original copy bridge charge");
        let invoice = charge.0.as_ref().expect("live original bridge invoice");
        if !charge.matches_funder(funder)
            || original
                .original_source_origin()
                .is_none_or(|origin| !funder.matches_original_source_origin(origin, invoice.lease))
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        let copied = original
            .copy_original_row_for(row, funder)
            .ok_or_else(crate::ordered_staged::exact_native_error)??;
        if !copied.invoice.matches(funder) {
            return Err(crate::ordered_staged::exact_native_error());
        }
        Ok(copied)
    }
}

impl AdmittedSourceRow {
    /// Replace only existing TxId scalars; the funded map and names stay put.
    #[doc(hidden)]
    pub fn rewrite_txid_placeholders(
        &mut self,
        origin_tx: contextdb_core::TxId,
        meta: &contextdb_core::TableMeta,
    ) {
        for column in meta
            .columns
            .iter()
            .filter(|column| matches!(column.column_type, contextdb_core::ColumnType::TxId))
        {
            if let Some(contextdb_core::Value::TxId(tx)) = self.row.values.get_mut(&column.name)
                && *tx == origin_tx
            {
                *tx = self.row.created_tx;
            }
        }
    }
}

/// Immutable stack projection; construction requires a validated selected scope.
#[doc(hidden)]
pub struct BorrowedOriginalVectorCopyInput<'a> {
    table: &'a str,
    column: &'a str,
    vector: &'a [f32],
    row_id: contextdb_core::RowId,
    created_tx: contextdb_core::TxId,
    deleted_tx: Option<contextdb_core::TxId>,
    lsn: contextdb_core::Lsn,
}
impl<'a> BorrowedOriginalVectorCopyInput<'a> {
    pub(crate) fn new(
        table: &'a str,
        column: &'a str,
        vector: &'a [f32],
        row_id: contextdb_core::RowId,
        created_tx: contextdb_core::TxId,
        deleted_tx: Option<contextdb_core::TxId>,
        lsn: contextdb_core::Lsn,
    ) -> Self {
        Self {
            table,
            column,
            vector,
            row_id,
            created_tx,
            deleted_tx,
            lsn,
        }
    }
    #[doc(hidden)]
    pub fn table(&self) -> &'a str {
        self.table
    }
    #[doc(hidden)]
    pub fn column(&self) -> &'a str {
        self.column
    }
    #[doc(hidden)]
    pub fn vector(&self) -> &'a [f32] {
        self.vector
    }
    #[doc(hidden)]
    pub fn row_id(&self) -> contextdb_core::RowId {
        self.row_id
    }
    #[doc(hidden)]
    pub fn created_tx(&self) -> contextdb_core::TxId {
        self.created_tx
    }
    #[doc(hidden)]
    pub fn deleted_tx(&self) -> Option<contextdb_core::TxId> {
        self.deleted_tx
    }
    #[doc(hidden)]
    pub fn lsn(&self) -> contextdb_core::Lsn {
        self.lsn
    }
}

impl OriginalVectorInvoice {
    #[doc(hidden)]
    pub fn original_copy_allocation_bytes(payload_bytes: usize) -> Result<usize> {
        payload_bytes
            .checked_add(source_arc_layout::<SourceInvoice>().size())
            .ok_or_else(|| Error::Other("source allocation size overflow".into()))
    }

    /// Transfer the original receiver's sole measured allocation.
    ///
    /// # Safety
    /// The original reservation must hold all admitted bytes, including the
    /// actual SourceInvoice ArcInner, under exactly origin and lease. Success
    /// disarms that reservation; refusal leaves its byte credit with the caller.
    #[doc(hidden)]
    pub unsafe fn from_original_copy_allocation(
        funder: &SourceFunder,
        admitted_bytes: usize,
        origin: NativeSourceOrigin,
        lease: SourceLease,
    ) -> Result<Self> {
        if admitted_bytes < source_arc_layout::<SourceInvoice>().size()
            || !funder.matches_original_source_origin(origin, lease)
        {
            return Err(Error::Other(
                "native source transfer origin mismatch".into(),
            ));
        }
        struct RetainedLease<'a> {
            funder: &'a SourceFunder,
            lease: SourceLease,
            armed: bool,
        }
        impl Drop for RetainedLease<'_> {
            fn drop(&mut self) {
                if self.armed {
                    self.funder.release_source(0, self.lease);
                }
            }
        }
        funder.retain_source_lease(lease);
        let mut retained = RetainedLease {
            funder,
            lease,
            armed: true,
        };
        let invoice_funder = funder.clone();
        let mut allocation = Arc::<SourceInvoice>::new_uninit();
        Arc::get_mut(&mut allocation)
            .expect("private original invoice has one owner")
            .write(SourceInvoice {
                funder: invoice_funder,
                lease,
                bytes: admitted_bytes,
            });
        // SAFETY: uniquely initialized; installation and disarm cannot fail.
        let invoice = Self::from_charge(SourceCharge(Some(unsafe { allocation.assume_init() })));
        retained.armed = false;
        Ok(invoice)
    }
}

impl OriginalCopyBridge {
    fn validate_vector_copy_receiver(&self, funder: &SourceFunder) -> Result<()> {
        let original = self.data.as_ref().expect("live original copy bridge");
        let charge = self
            .charge
            .as_ref()
            .expect("live original copy bridge charge");
        let invoice = charge.0.as_ref().expect("live original bridge invoice");
        if !charge.matches_funder(funder)
            || original
                .original_source_origin()
                .is_none_or(|origin| !funder.matches_original_source_origin(origin, invoice.lease))
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        Ok(())
    }
    fn validate_original_vector_copy(
        &self,
        copied: &AdmittedSourceVector,
        funder: &SourceFunder,
    ) -> Result<()> {
        let bridge = self
            .charge
            .as_ref()
            .expect("live original bridge charge")
            .0
            .as_ref()
            .expect("live original bridge invoice");
        let invoice = copied
            .invoice
            .charge
            .0
            .as_ref()
            .expect("live original vector invoice");
        let entry = &copied.entry;
        let vector_bytes = Layout::array::<f32>(entry.vector.len())
            .map_err(|_| crate::ordered_staged::exact_native_error())?
            .size();
        let payload = entry
            .index
            .table
            .len()
            .checked_add(entry.index.column.len())
            .and_then(|bytes| bytes.checked_add(vector_bytes))
            .ok_or_else(crate::ordered_staged::exact_native_error)?;
        if !copied.invoice.matches(funder)
            || invoice.lease.identity != bridge.lease.identity
            || invoice.lease.epoch != bridge.lease.epoch
            || invoice.bytes != OriginalVectorInvoice::original_copy_allocation_bytes(payload)?
            || entry.index.table.capacity() != entry.index.table.len()
            || entry.index.column.capacity() != entry.index.column.len()
            || entry.vector.capacity() != entry.vector.len()
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        Ok(())
    }
    pub(crate) fn copy_original_vector_for(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &SourceFunder,
    ) -> Result<AdmittedSourceVector> {
        self.validate_vector_copy_receiver(funder)?;
        let copied = self
            .data
            .as_ref()
            .expect("live original copy bridge")
            .copy_original_vector_for(entry, funder)
            .ok_or_else(crate::ordered_staged::exact_native_error)??;
        self.validate_original_vector_copy(&copied, funder)?;
        if !copied.invoice.matches(funder)
            || copied.entry.index != entry.index
            || copied.entry.row_id != entry.row_id
            || copied.entry.vector != entry.vector
            || copied.entry.created_tx != entry.created_tx
            || copied.entry.deleted_tx != entry.deleted_tx
            || copied.entry.lsn != entry.lsn
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        Ok(copied)
    }
    pub(crate) fn copy_original_vector_input_for(
        &self,
        input: BorrowedOriginalVectorCopyInput<'_>,
        funder: &SourceFunder,
    ) -> Result<AdmittedSourceVector> {
        self.validate_vector_copy_receiver(funder)?;
        let expected = (
            input.table,
            input.column,
            input.vector,
            input.row_id,
            input.created_tx,
            input.deleted_tx,
            input.lsn,
        );
        let copied = self
            .data
            .as_ref()
            .expect("live original copy bridge")
            .copy_original_vector_input_for(input, funder)
            .ok_or_else(crate::ordered_staged::exact_native_error)??;
        self.validate_original_vector_copy(&copied, funder)?;
        if !copied.invoice.matches(funder)
            || copied.entry.index.table != expected.0
            || copied.entry.index.column != expected.1
            || copied.entry.vector.as_slice() != expected.2
            || copied.entry.row_id != expected.3
            || copied.entry.created_tx != expected.4
            || copied.entry.deleted_tx != expected.5
            || copied.entry.lsn != expected.6
        {
            return Err(crate::ordered_staged::exact_native_error());
        }
        Ok(copied)
    }
}

impl OriginalCopyBridge {
    pub(crate) fn copy_original_eager_vector_for(
        &self,
        entry: &contextdb_core::VectorEntry,
        funder: &SourceFunder,
    ) -> Result<AdmittedEagerSourceVector> {
        let original = self.copy_original_vector_for(entry, funder)?;
        let bridge = self.clone();
        Ok(AdmittedEagerSourceVector::new(
            original.entry,
            original.invoice,
            bridge,
        ))
    }
    pub(crate) fn copy_original_eager_vector_input_for(
        &self,
        input: BorrowedOriginalVectorCopyInput<'_>,
        funder: &SourceFunder,
    ) -> Result<AdmittedEagerSourceVector> {
        let original = self.copy_original_vector_input_for(input, funder)?;
        let bridge = self.clone();
        Ok(AdmittedEagerSourceVector::new(
            original.entry,
            original.invoice,
            bridge,
        ))
    }
}

impl SourceFunder {
    /// Immutable exact-origin check for the retained original copy kernels.
    #[doc(hidden)]
    pub fn accepts_retained_original_copy(
        &self,
        origin: NativeSourceOrigin,
        lease: SourceLease,
    ) -> bool {
        self.matches_original_source_origin(origin, lease)
    }
}

pub(crate) fn changed_original_rows_identical(
    left: &contextdb_core::VersionedRow,
    right: &contextdb_core::VersionedRow,
) -> bool {
    if left.row_id != right.row_id
        || left.created_tx != right.created_tx
        || left.deleted_tx != right.deleted_tx
        || left.lsn != right.lsn
        || left.created_at != right.created_at
        || left.values.len() != right.values.len()
    {
        return false;
    }
    left.values.iter().all(|(name, value)| {
        right
            .values
            .get(name)
            .is_some_and(|other| match (value, other) {
                (contextdb_core::Value::Float64(left), contextdb_core::Value::Float64(right)) => {
                    left.to_bits() == right.to_bits()
                }
                (contextdb_core::Value::Vector(left), contextdb_core::Value::Vector(right)) => {
                    left.len() == right.len()
                        && left
                            .iter()
                            .zip(right)
                            .all(|(left, right)| left.to_bits() == right.to_bits())
                }
                _ => value == other,
            })
    })
}
