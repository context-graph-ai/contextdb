//! Private source-backed custody selection. No handler/factory activates it.
use super::Database;
use crate::{
    custody::{
        native_decode::Invalid, native_record_codec::*, native_records::*, native_selection::*,
        store::*,
    },
    executor::bounded::NativeReadOpenContext,
    owner_read::NativeReadRequest,
    persistence::writer_source::{NativeRawConfigCursor, WriterSnapshot},
};
use contextdb_core::{Incarnation, Lsn, TenantId, read_memory::ReadShared};
use std::sync::{Arc, Mutex, atomic::Ordering};

pub(crate) enum Selector<'a> {
    Prefix(&'a str),
    Group(&'a str),
}
struct Source<'a> {
    root: Arc<WriterSnapshot>,
    operation: &'a NativeReadRequest,
    context: &'a NativeReadOpenContext,
}
impl Source<'_> {
    fn cursor(&self, prefix: &str) -> Selected<ReadShared<Mutex<NativeRawConfigCursor>>> {
        Ok(self
            .root
            .native_raw_config_prefix(self.operation, self.context, prefix)?)
    }
    fn scan(
        &self,
        selectors: &[Selector<'_>],
        mut visit: impl FnMut(crate::persistence::writer_source::NativeRawConfigWindow) -> Selected<()>,
    ) -> Selected<()> {
        // Each selector seeks its own exact interval. Membership checks against
        // earlier selectors keep a union from retaining any record twice.
        for (i, selector) in selectors.iter().enumerate() {
            let covered = selectors[..i].iter().any(|earlier| match (selector, earlier) {
                (Selector::Prefix(p), Selector::Prefix(q)) => p.starts_with(*q),
                (Selector::Group(p), Selector::Group(q)) => p == q,
                _ => false,
            });
            if covered { continue; }
            let cursor = match selector {
                Selector::Prefix(prefix) => self.cursor(prefix)?,
                Selector::Group(group) => self.root.native_raw_custody_group(self.operation, self.context, group)?,
            };
            loop {
                let window = cursor.lock().unwrap_or_else(|p| p.into_inner()).visit_next(Ok)?;
                let Some(window) = window else { break };
                let selected = {
                    let view = window.view()?;
                    if !crate::custody::records::owned_key(view.key()) {
                        if matches!(selector, Selector::Group(_)) { return Err(Invalid::Canonical.into()); }
                        false
                    } else {
                        let record = decode_record(view.stored_bytes())?;
                        if let Selector::Group(group) = selector {
                            if !record.group_matches(&self.operation.owner(), group)? { return Err(Invalid::Canonical.into()); }
                        }
                        let mut duplicate = false;
                        for earlier in &selectors[..i] {
                            duplicate = match earlier {
                                Selector::Prefix(prefix) => view.key().starts_with(*prefix),
                                Selector::Group(group) => record.group_matches(&self.operation.owner(), group)?,
                            };
                            if duplicate { break; }
                        }
                        !duplicate
                    }
                };
                if selected { visit(window)?; }
            }
        }
        Ok(())
    }
}
impl Source<'_> {
    fn collect(&self, selectors: &[Selector<'_>]) -> Selected<NativeCustodySelection> {
        if selectors.iter().any(|selector| matches!(selector, Selector::Group(_))) {
            self.root.require_native_custody_groups()?;
        }
        let owner = self.operation.owner();
        let mut count = 0usize;
        self.scan(selectors, |window| {
            let view = window.view()?;
            decode_record(view.stored_bytes())?;
            count = count.checked_add(1).ok_or(Invalid::Codec)?;
            Ok(())
        })?;
        // Both passes pin the same committed root. Admission precedes every
        // retained slot and every record/key/signature scratch allocation.
        let mut selection = NativeCustodySelection::try_new(&owner, count)?;
        self.scan(selectors, |window| {
            selection.retain(NativeCustodyWindow::admit(window, &owner)?)
        })?;
        selection.sort_dedup();
        Ok(selection)
    }
    fn validate_companions(&self, selection: &NativeCustodySelection) -> Selected<()> {
        for selected in selection.windows() {
            let view = selected.view()?;
            let record = view.record()?;
            let owner = self.operation.owner();
            let mut text = NativeSelectionHold::try_new(&owner, 2)?;
            let mut selectors = NativeSelectionHold::try_new(&owner, 2)?;
            match record {
                RecordView::Control(_) => selectors.retain(Selector::Group("authority"))?,
                RecordView::Terminal { record: t, .. } => {
                    text.retain(NativeBuffer::format(
                        &owner,
                        format_args!("submission:{}", t.submission),
                    )?)?;
                    let object = crate::custody::native_record_codec::terminal_object(&t)?;
                    text.retain(NativeBuffer::format(
                        &owner,
                        format_args!(
                            "object:{}",
                            crate::custody::preparation::BorrowedHex(&object)
                        ),
                    )?)?;
                }
                RecordView::Root { submission, .. } => text.retain(NativeBuffer::format(
                    &owner,
                    format_args!("submission:{submission}"),
                )?)?,
                RecordView::MaterializedOwner(o) => text.retain(NativeBuffer::format(
                    &owner,
                    format_args!("submission:{}", o.submission),
                )?)?,
                RecordView::ObjectAdmission(a) => {
                    text.retain(NativeBuffer::format(
                        &owner,
                        format_args!(
                            "object:{}",
                            crate::custody::preparation::BorrowedHex(&a.object_digest)
                        ),
                    )?)?;
                    text.retain(NativeBuffer::format(
                        &owner,
                        format_args!(
                            "delivery_peer_evidence.v1.{}",
                            crate::custody::preparation::BorrowedHex(&a.certificate)
                        ),
                    )?)?;
                }
                _ => continue,
            }
            for key in text.as_slice() {
                let key = key.text()?;
                selectors.retain(if key.starts_with("delivery_") {
                    Selector::Prefix(key)
                } else {
                    Selector::Group(key)
                })?;
            }
            let companions = self.collect(selectors.as_slice())?;
            let mut views = NativeSelectionHold::try_new(&owner, companions.windows().len())?;
            for window in companions.windows() {
                views.retain(window.view()?)?;
            }
            verify_companions(&record, views.as_slice(), &owner)?;
        }
        Ok(())
    }
}
impl Database {
    fn native_custody_selection_on(
        &self,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
        selectors: &[Selector<'_>],
    ) -> Selected<NativeCustodySelection> {
        // Match the original publication exclusion interval without reading its
        // resident records or groups. Capture after taking this publication lock.
        let cache = self.custody_cache.lock();
        let publications = cache.as_ref().map(|store| &store.publications);
        let pinned = Source {
            root: self.read_native_custody_root(operation, context)?,
            operation,
            context,
        };
        let source = &pinned;
        let selection = source.collect(selectors)?;
        source.validate_companions(&selection)?;
        self.verify_native_custody_selection(
            &selection,
            source.operation,
            source.context,
            publications,
        )?;
        source.context.checkpoint().map_err(Failure::Unqualified)?;
        Ok(selection)
    }
    pub(crate) fn custody_records_native(
        &self,
        selectors: &[&str],
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<NativeCustodySelection> {
        if selectors.iter().any(|selector| selector.starts_with('@')) {
            self.read_native_custody_root(operation, context)?.require_native_custody_groups()?;
        }
        let owner = operation.owner();
        let mut admitted = NativeSelectionHold::try_new(&owner, selectors.len())?;
        for s in selectors {
            admitted.retain(match s.strip_prefix('@') {
                Some(g) => Selector::Group(g),
                None => Selector::Prefix(s),
            })?;
        }
        self.native_custody_selection_on(operation, context, admitted.as_slice())
    }
    pub(crate) fn custody_submission_native(
        &self,
        submission: uuid::Uuid,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<NativeCustodySelection> {
        self.read_native_custody_root(operation, context)?.require_native_custody_groups()?;
        let group =
            NativeBuffer::format(&operation.owner(), format_args!("submission:{submission}"))?;
        self.native_custody_selection_on(operation, context, &[Selector::Group(group.text()?)])
    }
    pub(crate) fn custody_authority_native(
        &self,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<NativeCustodySelection> {
        self.native_custody_selection_on(
            operation,
            context,
            &[Selector::Group("authority"), Selector::Group("policies")],
        )
    }
    fn verify_native_custody_selection(
        &self,
        selection: &NativeCustodySelection,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
        publications: Option<&Arc<std::sync::atomic::AtomicUsize>>,
    ) -> Selected<()> {
        // Borrow only the publication counter, never touch/initialize the Legacy
        // record/group maps. Preserve the original zero-before-load and
        // zero-per-comparison publication race rule.
        let checking = publications.is_none_or(|p| p.load(Ordering::SeqCst) == 0);
        let root = self.read_native_custody_root(operation, context)?;
        let cursor = root.native_raw_config_prefix(operation, context, "")?;
        for selected in selection.windows() {
            let persisted = cursor
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .visit_exact(selected.key(), Ok)?;
            let same = match persisted {
                Some(p) => {
                    let p = p.view()?;
                    let s = selected.view()?;
                    p.stored_bytes() == s.stored_bytes()
                }
                None => false,
            };
            if checking && publications.is_none_or(|p| p.load(Ordering::SeqCst) == 0) && !same {
                return Err(Invalid::Canonical.into());
            }
        }
        Ok(())
    }
}

/// All cold reads finish before a consumer takes a typed borrowed view. This
/// includes incarnation lookups, whose older exact cold-source fence is not
/// recursive. Raw windows may be retained safely across those lookups.
pub(crate) struct NativeOutcomeSelection {
    pub(crate) terminals: NativeCustodySelection,
    pub(crate) positions: NativeSelectionHold<(usize, (Lsn, u32))>,
    pub(crate) submissions: NativeSelectionHold<NativeCustodySelection>,
    pub(crate) authority: NativeCustodySelection,
    pub(crate) checkpoint: Option<NativeCustodySelection>,
    pub(crate) hub_incarnation: Incarnation,
    pub(crate) response_incarnation: Incarnation,
}
struct NamespaceInput<'a> {
    tenant: &'a str,
    hub: &'a str,
    hub_incarnation: Incarnation,
    edge: &'a str,
    edge_incarnation: Incarnation,
}
impl NamespaceInput<'_> {
    fn encode(&self, e: &mut NativeEncoder<'_>) -> crate::custody::native_decode::Decoded<()> {
        e.string(self.tenant);
        e.node(self.hub)?;
        e.raw(&self.hub_incarnation.0.to_be_bytes());
        e.node(self.edge)?;
        e.raw(&self.edge_incarnation.0.to_be_bytes());
        Ok(())
    }
    fn matches(&self, n: &Namespace<'_>) -> bool {
        self.tenant == n.tenant
            && self.hub == n.hub_node
            && self.hub_incarnation == n.hub_incarnation
            && self.edge == n.edge_node
            && self.edge_incarnation == n.edge_incarnation
    }
}
impl Database {
    pub(crate) fn custody_outcomes_native(
        &self,
        tenant: &TenantId,
        hub: &str,
        edge: &str,
        edge_incarnation: Incarnation,
        after: Option<(Lsn, u32)>,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<NativeOutcomeSelection> {
        self.custody_outcomes_native_with_after(tenant, hub, edge, edge_incarnation,
            operation, context, |_| Ok(after))
    }
    #[allow(clippy::too_many_arguments)] // Keep the existing selector inputs and private validator explicit.
    pub(crate) fn custody_outcomes_native_with_after(
        &self,
        tenant: &TenantId,
        hub: &str,
        edge: &str,
        edge_incarnation: Incarnation,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
        validate_after: impl FnOnce(Incarnation) -> Selected<Option<(Lsn, u32)>>,
    ) -> Selected<NativeOutcomeSelection> {
        // A missing derived root refuses before any allocated query or CONFIG
        // visit, including incarnation lookup. This scalar is already pinned.
        self.read_native_custody_root(operation, context)?.require_native_custody_groups()?;
        // First independently captured incarnation query, before ANY view.
        let hub_incarnation = self
            .existing_sync_incarnation_native(tenant, operation, context)?
            .ok_or(Invalid::Canonical)?;
        let after = validate_after(hub_incarnation)?;
        let ns = NamespaceInput {
            tenant: tenant.as_str(),
            hub,
            hub_incarnation,
            edge,
            edge_incarnation,
        };
        let ns_bytes = finite_bytes(&operation.owner(), |e| ns.encode(e))?;
        let prefix = NativeBuffer::format(
            &operation.owner(),
            format_args!("delivery_hub_order.v1.{}.",
                crate::custody::preparation::BorrowedHex(ns_bytes.bytes())),
        )?;
        let prefix_text = prefix.text()?;
        let lower = after.map(|p| NativeBuffer::format(&operation.owner(),
            format_args!("{}{:016x}.{:08x}", prefix_text, p.0.0, p.1))).transpose()?;
        // One pinned view covers order seek, group members, companion checks
        // and persisted-byte verification. Nothing before after is visited.
        let cache = self.custody_cache.lock();
        let publications = cache.as_ref().map(|store| &store.publications);
        let source = Source { root: self.read_native_custody_root(operation, context)?, operation, context };
        source.root.require_native_custody_groups()?;
        let count_orders = || -> Selected<usize> {
            let cursor = source.root.native_raw_config_prefix_after(operation, context,
                prefix.text()?, lower.as_ref().map(NativeBuffer::text).transpose()?)?;
            let mut count = 0usize;
            loop {
                let raw = cursor.lock().unwrap_or_else(|p| p.into_inner()).visit_next(Ok)?;
                let Some(raw) = raw else { break };
                let selected = NativeCustodyWindow::admit(raw, &operation.owner())?;
                let view = selected.view()?;
                match view.record()? {
                    RecordView::Order { namespace, position, .. }
                        if ns.matches(&namespace) && after.is_none_or(|p| position > p) => {
                            count = count.checked_add(1).ok_or(Invalid::Codec)?;
                        }
                    _ => return Err(Invalid::Canonical.into()),
                }
            }
            Ok(count)
        };
        let count = count_orders()?;
        let mut terminals = NativeCustodySelection::try_new(&operation.owner(), count)?;
        let mut positions = NativeSelectionHold::try_new(&operation.owner(), count)?;
        let mut submissions = NativeSelectionHold::try_new(&operation.owner(), count)?;
        let cursor = source.root.native_raw_config_prefix_after(operation, context,
            prefix.text()?, lower.as_ref().map(NativeBuffer::text).transpose()?)?;
        loop {
            let raw = cursor.lock().unwrap_or_else(|p| p.into_inner()).visit_next(Ok)?;
            let Some(raw) = raw else { break };
            let order = NativeCustodyWindow::admit(raw, &operation.owner())?;
            let (submission, position) = {
                let view = order.view()?;
                match view.record()? {
                    RecordView::Order { namespace, submission, position }
                        if ns.matches(&namespace) && after.is_none_or(|p| position > p) => (submission, position),
                    _ => return Err(Invalid::Canonical.into()),
                }
            };
            let group = NativeBuffer::format(&operation.owner(), format_args!("submission:{submission}"))?;
            let selection = source.collect(&[Selector::Group(group.text()?)])?;
            source.validate_companions(&selection)?;
            self.verify_native_custody_selection(&selection, operation, context, publications)?;
            let mut terminal_key = None;
            for member in selection.windows() {
                let view = member.view()?;
                if let RecordView::Terminal { edge: false, record: t } = view.record()? {
                    if !ns.matches(&t.namespace) || t.position != position || t.submission != submission || terminal_key.is_some() {
                        return Err(Invalid::Canonical.into());
                    }
                    terminal_key = Some(NativeBuffer::format(&operation.owner(), format_args!("{}", view.key()))?);
                }
            }
            let terminal_key = terminal_key.ok_or(Invalid::Companions)?;
            // Fetch one terminal loan at this same root. The packet adapter's
            // fields and terminal indexes remain exactly their original shape.
            let exact = source.cursor("")?;
            let raw = exact.lock().unwrap_or_else(|p| p.into_inner())
                .visit_exact(terminal_key.text()?, Ok)?.ok_or(Invalid::Canonical)?;
            terminals.retain(NativeCustodyWindow::admit(raw, &operation.owner())?)?;
            positions.retain((terminals.windows().len() - 1, position))?;
            submissions.retain(selection)?;
        }
        self.verify_native_custody_selection(&terminals, operation, context, publications)?;
        drop(cache);
        let authority = self.custody_authority_native(operation, context)?;
        let key = NativeBuffer::format(
            &operation.owner(),
            format_args!(
                "delivery_prefix_frontier.v1.{}",
                crate::custody::preparation::BorrowedHex(ns_bytes.bytes())
            ),
        )?;
        let checkpoint = self.custody_checkpoint_exact_native(key.text()?, operation, context)?;
        // Independent response incarnation query also occurs without a view.
        let response_incarnation = self
            .existing_sync_incarnation_native(tenant, operation, context)?
            .ok_or(Invalid::Canonical)?;
        context.checkpoint().map_err(Failure::Unqualified)?;
        Ok(NativeOutcomeSelection {
            terminals,
            positions,
            submissions,
            authority,
            checkpoint,
            hub_incarnation,
            response_incarnation,
        })
    }
    pub(crate) fn custody_checkpoint_exact_native(
        &self,
        key: &str,
        operation: &NativeReadRequest,
        context: &NativeReadOpenContext,
    ) -> Selected<Option<NativeCustodySelection>> {
        let cache = self.custody_cache.lock();
        let publications = cache.as_ref().map(|store| &store.publications);
        let root = self.read_native_custody_root(operation, context)?;
        let cursor = root.native_raw_config_prefix(operation, context, "")?;
        let raw = cursor
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .visit_exact(key, Ok)?;
        let Some(raw) = raw else { return Ok(None) };
        let mut selection = NativeCustodySelection::try_new(&operation.owner(), 1)?;
        let selected = NativeCustodyWindow::admit(raw, &operation.owner())?;
        {
            let view = selected.view()?;
            if !matches!(view.record()?, RecordView::CheckpointFrontier(_)) {
                return Err(Invalid::Codec.into());
            }
        }
        selection.retain(selected)?;
        self.verify_native_custody_selection(&selection, operation, context, publications)?;
        // The exact singleton retains its original fixed paid slot throughout
        // the packet consumer's lifetime.
        Ok(Some(selection))
    }
}
