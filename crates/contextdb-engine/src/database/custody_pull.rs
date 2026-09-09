//! Statements 9/15: hidden custody rows never become pull progress or history-sized pages.
//! This is a discardable scan optimization, not restore evidence. The signed
//! image-prefix checkpoint remains mandatory even when this index is warm.
use super::*;

#[derive(Clone)]
pub(crate) struct HiddenPullScan {
    peer: String,
    requested: Lsn,
    pub since: Lsn,
    through: Lsn,
    schema: Option<Lsn>,
    incarnation: Option<Incarnation>,
}
impl Database {
    pub(crate) fn custody_pull_scan(
        &self,
        tenant: &TenantId,
        peer: Option<&str>,
        requested: Lsn,
    ) -> Result<Option<HiddenPullScan>> {
        let Some(peer) = peer else {
            return Ok(None);
        };
        if !self
            .relational_store
            .table_meta
            .read()
            .values()
            .any(|meta| meta.delivery_manifest_tables.is_some())
        {
            return Ok(None);
        }
        // The caller holds the outbound schema lease. A declaration change
        // invalidates hidden progress, so a newly pullable old row is revisited.
        let schema = self.ddl_log.read().last().map(|(lsn, _)| *lsn);
        let incarnation = self.existing_sync_incarnation(tenant)?;
        let through = self.current_lsn();
        self.with_custody_store(|store| {
            let since = store
                .hidden_pulls
                .get(peer)
                .filter(|old| {
                    old.requested == requested
                        && old.schema == schema
                        && old.incarnation == incarnation
                        && old.through <= through
                })
                .map_or(requested, |old| old.through.max(requested));
            Ok(Some(HiddenPullScan {
                peer: peer.into(),
                requested,
                since,
                through,
                schema,
                incarnation,
            }))
        })
    }
    pub(crate) fn remember_hidden_custody_pull(&self, scan: HiddenPullScan) -> Result<()> {
        self.with_custody_store(|store| {
            // One entry per authenticated peer, independent of receipt history.
            // Only an entirely non-deliverable declaration-filtered prefix is
            // remembered. Purges, DDL, ordinary rows, edges and vectors forbid it.
            store.hidden_pulls.insert(scan.peer.clone(), scan);
            Ok(())
        })
    }
}
