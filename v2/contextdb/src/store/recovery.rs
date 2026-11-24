use super::{HeldFor, Store, disk};
use crate::types::Result;

impl Store {
    pub(crate) fn recovery_step(&self, allowance: usize) -> Result<u64> {
        let HeldFor::Writing { data, .. } = &self.held else {
            return Ok(0);
        };
        data.recovery_step(allowance)
            .map_err(|e| disk::failed(&self.folder, "recovery", e))
    }

    pub(crate) fn recovery_state(&self) -> Result<(Option<u64>, u64, u64)> {
        let HeldFor::Writing { data, .. } = &self.held else {
            unreachable!()
        };
        data.recovery_state()
            .map_err(|e| disk::failed(&self.folder, "recovery", e))
    }

    pub(crate) fn recovery_wait(&self) -> Result<Option<(u64, u64)>> {
        let (finding, bytes, limit) = self.recovery_state()?;
        // Recovery borrows only from the declared disk; a small limit is all reserve.
        let reserve = if finding.is_some() && limit != u64::MAX {
            limit.min((limit / 100).max(64 << 20))
        } else {
            0
        };
        if limit.saturating_sub(reserve).saturating_sub(bytes) >= 4096 {
            return Ok(None);
        }
        let reclaimable = if finding.is_some() {
            true
        } else {
            let (free, _, pending, _, _) = self.compaction_space(&mut 16)?;
            free == 0 && pending > 0
        };
        Ok(reclaimable.then_some((bytes, limit)))
    }

    pub(crate) fn disk_limit(&self, limit: Option<u64>) {
        if let HeldFor::Writing { data, .. } = &self.held {
            data.set_file_size_limit(limit);
        }
    }
}

impl HeldFor {
    pub(super) fn remembered_cache(backend: super::folder::DataFile) -> super::Stored<u64> {
        let cache = super::cache_under(None, None);
        if redb::StorageBackend::len(&backend)? == 0 {
            return Ok(cache);
        }
        let data = redb::Builder::new()
            .set_incremental_recovery(true)
            .set_cache_size(crate::memory::pages(cache))
            .open_read_only_with_backend(backend)?;
        match disk::remembered_limit(&data) {
            Ok(limit) => Ok(super::cache_under(limit, None)),
            Err(disk::Failed::Storage(redb::Error::TableDoesNotExist(_))) => Ok(cache),
            Err(error) => Err(error),
        }
    }
}
