//! Views borrow the actual staged scope; no authority is reconstructed here.
use super::*;
use crate::vector_ivf_observations::*;
impl control::ScopeRecord {
    pub(super) fn observed<'a>(&'a self, key: &'a [u8; 64]) -> Result<NativeIvfStoredScopeRef<'a>> {
        let families = self.families.each_ref().map(|f| {
            f.as_ref()
                .map(|f| -> Result<_> {
                    Ok(NativeIvfFamilyRef {
                        storage: f.storage,
                        generation: f.generation,
                        present: f.kinds.present,
                        roots: &f.kinds.roots,
                        frozen_policy_revision: f.policy_revision,
                        frozen_frontier: contextdb_core::Lsn(f.source_frontier),
                        frozen_source_identity: &f.source_identity,
                        frozen_entries: f.basis_entries,
                        frozen_live_entries: f.basis_live_entries,
                        current_commit_frontier: contextdb_core::Lsn(f.committed_through_lsn),
                        manifest: f.manifest.map(|m| m.manifest()).transpose()?,
                        reservoir_samples: f.reservoir_samples,
                    })
                })
                .transpose()
        });
        let [a, b, c] = families;
        Ok(NativeIvfStoredScopeRef {
            scope_key: key,
            index_digest: &self.index,
            tuple_digest: &self.tuple,
            table_generation: self.schema.table_generation,
            column_incarnation: self.schema.column_incarnation,
            families: [a?, b?, c?],
            serving: self.manifest()?,
            build: self.build_state()?,
            source_cursor: self
                .source_cursor
                .as_ref()
                .map(|s| NativeIvfSourceCursorRef {
                    canonical: &s.canonical,
                    membership: &s.membership,
                }),
            current: NativeIvfCoverageRef {
                digest: &self.current_membership_digest,
                entries: self.current_membership_entries,
                live_entries: self.current_membership_live_entries,
                frontier: contextdb_core::Lsn(self.committed_through_lsn),
            },
            canonical_complete: self.canonical_complete,
        })
    }
}
