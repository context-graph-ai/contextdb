//! Declaration reductions installed atomically beside their immutable history.
use super::writer_source::SourceWindowOwner;
use super::*;
use crate::sync_types::SchemaSyncCapability;
use contextdb_core::SyncDirection;

const DIRECTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_directions_by_table");
const CAPABILITIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("ddl_capabilities_by_table");

#[derive(Clone, Serialize, Deserialize)]
struct DirectionState {
    at: SyncDirection,
    after: SyncDirection,
    latest: SyncDirection,
    // Complete post-occurrence declaration, including stable column lifetimes.
    // Absence is explicit; it must never be filled from the live catalog.
    schema: Option<Vec<u8>>,
    ddl_key: String,
    ddl_digest: [u8; 32],
}

fn prefix(table: &str) -> String {
    let mut key = String::with_capacity(table.len() * 2 + 1);
    use std::fmt::Write;
    for byte in table.as_bytes() {
        write!(&mut key, "{byte:02x}").expect("string write");
    }
    key.push(':');
    key
}

impl RedbPersistence {
    pub(super) fn append_ddl_summary_in_write(
        &self,
        write: &redb::WriteTransaction,
        key: &str,
        change: &DdlChange,
    ) -> Result<()> {
        let _decode_scope = self.writer_decode_scope()?;
        let mut scratch = self.proof_admission();
        scratch.admit(
            change
                .estimated_bytes()
                .saturating_mul(64)
                .saturating_add(key.len().saturating_mul(4))
                .saturating_add(1024),
        )?;
        let name = match change {
            DdlChange::CreateTable { name, .. }
            | DdlChange::AlterTable { name, .. }
            | DdlChange::DropTable { name } => name.as_str(),
            DdlChange::CreateIndex { table, .. }
            | DdlChange::DropIndex { table, .. }
            | DdlChange::CreateTrigger { table, .. }
            | DdlChange::CreateEventType { table, .. } => table.as_str(),
            _ => return Ok(()),
        };
        let (position, ordinal) = key.split_once(':').unwrap_or((key, "0"));
        let position = position
            .parse::<u64>()
            .map_err(|_| Error::Other("invalid declaration LSN".into()))?;
        let ordinal = ordinal
            .parse::<u32>()
            .map_err(|_| Error::Other("invalid declaration ordinal".into()))?;
        let lower = prefix(name);
        let event_key = format!("{lower}{position:020}:{ordinal:010}");
        let mut directions = write.open_table(DIRECTIONS).map_err(Self::storage_error)?;
        let previous = directions
            .range(lower.as_str()..event_key.as_str())
            .map_err(Self::storage_error)?
            .next_back()
            .transpose()
            .map_err(Self::storage_error)?;
        let mut state = previous
            .map(|(_, value)| {
                scratch.admit(value.value().len().saturating_mul(64).saturating_add(1024))?;
                Self::decode::<DirectionState>(value.value())
            })
            .transpose()?
            .unwrap_or(DirectionState {
                at: contextdb_core::DEFAULT_SYNC_DIRECTION,
                after: contextdb_core::DEFAULT_SYNC_DIRECTION,
                latest: contextdb_core::DEFAULT_SYNC_DIRECTION,
                schema: None,
                ddl_key: String::new(),
                ddl_digest: [0; 32],
            });
        state.at = state.after;
        let previous_schema = state.schema.take();
        let mut meta = previous_schema
            .as_deref()
            .map(Self::decode_persisted_schema)
            .transpose()?
            .map(|(meta, _, _)| meta);
        let declared_direction = match change {
            DdlChange::CreateTable { constraints, .. } => Some(
                constraints
                    .iter()
                    .find_map(|text| crate::database::constraint_declares_sync_direction(text))
                    .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
            ),
            DdlChange::AlterTable { constraints, .. } => constraints
                .iter()
                .find_map(|text| crate::database::constraint_declares_sync_direction(text)),
            _ => None,
        };
        if let Some(direction) = declared_direction {
            state.at = direction;
            state.after = direction;
            state.latest = direction;
        }
        match change {
            DdlChange::CreateTable {
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
                ..
            } => {
                meta = Some(crate::database::rough_sync_table_meta(
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                ));
            }
            DdlChange::AlterTable {
                columns,
                constraints,
                foreign_keys,
                composite_foreign_keys,
                composite_unique,
                ..
            } => {
                if let Some(meta) = meta.as_mut() {
                    let incoming = crate::database::rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    );
                    crate::database::Database::apply_sync_alter_to_projected_table_meta(
                        name,
                        meta,
                        incoming,
                        constraints,
                    );
                }
            }
            DdlChange::DropTable { .. } => meta = None,
            DdlChange::CreateIndex {
                name: index,
                columns,
                ..
            } => {
                if let Some(meta) = meta.as_mut() {
                    meta.indexes.retain(|entry| entry.name != *index);
                    meta.indexes.push(contextdb_core::IndexDecl {
                        name: index.clone(),
                        columns: columns.clone(),
                        kind: contextdb_core::IndexKind::UserDeclared,
                    });
                }
            }
            DdlChange::DropIndex { name: index, .. } => {
                if let Some(meta) = meta.as_mut() {
                    meta.indexes.retain(|entry| entry.name != *index);
                }
            }
            _ => {}
        }
        if let Some(mut meta) = meta {
            // An ALTER without a SYNC declaration preserves the preceding
            // direction; checkpoint coverage must not change policy semantics.
            if declared_direction.is_none() {
                meta.sync_direction = previous_schema
                    .as_deref()
                    .map(Self::decode_persisted_schema)
                    .transpose()?
                    .and_then(|(meta, _, _)| meta.sync_direction);
            }
            // CREATE starts a new column lifetime namespace. ALTER replays
            // exactly this occurrence over the preceding complete checkpoint.
            let prior = if matches!(change, DdlChange::CreateTable { .. }) {
                None
            } else {
                previous_schema.as_deref()
            };
            state.schema = Some(Self::encode_schema_update(
                prior,
                &meta,
                std::iter::once(change),
                name,
            )?);
        } else {
            state.schema = None;
            if matches!(change, DdlChange::DropTable { .. }) {
                state.after = contextdb_core::DEFAULT_SYNC_DIRECTION;
            }
            // A pre-first absence remains absence, including raw DDL fixtures.
            if let DdlChange::AlterTable { constraints, .. } = change
                && let Some(direction) = constraints
                    .iter()
                    .find_map(|text| crate::database::constraint_declares_sync_direction(text))
            {
                state.at = direction;
                state.after = direction;
                state.latest = direction;
            }
        }
        state.ddl_key = key.to_owned();
        state.ddl_digest = *blake3::hash(&Self::encode(change)?).as_bytes();
        let encoded = Self::encode(&state)?;
        directions
            .insert(event_key.as_str(), encoded.as_slice())
            .map_err(Self::storage_error)?;
        drop(directions);
        if let DdlChange::CreateTable {
            columns,
            constraints,
            ..
        }
        | DdlChange::AlterTable {
            columns,
            constraints,
            ..
        } = change
        {
            let mut table = write
                .open_table(CAPABILITIES)
                .map_err(Self::storage_error)?;
            let mut union: BTreeSet<SchemaSyncCapability> = table
                .get(name)
                .map_err(Self::storage_error)?
                .map(|value| Self::decode(value.value()))
                .transpose()?
                .unwrap_or_default();
            for (column, text) in columns {
                union.extend(crate::sync_types::schema_capabilities_in_columns(&[
                    crate::database::rough_sync_column_def(column, text),
                ]));
            }
            if constraints.iter().any(|constraint| {
                constraint
                    .trim_start()
                    .to_ascii_uppercase()
                    .starts_with("MAINTAIN COUNT")
            }) {
                union.insert(SchemaSyncCapability::MaintainedIndexCounts);
            }
            let encoded = Self::encode(&union)?;
            table
                .insert(name, encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
        Ok(())
    }

    pub(crate) fn declared_direction_in_read(
        root: &WriterSnapshot,
        owner: &SourceWindowOwner,
        table: &str,
        lsn: Option<Lsn>,
    ) -> Result<Option<SyncDirection>> {
        let lower = prefix(table);
        let upper = format!("{lower}{:020}:~", lsn.unwrap_or(Lsn(u64::MAX)).0);
        let _key_charge = root.adapter_control(owner, lower.len() + upper.len() + 256)?;
        let read = root.owner_read()?;
        let directions = match read.open_table(DIRECTIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                if Self::witnessed_row_count_in_read(&read, DIRECTIONS.name())?.is_some() {
                    return Err(Error::StoreCorrupted {
                        path: DIRECTIONS.name().into(),
                        reason: "witnessed declaration checkpoints are absent".into(),
                    });
                }
                return Ok(None);
            }
            Err(error) => return Err(Self::storage_error(error)),
        };
        Self::verify_history_root(&read, DIRECTIONS.name())?;
        #[cfg(any(test, feature = "test-seams"))]
        {
            crate::database::resident_history_test_seams::note(|work| {
                work.direction_summary_reads += 1;
            });
            crate::writer_query_test_seams::note_direction_summary_read();
        }
        let entry = directions
            .range(lower.as_str()..=upper.as_str())
            .map_err(Self::storage_error)?
            .next_back()
            .transpose()
            .map_err(Self::storage_error)?;
        entry
            .map(|(key, value)| {
                let _body_charge = root.adapter_control(
                    owner,
                    value.value().len().saturating_mul(16).saturating_add(512),
                )?;
                let state: DirectionState = Self::decode(value.value())?;
                Ok(match lsn {
                    None => state.latest,
                    Some(lsn)
                        if key
                            .value()
                            .strip_prefix(&lower)
                            .and_then(|key| key.split(':').next())
                            .and_then(|key| key.parse::<u64>().ok())
                            == Some(lsn.0) =>
                    {
                        state.at
                    }
                    Some(_) => state.after,
                })
            })
            .transpose()
    }

    pub(crate) fn declared_meta_in_read(
        root: &WriterSnapshotHandle, owner: SourceWindowOwner, table: &str, lsn: Lsn,
    ) -> Result<Option<writer_source::WriterWindow<(TableMeta, Lsn, u32)>>> {
        let _control = root.adapter_control(&owner, table.len().saturating_mul(4).saturating_add(256))?;
        let lower = prefix(table);
        let upper = format!("{lower}{:020}:~", lsn.0);
        let read = root.owner_read()?;
        let summaries = match read.open_table(DIRECTIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        Self::verify_history_root(&read, DIRECTIONS.name())?;
        let entry = summaries.range(lower.as_str()..=upper.as_str()).map_err(Self::storage_error)?
            .next_back().transpose().map_err(Self::storage_error)?;
        let Some((coordinate, value)) = entry else { return Ok(None); };
        let memory = owner.image(root, "authored_declaration");
        let meta = memory.scope(|| {
            let state: DirectionState = Self::decode(value.value())?;
            let damage = |reason: &str| Error::StoreCorrupted { path: table.into(), reason: reason.into() };
            let (position, ordinal) = state.ddl_key.split_once(':').unwrap_or((&state.ddl_key, "0"));
            let position = position.parse::<u64>().map_err(|_| damage("authored declaration has an invalid DDL position"))?;
            let ordinal = ordinal.parse::<u32>().map_err(|_| damage("authored declaration has an invalid DDL ordinal"))?;
            if coordinate.value() != format!("{lower}{position:020}:{ordinal:010}") || position > lsn.0 {
                return Err(damage("authored declaration coordinate differs from its source occurrence"));
            }
            let log = read.open_table(DDL_LOG_TABLE).map_err(Self::storage_error)?;
            let occurrence = log.get(state.ddl_key.as_str()).map_err(Self::storage_error)?
                .ok_or_else(|| damage("authored declaration lost its source DDL occurrence"))?;
            if blake3::hash(occurrence.value()).as_bytes() != &state.ddl_digest {
                return Err(damage("authored declaration digest differs from its source DDL occurrence"));
            }
            let change: DdlChange = Self::decode(occurrence.value())?;
            let affected = match &change {
                DdlChange::CreateTable { name, .. } | DdlChange::AlterTable { name, .. }
                | DdlChange::DropTable { name } => Some(name.as_str()),
                DdlChange::CreateIndex { table, .. } | DdlChange::DropIndex { table, .. }
                | DdlChange::CreateTrigger { table, .. } | DdlChange::CreateEventType { table, .. } => Some(table.as_str()),
                _ => None,
            };
            if affected != Some(table) {
                return Err(damage("authored declaration is bound to a different table"));
            }
            state.schema.as_deref().map(Self::decode_persisted_schema).transpose()
                .map(|meta| meta.map(|(meta, _, _)| (meta, Lsn(position), ordinal)))
        })?;
        Ok(meta.map(|meta| writer_source::WriterWindow::from_owned(root, memory, meta)))
    }

    #[cfg(feature = "sync-orchestration")]
    pub(crate) fn authored_capabilities_in_read(
        root: &WriterSnapshot,
        owner: &SourceWindowOwner,
        table: &str,
    ) -> Result<BTreeSet<SchemaSyncCapability>> {
        let _charge = root.adapter_control(owner, table.len() + 512)?;
        let read = root.owner_read()?;
        let summaries = match read.open_table(CAPABILITIES) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(BTreeSet::new()),
            Err(error) => return Err(Self::storage_error(error)),
        };
        #[cfg(feature = "test-seams")]
        {
            crate::database::resident_history_test_seams::note(|work| {
                work.capability_summary_reads += 1;
            });
            crate::writer_query_test_seams::note_capability_summary_read();
        }
        summaries
            .get(table)
            .map_err(Self::storage_error)?
            .map(|value| Self::decode(value.value()))
            .transpose()
            .map(|value| value.unwrap_or_default())
    }
}

use crate::persistence::writer_source::{WriterSnapshotHandle};
