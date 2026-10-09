//! Declaration reductions installed atomically beside their immutable history.
use super::writer_source::SourceWindowOwner;
use super::*;
use crate::sync_types::SchemaSyncCapability;
use contextdb_core::SyncDirection;

const DIRECTIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("ddl_directions_by_table");
const CAPABILITIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("ddl_capabilities_by_table");

#[derive(Clone, Copy, Serialize, Deserialize)]
struct DirectionState {
    at: SyncDirection,
    after: SyncDirection,
    latest: SyncDirection,
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
        write: &redb::WriteTransaction,
        key: &str,
        change: &DdlChange,
    ) -> Result<()> {
        let (name, declaration, dropped) = match change {
            DdlChange::CreateTable {
                name, constraints, ..
            } => (
                name,
                Some(
                    constraints
                        .iter()
                        .find_map(|text| crate::database::constraint_declares_sync_direction(text))
                        .unwrap_or(contextdb_core::DEFAULT_SYNC_DIRECTION),
                ),
                false,
            ),
            DdlChange::AlterTable {
                name, constraints, ..
            } => (
                name,
                constraints
                    .iter()
                    .find_map(|text| crate::database::constraint_declares_sync_direction(text)),
                false,
            ),
            DdlChange::DropTable { name } => (name, None, true),
            _ => return Ok(()),
        };
        if declaration.is_some() || dropped {
            let position = key
                .split(':')
                .next()
                .ok_or_else(|| Error::Other("missing declaration position".into()))?;
            let lower = prefix(name);
            let event_key = format!("{lower}{position}");
            let mut directions = write.open_table(DIRECTIONS).map_err(Self::storage_error)?;
            let previous = directions
                .range(lower.as_str()..=event_key.as_str())
                .map_err(Self::storage_error)?
                .next_back()
                .transpose()
                .map_err(Self::storage_error)?;
            let mut state = previous
                .map(|(key, value)| {
                    let mut state: DirectionState = Self::decode(value.value())?;
                    if key.value() != event_key {
                        state.at = state.after;
                    }
                    Ok::<DirectionState, Error>(state)
                })
                .transpose()?
                .unwrap_or(DirectionState {
                    at: SyncDirection::Both,
                    after: SyncDirection::Both,
                    latest: SyncDirection::Both,
                });
            if let Some(direction) = declaration {
                state = DirectionState {
                    at: direction,
                    after: direction,
                    latest: direction,
                };
            }
            if dropped {
                state.after = SyncDirection::Both;
            }
            let encoded = Self::encode(&state)?;
            directions
                .insert(event_key.as_str(), encoded.as_slice())
                .map_err(Self::storage_error)?;
        }
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
                .get(name.as_str())
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
                .insert(name.as_str(), encoded.as_slice())
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
        let upper = format!("{lower}{:020}", lsn.unwrap_or(Lsn(u64::MAX)).0);
        let _key_charge = root.adapter_control(owner, lower.len() + upper.len() + 256)?;
        let read = root.owner_read()?;
        let directions = match read.open_table(DIRECTIONS) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(Self::storage_error(error)),
        };
        #[cfg(feature = "test-seams")]
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
                let state: DirectionState = Self::decode(value.value())?;
                Ok(match lsn {
                    None => state.latest,
                    Some(_) if key.value() == upper => state.at,
                    Some(_) => state.after,
                })
            })
            .transpose()
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
