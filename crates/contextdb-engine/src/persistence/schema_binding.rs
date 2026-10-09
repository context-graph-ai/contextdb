//! Column lifetime identities belong to the existing persisted schema record.
use super::*;

pub(super) const SCHEMA_MAGIC: &[u8; 8] = b"CDBSCH01";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedSchema {
    declaration: Vec<u8>,
    last_incarnation: u64,
    incarnations: Vec<u64>,
}

/// Captured meaning identity; graph generation and route remain independently bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VectorSchemaBinding {
    pub(crate) table_generation: u64,
    pub(crate) column_incarnation: u64,
}

impl VectorSchemaBinding {
    // Existing unbound framing marker. Declared tables never validate as zero.
    pub(crate) const UNBOUND: Self = Self {
        table_generation: 0,
        column_incarnation: 0,
    };
}

pub(super) struct EncodedSchemaUpdate {
    pub(super) bytes: Vec<u8>,
    _memory: Option<crate::memory_accounting::OwnedMemoryReservation>,
}

impl RedbPersistence {
    pub(super) fn decode_persisted_schema(bytes: &[u8]) -> Result<(TableMeta, u64, Vec<u64>)> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        if let Some(payload) = bytes.strip_prefix(SCHEMA_MAGIC) {
            type Shape = (InspectedSequence<u8>, u64, InspectedSequence<u64>);
            let (_, consumed) = bincode::serde::borrow_decode_from_slice::<Shape, _>(
                payload,
                bincode::config::standard(),
            )
            .map_err(|_| {
                Error::Other("persisted schema column incarnations cannot be inspected".into())
            })?;
            if consumed != payload.len() {
                return Err(Error::Other(
                    "persisted schema column incarnations have trailing bytes".into(),
                ));
            }
            let schema: PersistedSchema = Self::decode(payload)?;
            let (meta, _) = Self::decode_table_meta_versioned(&schema.declaration)?;
            // A u64 BTreeSet node holds at most eleven keys; 64 bytes per
            // input key plus one node covers leaf/internal nodes and headers.
            crate::read_image_memory::reserve(
                schema.incarnations.len().saturating_mul(64).saturating_add(128),
            )?;
            if schema.incarnations.len() != meta.columns.len()
                || schema
                    .incarnations
                    .iter()
                    .any(|id| *id == 0 || *id > schema.last_incarnation)
                || schema
                    .incarnations
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != schema.incarnations.len()
            {
                return Err(Error::Other(
                    "persisted schema column incarnations are invalid".into(),
                ));
            }
            return Ok((meta, schema.last_incarnation, schema.incarnations));
        }
        // The existing declaration decoder also serves raw schema fixtures.
        // Such a declaration has not allocated any lifetime identity.
        let (meta, _) = Self::decode_table_meta_versioned(bytes)?;
        Ok((meta, 0, Vec::new()))
    }

    pub(super) fn encode_schema_update<'a>(
        previous: Option<&[u8]>,
        meta: &TableMeta,
        ddl: impl IntoIterator<Item = &'a DdlChange>,
        name: &str,
    ) -> Result<Vec<u8>> {
        let previous = previous.map(Self::decode_persisted_schema).transpose()?;
        let (last, incarnations) = Self::project_schema_incarnations(previous, meta, ddl, name)?;
        let record = PersistedSchema {
            declaration: Self::encode(meta)?,
            last_incarnation: last,
            incarnations,
        };
        let mut bytes = SCHEMA_MAGIC.to_vec();
        bytes.extend(Self::encode(&record)?);
        Ok(bytes)
    }

    fn project_schema_incarnations<'a>(
        previous: Option<(TableMeta, u64, Vec<u64>)>,
        meta: &TableMeta,
        ddl: impl IntoIterator<Item = &'a DdlChange>,
        name: &str,
    ) -> Result<(u64, Vec<u64>)> {
        let (old, mut last, old_ids) = previous.unwrap_or_default();
        let same_meaning = |before: &contextdb_core::ColumnDef,
                            after: &contextdb_core::ColumnDef| {
            before.column_type == after.column_type
                && before.quantization == after.quantization
                && before.partition_key_columns == after.partition_key_columns
        };
        let mut projected = old.clone();
        let mut active = old
            .columns
            .into_iter()
            .zip(old_ids)
            .map(|(column, id)| (column.name.clone(), (column, id)))
            .collect::<HashMap<_, _>>();
        let mut allocate = || -> Result<u64> {
            last = last
                .checked_add(1)
                .ok_or_else(|| Error::Other("schema column incarnations are exhausted".into()))?;
            Ok(last)
        };
        // Replay explicit author operations in their original order. This also
        // catches drop/re-add or representation changes whose final text repeats.
        for change in ddl {
            match change {
                DdlChange::DropTable { name: table } if table == name => {
                    active.clear();
                    projected = TableMeta::default();
                }
                DdlChange::AlterTable {
                    name: table,
                    columns,
                    constraints,
                    foreign_keys,
                    composite_foreign_keys,
                    composite_unique,
                } if table == name => {
                    let incoming = crate::database::rough_sync_table_meta(
                        columns,
                        constraints,
                        foreign_keys,
                        composite_foreign_keys,
                        composite_unique,
                    );
                    crate::database::Database::apply_sync_alter_to_projected_table_meta(
                        name,
                        &mut projected,
                        incoming,
                        constraints,
                    );
                    let mut renames =
                        crate::database::sync_column_renames_from_constraints(constraints);
                    if let Some(rename) =
                        crate::database::sync_vector_rename_from_constraints(constraints)
                    {
                        renames.push(rename);
                    }
                    for (from, to) in renames {
                        if projected.columns.iter().any(|column| column.name == from)
                            || !projected.columns.iter().any(|column| column.name == to)
                        {
                            continue;
                        }
                        if let Some((mut column, id)) = active.remove(&from) {
                            column.name = to.clone();
                            active.insert(to, (column, id));
                        }
                    }
                    active.retain(|name, _| {
                        projected.columns.iter().any(|column| column.name == *name)
                    });
                    for column in &projected.columns {
                        let id = match active.get(&column.name) {
                            Some((before, id)) if same_meaning(before, column) => *id,
                            _ => allocate()?,
                        };
                        active.insert(column.name.clone(), (column.clone(), id));
                    }
                }
                _ => {}
            }
        }
        let mut incarnations = Vec::with_capacity(meta.columns.len());
        for column in &meta.columns {
            let id = match active.get(&column.name) {
                Some((before, id)) if same_meaning(before, column) => *id,
                _ => allocate()?,
            };
            incarnations.push(id);
        }
        Ok((last, incarnations))
    }

    fn changed_vector_incarnations<'a>(
        previous: (TableMeta, u64, Vec<u64>),
        meta: &TableMeta,
        ddl: impl IntoIterator<Item = &'a DdlChange>,
        name: &str,
    ) -> Result<(Vec<VectorIndexRef>, Vec<VectorIndexRef>)> {
        let (_, next) = Self::project_schema_incarnations(Some(previous.clone()), meta, ddl, name)?;
        let mut changed_routes = Vec::new();
        let mut retired_lifetimes = Vec::new();
        for (column, incarnation) in previous.0.columns.iter().zip(&previous.2) {
            if !matches!(column.column_type, ColumnType::Vector(_)) {
                continue;
            }
            let same_route = meta
                .columns
                .iter()
                .position(|next_column| next_column.name == column.name)
                .is_some_and(|position| next[position] == *incarnation);
            if !same_route {
                changed_routes.push(VectorIndexRef::new(name, &column.name));
            }
            // A rename retains its raw lifetime at the destination, even if a
            // later ADD gives the original name a different incarnation.
            if !next.contains(incarnation) {
                retired_lifetimes.push(VectorIndexRef::new(name, &column.name));
            }
        }
        Ok((changed_routes, retired_lifetimes))
    }

    pub(crate) fn received_changed_vector_incarnations(
        &self,
        name: &str,
        meta: &TableMeta,
        ddl: &[DdlChange],
    ) -> Result<(Vec<VectorIndexRef>, Vec<VectorIndexRef>)> {
        self.with_db(|db| {
            let read = db.begin_read().map_err(Self::storage_error)?;
            vector_paged::validate_pages_root(&read)?;
            let table = read.open_table(META_TABLE).map_err(Self::storage_error)?;
            let Some(previous) = table
                .get(Self::meta_key(name).as_str())
                .map_err(Self::storage_error)?
            else {
                return Ok((Vec::new(), Vec::new()));
            };
            let scratch = previous
                .value()
                .len()
                .saturating_add(meta.metadata_bytes())
                .saturating_mul(64)
                .saturating_add(8192);
            let _memory = self
                .proof_accountant()
                .map(|accountant| {
                    crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                        accountant,
                        scratch,
                        "sync",
                        "project_schema_incarnations",
                        "Raise MEMORY_LIMIT before changing this schema.",
                    )
                })
                .transpose()?;
            Self::changed_vector_incarnations(
                Self::decode_persisted_schema(previous.value())?,
                meta,
                ddl.iter(),
                name,
            )
        })
    }

    pub(crate) fn received_changed_vector_lifetimes_in_memory(
        name: &str,
        previous: &TableMeta,
        meta: &TableMeta,
        ddl: &[DdlChange],
    ) -> Result<(Vec<VectorIndexRef>, Vec<VectorIndexRef>)> {
        // This temporary projection detects operation meaning; it allocates no
        // durable identity and is not another schema lifetime owner.
        let last = previous.columns.len() as u64;
        Self::changed_vector_incarnations(
            (previous.clone(), last, (1..=last).collect()),
            meta,
            ddl.iter(),
            name,
        )
    }

    pub(super) fn schema_update_in_table<'a>(
        &self,
        table: &redb::Table<'_, &str, &[u8]>,
        name: &str,
        meta: &TableMeta,
        ddl: impl IntoIterator<Item = &'a DdlChange>,
    ) -> Result<EncodedSchemaUpdate> {
        let previous = table
            .get(Self::meta_key(name).as_str())
            .map_err(Self::storage_error)?;
        let scratch = previous
            .as_ref()
            .map_or(0, |value| value.value().len())
            .saturating_add(meta.metadata_bytes())
            .saturating_mul(64)
            .saturating_add(8192);
        let memory = self
            .proof_accountant()
            .map(|accountant| {
                crate::memory_accounting::OwnedMemoryReservation::try_new_for(
                    accountant,
                    scratch,
                    "ddl",
                    "encode_schema_incarnations",
                    "Raise MEMORY_LIMIT before changing this schema.",
                )
            })
            .transpose()?;
        let bytes = Self::encode_schema_update(
            previous.as_ref().map(|value| value.value()),
            meta,
            ddl,
            name,
        )?;
        Ok(EncodedSchemaUpdate {
            bytes,
            _memory: memory,
        })
    }

    pub(crate) fn vector_schema_binding(
        &self,
        index: &VectorIndexRef,
    ) -> Result<VectorSchemaBinding> {
        self.with_db(|db| {
            Self::vector_schema_binding_in_read(
                &db.begin_read().map_err(Self::storage_error)?,
                index,
            )
        })
    }

    pub(crate) fn vector_schema_binding_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
    ) -> Result<VectorSchemaBinding> {
        vector_paged::validate_pages_root(read)?;
        let meta = read.open_table(META_TABLE).map_err(Self::storage_error)?;
        let Some(value) = meta
            .get(Self::meta_key(&index.table).as_str())
            .map_err(Self::storage_error)?
        else {
            // Raw generation carriers need no schema that has not been declared.
            // A later declaration has a nonzero identity and cannot reuse this marker.
            return Ok(VectorSchemaBinding::UNBOUND);
        };
        let config = read.open_table(CONFIG_TABLE).map_err(Self::storage_error)?;
        let generation = config
            .get(
                crate::database::Database::durable_lineage_table_generation_key(&index.table)
                    .as_str(),
            )
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::Other("vector schema has no table generation".into()))?;
        Self::vector_schema_binding_from_records(value.value(), generation.value(), index)
    }

    pub(crate) fn vector_schema_binding_in_write(
        write: &redb::WriteTransaction,
        index: &VectorIndexRef,
    ) -> Result<VectorSchemaBinding> {
        let meta = write.open_table(META_TABLE).map_err(Self::storage_error)?;
        let Some(value) = meta
            .get(Self::meta_key(&index.table).as_str())
            .map_err(Self::storage_error)?
        else {
            // Raw generation carriers need no schema that has not been declared.
            // A later declaration has a nonzero identity and cannot reuse this marker.
            return Ok(VectorSchemaBinding::UNBOUND);
        };
        let config = write
            .open_table(CONFIG_TABLE)
            .map_err(Self::storage_error)?;
        let generation = config
            .get(
                crate::database::Database::durable_lineage_table_generation_key(&index.table)
                    .as_str(),
            )
            .map_err(Self::storage_error)?
            .ok_or_else(|| Error::Other("vector schema has no table generation".into()))?;
        Self::vector_schema_binding_from_records(value.value(), generation.value(), index)
    }

    fn vector_schema_binding_from_records(
        meta_bytes: &[u8],
        generation: &[u8],
        index: &VectorIndexRef,
    ) -> Result<VectorSchemaBinding> {
        let _decode_scope = crate::read_image_memory::ImageMemory::enter_decode_call()?;
        let (meta, _, ids) = Self::decode_persisted_schema(meta_bytes)?;
        let position = meta
            .columns
            .iter()
            .position(|column| {
                column.name == index.column && matches!(column.column_type, ColumnType::Vector(_))
            })
            .ok_or_else(|| Error::UnknownVectorIndex {
                index: index.clone(),
            })?;
        let column_incarnation = *ids
            .get(position)
            .ok_or_else(|| Error::Other("vector schema has no column incarnation".into()))?;
        let table_generation: u64 = Self::decode_exact(generation)?;
        if table_generation == 0 {
            return Err(Error::Other(
                "vector schema table generation is reserved".into(),
            ));
        }
        Ok(VectorSchemaBinding {
            table_generation,
            column_incarnation,
        })
    }

    pub(crate) fn validate_vector_schema_binding_in_read(
        read: &redb::ReadTransaction,
        index: &VectorIndexRef,
        expected: VectorSchemaBinding,
    ) -> Result<()> {
        if Self::vector_schema_binding_in_read(read, index)? != expected {
            return Err(vector_paged::corruption(
                "saved graph column incarnation disagrees with its captured schema",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_vector_schema_binding_in_write(
        write: &redb::WriteTransaction,
        index: &VectorIndexRef,
        expected: VectorSchemaBinding,
    ) -> Result<()> {
        if Self::vector_schema_binding_in_write(write, index)? != expected {
            return Err(vector_paged::corruption(
                "prepared graph column incarnation is stale after schema change",
            ));
        }
        Ok(())
    }
}

impl WriterSnapshot {
    /// Preserve the schema owner's identity counters from the checkpoint's root.
    pub(crate) fn copy_schema_into(
        self: &Arc<Self>,
        owner: writer_source::SourceWindowOwner,
        artifact: &RedbPersistence,
    ) -> Result<()> {
        let mut cursor = self.cursor(
            owner,
            "meta",
            writer_source::WriterCursorFamily::Text,
            std::ops::Bound::Included(b"table:"),
            std::ops::Bound::Excluded(b"table;"),
            false,
        )?;
        while let Some(window) = cursor.next(|_, bytes| {
            crate::read_image_memory::reserve(bytes.len().saturating_mul(32).saturating_add(4096))?;
            RedbPersistence::decode_persisted_schema(bytes)?;
            Ok(bytes.to_vec())
        })? {
            let key = std::str::from_utf8(window.position())
                .map_err(|_| Error::Other("schema key is not UTF-8".into()))?;
            artifact.with_db(|db| {
                let write = db.begin_write().map_err(RedbPersistence::storage_error)?;
                write
                    .open_table(META_TABLE)
                    .map_err(RedbPersistence::storage_error)?
                    .insert(key, window.value.as_slice())
                    .map_err(RedbPersistence::storage_error)?;
                vector_paged::bind_pages_root(&write)?;
                RedbPersistence::write_writer_recovery_bounds_in_write(&write, None)?;
                write.commit().map_err(RedbPersistence::storage_error)
            })?;
        }
        Ok(())
    }
}
