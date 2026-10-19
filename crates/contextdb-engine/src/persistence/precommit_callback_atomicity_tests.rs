//! The private pre-commit callback is fallible before its enclosing Redb
//! transaction commits. These controls exercise that exact callback rather
//! than a pause or an externally captured transaction handle.

use super::*;

fn options() -> FlushDataOptions<'static> {
    FlushDataOptions {
        local_erasure: None,
        sink_events: &[],
        trigger_audits: &[],
        schema_ddl: SchemaDdlPersistence {
            event_bus: None,
            trigger: None,
        },
        max_sink_queue_depth: usize::MAX,
        snapshots: FlushDataSnapshots::default(),
        received_schema: None,
        partition_mutations: None,
    }
}

fn config_write(key: &str, lsn: u64) -> WriteSet {
    let mut write_set = WriteSet::new();
    write_set.commit_lsn = Some(Lsn(lsn));
    write_set.finalized_tx = Some(TxId(lsn));
    write_set.requires_commit_lsn = true;
    write_set
        .config_writes
        .push((key.to_owned(), format!("value-{lsn}").into_bytes()));
    write_set
}

fn durable_state(store: &RedbPersistence, key: &str) -> (Option<Vec<u8>>, Vec<(u8, Vec<u8>)>) {
    store
        .with_db(|database| {
            let read = database
                .begin_read()
                .map_err(RedbPersistence::storage_error)?;
            let value = match read.open_table(CONFIG_TABLE) {
                Ok(table) => table
                    .get(key)
                    .map_err(RedbPersistence::storage_error)?
                    .map(|value| value.value().to_vec()),
                Err(redb::TableError::TableDoesNotExist(_)) => None,
                Err(error) => return Err(RedbPersistence::storage_error(error)),
            };
            let roots = read
                .open_table(CONTENT_IDENTITY_ROOTS_TABLE)
                .map_err(RedbPersistence::storage_error)?
                .iter()
                .map_err(RedbPersistence::storage_error)?
                .map(|entry| {
                    let (family, root) = entry.map_err(RedbPersistence::storage_error)?;
                    Ok((family.value(), root.value().to_vec()))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok((value, roots))
        })
        .unwrap()
}

#[test]
fn callback_refusal_after_real_identity_proof_aborts_durable_state_and_retries() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("callback.db");
    let database = crate::Database::open(&path).unwrap();
    database.close().unwrap();
    let store = RedbPersistence::open_with_cache(&path, 0).unwrap();
    let key = "precommit_callback_atomicity";
    let before = durable_state(&store, key);
    let write_set = config_write(key, 1);

    let error = store
        .flush_data_with_precommit(&write_set, &[], options(), || {
            Err(Error::Other(
                "deliberate post-proof callback refusal".into(),
            ))
        })
        .unwrap_err();
    assert!(
        matches!(error, Error::Other(ref message) if message == "deliberate post-proof callback refusal"),
        "the real callback Result, not a transaction capture or pause, must abort the flush: {error:?}"
    );
    assert_eq!(
        durable_state(&store, key),
        before,
        "the failed post-proof callback must leave neither the config row nor an advanced identity root durable"
    );

    store
        .flush_data_with_precommit(&write_set, &[], options(), || Ok(()))
        .unwrap();
    let after_retry = durable_state(&store, key);
    assert_eq!(after_retry.0, Some(b"value-1".to_vec()));
    assert_ne!(
        after_retry.1, before.1,
        "retry must run the actual identity proof and commit its root exactly once after the refusal"
    );
}
