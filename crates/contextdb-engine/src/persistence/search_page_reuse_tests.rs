//! Where a vector search's authentication work actually goes, and what a
//! resident repeat of the same search is allowed to re-prove.
//!
//! Two independent observations are taken side by side, deliberately:
//!
//! * `take_work_for_test` / `take_work_bytes_for_test` are the **actual
//!   call-site physical bytes** — the length Redb hands back at the two places
//!   a proof chunk is genuinely read from the store
//!   (`read_node_charged`, `resolve_key`). Nothing else passes through them.
//! * `ReadWorkObservationScope` is the logical decode/digest census at the
//!   authentication call sites: rows and vectors decoded, rows and vectors
//!   hashed, and the logical bytes fed to Blake3.
//!
//! A claim supported by only one of the two is not made here.

use super::*;
use crate::direct_file_reader::test_seams::{
    ReadWorkCounters, ReadWorkObservationScope, ReadWorkPhase,
};
use crate::persistence::content_identity::{
    RESIDENT_PROOF_CHUNK_BYTES, reset_work_for_test, take_work_bytes_for_test, take_work_for_test,
};
use crate::{Database, MaintenancePolicy, ReadSession, ReadSessionOptions};
use contextdb_core::{Value, read_contract::ReadRoute};
use std::collections::HashMap;

pub(super) const DIM: usize = 768;
pub(super) const TEXT: usize = 512;
const CONTEXTS: usize = 2;
const SESSION_BYTES: usize = 64 * 1024 * 1024;

pub(super) fn row_uuid(row: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0x5eed_0000_0000_0000_0000_0000_0000_0000 + row as u128)
}

pub(super) fn context_uuid(context: usize) -> uuid::Uuid {
    uuid::Uuid::from_u128(0xc0de_0000_0000_0000_0000_0000_0000_0000 + context as u128)
}

pub(super) fn payload(row: usize) -> String {
    format!("{row:08x}").repeat(TEXT / 8)
}

/// Fixed finite unit vectors: no runtime threshold, no randomness between runs.
pub(super) fn vector(row: usize) -> Vec<f32> {
    let mut state = row as u64 + 0x9e37_79b9;
    let mut values: Vec<f32> = (0..DIM)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state as u32 as f64 / u32::MAX as f64 - 0.5) as f32
        })
        .collect();
    let norm = values.iter().map(|x| x * x).sum::<f32>().sqrt();
    for value in &mut values {
        *value /= norm;
    }
    values
}

pub(super) fn seed(path: &Path, rows_per_context: usize) {
    let rows = CONTEXTS * rows_per_context;
    let db = Database::open(path).expect("open the writable fixture");
    db.set_maintenance_policy(MaintenancePolicy::CallerDriven);
    db.execute(
        "CREATE TABLE search_work (id UUID PRIMARY KEY, context_id UUID CONTEXT_ID NOT NULL, \
         payload TEXT NOT NULL, embedding VECTOR(768) PARTITION_KEY (context_id) MAX_PARTITIONS 2)",
        &HashMap::new(),
    )
    .expect("declare the searched table");
    db.execute(
        "CREATE INDEX search_context ON search_work (context_id, id)",
        &HashMap::new(),
    )
    .expect("declare the context index");
    let tx = db.begin().expect("one commit for the whole fixture");
    for row in 0..rows {
        db.execute_in_tx(
            tx,
            "INSERT INTO search_work VALUES ($id, $context, $payload, $embedding)",
            &HashMap::from([
                ("id".into(), Value::Uuid(row_uuid(row))),
                (
                    "context".into(),
                    Value::Uuid(context_uuid(row / rows_per_context)),
                ),
                ("payload".into(), Value::Text(payload(row))),
                ("embedding".into(), Value::Vector(vector(row))),
            ]),
        )
        .expect("insert one fixture row");
    }
    db.commit(tx).expect("commit the fixture");
    for _ in 0..64 {
        db.run_maintenance_cycle()
            .expect("drive caller-owned build");
        let status = db.vector_store_for_test().hnsw_maintenance_status();
        if status.ready_partitions == CONTEXTS && status.remaining_partitions == 0 {
            break;
        }
    }
    db.close().expect("close the fixture");
}

pub(super) fn options() -> ReadSessionOptions {
    let mut options = ReadSessionOptions {
        memory_limit: Some(SESSION_BYTES),
        ..Default::default()
    };
    options.limits.memory = SESSION_BYTES as u64;
    options.limits.result_rows = 10_000;
    options.limits.result_bytes = 16 * 1024 * 1024;
    options.limits.work = 1_000_000;
    options.limits.active_ms = 60_000;
    options
}

pub(super) fn params(context: usize, query_row: usize) -> HashMap<String, Value> {
    HashMap::from([
        ("context".into(), Value::Uuid(context_uuid(context))),
        ("q".into(), Value::Vector(vector(query_row))),
    ])
}

/// The route a real `cg find` takes: no mode clause, so the declared default
/// (AUTO) picks the maintained index when a generation is ready.
pub(super) const FIND: &str = "SELECT id, payload FROM search_work WHERE context_id = $context \
     ORDER BY embedding <=> $q LIMIT 10";

/// The same range with the exhaustive route forced, kept beside it because the
/// two attribute very differently and only one of them is the product journey.
pub(super) const FIND_EXACT: &str = "SELECT id, payload FROM search_work WHERE context_id = $context \
     ORDER BY embedding <=> $q USE VECTOR EXACT LIMIT 10";

/// The `show` shape: one row by primary key, no vector work.
pub(super) const SHOW: &str = "SELECT id, payload FROM search_work WHERE id = $id";

/// One observation of one statement, from both seams at once.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Attribution {
    /// Actual Redb reads of a proof chunk, counted at the read itself.
    pub(super) chunk_reads: u64,
    /// Actual bytes those reads handed back, summed at the same call sites.
    pub(super) chunk_bytes: u64,
    pub(super) work: ReadWorkCounters,
    /// Laptop wall clock for this one statement. Recorded because it makes the
    /// split between chunk work and body work concrete; it is not used as
    /// a latency assertion.
    pub(super) micros: u128,
}

impl Attribution {
    fn line(&self, label: &str) -> String {
        format!(
            "{label:<34} chunk_reads={:<7} chunk_bytes={:<10} decoded_rows={:<6} \
             decoded_vectors={:<6} hashed_rows={:<6} hashed_vectors={:<6} hashed_bytes={:<10} us={}",
            self.chunk_reads,
            self.chunk_bytes,
            self.work.decoded_rows,
            self.work.decoded_vectors,
            self.work.hashed_rows,
            self.work.hashed_vectors,
            self.work.hashed_bytes,
            self.micros,
        )
    }
}

pub(super) fn observe<T>(run: impl FnOnce() -> T) -> (T, Attribution) {
    reset_work_for_test();
    let scope = ReadWorkObservationScope::enter(ReadWorkPhase::Query);
    let started = std::time::Instant::now();
    let value = run();
    let micros = started.elapsed().as_micros();
    let work = scope.counters();
    drop(scope);
    let chunk_reads = take_work_for_test().0;
    let chunk_bytes = take_work_bytes_for_test().0;
    (
        value,
        Attribution {
            chunk_reads,
            chunk_bytes,
            work,
            micros,
        },
    )
}

pub(super) fn search(
    reader: &ReadSession,
    statement: &str,
    context: usize,
    query_row: usize,
) -> usize {
    reader
        .execute(statement, &params(context, query_row))
        .expect("the file-route vector search answers")
        .rows
        .len()
}

pub(super) fn show(reader: &ReadSession, row: usize) -> usize {
    reader
        .execute(
            SHOW,
            &HashMap::from([("id".into(), Value::Uuid(row_uuid(row)))]),
        )
        .expect("the file-route point read answers")
        .rows
        .len()
}

/// Attribution only: where a search's verification work actually goes, cold
/// and on a resident repeat, on both vector routes and on the point read.
/// It asserts only that the answers are whole; the split is what it reports.
fn attribute(rows_per_context: usize) {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("search-page-reuse.db");
    seed(&path, rows_per_context);

    let reader = ReadSession::open_with_options(&path, options()).expect("open the file reader");
    assert_eq!(reader.route(), ReadRoute::File);

    let (find_cold, a_find_cold) = observe(|| search(&reader, FIND, 0, 3));
    let (find_warm, a_find_warm) = observe(|| search(&reader, FIND, 0, 3));
    let (find_other, a_find_other) = observe(|| search(&reader, FIND, 0, 61));
    let (show_cold, a_show_cold) = observe(|| show(&reader, 5));
    let (show_warm, a_show_warm) = observe(|| show(&reader, 5));
    let (show_other, a_show_other) = observe(|| show(&reader, 71));
    let (exact_cold, a_exact_cold) =
        observe(|| search(&reader, FIND_EXACT, 1, rows_per_context + 7));
    let (exact_warm, a_exact_warm) =
        observe(|| search(&reader, FIND_EXACT, 1, rows_per_context + 7));

    println!("--- attribution: {rows_per_context} rows per partition, {CONTEXTS} partitions ---");
    println!("{}", a_find_cold.line("find (default route), cold"));
    println!("{}", a_find_warm.line("find, identical repeat"));
    println!("{}", a_find_other.line("find, other query, same range"));
    println!("{}", a_show_cold.line("show by key, cold"));
    println!("{}", a_show_warm.line("show by key, identical repeat"));
    println!("{}", a_show_other.line("show by key, other row"));
    println!("{}", a_exact_cold.line("exhaustive route, cold"));
    println!("{}", a_exact_warm.line("exhaustive route, repeat"));

    assert_eq!(find_cold, 10);
    assert_eq!(find_warm, 10);
    assert_eq!(find_other, 10);
    assert_eq!(show_cold, 1);
    assert_eq!(show_warm, 1);
    assert_eq!(show_other, 1);
    assert_eq!(exact_cold, 10);
    assert_eq!(exact_warm, 10);
}

#[test]
fn where_a_repeat_resident_search_spends_its_verification_work_inside_the_witness_set() {
    attribute(128);
}

#[test]
fn where_a_repeat_resident_search_spends_its_verification_work_past_the_witness_set() {
    attribute(1024);
}

// ---------------------------------------------------------------------------
// Proofs
// ---------------------------------------------------------------------------

const TABLE: &str = "search_work";

fn load(path: &Path, limit: usize) -> ReadPersistenceLoad {
    load_read_image_with_memory(path, Some(limit)).expect("open the retained committed image")
}

fn snapshot_of(load: &ReadPersistenceLoad) -> SnapshotId {
    SnapshotId(
        load.image
            .newest_committed_tx_for_test()
            .map(|tx| tx.0)
            .expect("the fixture committed at least once"),
    )
}

/// Walk and authenticate every live row of the fixture table through the
/// production retained-source route. This is the same `verify()` hot path a
/// search takes for the rows and row vectors it projects.
fn walk_rows(load: &ReadPersistenceLoad) -> Result<usize> {
    let meta = &load.image.table_meta[TABLE];
    let snapshot = snapshot_of(load);
    let mut seen = 0;
    let declared_reader = crate::vector_read_memory::DeclaredReadMemory::from_reader_limit(
        load.source
            .verified_ordered_runs
            .memory
            .accountant
            .usage()
            .limit
            .map(|bytes| bytes as u64)
            .unwrap_or(u64::MAX),
    );
    let read_memory = declared_reader.child()?;
    load.source.bounded_visit_relational_rows::<Error>(
        TABLE,
        meta,
        snapshot,
        |_| {
            seen += 1;
            Ok(())
        },
        read_memory,
    )?;
    Ok(seen)
}

fn used_bytes(load: &ReadPersistenceLoad) -> usize {
    load.source
        .verified_ordered_runs
        .memory
        .accountant
        .usage()
        .used
}

/// An immutable proof chunk is read, decoded and proved against the committed
/// root ONCE while it stays resident, and the range that needed it is answered
/// from the retained authenticated copy afterwards. Eviction puts the cost
/// back exactly where it was.
#[test]
fn a_resident_authenticated_chunk_is_proved_once_and_re_proved_after_eviction() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("resident-chunks.db");
    seed(&path, 128);
    let load = load(&path, 64 * 1024 * 1024);

    let (cold_rows, cold) = observe(|| walk_rows(&load).expect("the cold walk authenticates"));
    let (chunks, bytes, _) = load.source.resident_proof_chunks_for_test();
    assert_eq!(cold_rows, 2 * 128);
    assert!(cold.chunk_reads > 0, "the cold walk really did read chunks");
    assert!(chunks > 0, "the cold walk retained what it proved");
    assert!(
        cold.chunk_bytes > 0 && bytes > 0,
        "both observations see the same cold load: {} physical bytes read, {bytes} retained",
        cold.chunk_bytes
    );
    assert!(
        bytes <= RESIDENT_PROOF_CHUNK_BYTES,
        "the retained set stays inside the reservation the source already holds: {bytes}"
    );

    let (warm_rows, warm) = observe(|| walk_rows(&load).expect("the resident walk authenticates"));
    assert_eq!(warm_rows, cold_rows, "the answer is unchanged");
    assert_eq!(
        (warm.chunk_reads, warm.chunk_bytes),
        (0, 0),
        "a repeat over resident proof state performs no physical proof read at all"
    );
    assert!(
        warm.work.hashed_bytes < cold.work.hashed_bytes,
        "and it hashes less: {} against {}",
        warm.work.hashed_bytes,
        cold.work.hashed_bytes
    );

    load.source.evict_resident_proof_chunks_for_test();
    load.source.evict_record_witnesses_for_test();
    assert_eq!(
        load.source.resident_proof_chunks_for_test(),
        (0, 0, 0),
        "nothing is retained after eviction"
    );

    let (reload_rows, reload) =
        observe(|| walk_rows(&load).expect("the reloaded walk authenticates"));
    assert_eq!(reload_rows, cold_rows);
    assert_eq!(
        (reload.chunk_reads, reload.chunk_bytes),
        (cold.chunk_reads, cold.chunk_bytes),
        "an evicted chunk is read, decoded and proved against the committed root again"
    );
    assert_eq!(
        load.source.resident_proof_chunks_for_test().0,
        chunks,
        "and it becomes resident again"
    );
}

/// The chunk reuse is the chunks, not the 512-record witness set beside it.
/// With every witness given back and every chunk left resident, the same
/// records are authenticated again from the committed root — walking the tree
/// in memory, reading no page and hashing no page.
#[test]
fn a_record_re_authenticated_from_the_root_still_reads_no_resident_chunk() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("witness-independent.db");
    seed(&path, 128);
    let load = load(&path, 64 * 1024 * 1024);

    let (_, cold) = observe(|| walk_rows(&load).expect("the cold walk authenticates"));
    assert!(cold.chunk_reads > 0);
    let (chunks, _, _) = load.source.resident_proof_chunks_for_test();

    let witnesses = load.source.evict_record_witnesses_for_test();
    assert!(witnesses > 0, "the cold walk did remember records");
    assert_eq!(
        load.source.resident_proof_chunks_for_test().0,
        chunks,
        "and the chunks are untouched by that"
    );

    let (rows, rewalk) = observe(|| {
        walk_rows(&load).expect("every record re-authenticates from the committed root")
    });
    assert_eq!(rows, 2 * 128, "the answer is unchanged");
    assert_eq!(
        (rewalk.chunk_reads, rewalk.chunk_bytes),
        (0, 0),
        "no record forgotten by the witness set re-reads a chunk that is still resident"
    );
}

/// The resident set is sized by what the SESSION declared, not by a
/// compile-time ceiling: a reader that declares more read memory keeps more of
/// its own proof resident. Its floor is free — it lives in the reservation the
/// source already took — and everything past the floor is admitted through the
/// session's own accountant and released the moment the chunk goes.
#[test]
fn a_retained_authenticated_chunk_is_sized_by_the_declared_session_and_charged_past_its_floor() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("charged-chunks.db");
    seed(&path, 512);

    let small = load(&path, 8 * 1024 * 1024);
    let large = load(&path, 64 * 1024 * 1024);
    assert_eq!(
        small.source.resident_proof_chunk_ceiling_for_test(),
        8 * 1024 * 1024 / 32,
        "the ceiling is this session's own share of its own declaration"
    );
    assert_eq!(
        large.source.resident_proof_chunk_ceiling_for_test(),
        64 * 1024 * 1024 / 32,
        "a reader that declares eight times as much keeps eight times as much resident"
    );

    walk_rows(&large).expect("the cold walk authenticates");
    let (chunks, bytes, charged) = large.source.resident_proof_chunks_for_test();
    let held = used_bytes(&large);
    assert!(chunks > 0 && bytes > 0);
    assert!(
        bytes <= large.source.resident_proof_chunk_ceiling_for_test(),
        "the retained set stays inside its ceiling: {bytes}"
    );
    assert!(
        bytes > RESIDENT_PROOF_CHUNK_BYTES && charged > 0,
        "this range needs more than the free floor, so some of it is charged: \
         {bytes} bytes, {charged} charged, floor {RESIDENT_PROOF_CHUNK_BYTES}"
    );
    assert!(
        bytes - charged <= RESIDENT_PROOF_CHUNK_BYTES,
        "and no more than the floor is held for free: {} of {RESIDENT_PROOF_CHUNK_BYTES}",
        bytes - charged
    );

    // One live charge covers each shared page until its last holder drops it. Eviction
    // here drops BOTH holders together (`evict_resident_proof_chunks_for_test`
    // clears the resident set and this statement's own retained set), so what
    // comes back is the resident set's charge (`charged`, above) PLUS this
    // statement's own admission -- leaf-locator overhead and any page only
    // the statement kept -- not the resident charge alone.
    let statement_charged = large.source.statement_own_charge_for_test();
    large.source.evict_resident_proof_chunks_for_test();
    let after = used_bytes(&large);
    assert_eq!(
        held - after,
        charged + statement_charged,
        "eviction gives back the resident set's charge plus the statement set's own charge"
    );
}

/// A retained proof can never be the reason a read is refused. With every
/// remaining byte of the declared session taken by something else and the
/// resident set full, the next read is answered — the set gives its own bytes
/// back before the proof admission refuses.
#[test]
fn a_read_is_answered_although_a_full_resident_set_is_holding_the_last_declared_bytes() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("reclaim-under-pressure.db");
    seed(&path, 512);
    let load = load(&path, 64 * 1024 * 1024);

    walk_rows(&load).expect("the cold walk authenticates");
    let (_, _, charged) = load.source.resident_proof_chunks_for_test();
    assert!(charged > 0, "the set really is holding session bytes");

    // Take everything still free, so the next walk's proof admissions can only
    // be served by the resident chunks giving their bytes back.
    let accountant = load.source.verified_ordered_runs.memory.accountant.clone();
    let available = accountant
        .usage()
        .available
        .expect("the fixture declares a limit");
    accountant
        .try_allocate_for(available, "test", "hold_the_rest", "")
        .expect("take the remaining declared bytes");

    let rows = walk_rows(&load).expect("a read is never refused while a cache is holding bytes");
    let (_, _, charged_after) = load.source.resident_proof_chunks_for_test();
    accountant.release(available);

    assert_eq!(rows, 2 * 512, "and the answer is whole");
    assert!(
        charged_after < charged,
        "the set gave its session bytes back rather than refuse the read: \
         {charged} -> {charged_after}"
    );
}

/// A resident proof cannot cover content that changed underneath it. The
/// chunks that prove the damaged row are already resident and are reused; the
/// row's own content is hashed and compared against them, so the read is the
/// existing typed corruption error and no row body is delivered.
#[test]
fn a_changed_row_body_is_typed_corruption_although_its_proof_chunks_are_resident() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("changed-body.db");
    seed(&path, 128);

    // Warm the resident set on the undamaged image first, then damage one row
    // body and prove the retained chunks still refuse it.
    {
        let warm = load(&path, 64 * 1024 * 1024);
        walk_rows(&warm).expect("the undamaged walk authenticates");
        assert!(warm.source.resident_proof_chunks_for_test().0 > 0);
    }

    let meta = declared_meta(&path);
    change_one_row_body(&path, &meta);

    let load = load(&path, 64 * 1024 * 1024);
    let (cold_result, cold) = observe(|| walk_rows(&load));
    let error = walk_rows(&load).expect_err("a changed row body cannot be authenticated");
    assert!(
        matches!(error, Error::StoreCorrupted { .. }),
        "the read reports the existing typed corruption error: {error:?}"
    );
    assert!(
        load.source.resident_proof_chunks_for_test().0 == 0,
        "row-head authentication refuses the changed body before retaining proof chunks"
    );
    assert!(matches!(cold_result, Err(Error::StoreCorrupted { .. })));
    assert_eq!(
        cold.chunk_reads, 0,
        "the damaged body refuses before proof loading"
    );
}

/// Rewrite one stored row body in place, keeping its physical key, so the
/// failure is content identity rather than the physical-key check beside it.
fn change_one_row_body(path: &Path, meta: &TableMeta) {
    let database = redb::Database::open(path).expect("open the committed fixture through Redb");
    let write = database.begin_write().expect("one write for the damage");
    {
        let name = RedbPersistence::rel_table_name(TABLE);
        let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name.as_str());
        let mut relation = write.open_table(definition).expect("the row table exists");
        let (key, mut row) = {
            let first = relation
                .iter()
                .expect("iterate the row table")
                .next()
                .expect("the fixture has rows")
                .expect("read one stored row");
            (
                first.0.value().to_vec(),
                RedbPersistence::decode_versioned_row(first.1.value(), Some(meta))
                    .expect("the stored row decodes"),
            )
        };
        row.values
            .insert("payload".to_owned(), Value::Text("changed".repeat(64)));
        let encoded = RedbPersistence::encode_versioned_row(&row, Some(meta))
            .expect("re-encode the changed row");
        assert_eq!(
            RedbPersistence::rel_row_key(&row),
            key,
            "the damage keeps the physical key, so content identity is what refuses it"
        );
        relation
            .insert(key.as_slice(), encoded.as_slice())
            .expect("store the changed body");
    }
    write.commit().expect("commit the damage");
}

/// The same reuse, through the public reader on the file route, on the journey
/// a user actually takes. A vector search answers; the identical search
/// repeated over the same resident proof state reads no chunk at all, and a
/// DIFFERENT search over the same range — one the witness set cannot answer —
/// still reads far fewer chunks than the cold one did.
///
/// Nothing is claimed for the row and vector BODIES it loads: those are
/// page-on-demand and are re-verified every time they are loaded.
#[test]
fn a_repeat_resident_search_on_the_public_reader_re_proves_fewer_chunks() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("public-repeat.db");
    seed(&path, 128);

    let reader = ReadSession::open_with_options(&path, options()).expect("open the file reader");
    assert_eq!(reader.route(), ReadRoute::File);

    let (cold_rows, cold) = observe(|| search(&reader, FIND, 0, 3));
    let (warm_rows, warm) = observe(|| search(&reader, FIND, 0, 3));
    let (other_rows, other) = observe(|| search(&reader, FIND, 0, 61));
    assert_eq!(cold_rows, 10);
    assert_eq!(warm_rows, 10);
    assert_eq!(other_rows, 10);
    assert!(
        cold.chunk_reads > 0 && cold.chunk_bytes > 0,
        "the cold search really did prove chunks against the committed root"
    );
    assert_eq!(
        (warm.chunk_reads, warm.chunk_bytes),
        (0, 0),
        "the identical repeat performs no physical proof-chunk read"
    );
    assert!(
        other.chunk_reads > warm.chunk_reads && other.chunk_reads < cold.chunk_reads,
        "a different search over the same resident range re-proves far fewer chunks: \
         {} against {} cold",
        other.chunk_reads,
        cold.chunk_reads
    );
    assert_eq!(
        (cold.chunk_reads, warm.chunk_reads, other.chunk_reads),
        (13, 0, 5),
        "fixed proof windows on this authenticated engine path"
    );
    assert!(
        other.work.hashed_bytes < cold.work.hashed_bytes,
        "and hashes less: {} against {}",
        other.work.hashed_bytes,
        cold.work.hashed_bytes
    );

    // A point read by primary key — the `show` shape — is the clearest case:
    // its whole proof path is resident after the first one.
    let (_, show_cold) = observe(|| show(&reader, 5));
    let (_, show_other) = observe(|| show(&reader, 71));
    assert_eq!(
        (show_other.chunk_reads, show_other.chunk_bytes),
        (0, 0),
        "a point read of a different row re-proves no chunk: cold was {} reads",
        show_cold.chunk_reads
    );
    assert!(
        show_other.work.decoded_rows > 0,
        "and it is honest about the row body it still loads and re-verifies"
    );
}

/// A damaged proof chunk is the existing typed corruption error even though
/// this reader is holding, and reusing, other chunks it has already proved —
/// and it stays that way on the next read, because a chunk that failed its
/// parent's hash is never retained.
///
/// Note on what is reachable here. Within ONE pinned snapshot the stored bytes
/// behind a resident chunk cannot change: Redb keeps the reader's pages alive
/// for the life of its read handle, so a later writer's pages are new ones the
/// pinned reader never sees. There is therefore no in-process way to damage
/// the exact bytes a resident chunk came from and read them again through that
/// same snapshot. What CAN be damaged, and is damaged here, is a chunk the
/// resident set does not yet hold, reached while the set is warm — which is
/// the case that matters: a resident ancestor must not vouch for a child.
#[test]
fn a_damaged_chunk_is_typed_corruption_while_other_chunks_are_resident_and_reused() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("damaged-chunk.db");
    seed(&path, 128);
    let meta = declared_meta(&path);

    // Prove the undamaged image first, so the shape of the refusal below is
    // known to be the damage and not the fixture.
    {
        let clean = load(&path, 64 * 1024 * 1024);
        walk_rows(&clean).expect("the undamaged walk authenticates");
        assert!(clean.source.resident_proof_chunks_for_test().0 > 0);
    }

    let damaged = damage_one_relational_leaf_chunk(&path);
    assert!(damaged, "the fixture has a Relational leaf chunk to damage");

    let load = load(&path, 64 * 1024 * 1024);
    // Meet the damaged chunk with the other chunks on its path already
    // resident and reused: a resident ancestor must not vouch for a child.
    let first = walk_rows(&load).expect_err("a damaged chunk cannot authenticate a record");
    assert!(
        matches!(first, Error::StoreCorrupted { .. }),
        "the read reports the existing typed corruption error: {first:?}"
    );
    let (chunks, _, _) = load.source.resident_proof_chunks_for_test();
    assert!(
        chunks > 0,
        "the damaged chunk was offered while other chunks on its path were already resident"
    );
    let _ = &meta;

    let again = walk_rows(&load).expect_err("and it is still corruption on the next read");
    assert!(
        matches!(again, Error::StoreCorrupted { .. }),
        "a chunk that failed its parent's hash is never retained: {again:?}"
    );
    assert_eq!(
        first.to_string(),
        again.to_string(),
        "the second read reports the same refusal, not a cached answer"
    );
}

fn declared_meta(path: &Path) -> TableMeta {
    let database = Database::open(path).expect("reopen to read the declaration");
    let meta = database.table_meta(TABLE).expect("the declared table");
    database.close().expect("close before the raw edit");
    meta
}

/// Rewrite one stored Relational leaf chunk so that it is self-consistent but
/// no longer the chunk its parent names. That is the damage a reused proof
/// must never paper over.
fn damage_one_relational_leaf_chunk(path: &Path) -> bool {
    use crate::persistence::content_identity::test_access as chunks;
    let database = redb::Database::open(path).expect("open the committed fixture through Redb");
    let write = database.begin_write().expect("one write for the damage");
    let damaged = chunks::damage_one_leaf_chunk_in_write(&write);
    write.commit().expect("commit the damage");
    damaged
}

/// A saved-graph search's shared authenticated proof pages (`content_proof`,
/// capped at 8 MiB per statement) stay resident across the whole cursor walk
/// -- that is the reuse `search_page_reuse_tests` documents everywhere else
/// in this file. Whatever is retained must be visible to the session that
/// declared the memory it occupies: `charged_bytes` must grow by at least
/// as much, and every one of the retained bytes must be covered either by a
/// live session charge or by the reader's own fixed free floor, never by
/// neither.
#[test]
fn retained_proof_pages_are_accounted_to_the_declared_session() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("charged-proof-pages.db");
    seed(&path, 1024);

    let mut opts = options();
    // Force the cursor to stay open past its first page, so the proof pages
    // a saved-graph search retains are still resident -- not yet released by
    // `release_statement_proof_reuse` -- at the moment this checks them.
    opts.limits.cursor_page_rows = 1;
    let reader = ReadSession::open_with_options(&path, opts).expect("open the file reader");

    let baseline = reader
        .charged_bytes()
        .expect("read the session's charged bytes");
    let (_rows, attribution) = observe(|| {
        let cursor = reader
            .open_cursor(FIND, &params(0, 3))
            .expect("open a saved-graph search cursor");
        let page = cursor.first_page().clone();
        assert!(
            page.has_more,
            "the fixture must need more than one page, so the cursor -- and its retained \
             proof pages -- are still open when this checks them"
        );
        page.rows.len()
    });
    let charged = reader
        .charged_bytes()
        .expect("read the session's charged bytes");
    let (retained, accounted) = reader
        .file_retained_content_proof_bytes_for_test()
        .expect("a file-route reader reports its retained proof-page bytes");

    assert!(
        attribution.chunk_reads > 0 && attribution.chunk_bytes > 0,
        "the search really did prove chunks against the committed root: {attribution:?}"
    );
    assert!(
        retained > 0,
        "the open cursor's saved-graph search must still be holding shared proof pages \
         (content_proof) while it has more pages left to answer"
    );
    assert!(
        charged >= baseline.saturating_add(retained as u64),
        "the session's charged bytes must grow by at least the proof pages this open cursor \
         is retaining: baseline={baseline} charged={charged} retained={retained}"
    );
    assert_eq!(
        accounted,
        retained,
        "every retained proof-page byte must be covered by a live session charge or the \
         reader's fixed free floor -- today {} of {retained} retained bytes are neither: a \
         page proved once for this statement's `verify_with_batch` call is kept afterward \
         with `_charge: None`, so it occupies real memory the declared session never learns \
         about",
        retained.saturating_sub(accounted)
    );
}

/// The smallest declared session at which this fixture's single-page `FIND`
/// search (`cursor_page_rows = 1`, first page only) completes -- this
/// search's own actual proof working set, converged on the same way
/// `reader_with_available_within` in `history_request_memory.rs` converges on
/// a target: repeatedly opening a fresh reader at a candidate declaration and
/// observing the real pass/fail boundary, never a byte count assumed to
/// transfer from one declaration to another. Reuse (the resident and statement
/// proof-page caches) gives way rather than cost anything the search does
/// not need, so this boundary is the search's genuine working set, not the
/// larger amount a generous session happens to retain afterward.
fn smallest_declaration_that_completes_the_single_page_find(path: &Path) -> usize {
    fn completes_at(path: &Path, declared: usize) -> bool {
        let mut opts = options();
        opts.limits.cursor_page_rows = 1;
        opts.memory_limit = Some(declared);
        opts.limits.memory = declared as u64;
        match ReadSession::open_with_options(path, opts) {
            Ok(reader) => reader.open_cursor(FIND, &params(0, 3)).is_ok(),
            Err(_) => false,
        }
    }

    // A declaration's own fixed reservations (the resident proof cache's
    // ceiling among them) scale with the declaration itself, so a byte count
    // measured against one declared session is not a safe floor against a
    // different, smaller one -- zero is the only floor that is always safe.
    let mut lo = 0usize;
    let mut hi = SESSION_BYTES;
    assert!(
        completes_at(path, hi),
        "this file's own declared session ceiling must be enough for this search"
    );
    assert!(
        !completes_at(path, lo),
        "a zero-byte declaration must not already be enough to complete a search"
    );
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if completes_at(path, mid) {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    hi
}

/// A declared session too small for the proof working set one saved-graph
/// search actually needs must refuse that search with a typed memory
/// refusal -- never silently exceed the declaration and answer anyway. A typed
/// refusal happens only when the actual proof working set cannot fit --
/// optional reuse (the resident and statement proof-page caches, which is
/// what this search retains afterward once it succeeds) gives way first. A
/// declaration at or above this same working set but below what cumulative
/// reuse across a long walk grows to still completes; that is what
/// `a_tight_session_completes_a_long_walk_instead_of_refusing_it_for_cumulative_reuse`
/// proves, on a different fixture built for that purpose, and is not
/// duplicated here.
#[test]
fn a_session_too_small_for_retained_proof_pages_is_refused_not_silently_exceeded() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("tight-declared-proof.db");
    seed(&path, 1024);

    let working_set = smallest_declaration_that_completes_the_single_page_find(&path);

    let mut tight = options();
    tight.limits.cursor_page_rows = 1;
    let declared = working_set - 1;
    tight.memory_limit = Some(declared);
    tight.limits.memory = declared as u64;
    let reader = ReadSession::open_with_options(&path, tight)
        .expect("a declaration one byte below the search's own working set still opens the reader");

    let outcome = reader.open_cursor(FIND, &params(0, 3));
    let outcome_error = outcome.as_ref().err().map(ToString::to_string);
    // `open_cursor`'s first-page fetch surfaces a bounded-read table-scan
    // refusal (a per-row admission distinct from content-identity proof-page
    // admission) through one of `read_session.rs`'s many existing internal
    // `Error::Other(error.to_string())` boundary conversions, which keep the
    // original typed error's message verbatim but drop the Rust enum
    // discriminant -- a long-standing, widespread pattern in this module,
    // not something the shared-page-ownership repair changed. So a typed
    // memory refusal here is either the bare `MemoryBudgetExceeded` variant
    // or an `Other` carrying that exact message.
    let is_memory_budget_refusal = matches!(
        outcome,
        Err(contextdb_core::Error::MemoryBudgetExceeded { .. })
    ) || outcome_error
        .as_deref()
        .is_some_and(|message| message.starts_with("memory budget exceeded"));
    assert!(
        is_memory_budget_refusal,
        "a session declared one byte below this search's own proof working set ({working_set}) \
         must be refused with a typed memory error, not silently answered: declared={declared} \
         outcome_ok={} outcome_error={outcome_error:?}",
        outcome.is_ok()
    );

    // The typed refusal is one statement's answer, not the session's end.
    assert_eq!(
        show(&reader, 5),
        1,
        "the session is still usable after the refusal"
    );
}

/// A page a statement copied out of residency shares the *decoded allocation*
/// with the resident set's own copy, but not its charge: `verified_node`
/// wraps the reused page in a brand new `VerifiedReadNode` with
/// `_charge: None, _floor: None` (`content_identity.rs:4432-4443`), and the
/// per-record path never routes that reused page's bytes through this
/// statement's own `charge` field either (`retain()` already re-admitted it
/// into the resident set before the caller's uncovered-bytes check runs).
/// After resident eviction, the open statement's own charge must continue to
/// cover the page until the statement releases it.
#[test]
fn a_proof_page_a_statement_still_holds_stays_charged_after_the_cache_evicts_it() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("evicted-while-held.db");
    seed(&path, 1024);

    let mut opts = options();
    // Force the cursor to stay open past its first page, so this statement's
    // retained proof pages are still resident -- not yet released by
    // `release_statement_proof_reuse` -- when eviction is forced below.
    opts.limits.cursor_page_rows = 1;
    let reader = ReadSession::open_with_options(&path, opts).expect("open the file reader");

    let cursor = reader
        .open_cursor(FIND, &params(0, 3))
        .expect("open a saved-graph search cursor");
    assert!(
        cursor.first_page().has_more,
        "the fixture must need more than one page, so this statement's retained proof pages \
         are still resident when eviction is forced below"
    );

    let (retained_before, accounted_before) = reader
        .file_retained_content_proof_bytes_for_test()
        .expect("a file-route reader reports its retained proof-page bytes");
    assert!(
        retained_before > 0,
        "the open cursor really did retain proof pages"
    );
    assert_eq!(
        accounted_before, retained_before,
        "before any eviction, every retained byte is covered by a live session charge or the \
         reader's fixed free floor -- exactly the repaired invariant \
         `retained_proof_pages_are_accounted_to_the_declared_session` pins"
    );

    let evicted = reader
        .file_evict_resident_proof_chunks_only_for_test()
        .expect("a file-route reader can force its resident set to evict");
    assert!(
        evicted > 0,
        "the resident set really did hold chunks to evict"
    );

    // The statement never dropped the cursor: it still holds exactly the same
    // pages it held a moment ago.
    let (retained_after, accounted_after) = reader
        .file_retained_content_proof_bytes_for_test()
        .expect("a file-route reader reports its retained proof-page bytes");
    assert_eq!(
        retained_after, retained_before,
        "this statement is still holding its own copy of every page it retained -- forcing the \
         resident set to evict does not, and must not, make the statement forget what it holds"
    );
    assert_eq!(
        accounted_after, retained_after,
        "a page this statement still holds must stay charged to the declared session after the \
         resident cache evicts its own copy of it -- today the resident set's eviction silently \
         drops the charge for every page this statement copied out of residency, so only {} of \
         {retained_after} bytes this statement is still holding are accounted for anywhere: \
         accounted_before={accounted_before} retained_before={retained_before}",
        accounted_after
    );

    drop(cursor);
}

/// A session declared for the actual proof working set -- what one record's
/// own streaming verification needs resident right now -- must complete a
/// long walk over many records even when it is far too small to hold,
/// cumulatively, every distinct page the whole walk ever proves. Reuse across
/// records is an optimization the declared session did not have to pay for up
/// front; it must give way (stop growing, let old entries go) rather than
/// turn a completable proof into a refusal. `admit_growth` must account for
/// the current working set, allowing old reusable pages to leave as the walk
/// advances rather than retaining a cumulative charge for the entire walk.
#[test]
fn a_tight_session_completes_a_long_walk_instead_of_refusing_it_for_cumulative_reuse() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("tight-streaming-not-cumulative.db");
    seed(&path, 4096);

    // Comfortably above what opening the committed image and any single
    // record's own tree-descent working set need (a few pages at most, well
    // under 4 KiB), but small enough that this long walk's cumulative
    // cross-record reuse -- charged once per newly proved page and never
    // released until the whole statement resets -- exhausts it before the
    // walk finishes, even though the search itself never needed more than a
    // handful of pages resident at any one instant.
    const TIGHT_DECLARED_BYTES: usize = 256 * 1024;
    let load = load_read_image_with_memory(&path, Some(TIGHT_DECLARED_BYTES))
        .expect("a session this size still opens the retained committed image");

    let outcome = walk_rows(&load);
    assert!(
        outcome.is_ok(),
        "a session declared for one record's own streaming working set must complete a long \
         walk over many records by letting cross-record reuse give way, not refuse it for the \
         reuse cache's cumulative growth: declared={TIGHT_DECLARED_BYTES} outcome={outcome:?}"
    );
    assert_eq!(
        outcome.expect("checked above"),
        2 * 4096,
        "every row is still answered once the walk completes"
    );
}

/// However a statement ends -- it answers every row and its cursor is closed,
/// it is refused for want of memory, or it is closed before it finishes --
/// this statement's own retained proof-page set (`content_proof`) must return
/// to empty: no node, no leaf locator, and no charge left behind. A page left
/// resident past its statement without a session charge, a reader floor, or
/// the resident set's own charge covering it is exactly the ownership gap
/// this whole file is about; cleanup is the other end of the same invariant.
#[test]
fn statement_cleanup_after_success_refusal_and_cancellation_leaves_no_uncharged_page() {
    let root = tempfile::tempdir().expect("fixture directory");
    let path = root.path().join("statement-cleanup.db");
    seed(&path, 1024);

    // -- success: a fully drained, explicitly closed cursor gives back every
    // page its statement retained.
    {
        let mut opts = options();
        opts.limits.cursor_page_rows = 1;
        let reader = ReadSession::open_with_options(&path, opts).expect("open the file reader");
        let mut cursor = reader
            .open_cursor(FIND, &params(0, 3))
            .expect("open a saved-graph search cursor");
        let mut page = cursor.first_page().clone();
        while page.has_more {
            page = cursor
                .fetch(std::num::NonZeroUsize::new(1))
                .expect("the drained walk answers every remaining page");
        }
        cursor.close().expect("close the fully drained cursor");
        let (retained, accounted) = reader
            .file_retained_content_proof_bytes_for_test()
            .expect("a file-route reader reports its retained proof-page bytes");
        assert_eq!(
            (retained, accounted),
            (0, 0),
            "a fully drained, closed statement must leave no node or leaf retained and no \
             charge outstanding for it: retained={retained} accounted={accounted}"
        );
    }

    // -- refusal: a declaration one byte below this exact fixture's own
    // single-page search working set -- the same boundary
    // `a_session_too_small_for_retained_proof_pages_is_refused_not_silently_exceeded`
    // derives and refuses (not the now-optional cumulative-reuse size the
    // old test relied on) -- resets its own statement set as part of
    // refusing.
    let refusal_ran = {
        let working_set = smallest_declaration_that_completes_the_single_page_find(&path);
        let mut tight = options();
        tight.limits.cursor_page_rows = 1;
        let declared = working_set - 1;
        tight.memory_limit = Some(declared);
        tight.limits.memory = declared as u64;
        let reader = ReadSession::open_with_options(&path, tight).expect(
            "a declaration one byte below the search's own working set still opens the reader",
        );
        let outcome = reader.open_cursor(FIND, &params(0, 3));
        let outcome_error = outcome.as_ref().err().map(ToString::to_string);
        assert!(
            outcome.is_err(),
            "declared={declared} is below this search's own proof working set ({working_set}) \
             and must be refused, not silently answered: outcome_error={outcome_error:?}"
        );
        let (retained_after_refusal, accounted_after_refusal) = reader
            .file_retained_content_proof_bytes_for_test()
            .expect("a file-route reader reports its retained proof-page bytes");
        assert_eq!(
            (retained_after_refusal, accounted_after_refusal),
            (0, 0),
            "a refused statement must leave no node or leaf retained and no charge \
             outstanding for it: retained={retained_after_refusal} \
             accounted={accounted_after_refusal}"
        );
        true
    };
    assert!(
        refusal_ran,
        "the refusal sub-scenario must actually run before the cancellation sub-scenario below"
    );

    // -- cancellation: a cursor closed before it finishes gives back exactly
    // what a fully drained one does.
    let cancellation_ran = {
        let mut opts = options();
        opts.limits.cursor_page_rows = 1;
        let reader = ReadSession::open_with_options(&path, opts).expect("open the file reader");
        let cursor = reader
            .open_cursor(FIND, &params(0, 3))
            .expect("open a saved-graph search cursor");
        assert!(
            cursor.first_page().has_more,
            "the fixture must need more than one page, so this cursor is closed while the \
             search still has work left -- a genuine cancellation, not an early success"
        );
        let (retained_before_close, _) = reader
            .file_retained_content_proof_bytes_for_test()
            .expect("a file-route reader reports its retained proof-page bytes");
        assert!(
            retained_before_close > 0,
            "the still-open cursor really did retain proof pages before it is cancelled"
        );
        cursor
            .close()
            .expect("close the cursor before its search finishes");
        let (retained_after_close, accounted_after_close) = reader
            .file_retained_content_proof_bytes_for_test()
            .expect("a file-route reader reports its retained proof-page bytes");
        assert_eq!(
            (retained_after_close, accounted_after_close),
            (0, 0),
            "a statement cancelled before it finishes must leave no node or leaf retained and \
             no charge outstanding for it, exactly as a completed one does: \
             retained={retained_after_close} accounted={accounted_after_close}"
        );
        true
    };
    assert!(
        cancellation_ran,
        "the cancellation sub-scenario must actually run, not be skipped by an earlier panic \
         in the success or refusal sub-scenarios above"
    );
}
