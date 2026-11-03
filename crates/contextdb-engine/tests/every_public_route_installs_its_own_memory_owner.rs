//! Every public route answers or refuses on its own: a caller never needs a
//! memory owner left on its thread by an earlier call.
//!
//! The memory a public operation decodes is charged to an owner that the
//! operation installs for itself. A consumer calls a route from whatever
//! thread it has; that thread carries no owner. So each route below is
//! called on a freshly spawned thread against a small populated store, and
//! its result must be an answer or a typed refusal the caller can act on,
//! never the internal "no owner installed" or "no pinned writer" refusal and
//! never a panic.
//!
//! The table covers every exported route whose call chain reaches either
//! internal refusal (`LISTED_ROUTES`). A route a consumer cannot reach
//! through the public crate API is named in `UNREACHABLE_BY_CONSUMERS` with
//! the reason; none is skipped silently, and
//! `the_route_table_names_every_refusal_reachable_route_once` keeps the two
//! lists and the driven rows equal to `LISTED_ROUTES`.

#[path = "every_public_route_installs_its_own_memory_owner/blob_routes.rs"]
mod blob_routes;
#[path = "every_public_route_installs_its_own_memory_owner/cli_routes.rs"]
mod cli_routes;
#[path = "every_public_route_installs_its_own_memory_owner/database_routes.rs"]
mod database_routes;
#[path = "every_public_route_installs_its_own_memory_owner/fixture.rs"]
mod fixture;
#[path = "every_public_route_installs_its_own_memory_owner/listed_routes.rs"]
mod listed_routes;
#[path = "every_public_route_installs_its_own_memory_owner/network_routes.rs"]
mod network_routes;
#[path = "every_public_route_installs_its_own_memory_owner/persistence_routes.rs"]
mod persistence_routes;
#[path = "every_public_route_installs_its_own_memory_owner/read_session_routes.rs"]
mod read_session_routes;
#[path = "every_public_route_installs_its_own_memory_owner/readback.rs"]
mod readback;
#[path = "every_public_route_installs_its_own_memory_owner/server_routes.rs"]
mod server_routes;
#[path = "every_public_route_installs_its_own_memory_owner/sync_routes.rs"]
mod sync_routes;
#[path = "every_public_route_installs_its_own_memory_owner/work_ledger_routes.rs"]
mod work_ledger_routes;

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::panic::AssertUnwindSafe;

/// The internal refusals a public caller must never see. The first two are
/// the decode-owner and preparation-writer refusals; the others are the
/// same missing-owner class under the names the engine itself groups with
/// them.
const MISSING_OWNER_REFUSALS: &[&str] = &[
    "requires an operation memory owner",
    "requires its pinned native original writer",
    "writer decode requires its declared memory accountant",
    "checkpoint source requires its installed accountant identity",
    "preparation original writer registration mismatch",
];

/// One public route and how to drive it once.
pub struct Row {
    pub route: &'static str,
    pub drive: fn() -> Outcome,
}

#[derive(Debug)]
pub enum Outcome {
    Answered,
    Refused(String),
    /// A failure whose only signal is a code, so it cannot be told apart
    /// from the missing-owner refusal; it fails the row.
    Unexplained(String),
    /// A route with a positive expectation answered with anything other
    /// than the expected value, a refusal of any kind included.
    Wrong(String),
    Panicked(String),
}

fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|text| (*text).to_string())
        })
        .unwrap_or_else(|| "a panic without a message".to_string())
}

fn answer<T, E: Debug>(result: Result<T, E>) -> Outcome {
    match result {
        Ok(_) => Outcome::Answered,
        Err(error) => Outcome::Refused(format!("{error:?}")),
    }
}

/// Call `route` on a newly spawned thread, which holds no memory owner.
pub fn call<T, E: Debug>(route: impl FnOnce() -> Result<T, E> + Send) -> Outcome {
    std::thread::scope(|scope| match scope.spawn(move || answer(route())).join() {
        Ok(outcome) => outcome,
        Err(payload) => Outcome::Panicked(panic_text(payload)),
    })
}

/// Call a route that returns nothing; its only failure is a panic.
pub fn call_unit(route: impl FnOnce() + Send) -> Outcome {
    call(move || {
        route();
        Ok::<(), ()>(())
    })
}

/// Call an async route on a newly spawned thread, driving it on that
/// thread's own single-threaded runtime.
pub fn call_async<T, E: Debug, F>(route: impl FnOnce() -> F + Send) -> Outcome
where
    F: std::future::Future<Output = Result<T, E>>,
{
    call(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for the calling thread")
            .block_on(route())
    })
}

/// Call an async route on a fresh thread and check the value it answered
/// with. Unlike the general table, every refusal fails here, as does a
/// wrong answer (a claim outcome that is not a synced win, a poll that
/// executed none of the hub's work).
pub fn call_async_expecting<T, E: Debug, F>(
    route: impl FnOnce() -> F + Send,
    expect: impl FnOnce(T) -> Result<(), String> + Send,
) -> Outcome
where
    F: std::future::Future<Output = Result<T, E>>,
{
    std::thread::scope(|scope| {
        let joined = scope
            .spawn(move || {
                let answered = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("a runtime for the calling thread")
                    .block_on(route());
                match answered {
                    Err(error) => Outcome::Wrong(format!("the route refused: {error:?}")),
                    Ok(value) => match expect(value) {
                        Ok(()) => Outcome::Answered,
                        Err(why) => Outcome::Wrong(why),
                    },
                }
            })
            .join();
        match joined {
            Ok(outcome) => outcome,
            Err(payload) => Outcome::Panicked(panic_text(payload)),
        }
    })
}

/// Every tracing event at `floor` or above emitted on the thread that
/// installs it, as text.
#[derive(Clone)]
pub struct EventLog {
    floor: tracing::Level,
    events: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    raise_on: Option<(&'static str, std::sync::Arc<std::sync::atomic::AtomicBool>)>,
}

impl EventLog {
    pub fn at(floor: tracing::Level) -> Self {
        Self {
            floor,
            events: Default::default(),
            raise_on: None,
        }
    }

    /// Also raise `flag` the moment an event containing `needle` is emitted.
    pub fn raising(
        mut self,
        needle: &'static str,
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        self.raise_on = Some((needle, flag));
        self
    }

    /// Install for the calling thread until the guard drops.
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_default(tracing_subscriber::registry().with(self.clone()))
    }

    pub fn first(&self) -> Option<String> {
        self.events.lock().expect("event log").first().cloned()
    }

    pub fn find(&self, needle: &str) -> Option<String> {
        self.events
            .lock()
            .expect("event log")
            .iter()
            .find(|event| event.contains(needle))
            .cloned()
    }

    pub fn text(&self) -> String {
        self.events.lock().expect("event log").join(" | ")
    }
}

struct EventText(String);

impl tracing::field::Visit for EventText {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn Debug) {
        self.0.push_str(&format!("{}={value:?} ", field.name()));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EventLog {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if *event.metadata().level() <= self.floor {
            let mut text = EventText(String::new());
            event.record(&mut text);
            if let Some((needle, flag)) = &self.raise_on
                && text.0.contains(needle)
            {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
            self.events.lock().expect("event log").push(text.0);
        }
    }
}

fn fails(outcome: &Outcome) -> bool {
    match outcome {
        Outcome::Answered => false,
        Outcome::Refused(reason) => MISSING_OWNER_REFUSALS
            .iter()
            .any(|refusal| reason.contains(refusal)),
        Outcome::Unexplained(_) | Outcome::Wrong(_) | Outcome::Panicked(_) => true,
    }
}

/// Drive every row, print each outcome, and fail with every route whose
/// outcome was a missing-owner refusal or a panic.
pub fn assert_routes_answer(rows: &[Row]) {
    let mut failures = Vec::new();
    for row in rows {
        let outcome = match std::panic::catch_unwind(AssertUnwindSafe(row.drive)) {
            Ok(outcome) => outcome,
            Err(payload) => Outcome::Panicked(format!(
                "the route's fixture could not be built: {}",
                panic_text(payload)
            )),
        };
        println!("{} => {outcome:?}", row.route);
        if fails(&outcome) {
            failures.push(format!("{}: {outcome:?}", row.route));
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} public routes failed for want of an owner they should install themselves:\n{}",
        failures.len(),
        rows.len(),
        failures.join("\n")
    );
}

/// Routes a consumer cannot call through the public API of the crate that
/// defines them, with the reason.
const UNREACHABLE_BY_CONSUMERS: &[(&str, &str)] = &[
    (
        "contextdb_engine::Database::production_smoke_sync_incarnation",
        "compiled only with the `production-smoke-driver` feature, the installed-release smoke facility; no consumer build enables it",
    ),
    (
        "contextdb_engine::persistence::read_persistence_test_scaffold::load_read_image_for_test_adapter",
        "its module exists only with the `test-seams` feature",
    ),
    (
        "contextdb_engine::persistence::RedbPersistence::rewrite_change_log",
        "compiled only with the `test-seams` feature: a fixture tool that cannot keep a received unit's history grouping",
    ),
    (
        "contextdb_engine::executor::bounded_read_test_support::execute",
        "test support for the engine's bounded-read suites, not a consumer route",
    ),
    (
        "contextdb_engine::executor::bounded_read_test_support::open_cursor",
        "test support for the engine's bounded-read suites, not a consumer route",
    ),
    (
        "contextdb_engine::SyncClient::with_authenticated_transport_for_test_with_read_memory",
        "compiled only with the `test-seams` feature",
    ),
    (
        "contextdb_engine::SyncClient::with_authenticated_transport_and_identity_for_test_with_read_memory",
        "compiled only with the `test-seams` feature",
    ),
    (
        "contextdb_engine::SyncServer::with_authenticated_transport_for_test_with_read_budget_provider",
        "compiled only with the `test-seams` feature",
    ),
    (
        "contextdb_engine::SyncServer::with_authenticated_transport_and_identity_for_test_with_read_budget_provider",
        "compiled only with the `test-seams` feature",
    ),
    (
        "contextdb_engine::read_contract::decode_cursor_page",
        "`read_contract` is a private module without the `test-seams` feature; consumers read pages through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_cursor_page_under_memory_ceiling",
        "`read_contract` is a private module without the `test-seams` feature; consumers read pages through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_metadata_body",
        "`read_contract` is a private module without the `test-seams` feature; consumers read metadata through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_metadata_page",
        "`read_contract` is a private module without the `test-seams` feature; consumers read metadata through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_metadata_page_under_memory_ceiling",
        "`read_contract` is a private module without the `test-seams` feature; consumers read metadata through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_query_result",
        "`read_contract` is a private module without the `test-seams` feature; consumers read answers through `ReadSession`",
    ),
    (
        "contextdb_engine::read_contract::decode_query_result_under_memory_ceiling",
        "`read_contract` is a private module without the `test-seams` feature; consumers read answers through `ReadSession`",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectFileReader::open",
        "`direct_file_reader` is a private module without the `test-seams` feature and `DirectFileReader` is not re-exported; `ReadSession`'s file route drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectFileReader::execute",
        "`direct_file_reader` is a private module without the `test-seams` feature and `DirectFileReader` is not re-exported; `ReadSession::execute` drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectFileReader::execute_with_cancellation",
        "`direct_file_reader` is a private module without the `test-seams` feature and `DirectFileReader` is not re-exported; `ReadSession::execute_with_cancellation` drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectFileReader::open_cursor",
        "`direct_file_reader` is a private module without the `test-seams` feature and `DirectFileReader` is not re-exported; `ReadSession::open_cursor` drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectFileReader::open_cursor_with_cancellation",
        "`direct_file_reader` is a private module without the `test-seams` feature and `DirectFileReader` is not re-exported; `ReadSession::open_cursor_with_cancellation` drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectCursor::fetch",
        "`direct_file_reader` is a private module without the `test-seams` feature; `ReadCursor` paging drives it",
    ),
    (
        "contextdb_engine::direct_file_reader::DirectCursor::fetch_with_cancellation",
        "`direct_file_reader` is a private module without the `test-seams` feature; `ReadCursor` paging drives it",
    ),
    (
        "contextdb_engine::local_transport::authentication::authenticate_framed_stream_handshake",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::carrier::UnixLocalCarrier::probe_stale",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::carrier::UnixLocalCarrier::receive_message",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::carrier::UnixLocalCarrier::receive_response",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::carrier::UnixLocalCarrier::send_message",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::carrier::UnixLocalCarrier::write_request",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::decode_handshake_exact",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::decode_message_exact",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::receive_framed_ordinary_result",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::LocalEngineFailure::into_error",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::LocalProtocolBoundary::encode_frame",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::LocalProtocolBoundary::receive_frame",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::LocalProtocolBoundary::receive_ordinary_result_frame",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::framing::OrdinaryResultReceiver::receive",
        "`local_transport` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::local_transport::stale::reconcile_stale_channel",
        "`local_transport` is a private module without the `test-seams` feature; a writer open drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::connect",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::encode_envelope",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::read_envelope",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::receive_ordinary",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::request",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_engine::owner_read::client::OwnerClient::request_watching",
        "`owner_read` is a private module without the `test-seams` feature; `ReadSession`'s owner route drives it",
    ),
    (
        "contextdb_cli::repl::feed_line",
        "`repl` is a private module of the CLI and `feed_line` is not re-exported; `contextdb_cli::run` drives it for every input line",
    ),
    (
        "contextdb_engine::owner_read::service::OwnerReadService::start",
        "`owner_read` is a private module without the `test-seams` feature; a writer open with owner reads enabled starts it",
    ),
];

/// Vector-crate routes. `contextdb-vector` does not depend on the engine, so
/// these reach either refusal only through engine trait objects (durable
/// membership sources, paged persistence, memory budgets) that the engine
/// installs in its own vector store. No public engine route hands that store
/// out; the only accessors are `Database::vector_store_for_test` and its
/// reader twins under the `test-seams` feature. A store a consumer builds
/// from `contextdb-vector` itself carries no engine source, so neither
/// refusal is on its path. The engine routes that drive these
/// (`Database::execute`, `Database::query_vector`,
/// `Database::run_maintenance_cycle`, the sync apply routes) are driven above.
const VECTOR_STORE_ROUTES: &[&str] = &[
    "contextdb_vector::MemVectorExecutor::bounded_brute_force_step",
    "contextdb_vector::MemVectorExecutor::bounded_brute_force_step_charged",
    "contextdb_vector::MemVectorExecutor::bounded_brute_force_step_with_query",
    "contextdb_vector::MemVectorExecutor::search_with_mode",
    "contextdb_vector::MemVectorExecutor::search_with_mode_admission",
    "contextdb_vector::MemVectorExecutor::search_with_mode_owned",
    "contextdb_vector::MemVectorExecutor::search_with_strategy",
    "contextdb_vector::VectorStore::apply_changes_with_accountant",
    "contextdb_vector::VectorStore::apply_changes_with_accountant_ref",
    "contextdb_vector::VectorStore::apply_deletes",
    "contextdb_vector::VectorStore::apply_deletes_with_accountant",
    "contextdb_vector::VectorStore::apply_inserts",
    "contextdb_vector::VectorStore::apply_inserts_with_accountant",
    "contextdb_vector::VectorStore::apply_moves",
    "contextdb_vector::VectorStore::apply_moves_with_accountant",
    "contextdb_vector::VectorStore::apply_partitioned_changes_with_accountant",
    "contextdb_vector::VectorStore::apply_partitioned_deletes",
    "contextdb_vector::VectorStore::apply_partitioned_inserts",
    "contextdb_vector::VectorStore::apply_partitioned_inserts_with_accountant",
    "contextdb_vector::VectorStore::apply_partitioned_moves",
    "contextdb_vector::VectorStore::apply_partitioned_moves_with_accountant",
    "contextdb_vector::VectorStore::durable_entries_of_partitions_containing_rows",
    "contextdb_vector::VectorStore::ensure_all_raw_vectors_loaded",
    "contextdb_vector::VectorStore::ensure_raw_partition_loaded",
    "contextdb_vector::VectorStore::ensure_raw_partitions_containing_rows",
    "contextdb_vector::VectorStore::entries_for_index",
    "contextdb_vector::VectorStore::entries_for_partition",
    "contextdb_vector::VectorStore::entries_for_partition_at_snapshot",
    "contextdb_vector::VectorStore::entries_for_partition_generation_at_snapshot",
    "contextdb_vector::VectorStore::load_entries_for_rows",
    "contextdb_vector::VectorStore::load_live_entries_for_row",
    "contextdb_vector::VectorStore::load_live_entry_for_row",
    "contextdb_vector::VectorStore::partition_info",
    "contextdb_vector::VectorStore::partition_infos",
    "contextdb_vector::VectorStore::preload_dormant_partition_generation",
    "contextdb_vector::VectorStore::prepare_partitioned_batch",
    "contextdb_vector::VectorStore::prepare_partitioned_batch_with_registered_snapshots",
    "contextdb_vector::VectorStore::recheck_policy_maintenance_failures",
    "contextdb_vector::VectorStore::register_index_with_layout",
    "contextdb_vector::VectorStore::step_partition_paged_build",
    "contextdb_vector::VectorStore::step_partition_paged_build_with_plan",
    "contextdb_vector::VectorStore::storage_bytes_per_entry",
    "contextdb_vector::VectorStore::storage_bytes_per_entry_in_partition",
    "contextdb_vector::VectorStore::try_all_entries",
    "contextdb_vector::VectorStore::try_all_partitioned_entries",
    "contextdb_vector::VectorStore::try_live_entry_for_row_in_partition",
    "contextdb_vector::VectorStore::try_partition_info",
    "contextdb_vector::VectorStore::try_partitioned_entries_matching",
    "contextdb_vector::mem::BoundedVectorCandidateGroups::has_pending_tail_without_load",
    "contextdb_vector::store::IndexState::preload_dormant_sealed_generation",
];

/// Routes in a crate with no path to either refusal. None remain.
const CANNOT_REACH_EITHER_REFUSAL: &[(&str, &str)] = &[];

fn driven_rows() -> Vec<Row> {
    let mut rows = Vec::new();
    rows.extend(database_routes::rows());
    rows.extend(work_ledger_routes::rows());
    rows.extend(persistence_routes::rows());
    rows.extend(read_session_routes::rows());
    rows.extend(sync_routes::rows());
    rows.extend(blob_routes::rows());
    rows.extend(network_routes::rows());
    rows.extend(server_routes::rows());
    rows.extend(cli_routes::rows());
    rows
}

#[test]
fn the_route_table_names_every_refusal_reachable_route_once() {
    let listed: BTreeSet<&str> = listed_routes::LISTED_ROUTES.iter().copied().collect();
    assert_eq!(
        listed.len(),
        listed_routes::LISTED_ROUTES.len(),
        "each listed route is named once"
    );
    let mut accounted = Vec::new();
    accounted.extend(driven_rows().iter().map(|row| row.route));
    accounted.extend(UNREACHABLE_BY_CONSUMERS.iter().map(|(route, _)| *route));
    accounted.extend(VECTOR_STORE_ROUTES.iter().copied());
    accounted.extend(CANNOT_REACH_EITHER_REFUSAL.iter().map(|(route, _)| *route));
    let unique: BTreeSet<&str> = accounted.iter().copied().collect();
    let repeated: Vec<&str> = {
        let mut seen = BTreeSet::new();
        accounted
            .iter()
            .copied()
            .filter(|route| !seen.insert(*route))
            .collect()
    };
    assert!(
        repeated.is_empty(),
        "routes accounted for twice: {repeated:?}"
    );
    let missing: Vec<&&str> = listed.difference(&unique).collect();
    let extra: Vec<&&str> = unique.difference(&listed).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "every listed route is driven or named unreachable with a reason, and nothing else is: missing {missing:?}, not listed {extra:?}"
    );
}

#[test]
fn a_positive_expectation_fails_every_refusal_and_every_wrong_answer() {
    let answered = |value: u32| {
        call_async_expecting(
            move || async move { Ok::<_, contextdb_core::Error>(value) },
            |value| match value {
                7 => Ok(()),
                other => Err(format!("expected 7, answered {other}")),
            },
        )
    };
    assert!(!fails(&answered(7)), "the expected answer passes");
    assert!(fails(&answered(8)), "a wrong successful answer fails");
    for refusal in [
        contextdb_core::Error::ReadCancelled,
        contextdb_core::Error::Other("an unrelated refusal".into()),
    ] {
        let outcome =
            call_async_expecting(move || async move { Err::<u32, _>(refusal) }, |_| Ok(()));
        assert!(
            fails(&outcome),
            "a refusal without the owner text fails: {outcome:?}"
        );
    }
    assert!(
        !fails(&Outcome::Refused(format!(
            "{:?}",
            contextdb_core::Error::ReadCancelled
        ))),
        "the general table still accepts a typed refusal that is not the owner refusal"
    );
}

#[test]
fn database_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&database_routes::rows());
}

#[test]
fn work_ledger_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&work_ledger_routes::rows());
}

#[test]
fn storage_layer_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&persistence_routes::rows());
}

#[test]
fn read_session_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&read_session_routes::rows());
}

#[test]
fn sync_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&sync_routes::rows());
}

#[test]
fn blob_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&blob_routes::rows());
}

#[test]
fn endpoint_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&network_routes::rows());
}

#[test]
fn server_work_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&server_routes::rows());
}

#[test]
fn cli_routes_answer_without_an_ambient_owner() {
    assert_routes_answer(&cli_routes::rows());
}
