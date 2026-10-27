//! "Vigil and unpartitioned vectors" and "Writable startup": populated vector
//! memberships load and release within bounded memory, including unpartitioned columns.
use crate::resident_history_support as support;
use contextdb_core::{Value, VectorPartitionKey, Wallclock};
use contextdb_engine::database::resident_history_test_seams::observe;
use contextdb_engine::read_work_probe::{
    self,
    allocator_events::{Request, Window},
};
use contextdb_engine::vector_read_memory::{self, statement_child_proof as statement};
use contextdb_vector::VectorPartitionRef;
use std::alloc::Layout;
use support::*;

// These filters select actual allocator events, never stand in for a request.
// EXACT retains index names and route receipts, not candidate/score arrays.
fn membership_request_layouts(table: &str) -> [Layout; 9] {
    let (provider, token) = read_work_probe::retained_trace_payload_layouts();
    let provider = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(provider)
        .unwrap()
        .0
        .pad_to_align();
    [
        Layout::array::<u8>(table.len()).unwrap(),
        Layout::array::<u8>("v".len()).unwrap(),
        Layout::array::<contextdb_vector::mem::VectorPartitionSearchTrace>(1).unwrap(),
        Layout::array::<contextdb_vector::mem::VectorPartitionSearchTrace>(2).unwrap(),
        Layout::array::<contextdb_vector::mem::VectorPartitionSearchTrace>(3).unwrap(),
        Layout::array::<contextdb_vector::mem::VectorPartitionSearchTrace>(4).unwrap(),
        provider,
        contextdb_core::read_memory::ReadShared::<contextdb_core::read_memory::ReadCredit>::layout(
        ),
        token,
    ]
}

fn membership_request(window: &Window, payload: usize, layout: (usize, usize)) -> Request {
    assert_ne!(payload, 0);
    assert_ne!(layout.0, 0);
    let request = window
        .live_request(payload)
        .expect("actual live allocator request required");
    assert_eq!((request.bytes, request.align), layout);
    assert!(request.ordinal > 0 && request.event_ordinal > 0);
    let mut matches = 0;
    for index in 0..window.request_count().unwrap() {
        if window.request(index).unwrap() == request {
            matches += 1;
            assert_eq!(window.physical_free_event(index).unwrap(), None);
        }
    }
    assert_eq!(
        matches, 1,
        "one original pointer/Layout/ordinal, still physically live"
    );
    request
}

fn membership_control_request(window: &Window, control: statement::ControlAllocation) -> Request {
    let actual = control
        .actual_request
        .expect("native constructor must supply its actual request");
    let request = membership_request(window, control.payload, control.layout);
    assert_eq!((request.pointer, request.bytes, request.align), actual);
    assert_eq!(request.ordinal, control.ordinal);
    request
}

fn membership_parent_equation(reader: &statement::Observation, inline: u64, parent: u64) {
    let movement = reader.identity;
    assert_eq!(movement.target_last_held, inline);
    let delta = inline as i128 - movement.target_initial_held as i128;
    assert_eq!(
        movement.target_parent_net, delta,
        "forwarded parent movement equals the independently read original inline delta"
    );
    assert_eq!(
        movement.direct_parent_net,
        movement.direct_parent_reserves as i128 - movement.direct_parent_releases as i128
    );
    let expected = reader.parent_before as i128
        + delta
        + movement.sibling_parent_net
        + movement.direct_parent_net
        + movement.target_control_parent_net;
    assert_eq!(
        parent as i128, expected,
        "parent baseline plus real target, sibling, direct and legacy-control movement"
    );
    assert_eq!(reader.complete_parent_mismatches, 0);
    assert_eq!(reader.parent_funding_mismatches, 0);
}

#[expect(
    clippy::manual_contains,
    reason = "The bounded membership assertion spells out the equality check for each child."
)]
fn membership_retained_and_final_free(
    db: &contextdb_engine::Database,
    window: Window,
    parent: &vector_read_memory::DeclaredReadRoot,
) {
    let reader = statement::current().expect("original statement observation stays active");
    let trace = read_work_probe::retained_trace_invoice(db, |_| {})
        .expect("genuine retained trace required");
    assert_eq!(
        reader
            .trace_new
            .expect("actual trace publication receipt required"),
        trace
    );
    let produced = reader
        .trace_produced
        .expect("actual trace handoff receipt required");
    assert_eq!(produced.allocations, trace.allocations);
    assert_eq!(produced.capacities, trace.capacities);
    assert_eq!(produced.prepaid_bytes, trace.prepaid_bytes);
    assert_eq!(produced.original_payer, trace.original_payer);
    assert_eq!(produced.owner_payload_pointer, trace.owner_payload_pointer);
    let native = reader
        .native_controls
        .expect("original native constructor receipts required");
    let provider = reader
        .trace_owner_controls
        .expect("original trace-provider receipts required");
    let block = membership_control_request(&window, native.block);
    let expected_child = vector_read_memory::native_control_layouts_for_test().1;
    assert_eq!(
        (block.bytes, block.align),
        (expected_child.size(), expected_child.align()),
        "typed native child/invoice inventory is independent of the observed request"
    );
    assert_eq!(
        block.funding_before_system,
        Some((native.parent_counter, native.parent_after_block)),
        "the actual allocator samples the original root BEFORE requesting the child block"
    );
    let keeper = native.keeper.expect("original native keeper required");
    let adapter = native.adapter.expect("original native adapter required");
    let native_keeper = membership_control_request(&window, keeper);
    let native_adapter = membership_control_request(&window, adapter);
    assert_eq!(native.owner, block.pointer);
    assert_eq!(native.owner, reader.owner_identity);
    assert_eq!(native.parent, reader.parent_identity);
    assert_eq!(native.payload, reader.native_payload_identity);
    assert_eq!(native.counter, reader.native_counter);
    assert!(block.contains(native.payload) && block.contains(native.counter));
    assert_eq!(block.bytes, reader.control_bytes);
    assert_eq!(native.initial_child_bytes, 0);
    assert_eq!(
        native.parent_after_block,
        native
            .parent_before_block
            .expect("original block admission")
            + block.bytes as u64
    );
    assert_eq!(native.keeper_payer, Some(native.owner));
    assert_eq!(native.adapter_payer, Some(native.owner));
    assert!(!native.keeper_refused && !native.adapter_refused);
    assert_eq!(native.keeper_credit_bytes, native_keeper.bytes);
    assert_eq!(native.adapter_invoice_bytes, native_adapter.bytes);
    assert_eq!(native.outer_keeper, keeper.payload);
    assert_eq!(native.inner_keeper, keeper.payload);
    let keeper_layout = contextdb_core::read_memory::ReadShared::<
        contextdb_core::read_memory::ReadCredit,
    >::layout();
    assert_eq!(
        (native_keeper.bytes, native_keeper.align),
        (keeper_layout.size(), keeper_layout.align())
    );
    let adapter_payload = Layout::from_size_align(
        native.adapter_payload_layout.0,
        native.adapter_payload_layout.1,
    )
    .unwrap();
    let adapter_layout = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(adapter_payload)
        .unwrap()
        .0
        .pad_to_align();
    assert_eq!(
        (native_adapter.bytes, native_adapter.align),
        (adapter_layout.size(), adapter_layout.align())
    );
    let kb = native.keeper_before.expect("native keeper admission entry");
    let ka = native
        .keeper_after
        .expect("native keeper admission completion");
    let ab = native
        .adapter_before
        .expect("native adapter admission entry");
    let aa = native
        .adapter_after
        .expect("native adapter admission completion");
    for request in [native_keeper, native_adapter] {
        assert_eq!(
            request.funding_before_system,
            Some((native.counter, aa.child_bytes)),
            "both real native controls were fully admitted before either System request"
        );
    }
    for sample in [kb, ka, ab, aa] {
        assert_eq!(sample.child_counter, native.counter);
        assert_eq!(sample.parent_counter, native.parent_counter);
    }
    assert_eq!(kb.child_bytes, 0);
    assert_eq!(ka.child_bytes, native_keeper.bytes as u64);
    assert_eq!(ab.child_bytes, ka.child_bytes);
    assert_eq!(
        aa.child_bytes,
        (native_keeper.bytes + native_adapter.bytes) as u64
    );
    assert_eq!(
        ka.parent_bytes,
        kb.parent_bytes + native_keeper.bytes as u64
    );
    assert_eq!(ab.parent_bytes, ka.parent_bytes);
    assert_eq!(
        aa.parent_bytes,
        ab.parent_bytes + native_adapter.bytes as u64
    );
    assert!(
        block.ordinal < native_keeper.ordinal && native_keeper.ordinal < native_adapter.ordinal
    );

    assert_eq!(trace.original_payer, native.owner);
    assert_eq!(trace.forwarding_payer, native.owner);
    assert_eq!(provider.payer, native.owner);
    assert_eq!(provider.parent, native.parent);
    assert_eq!(provider.counter, native.counter);
    assert_eq!(provider.parent_counter, native.parent_counter);
    assert_eq!(provider.native_forwarder, adapter.payload);
    assert_eq!(provider.native_forwarder, reader.native_forwarder_identity);
    assert_eq!(
        provider.native_keeper, keeper.payload,
        "the retained underlying invoice aliases Kn; it is counted only once"
    );
    assert_eq!(provider.native_adapter_invoice_bytes, native_adapter.bytes);
    assert_eq!(provider.provider_payload, trace.owner_payload_pointer);
    assert!(trace.owner_control_invoice_present && trace.admission_control_invoice_present);
    let (provider_payload, token_layout) = read_work_probe::retained_trace_payload_layouts();
    let provider_layout = Layout::new::<[std::sync::atomic::AtomicUsize; 2]>()
        .extend(provider_payload)
        .unwrap()
        .0
        .pad_to_align();
    assert_eq!(
        provider.provider_layout,
        (provider_layout.size(), provider_layout.align())
    );
    assert_eq!(
        provider.keeper_layout,
        (keeper_layout.size(), keeper_layout.align())
    );
    let trace_provider =
        membership_request(&window, provider.provider_payload, provider.provider_layout);
    let trace_keeper = membership_request(&window, provider.keeper_payload, provider.keeper_layout);
    for request in [trace_provider, trace_keeper] {
        assert_eq!(
            request.funding_before_system,
            Some((native.counter, provider.after_admission)),
            "both real trace controls were paid on the original child before System"
        );
    }
    assert_eq!(
        provider.after_admission,
        provider.before_admission + (trace_provider.bytes + trace_keeper.bytes) as u64
    );
    assert_eq!(
        (trace.prepaid_control.1, trace.prepaid_control.2),
        (token_layout.size(), token_layout.align())
    );
    let token = membership_request(
        &window,
        trace.prepaid_control.0,
        (trace.prepaid_control.1, trace.prepaid_control.2),
    );
    assert!(
        token.funding_before_system.is_some(),
        "prepaid token requires an independent pre-System original-payer sample"
    );
    assert_eq!(token.funding_before_system.unwrap().0, native.counter);
    assert_eq!(token.pointer, trace.prepaid_control.0);
    assert_eq!(token.bytes, trace.prepaid_control_bytes);
    assert!(
        native_adapter.ordinal < trace_keeper.ordinal
            && trace_keeper.ordinal < trace_provider.ordinal
            && trace_provider.ordinal < token.ordinal
    );

    // Six possible P buffers, B, At, Kt, An and Kn. N is parent-owned.
    let mut child = [None; 11];
    let mut count = 0;
    let mut payload_bytes = 0usize;
    let payload_layouts = [
        Layout::array::<u8>(trace.capacities[0]).unwrap(),
        Layout::array::<u8>(trace.capacities[1]).unwrap(),
        Layout::array::<contextdb_vector::mem::VectorPartitionSearchTrace>(trace.capacities[2])
            .unwrap(),
        Layout::array::<contextdb_core::RowId>(trace.capacities[3]).unwrap(),
        Layout::array::<contextdb_core::RowId>(trace.capacities[4]).unwrap(),
        Layout::array::<contextdb_vector::store::VectorGraphGeneration>(trace.capacities[5])
            .unwrap(),
    ];
    for ((payload, bytes, align), expected) in trace.allocations.into_iter().zip(payload_layouts) {
        assert_eq!((bytes, align), (expected.size(), expected.align()));
        if bytes == 0 {
            continue;
        }
        let request = membership_request(&window, payload, (bytes, align));
        assert_eq!(request.pointer, payload);
        assert!(request.ordinal < trace_keeper.ordinal);
        payload_bytes = payload_bytes.checked_add(request.bytes).unwrap();
        child[count] = Some(request);
        count += 1;
    }
    assert!(
        payload_bytes > 0,
        "retained payload proof must not be vacuous"
    );
    assert_eq!(payload_bytes, trace.prepaid_bytes);
    let payload_count = count;
    for request in [
        token,
        trace_provider,
        trace_keeper,
        native_adapter,
        native_keeper,
    ] {
        child[count] = Some(request);
        count += 1;
    }
    for (index, request) in child[..count].iter().enumerate() {
        let request = request.unwrap();
        assert_ne!(request.pointer, block.pointer);
        assert!(
            child[..index]
                .iter()
                .all(|earlier| earlier.unwrap().pointer != request.pointer),
            "each original allocation enters the invoice exactly once"
        );
    }
    for index in 0..window.request_count().unwrap() {
        let request = window.request(index).unwrap();
        if window.physical_free_event(index).unwrap().is_none() {
            assert!(
                request == block || child[..count].iter().any(|entry| *entry == Some(request)),
                "orphan live selected request is outside the actual retained control/payload graph: {request:?}"
            );
        }
    }
    let debit = trace
        .prepaid_bytes
        .checked_add(trace.additional_bytes)
        .unwrap()
        .checked_add(trace.score_credit_bytes)
        .unwrap()
        .checked_add(token.bytes)
        .unwrap()
        .checked_add(trace_provider.bytes)
        .unwrap()
        .checked_add(trace_keeper.bytes)
        .unwrap()
        .checked_add(native_adapter.bytes)
        .unwrap()
        .checked_add(native_keeper.bytes)
        .unwrap() as u64;
    let mut pins = None;
    let pinned_trace = read_work_probe::retained_trace_invoice(db, |owner| {
        pins = vector_read_memory::pin_original_native_for_test(&window, native, parent, owner);
    })
    .expect("borrow the genuine retained trace payer at the original pin cut");
    assert_eq!(
        pinned_trace, trace,
        "pin the same retained trace, without replacement"
    );
    let (native_pin, parent_pin) =
        pins.expect("pin the genuine original native owner and its original parent separately");
    let funding = native_pin.sample();
    assert!(funding.alive && funding.parent_alive);
    assert_eq!(funding.owner_identity, native.owner);
    assert_eq!(funding.parent_identity, native.parent);
    assert_eq!(native_pin.counter().address(), native.counter);
    assert_eq!(parent_pin.counter().address(), native.parent_counter);
    assert_eq!(
        native_pin.counter().value(),
        debit,
        "P + B + At + Kt + An + Kn + additional + score equals the original inline debit"
    );
    assert_eq!(funding.charged_bytes, debit);
    assert_eq!(reader.final_bytes, debit);
    assert_eq!(
        reader.identity.target_reserves as i128 - reader.identity.target_releases as i128,
        debit as i128 - reader.identity.target_initial_held as i128
    );
    assert_eq!(
        reader.drops, 0,
        "the genuine retained owner has not retired yet"
    );
    assert!(!reader.identity.target_drop_seen);
    assert_eq!(funding.parent_charged_bytes, parent_pin.charged_bytes());
    membership_parent_equation(
        &reader,
        native_pin.counter().value(),
        parent_pin.charged_bytes(),
    );
    window
        .status()
        .expect("all actual requests must qualify before cleanup");

    // Each watch retains the same genuine original child counter guard.
    let child_counter = native_pin.counter();
    let watches: [Option<_>; 11] = std::array::from_fn(|index| {
        child[index].map(|request| window.watch(request, child_counter.clone()).unwrap())
    });
    db.__reset_last_query_vector_trace_for_test();
    assert!(read_work_probe::retained_trace_invoice(db, |_| {}).is_none());
    let frees: [Option<_>; 11] = std::array::from_fn(|index| {
        watches[index].as_ref().map(|watch| {
            watch
                .receipt()
                .expect("every original child request physically freed")
        })
    });
    let token_free = frees[payload_count].unwrap();
    let provider_free = frees[payload_count + 1].unwrap();
    let keeper_free = frees[payload_count + 2].unwrap();
    let adapter_free = frees[payload_count + 3].unwrap();
    let native_keeper_free = frees[payload_count + 4].unwrap();
    // Prove the original invoices' custody, not merely enough aggregate slack
    // to hide an early refund behind a different already-freed allocation.
    // P is one prepaid invoice, refunded by B's payload before B deallocates.
    for free in &frees[..payload_count] {
        let free = free.unwrap();
        assert!(free.event_ordinal < token_free.event_ordinal);
        assert_eq!(free.before_system, debit);
        assert_eq!(free.after_system, debit);
    }
    assert!(token_free.event_ordinal < adapter_free.event_ordinal);
    assert!(adapter_free.event_ordinal < native_keeper_free.event_ordinal);
    assert!(native_keeper_free.event_ordinal < provider_free.event_ordinal);
    assert!(provider_free.event_ordinal < keeper_free.event_ordinal);
    let controls =
        (trace_provider.bytes + trace_keeper.bytes + native_adapter.bytes + native_keeper.bytes)
            as u64;
    for (free, expected) in [
        (
            token_free,
            controls + token.bytes as u64 + trace.additional_bytes as u64,
        ),
        (adapter_free, controls),
        (native_keeper_free, controls - native_adapter.bytes as u64),
        (
            provider_free,
            (trace_provider.bytes + trace_keeper.bytes) as u64,
        ),
        (keeper_free, trace_keeper.bytes as u64),
    ] {
        assert_eq!(
            free.before_system, expected,
            "the exact original invoice is still paid before physical free"
        );
        assert_eq!(
            free.after_system, expected,
            "the exact original invoice stays paid immediately after physical free"
        );
    }
    for (index, free) in frees[..count].iter().enumerate() {
        let free = free.unwrap();
        assert_eq!(free.request, child[index].unwrap());
        assert_eq!(free.counter, native.counter);
        assert_eq!(free.physical_frees, 1);
        assert!(free.event_ordinal > free.request.event_ordinal);
        assert_eq!(
            free.before_system, free.after_system,
            "the original credit cannot return inside System.dealloc"
        );
        let still_physical = frees[..count]
            .iter()
            .map(|other| other.unwrap())
            .filter(|other| other.event_ordinal >= free.event_ordinal)
            .try_fold(0u64, |sum, other| {
                sum.checked_add(other.request.bytes as u64)
            })
            .unwrap();
        assert!(
            free.before_system >= still_physical && free.after_system >= still_physical,
            "every not-yet-freed original allocation remains funded on BOTH sides of System.dealloc"
        );
        assert!(
            frees[..index]
                .iter()
                .all(|other| other.unwrap().event_ordinal != free.event_ordinal)
        );
    }
    assert_eq!(
        native_pin.counter().value(),
        0,
        "actual child-owned buffers are gone and the genuine inline child is exactly zero"
    );
    let before_native_free = parent_pin.charged_bytes();
    assert!(before_native_free >= block.bytes as u64);
    let cleared = statement::current().unwrap();
    membership_parent_equation(&cleared, native_pin.counter().value(), before_native_free);
    drop(watches); // Release every watch's original child guard before N.
    drop(child_counter); // Release the remaining original child counter guard before N.
    let parent_counter = parent_pin.counter();
    let block_watch = window.watch(block, parent_counter.clone()).unwrap();
    drop(native_pin); // Release the last genuine native pin, not a forwarding owner.
    let freed = block_watch
        .receipt()
        .expect("N must physically free exactly once");
    assert_eq!(freed.request, block);
    assert_eq!(freed.counter, native.parent_counter);
    assert_eq!(freed.physical_frees, 1);
    assert!(freed.event_ordinal > block.event_ordinal);
    assert!(
        frees[..count]
            .iter()
            .all(|free| free.unwrap().event_ordinal < freed.event_ordinal)
    );
    assert_eq!(freed.before_system, before_native_free);
    assert_eq!(
        freed.after_system, before_native_free,
        "the full original N charge remains on the live parent immediately after System.dealloc"
    );
    assert_eq!(
        parent_pin.charged_bytes(),
        before_native_free - block.bytes as u64,
        "only after N physically frees does the original parent receive its exact refund"
    );
    let retired = statement::current().unwrap();
    assert_eq!(retired.final_bytes, 0);
    assert_eq!(retired.drops, 1);
    assert!(retired.identity.target_drop_seen);
    let retired_drop = retired
        .identity
        .target_drop
        .expect("genuine target retirement required");
    assert_eq!(retired_drop.child_identity, native.owner);
    assert_eq!(retired_drop.child_after, 0);
    assert_eq!(retired_drop.parent_identity, native.parent);
    assert_eq!(retired_drop.child_control_bytes, block.bytes);
    membership_parent_equation(&retired, 0, parent_pin.charged_bytes());
    drop(block_watch);
    drop(parent_counter);
    window
        .finish()
        .expect("no missing/overflow/stale/foreign/reentrant allocation or free evidence");
}

fn displace_membership_pages(db: &contextdb_engine::Database) {
    let before = db.__writer_cache_stats_for_test().unwrap();
    for id in 0..64 {
        assert_eq!(
            exec(
                db,
                &format!("SELECT body FROM membership_pressure WHERE id = {id}")
            ),
            vec![vec![Value::Text("p".repeat(262144))]]
        );
    }
    let after = db.__writer_cache_stats_for_test().unwrap();
    assert!(
        after.2 > before.2,
        "disjoint durable body pages must actually evict membership source pages"
    );
}

#[test]
#[expect(
    clippy::result_large_err,
    reason = "Public Error keeps inline NativeRefusal diagnostics so an exhausted budget can return its error without allocating."
)]
fn unpartitioned_vector_membership_pages_reload_at_constant_residency() {
    if isolated_test(
        module_path!(),
        "unpartitioned_vector_membership_pages_reload_at_constant_residency",
        "CONTEXTDB_MEMBERSHIP_ALLOCATION_ARM",
    ) {
        return;
    }
    let _clock = Wallclock::test_clock_guard(|| 1_700_000_000_000);
    let mut directory_bytes = Vec::new();
    const DIM: usize = 256;
    const MEMBERSHIP_WORKSPACE: usize = 64 * 1024;
    for n in [512, 1024] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("membership.db");
        let db = open(&path, Some(64 * 1024 * 1024));
        exec(
            &db,
            "CREATE TABLE unpartitioned_items (id INT PRIMARY KEY, v VECTOR(256)) HISTORY ALL",
        );
        exec(
            &db,
            "CREATE TABLE partitioned_items (id INT PRIMARY KEY, scope INT NOT NULL, v VECTOR(256) PARTITION_KEY (scope) MAX_PARTITIONS 4) HISTORY ALL",
        );
        for id in 0..n {
            insert_vector(&db, "unpartitioned_items", id, None, DIM);
            insert_vector(&db, "partitioned_items", id, Some((id % 2) as i64), DIM);
        }
        exec(
            &db,
            "CREATE TABLE membership_pressure (id INT PRIMARY KEY, body TEXT)",
        );
        for id in 0..64 {
            db.execute(
                "INSERT INTO membership_pressure VALUES ($id,$body)",
                &std::collections::HashMap::from([
                    ("id".into(), Value::Int64(id)),
                    ("body".into(), Value::Text("p".repeat(262144))),
                ]),
            )
            .unwrap();
        }
        let old = db.snapshot();
        db.close().unwrap();
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        let store = db.vector_store_for_test();
        let initial = store.resident_membership_directory_bytes_for_test();
        let mut reloaded = 0;
        for (table, key, scope) in [
            (
                "unpartitioned_items",
                VectorPartitionKey::unpartitioned(),
                None,
            ),
            ("partitioned_items", partition(0), Some(0)),
            ("partitioned_items", partition(1), Some(1)),
        ] {
            let ids = (0..n)
                .filter(|id| scope.is_none_or(|scope| (*id % 2) as i64 == scope))
                .collect::<Vec<_>>();
            assert_eq!(
                vector_rows(&db, table, old, scope),
                vector_oracle(ids.iter().copied(), DIM)
            );
            assert_eq!(
                exact_partition_ids(&db, table, old, scope, DIM, n),
                ids.iter().map(|id| *id as i64).collect::<Vec<_>>()
            );
            // Drop any legacy body owner, then displace the actual shared page
            // cache. A streamed implementation need not have a partition body.
            store.evict_raw_partition_for_test(&index(table), &key);
            displace_membership_pages(&db);
            let receipt = db.writer_memory_ownership_receipt_for_test();
            // Limit transitions preserve the cache allocated at open. Admit only
            // one workspace above the actual settled owners, including that cache.
            let operation_limit = receipt.named_owner_total + MEMBERSHIP_WORKSPACE;
            assert!(
                n * DIM * std::mem::size_of::<f32>() > MEMBERSHIP_WORKSPACE,
                "active unpartitioned payload cannot fit the admitted membership workspace"
            );
            db.set_memory_limit(Some(operation_limit)).unwrap();
            let baseline = db.accountant().usage().used;
            db.accountant().reset_high_water_for_test();
            // Observe the existing declared parent and the child acquired by
            // the ordinary public statement door; neither owner is substituted.
            let reader_parent = db.declared_read_memory_for_test();
            let query = || {
                db.execute_at_snapshot(
                    &format!(
                        "SELECT id FROM {table}{} ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
                        scope.map_or(String::new(), |scope| format!(" WHERE scope = {scope}"))
                    ),
                    &std::collections::HashMap::from([(
                        "v".into(),
                        Value::Vector(vector(ids[0], DIM)),
                    )]),
                    old,
                )
            };
            #[cfg(target_os = "linux")]
            let allocation_baseline = {
                use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
                allocation::reset_allocation_peak_for_test();
                allocation::live_allocations_for_test()
            };
            let window = Window::begin(&membership_request_layouts(table))
                .expect("fixed nonallocating recorder before the genuine measured query");
            let ((), reader) = statement::observe(
                &reader_parent,
                MEMBERSHIP_WORKSPACE as u64,
                || {
                    let (bounded, bounded_work) = observe(query);
                    let reader = statement::current().expect("live original statement receipt");
                    assert!(
                        bounded.is_ok(),
                        "active membership larger than workspace must stream through admitted source windows: {bounded:?}; {bounded_work:?}"
                    );
                    assert_eq!(
                        bounded.unwrap().rows(),
                        vec![vec![Value::Int64(ids[0] as i64)]]
                    );
                    #[cfg(target_os = "linux")]
                    {
                        use crate::writer_reopen_and_doubling_keep_a_fixed_resident_envelope as allocation;
                        let actual_peak = allocation::allocation_peak_for_test()
                            .saturating_sub(allocation_baseline);
                        assert!(
                            actual_peak <= MEMBERSHIP_WORKSPACE,
                            "real membership allocation peak must fit the workspace, including uncharged temporary directories: {actual_peak}"
                        );
                    }
                    let peak = db
                        .accountant()
                        .high_water_for_test()
                        .saturating_sub(baseline);
                    // This isolated, caller-driven read uses the cache reservation
                    // already settled at open. New reader work cannot debit the writer.
                    assert_eq!(peak, 0, "reader work must not debit the writer: {reader:?}");
                    assert_eq!(db.accountant().usage().used, baseline);
                    assert!(db.accountant().high_water_for_test() <= operation_limit);
                    assert_eq!(reader.children, 1, "one actual statement child: {reader:?}");
                    assert_ne!(reader.owner_identity, reader.parent_identity);
                    assert_eq!(reader.limit, MEMBERSHIP_WORKSPACE as u64);
                    assert!(
                        reader.peak > reader.control_bytes as u64
                            && reader.peak <= MEMBERSHIP_WORKSPACE as u64,
                        "genuine child peak includes its prepaid control: {reader:?}"
                    );
                    assert_eq!(reader.parent_funding_mismatches, 0);
                    assert!(reader.parent_before + reader.peak <= MEMBERSHIP_WORKSPACE as u64);
                    assert_eq!(reader.refusals, 0);
                    assert!(
                        bounded_work.workspace_peak > 0
                            && bounded_work.workspace_peak <= MEMBERSHIP_WORKSPACE,
                        "genuine live capture has bounded workspace: {bounded_work:?}"
                    );
                    assert!(reader.workspace_samples > 0, "live owner proof: {reader:?}");
                    assert_eq!(reader.foreign_workspace_owners, 0);
                    assert_eq!(reader.underfunded_workspace_samples, 0);
                    assert!(
                        bounded_work.membership_keys_examined >= ids.len(),
                        "no resident mirror fallback; every exact candidate comes from durable membership: {bounded_work:?}"
                    );
                    membership_retained_and_final_free(&db, window, &reader_parent);
                },
            );
            assert_eq!(
                reader.final_bytes, 0,
                "no unpaid statement bytes after actual final free: {reader:?}"
            );
            assert_eq!(
                reader.drops, 1,
                "statement child must retire after actual final free: {reader:?}"
            );
            membership_parent_equation(&reader, 0, reader.parent_after);
            assert_eq!(reader.parent_equation.expected, reader.parent_after as i128);
            assert_eq!(reader.parent_equation.actual, reader.parent_after);
            assert_eq!(reader.complete_parent_expected, reader.parent_after as i128);
            assert_eq!(reader.complete_parent_actual, reader.parent_after);
            // A second row through the SAME public statement/child path has
            // room for the real child control and no query workspace. The real
            // executing child must refuse, without losing a pre-existing debit.
            let refusal_baseline = reader_parent.retained_bytes_for_test();
            assert_eq!(refusal_baseline, reader.parent_after);
            let refusal_limit = refusal_baseline + reader.control_bytes as u64;
            assert!(refusal_limit < MEMBERSHIP_WORKSPACE as u64);
            let (refused, refused_reader) =
                contextdb_engine::vector_read_memory::statement_child_proof::observe(
                    &reader_parent,
                    refusal_limit,
                    query,
                );
            assert!(
                matches!(&refused, Err(contextdb_core::Error::MemoryBudgetExceeded {
                    budget_limit_bytes, ..
                }) if *budget_limit_bytes == refusal_limit as usize),
                "the real reader cap must cause a typed refusal: {refused:?}; {refused_reader:?}"
            );
            assert_eq!(refused_reader.children, 1);
            assert_eq!(refused_reader.refusals, 1);
            assert_eq!(refused_reader.peak, refused_reader.control_bytes as u64);
            assert_eq!(refused_reader.parent_funding_mismatches, 0);
            assert_eq!(refused_reader.final_bytes, 0);
            assert_eq!(refused_reader.drops, 1);
            assert_eq!(refused_reader.parent_before, refusal_baseline);
            assert_eq!(refused_reader.parent_after, refusal_baseline);
            assert_eq!(db.accountant().usage().used, baseline);
            assert_eq!(
                db.accountant()
                    .high_water_for_test()
                    .saturating_sub(baseline),
                0
            );
            db.set_memory_limit(Some(64 * 1024 * 1024)).unwrap();
            store.evict_raw_partition_for_test(&index(table), &key);
            displace_membership_pages(&db);
            let before = db.__writer_cache_stats_for_test().unwrap();
            let (answer, work) = observe(|| exact_partition_ids(&db, table, old, scope, DIM, n));
            assert_eq!(answer, ids.iter().map(|id| *id as i64).collect::<Vec<_>>());
            let after = db.__writer_cache_stats_for_test().unwrap();
            assert!(
                after.1 > before.1,
                "eviction requires a real durable membership/body page reload"
            );
            assert!(
                work.membership_envelopes_verified >= ids.len()
                    && work.membership_keys_examined >= ids.len(),
                "each reloaded membership envelope is verified: {work:?}"
            );
            reloaded += 1;
        }
        assert_eq!(reloaded, 3);
        exec(&db, "UPDATE partitioned_items SET scope = 2 WHERE id = 0");
        exec(&db, "DELETE FROM unpartitioned_items WHERE id = 1");
        let current = db.snapshot();
        for (table, scope, ids) in [
            (
                "unpartitioned_items",
                None,
                (0..n).filter(|id| *id != 1).collect::<Vec<_>>(),
            ),
            ("partitioned_items", Some(0), (2..n).step_by(2).collect()),
            ("partitioned_items", Some(1), (1..n).step_by(2).collect()),
            ("partitioned_items", Some(2), vec![0]),
        ] {
            assert_eq!(
                vector_rows(&db, table, current, scope),
                vector_oracle(ids.iter().copied(), DIM),
                "every current body after moves/deletes"
            );
            assert_eq!(
                exact_partition_ids(&db, table, current, scope, DIM, n),
                ids.iter().map(|id| *id as i64).collect::<Vec<_>>(),
                "runtime membership after moves/deletes"
            );
        }
        assert_eq!(
            exact_partition_ids(&db, "unpartitioned_items", old, None, DIM, n),
            (0..n as i64).collect::<Vec<_>>()
        );
        for scope in 0..2 {
            assert_eq!(
                exact_partition_ids(&db, "partitioned_items", old, Some(scope), DIM, n),
                (0..n as i64)
                    .filter(|id| id % 2 == scope)
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                vector_rows(&db, "partitioned_items", old, Some(scope)),
                vector_oracle((0..n).filter(|id| *id % 2 == scope as usize), DIM)
            );
        }
        assert_eq!(
            vector_rows(&db, "unpartitioned_items", old, None),
            vector_oracle(0..n, DIM)
        );
        let receipt = db.writer_memory_ownership_receipt_for_test();
        assert_eq!(receipt.reconciliation_gap, 0);
        directory_bytes.push((
            initial,
            store.resident_membership_directory_bytes_for_test(),
        ));
        db.close().unwrap();
        drop(store);
        drop(db);
        let db = open(&path, Some(64 * 1024 * 1024));
        assert_eq!(
            exact_partition_ids(&db, "unpartitioned_items", current, None, DIM, n),
            (0..n as i64).filter(|id| *id != 1).collect::<Vec<_>>()
        );
        assert_eq!(
            exact_partition_ids(&db, "partitioned_items", current, Some(2), DIM, n),
            vec![0]
        );
        assert_eq!(
            vector_rows(&db, "unpartitioned_items", old, None),
            vector_oracle(0..n, DIM)
        );
        // Reopen the deliberately damaged fixture so the reader root includes
        // the changed envelope. Eager open and cold membership use may both
        // discover corruption, but either must verify this checksum.
        let route = VectorPartitionRef::new(
            index("unpartitioned_items"),
            VectorPartitionKey::unpartitioned(),
        );
        db.__damage_membership_envelope_for_test(&route).unwrap();
        db.close().unwrap();
        drop(db);
        let (damaged_open, open_work) = observe(|| {
            contextdb_engine::Database::open_with_options(
                &path,
                contextdb_engine::DatabaseOpenOptions {
                    maintenance_policy: contextdb_engine::MaintenancePolicy::CallerDriven,
                    memory_limit: Some(64 * 1024 * 1024),
                    ..Default::default()
                },
            )
        });
        let (refused, corruption_work) = match damaged_open {
            Err(error) => (Err(error), open_work),
            Ok(db) => {
                displace_membership_pages(&db);
                let (answer, work) = observe(|| {
                    db.execute(
                        "SELECT id FROM unpartitioned_items ORDER BY v <=> $v USE VECTOR EXACT LIMIT 1",
                        &std::collections::HashMap::from([(
                            "v".into(),
                            Value::Vector(vector(0, DIM)),
                        )]),
                    )
                });
                db.close().unwrap();
                (answer, work)
            }
        };
        assert!(
            matches!(&refused, Err(contextdb_core::Error::StoreCorrupted { reason, .. }) if reason.contains("checksum mismatch")),
            "checksum corruption must be a typed reload refusal: {refused:?}"
        );
        assert!(
            corruption_work.membership_envelopes_verified > 0,
            "the refusal must come from verifying a reloaded membership: {corruption_work:?}"
        );
    }
    assert!(
        directory_bytes
            .iter()
            .all(|(open, used)| *open == 0 && *used == 0),
        "whole membership directories must never remain resident: {directory_bytes:?}"
    );
}
