//! Independent rows for the selected native JSON producers. No digest-validation
//! claim: malformed inner strings are injected into actual serialized roots.
use super::*;
use crate::persistence::{original_event_decode, PersistedVersionedRow, PersistedValue};
use contextdb_core::read_memory::NativeRefusal;
use std::cell::{Cell, RefCell};

// Proof-only call-site provenance. Raw physical accounting never consults this
// phase and never subtracts an allocation. Ordinary rows remain EngineProduce.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ProofPhase {
    #[default]
    EngineProduce,
    DriverInitiate,
    EngineDrop,
    DriverCatchCleanup,
}
#[derive(Clone, Copy, Debug, Default)]
struct UnwindPhases {
    phase: ProofPhase,
    initiate_at: usize,
    drop_at: usize,
    cleanup_at: usize,
    invalid: bool,
}
thread_local! {
    static UNWIND_PHASES: Cell<UnwindPhases> = const { Cell::new(UnwindPhases {
        phase: ProofPhase::EngineProduce, initiate_at: 0, drop_at: 0, cleanup_at: 0, invalid: false,
    }) };
}
// No assertion, formatting, allocation, or user code in an unwinding Drop.
// A bad transition or deferred observer borrow makes qualification fail closed.
fn mark_unwind_phase(next: ProofPhase) {
    drain_events();
    UNWIND_PHASES.with(|slot| {
        let mut phases = slot.get();
        let n = HISTORY_COUNT.with(Cell::get);
        phases.invalid |= EVENT_COUNT.with(Cell::get) != 0;
        match (phases.phase, next) {
            (ProofPhase::EngineProduce, ProofPhase::DriverInitiate) => phases.initiate_at = n,
            (ProofPhase::DriverInitiate, ProofPhase::EngineDrop) => phases.drop_at = n,
            (ProofPhase::EngineDrop, ProofPhase::DriverCatchCleanup) => phases.cleanup_at = n,
            _ => phases.invalid = true,
        }
        phases.phase = next;
        slot.set(phases);
    });
}
// The wrapper itself has no implicit engine fields left to destroy after Drop:
// take() leaves None, and explicit drop finishes the SAME value before cleanup.
struct UnwindEngineValue<T> { value: Option<T> }
impl<T> Drop for UnwindEngineValue<T> {
    fn drop(&mut self) {
        mark_unwind_phase(ProofPhase::EngineDrop);
        drop(self.value.take());
        mark_unwind_phase(ProofPhase::DriverCatchCleanup);
    }
}

#[derive(Clone, Copy, Debug)]
struct Request {
    generation: usize, pointer: usize, bytes: usize, align: usize,
    allocated: usize, before_free: usize, after_free: usize, frees: usize,
    prepared: bool,
}
impl Request {
    const EMPTY: Self = Self { generation: 0, pointer: 0, bytes: 0, align: 0,
        allocated: 0, before_free: 0, after_free: 0, frees: 0, prepared: false };
}
#[derive(Clone, Copy, Default, Debug)]
struct Receipt {
    counter: usize, baseline: usize, count: usize, live: usize, peak: usize,
    gap: usize, invalid: bool, prepared: bool, native: usize, text: usize,
    prepared_at: usize, denial: Option<NativeRefusal>, post_denial_non_native: usize,
    candidates: usize, candidate_live: usize, candidate_peak: usize,
    output: usize, output_overlap: usize, rendered_at: usize, destroyed_at: usize,
    category: Option<serde_json::error::Category>, line: usize, column: usize,
    finite: Option<&'static str>, diagnoses: usize, json_refusal: bool,
}
thread_local! {
    // Large history stays in static TLS, never returned/copied on a test stack.
    static REQUESTS: RefCell<[Request; 2048]> = const { RefCell::new([Request::EMPTY; 2048]) };
    static RECEIPT: Cell<Receipt> = const { Cell::new(Receipt {
        counter: 0, baseline: 0, count: 0, live: 0, peak: 0, gap: 0,
        invalid: false, prepared: false, native: 0, text: 0, prepared_at: 0,
        denial: None, post_denial_non_native: 0, candidates: 0, candidate_live: 0,
        candidate_peak: 0, output: 0, output_overlap: 0, rendered_at: 0,
        destroyed_at: 0, category: None, line: 0, column: 0, finite: None, diagnoses: 0, json_refusal: false,
    }) };
}
fn used(r: &Receipt) -> usize {
    // Guard borrows the live accountant for the complete interval.
    unsafe { &*(r.counter as *const AtomicUsize) }.load(AtomicOrdering::SeqCst)
}
#[derive(Clone, Copy, Debug)]
struct GapEvent { event: AllocationEvent, generation: usize, live: usize, gap: usize }
thread_local! {
    static FIRST_GAP: Cell<Option<GapEvent>> = const { Cell::new(None) };
    static WORST_GAP: Cell<Option<GapEvent>> = const { Cell::new(None) };
    static HISTORY: [Cell<AllocationEvent>; 2048] = const { [const { Cell::new(AllocationEvent::EMPTY) }; 2048] };
    static HISTORY_COUNT: Cell<usize> = const { Cell::new(0) };
    // Root, exact test subrow, input ordinal (or replay budget).
    static SUBROW: Cell<(&'static str, &'static str, usize)> = const { Cell::new(("none", "none", 0)) };
}
fn subrow(root: &'static str, row: &'static str, ordinal: usize) { SUBROW.with(|s| s.set((root,row,ordinal))); }
fn sample(r: &mut Receipt, event: AllocationEvent, generation: usize) {
    let gap = r.live.saturating_sub(event.funding.saturating_sub(r.baseline));
    if gap > 0 {
        let witness = GapEvent { event, generation, live: r.live, gap };
        FIRST_GAP.with(|s| { if s.get().is_none() { s.set(Some(witness)); } });
        WORST_GAP.with(|s| { if s.get().is_none_or(|old| gap > old.gap) { s.set(Some(witness)); } });
    }
    r.gap = r.gap.max(gap);
}
// A proof consumer can allocate while reading REQUESTS. Record that request
// too, with its original funding,
// but defer history mutation until the consumer releases its RefCell borrow.
// No callback formats, allocates, panics on a conflicting borrow, or drops an
// event silently. Overflow invalidates the proof, just like history overflow.
#[derive(Clone, Copy, Debug)]
struct AllocationEvent {
    pointer: usize, bytes: usize, align: usize, funding: usize, kind: u8,
    prepared: bool, denied: bool, native: usize, text: usize, prepared_at: usize,
    proof_phase: ProofPhase, denial: Option<NativeRefusal>, subrow: (&'static str, &'static str, usize),
}
impl AllocationEvent {
    const EMPTY: Self = Self { pointer: 0, bytes: 0, align: 0, funding: 0, kind: 0, prepared: false, denied: false, native: 0, text: 0, prepared_at: 0, proof_phase: ProofPhase::EngineProduce, denial: None, subrow: ("none", "none", 0) };
}
thread_local! {
    static EVENTS: [Cell<AllocationEvent>; 2048] = const { [const { Cell::new(AllocationEvent::EMPTY) }; 2048] };
    static EVENT_COUNT: Cell<usize> = const { Cell::new(0) };
}
// Called only after BOTH producer and aggregate allocation windows close.
fn dump_event(label: &str, index: usize, e: AllocationEvent) {
    let phase = match e.kind { 0 => "allocated", 1 => "before physical free", 2 => "after physical free", _ => "invalid" };
    eprintln!("{label} {index}: pointer={:#x} Layout={}/{} phase={} funding={} prepared={} native={} text={} prepared_at={} denied={} first_denial={:?} subrow={:?} proof_phase={:?}",
        e.pointer, e.bytes, e.align, phase, e.funding, e.prepared, e.native, e.text, e.prepared_at, e.denied, e.denial, e.subrow, e.proof_phase);
}
fn drain_events() {
    REQUESTS.with(|slot| {
        let Ok(mut entries) = slot.try_borrow_mut() else { return; };
        RECEIPT.with(|cell| {
            let mut r = cell.get();
            if r.counter == 0 { return; }
            EVENTS.with(|events| EVENT_COUNT.with(|count| {
                for slot in &events[..count.replace(0)] {
                    let event = slot.replace(AllocationEvent::EMPTY);
                    HISTORY.with(|history| HISTORY_COUNT.with(|count| {
                        let n = count.get();
                        if n == history.len() { r.invalid = true; }
                        else { history[n].set(event); count.set(n + 1); }
                    }));
                    if event.kind == 0 {
                        if r.count == entries.len() || entries[..r.count].iter().any(|e| e.pointer == event.pointer && e.frees == 0) {
                            r.invalid = true; continue;
                        }
                        entries[r.count] = Request { generation: r.count + 1, pointer: event.pointer,
                            bytes: event.bytes, align: event.align, allocated: event.funding,
                            before_free: 0, after_free: 0, frees: 0, prepared: event.prepared };
                        r.count += 1; r.live += event.bytes; r.peak = r.peak.max(r.live);
                        // Conservative constructor candidates, never attribution by size.
                        if event.prepared && (event.bytes, event.align) == (40, 8) {
                            r.candidates += 1; r.candidate_live += 40;
                            r.candidate_peak = r.candidate_peak.max(r.candidate_live);
                        }
                        if event.denied && (event.bytes, event.align) != (40, 8) {
                            r.post_denial_non_native += 1;
                        }
                        let generation = r.count; sample(&mut r, event, generation);
                    } else {
                        // Pre-existing fixture/owner storage is outside this window.
                        let Some(e) = entries[..r.count].iter_mut().rev().find(|e| e.pointer == event.pointer && e.frees == 0) else { continue; };
                        if (e.bytes, e.align) != (event.bytes, event.align) { r.invalid = true; }
                        // Keep the original before/after-System.dealloc samples;
                        // physical retirement occurs only after the latter sample.
                        sample(&mut r, event, e.generation);
                        if event.kind == 2 {
                            e.after_free = event.funding; e.frees += 1; r.live -= e.bytes;
                            if e.prepared && (e.bytes, e.align) == (40, 8) { r.candidate_live -= 40; }
                        } else { e.before_free = event.funding; }
                    }
                }
            }));
            cell.set(r);
        });
    });
}
fn allocation_event(pointer: usize, layout: Layout, kind: u8) {
    let r = RECEIPT.with(Cell::get);
    if r.counter == 0 { return; }
    // Capture before deferred processing: a later invoice cannot fund an old
    // allocation, and a later refund cannot change its physical-free sample.
    let event = AllocationEvent { pointer, bytes: layout.size(), align: layout.align(), funding: used(&r), kind, prepared: r.prepared, denied: r.denial.is_some(), native: r.native, text: r.text, prepared_at: r.prepared_at, proof_phase: UNWIND_PHASES.with(|s| s.get().phase), denial: r.denial, subrow: SUBROW.with(Cell::get) };
    drain_events();
    EVENTS.with(|events| EVENT_COUNT.with(|count| {
        let n = count.get();
        if n == events.len() {
            RECEIPT.with(|cell| { let mut r = cell.get(); r.invalid = true; cell.set(r); });
        } else { events[n].set(event); count.set(n + 1); }
    }));
    drain_events();
}
pub(crate) fn allocated(pointer: usize, layout: Layout) {
    allocation_event(pointer, layout, 0);
}
pub(crate) fn freed(pointer: usize, layout: Layout, after: bool) {
    allocation_event(pointer, layout, if after { 2 } else { 1 });
}
pub(crate) fn prepared(native: usize, text: usize) {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter == 0 { return; }
        r.prepared = true; r.native = native; r.text = text; r.prepared_at = used(&r); cell.set(r); });
}
pub(crate) fn refused(error: NativeRefusal) {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter == 0 { return; }
        if r.denial.is_none() { r.denial = Some(error); } cell.set(r); });
}
pub(crate) fn json_refusal() {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter != 0 { r.json_refusal = true; cell.set(r); } });
}
pub(crate) fn diagnosis(error: &serde_json::Error, finite: Option<&'static str>) {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter == 0 { return; }
        r.category = Some(error.classify()); r.line = error.line(); r.column = error.column();
        r.finite = finite; r.diagnoses += 1; cell.set(r); });
}
pub(crate) fn rendered(pointer: usize) {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter == 0 { return; }
        r.output = pointer; r.rendered_at = used(&r); r.output_overlap = r.live; cell.set(r); });
}
pub(crate) fn native_destroyed() {
    RECEIPT.with(|cell| { let mut r = cell.get(); if r.counter == 0 { return; }
        r.destroyed_at = used(&r); cell.set(r); });
}
struct Observation<'a>(&'a MemoryAccountant, Option<Receipt>);
impl<'a> Observation<'a> {
    fn begin(owner: &'a MemoryAccountant) -> Self {
        assert_eq!(RECEIPT.with(Cell::get).counter, 0);
        EVENT_COUNT.with(|count| count.set(0));
        UNWIND_PHASES.with(|s| s.set(UnwindPhases::default()));
        HISTORY_COUNT.with(|count| count.set(0));
        FIRST_GAP.with(|s| s.set(None)); WORST_GAP.with(|s| s.set(None));
        REQUESTS.with(|slot| { for e in slot.borrow_mut().iter_mut() { *e = Request::EMPTY; } });
        RECEIPT.with(|slot| slot.set(Receipt { counter: owner.writer_registration_observation_for_test().4,
            baseline: owner.usage().used, ..Default::default() }));
        Self(owner, None)
    }
    fn receipt(&self) -> Receipt { self.1.unwrap_or_else(|| { drain_events(); RECEIPT.with(Cell::get) }) }
    fn stop(mut self) -> Self {
        self.1 = Some(self.receipt());
        RECEIPT.with(|cell| cell.set(Default::default()));
        self
    }
    fn all_freed(&self) {
        let r = self.receipt();
        // End producer observation BEFORE diagnostic I/O or assertion machinery.
        // Callers using funded_requests return stop() and invoke this outside it.
        RECEIPT.with(|cell| cell.set(Default::default()));
        assert_eq!(MEASURING_HERE.with(Cell::get), 0, "diagnostics require closed aggregate window");
        if r.gap != 0 || r.invalid || r.live != 0 {
            eprintln!("selected JSON subrow {:?} receipt {:?}", SUBROW.with(Cell::get), r);
            for (label, witness) in [("FIRST", FIRST_GAP.with(Cell::get)), ("WORST", WORST_GAP.with(Cell::get))] {
                if let Some(w) = witness {
                    eprintln!("{label} generation={} live={} gap={}", w.generation, w.live, w.gap);
                    dump_event(label, 0, w.event);
                }
            }
            REQUESTS.with(|slot| { for e in &slot.borrow()[..r.count] { eprintln!("request {e:?}"); } });
            HISTORY.with(|history| HISTORY_COUNT.with(|count| { for (i,event) in history[..count.get()].iter().enumerate() { dump_event("event", i, event.get()); } }));
        }
        assert!(!r.invalid); assert_eq!(r.gap, 0, "original invoices cover every simultaneous native request");
        assert_eq!(r.live, 0);
        REQUESTS.with(|slot| { for e in &slot.borrow()[..r.count] {
            assert!(e.generation > 0); assert_ne!(e.pointer, 0); assert_eq!(e.frees, 1);
            assert!(e.allocated >= r.baseline + e.bytes);
            assert!(e.before_free >= r.baseline + e.bytes);
            assert_eq!(e.before_free, e.after_free, "physical free precedes refund");
        } });
        assert_eq!(self.0.usage().used, r.baseline);
    }
}
impl Drop for Observation<'_> { fn drop(&mut self) { RECEIPT.with(|slot| slot.set(Default::default())); } }

#[derive(Clone, Copy, Debug)]
enum Root { Row, Queue }
impl Root {
    fn name(self) -> &'static str { match self { Self::Row => "persisted row", Self::Queue => "sink queue" } }
    fn fixture(self, input: &str) -> Vec<u8> {
        let needle = serde_json::Value::String("selected_json_fixture_needle".into());
        let bytes = match self {
            Self::Row => {
                let persisted = PersistedVersionedRow { row_id: RowId(7),
                    values: [("j".into(), PersistedValue::Plain(Value::Json(needle.clone())))].into(),
                    created_tx: TxId(3), deleted_tx: None, lsn: Lsn(9), created_at: None };
                bincode::serde::encode_to_vec(&persisted, bincode::config::standard()).unwrap()
            }
            Self::Queue => {
                // Construct the ACTUAL type via its existing human decoder;
                // default gate is then serialized by the real binary codec.
                let entry: crate::database::event_bus::SinkQueueEntry = serde_json::from_value(serde_json::json!({
                    "id": 17, "row_id": 7, "attempts": 2,
                    "event": { "action_id": 17, "event_type": "e", "table": "t", "severity": "s",
                        "at_lsn": 9, "row_values": { "j": {"Json": serde_json::to_string(&needle).unwrap()} } }
                })).unwrap();
                bincode::serde::encode_to_vec(&entry, bincode::config::standard()).unwrap()
            }
        };
        let old = bincode::serde::encode_to_vec(serde_json::to_string(&needle).unwrap(), bincode::config::standard()).unwrap();
        let new = bincode::serde::encode_to_vec(input, bincode::config::standard()).unwrap();
        let hits: Vec<_> = bytes.windows(old.len()).enumerate().filter_map(|(i,b)| (b == old).then_some(i)).collect();
        assert_eq!(hits.len(), 1, "replace precisely the real Value::Json serialized String");
        let mut changed = bytes; changed.splice(hits[0]..hits[0]+old.len(), new); changed
    }
    fn decode(self, original: &crate::memory_accounting::OwnedMemoryReservation, bytes: &[u8]) -> Result<()> {
        match self {
            Self::Row => original_event_decode::decode_row(original, bytes).map(drop),
            Self::Queue => original_event_decode::decode_sink_entry(original, bytes).map(drop),
        }
    }
    fn assert_value(self, original: &crate::memory_accounting::OwnedMemoryReservation, bytes: &[u8], expected: &serde_json::Value) {
        match self {
            Self::Row => {
                let decoded = original_event_decode::decode_row(original, bytes).unwrap();
                assert!(matches!(&decoded.value.values["j"], PersistedValue::Plain(Value::Json(v)) if v == expected));
                assert_eq!((decoded.value.row_id, decoded.value.created_tx, decoded.value.lsn), (RowId(7), TxId(3), Lsn(9)));
                drop(decoded);
                // The real outer caller is exercised as well as its selected root.
                let frame = OriginalPersistenceFrame::new(original).unwrap();
                let row = RedbPersistence::decode_versioned_row_original(&frame, bytes, None).unwrap();
                assert_eq!(row.values["j"], Value::Json(expected.clone()));
                drop(row); drop(frame);
            }
            Self::Queue => {
                let decoded = original_event_decode::decode_sink_entry(original, bytes).unwrap();
                assert_eq!(decoded.value.event.row_values["j"], Value::Json(expected.clone()));
                assert_eq!((decoded.value.id, decoded.value.event.action_id, decoded.value.attempts), (17,17,2)); drop(decoded);
            }
        }
    }
}
fn qualified(error: &Error) -> bool {
    matches!(error, Error::FundedEngine(source) if source.source_is_qualified())
}
fn native_message(root: Root, bytes: &[u8], input: &str) -> (String, serde_json::error::Category, usize, usize) {
    let error = serde_json::from_str::<serde_json::Value>(input).unwrap_err();
    // Actual original concrete native decoder, same bytes and same root.
    // Preserve bincode's genuine OtherString wrapper, including its escaping.
    let native = match root {
        Root::Row => bincode::serde::decode_from_slice::<PersistedVersionedRow, _>(bytes, bincode::config::standard()).err().expect("malformed row JSON"),
        Root::Queue => bincode::serde::decode_from_slice::<crate::database::event_bus::SinkQueueEntry, _>(bytes, bincode::config::standard()).err().expect("malformed queue JSON"),
    };
    (format!("bincode decode error: {native}"), error.classify(), error.line(), error.column())
}

// Acceptance: real selected-root syntax/EOF parity, 171+119 independent
// prepayment and physical lifetimes. Existing mapped helpers have frozen bodies;
// this is a new thin entry over parallel rows, not a weakened replacement.
#[test]
#[serial]
#[cfg(all(target_arch="x86_64",target_os="linux",target_env="gnu",target_pointer_width="64"))]
fn selected_json_native_parity_and_physical_lifetime_rows() {
    let writer = MemoryAccountant::writer_no_limit().unwrap(); let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap(); let original = writer.reserve_assembly_original(&funder).unwrap();
    let baseline = owner.usage().used;
    let mut inputs: Vec<String> = ["", "[", "{", "[null,]", "{\"a\":}", "[1,", "{\"a\":[\"partial\",}",
        "{\"a\": [1,2],\n\"b\":false,}", "[\"escape\\uD800\"]", "[1e+]", "[true] trailing"].into_iter().map(str::to_owned).collect();
    for depth in [1,2,16,126,127,128] {
        inputs.push(format!("{}!{}", "[".repeat(depth), "]".repeat(depth)));
        inputs.push("[".repeat(depth));
        inputs.push(format!("{}!{}", "{\"a\":".repeat(depth), "}".repeat(depth)));
    }
    for root in [Root::Row, Root::Queue] {
        for (ordinal, input) in inputs.iter().enumerate() {
            let bytes = root.fixture(input); let expected = native_message(root, &bytes, input);
            subrow(root.name(), "native parity", ordinal);
            let ((receipt, after_drop, observation, declaration_receipt), gap) = funded_requests(&owner, || {
                // The existing aggregate receipt has 65 active slots and a
                // deliberately small stack copy. Deep objects retain more
                // keys; the static pointer/generation oracle still covers
                // every request there without enlarging that old receipt.
                let declaration = (input.matches("\"a\":").count() < 32).then(declaration_observation);
                let observation = Observation::begin(&owner);
                let error = root.decode(&original, &bytes).unwrap_err();
                let receipt = observation.receipt();
                assert!(qualified(&error), "only intended syntax/EOF is positive");
                assert_eq!((receipt.native,receipt.text), (171,119));
                assert_eq!((receipt.category,receipt.line,receipt.column), (Some(expected.1),expected.2,expected.3));
                assert_eq!(receipt.finite, None); assert_eq!(receipt.diagnoses,1);
                assert_eq!(receipt.rendered_at, receipt.destroyed_at, "native source dies while its original invoice remains");
                assert!(receipt.candidates > 0);
                assert!(receipt.candidate_peak <= 3*40, "conservative constructor candidate coexistence exceeds native invoice");
                assert_eq!(receipt.candidate_live,0);
                if input == "[" || input == "{" {
                    assert_eq!(receipt.candidates,2,"payload-free root proves primary and secondary native construction");
                    assert_eq!(receipt.candidate_peak,80,"primary remains live while native cleanup constructs its secondary");
                }
                assert!(receipt.output_overlap > receipt.live, "native source and converted output coexist");
                // Read authentic public Display into stack storage, no observer allocation.
                use std::io::Write;
                let mut text = std::io::Cursor::new([0u8;512]); write!(&mut text,"{error}").unwrap();
                assert_eq!(&text.get_ref()[..text.position() as usize], expected.0.as_bytes());
                let moved = Some(error); let held = owner.usage().used;
                REQUESTS.with(|slot| {
                    let entries=slot.borrow(); let output=entries[..receipt.count].iter().find(|e|e.pointer==receipt.output).unwrap();
                    assert_eq!((output.bytes,output.frees),(119,0));
                });
                assert!(held > baseline); drop(moved); let observation = observation.stop();
                let declaration_receipt = declaration.map(|declaration| (declaration.physical_receipt().0, declaration.evidence().complete));
                (receipt,owner.usage().used,observation,declaration_receipt)
            });
            observation.all_freed(); drop(observation);
            if let Some((physical, complete)) = declaration_receipt { assert_eq!(physical,0); assert!(complete); }
            assert_eq!(gap,0); assert_eq!(after_drop,baseline); assert!(receipt.peak > 119);
        }
        for input in ["null", "[]", "{}", "[1,-2,1.25,1e+20,true,false,null]", r#"{"a":"plain","b":"\u0061\n\"\\","a":"last","unicode":"λ"}"#] {
            let expected=serde_json::from_str(input).unwrap(); let bytes=root.fixture(input);
            root.assert_value(&original,&bytes,&expected); assert_eq!(owner.usage().used,baseline);
        }
        for depth in [126,127] {
            let input=format!("{}null{}","[".repeat(depth),"]".repeat(depth)); let bytes=root.fixture(&input);
            let expected=serde_json::from_str(&input).unwrap(); root.assert_value(&original,&bytes,&expected);
        }
    }
}

#[test]
#[serial]
#[cfg(all(target_arch="x86_64",target_os="linux",target_env="gnu",target_pointer_width="64"))]
fn selected_json_first_refusal_and_bootstrap_rows() {
    let writer=MemoryAccountant::writer_no_limit().unwrap(); let owner=writer.accountant();
    let funder=writer.prepare_source_funder().unwrap(); let original=writer.reserve_assembly_original(&funder).unwrap();
    let baseline=owner.usage().used;
    for root in [Root::Row,Root::Queue] {
        // Empty JSON has no payload: the last live bootstrap request reveals
        // the complete preparation floor, independently of numeric formulas.
        subrow(root.name(), "bootstrap floor", 0);
        let empty=root.fixture("");
        let floor={ let o=Observation::begin(&owner); drop(root.decode(&original,&empty).unwrap_err());
            let r=o.receipt(); let floor=r.prepared_at; o.all_freed(); floor };
        assert!(floor>baseline);
        subrow(root.name(), "baseline denial", 0);
        owner.set_budget(Some(baseline)).unwrap();
        let o=Observation::begin(&owner); drop(root.decode(&original,&empty).unwrap_err());
        assert_eq!(o.receipt().count,0); assert!(!o.receipt().prepared); o.all_freed(); drop(o);
        subrow(root.name(), "floor minus one", 0);
        owner.set_budget(Some(floor-1)).unwrap();
        let o=Observation::begin(&owner); let error=root.decode(&original,&empty).unwrap_err();
        assert!(crate::read_image_memory::is_memory_refusal(&error));
        assert!(!o.receipt().prepared,"one byte short of bootstrap never enters JSON"); drop(error);o.all_freed();drop(o);
        owner.set_budget(None).unwrap();
        // Find a first actual JSON payload refusal by replaying the same real
        // root with each original allocation's admitted funding minus one.
        let input=format!("[\"{}\",{{\"later\":[1,2,3]}}]","x".repeat(8192));let bytes=root.fixture(&input);
        let mut limits=Vec::new();
        subrow(root.name(), "replay discovery", 0);
        {let o=Observation::begin(&owner);root.decode(&original,&bytes).unwrap();let o=o.stop();let r=o.receipt();
            // Copy ALL request-derived limits only after the producer window.
            REQUESTS.with(|slot|{for e in &slot.borrow()[..r.count]{if e.prepared{limits.push(e.allocated-1);}}});o.all_freed();}
        limits.sort_unstable();limits.dedup();let mut captured=0;
        for cap in limits {
            owner.set_budget(Some(cap)).unwrap();
            subrow(root.name(), "first refusal replay", cap);
            let ((o,physical,complete),gap)=funded_requests(&owner,||{
                let declaration=declaration_observation();let o=Observation::begin(&owner);
                if let Err(error)=root.decode(&original,&bytes){
                    let r=o.receipt();
                    if let Some(NativeRefusal::MemoryBudgetExceeded{subsystem,operation,requested_bytes,available_bytes,budget_limit_bytes,hint})=r.denial.filter(|_| r.json_refusal) {
                        captured+=1;assert_eq!(r.post_denial_non_native,0,"no native Message, new text or late carrier after denial");
                        assert_eq!(r.diagnoses,0);assert!(qualified(&error));
                        match error.engine_diagnostic(){
                            Error::MemoryBudgetExceeded{subsystem:s,operation:op,requested_bytes:n,available_bytes:a,budget_limit_bytes:b,hint:h}=>{
                                assert_eq!((s.as_ref(),op.as_ref(),*n,*a,*b,h.as_ref()),(subsystem,operation,requested_bytes,available_bytes,budget_limit_bytes,hint));
                            },other=>panic!("authentic first scalar must survive: {other:?}"),
                        }
                        assert_eq!(budget_limit_bytes,cap);assert!(requested_bytes>available_bytes);
                    }
                    drop(error);
                }
                let o=o.stop();let physical=declaration.physical_receipt().0;let complete=declaration.evidence().complete;
                (o,physical,complete)
            });o.all_freed();drop(o);assert_eq!(physical,0);assert!(complete);assert_eq!(gap,0);assert_eq!(owner.usage().used,baseline);owner.set_budget(None).unwrap();
        }
        assert!(captured>0,"must reach a real selected JSON payload refusal");
        owner.set_budget(None).unwrap();
        let o=Observation::begin(&owner);
        subrow(root.name(), "text allocator refusal", 0);
        let layout=Layout::from_size_align(119,1).unwrap();
        crate::vector_read_memory::native_ownership_proof::arm_refuse_one_live_layout(layout);
        let (result,_,_)=native_requests_for_test(||root.decode(&original,&empty));
        let error=result.unwrap_err();
        assert!(!crate::vector_read_memory::native_ownership_proof::refusal_armed_for_test());
        assert!(matches!(error.engine_diagnostic(),Error::NativeEngineOther(NativeRefusal::AllocatorFailed{site:"diagnostic text",layout:l}) if *l==layout));
        assert!(!crate::read_image_memory::is_memory_refusal(&error));
        assert!(!o.receipt().prepared);assert_eq!(o.receipt().diagnoses,0);
        drop(error);o.all_freed();
    }
}

#[test]
#[serial]
#[cfg(all(target_arch="x86_64",target_os="linux",target_env="gnu",target_pointer_width="64"))]
fn selected_json_unknown_data_io_and_unwind_rows() {
    use crate::funded_error::AdmittedJsonDiagnostic;
    use serde::de::Error as _;
    struct RawIo;
    impl std::io::Read for RawIo {
        fn read(&mut self,_:&mut[u8])->std::io::Result<usize>{Err(std::io::Error::from_raw_os_error(5))}
    }
    let writer=MemoryAccountant::writer_no_limit().unwrap();let owner=writer.accountant();
    let funder=writer.prepare_source_funder().unwrap();let original=writer.reserve_assembly_original(&funder).unwrap();
    for io in [false,true] {
        subrow("native diagnostic", if io { "genuine IO" } else { "opaque Data" }, 0);
        let o=Observation::begin(&owner);
        let prepared=AdmittedJsonDiagnostic::prepare(&original).unwrap();
        let native=if io {serde_json::from_reader::<_,serde_json::Value>(RawIo).unwrap_err()}
            else {serde_json::Error::custom("opaque foreign visitor detail")};
        let category=native.classify();let position=(native.line(),native.column());
        let (native,prepared)=prepared.finish(native,None,false).unwrap_err();
        assert_eq!(native.classify(),category);assert_eq!((native.line(),native.column()),position);
        assert_eq!(o.receipt().output,0,"unknown Data and genuine I/O never receive a converted positive certificate");
        drop(native);drop(prepared);o.all_freed();
    }
    for root in [Root::Row,Root::Queue] {
        let bytes=root.fixture("[\"partial\",}");
        // Calibrate in this executing tuple immediately before each real root.
        // No historical Layout/count/generation is an ownership certificate.
        let (control, identity) = application_unwind_window(&owner, None);
        let layout = report_application_unwind(&control, identity);
        let control_proof = unwind_proof(&control, identity, layout).expect("actual driver control provenance");
        assert_eq!(control_proof.engine_gap, 0);
        drop(control);
        let payload: Box<dyn std::any::Any + Send> = Box::new(7u8);
        let sent = (&*payload as *const (dyn std::any::Any + Send)) as *const () as usize;
        subrow(root.name(), "real unwind", 0);
        let o=Observation::begin(&owner);
        let panic=std::panic::catch_unwind(std::panic::AssertUnwindSafe(||{
            let error=root.decode(&original,&bytes).unwrap_err();assert!(qualified(&error));
            // error is moved, never cloned/rebuilt. Decoder temporaries are
            // already gone; remaining captures are references and this payload.
            let _escaping=UnwindEngineValue { value: Some(error) };
            mark_unwind_phase(ProofPhase::DriverInitiate);
            std::panic::resume_unwind(payload);
        }));
        let (returned, returned_value) = panic_identity(&panic);
        drop(panic);
        let o=o.stop();
        let identity=ApplicationUnwindIdentity { sent, returned, returned_value,
            destructor_pointer: 0, destructor_generation: 0 };
        // Different, explicitly adjudicated oracle for this real-unwind row;
        // ordinary all_freed() and its raw zero-gap contract remain unchanged.
        report_unwind_ledger(&o, identity);
        let proof=unwind_proof(&o, identity, layout).expect("real root unwind provenance");
        assert_eq!(proof.engine_gap, 0, "every engine production/destruction request is prepaid");
        assert_eq!(proof.raw_gap, control_proof.raw_gap, "raw driver gap remains visible");
        assert!(proof.engine_requests > 0, "same real engine error was physically observed");
        drop(o);
        subrow(root.name(), "escaped operation", 0);
        let o=Observation::begin(&owner);
        let escaped={
            let operation=writer.reserve_assembly_original(&funder).unwrap();
            root.decode(&operation,&bytes).unwrap_err()
        };
        assert!(qualified(&escaped));assert!(o.receipt().live>0);
        assert_eq!(o.receipt().candidate_live,0,"native backing died before the original operation scope escaped");
        drop(escaped);o.all_freed();drop(o);
        root.decode(&original,&root.fixture("null")).unwrap();
    }
}

const _: () = assert!(std::mem::size_of::<Receipt>() <= 512);

// Source-only revision: UNCOMPILED / RUNTIME_UNRUN. These controls retain the
// complete raw unfunded gap and calibrate the separate proof-only classifier.
// They never certify a foreign native source or modify production funding.
#[derive(Clone, Copy, Debug)]
struct ApplicationUnwindIdentity {
    sent: usize,
    returned: usize,
    returned_value: Option<u8>,
    destructor_pointer: usize,
    destructor_generation: usize,
}

// A stack-only adversary. Its Layout comes from the preceding actual driver
// observation, never a mirrored private exception struct or a byte-size rule.
struct UnfundedUnwindDrop<'a> {
    layout: Option<Layout>,
    pointer: &'a Cell<usize>,
    generation: &'a Cell<usize>,
}
impl Drop for UnfundedUnwindDrop<'_> {
    fn drop(&mut self) {
        let Some(layout) = self.layout else { return; };
        subrow("application unwind", "unfunded destructor begin", 0);
        // No invoice or engine object: the entire new generation is unfunded.
        unsafe {
            let pointer = std::alloc::alloc(layout);
            self.pointer.set(pointer as usize);
            if !pointer.is_null() {
                // Make the actual physical request observable to the allocator;
                // an allocation failure is checked after the window closes.
                std::ptr::write_volatile(pointer, 1);
                std::hint::black_box(pointer);
                self.generation.set(RECEIPT.with(Cell::get).count);
                std::alloc::dealloc(pointer, layout);
            }
        }
        subrow("application unwind", "unfunded destructor end", 0);
    }
}

fn application_unwind_window(
    owner: &MemoryAccountant,
    destructor_layout: Option<Layout>,
) -> (Observation<'_>, ApplicationUnwindIdentity) {
    // Same pre-existing application payload as the original real-unwind row.
    // The owner and its original credit also predate the observation window.
    let payload: Box<dyn std::any::Any + Send> = Box::new(7u8);
    let sent = (&*payload as *const (dyn std::any::Any + Send)) as *const () as usize;
    let destructor_pointer = Cell::new(0);
    let destructor_generation = Cell::new(0);
    subrow("application unwind", "driver", 0);
    let observation = Observation::begin(owner);
    // Only the real application unwind, payload identity reads and destruction
    // run inside this window. No engine codec, error or diagnostic preparation.
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _adversary = UnwindEngineValue { value: Some(UnfundedUnwindDrop {
            layout: destructor_layout,
            pointer: &destructor_pointer,
            generation: &destructor_generation,
        }) };
        mark_unwind_phase(ProofPhase::DriverInitiate);
        std::panic::resume_unwind(payload);
    }));
    let (returned, returned_value) = panic_identity(&caught);
    // Retain the pre-existing payload's physical free in the complete event
    // history, although its allocation is correctly outside the request window.
    drop(caught);
    let observation = observation.stop();
    (observation, ApplicationUnwindIdentity {
        sent, returned, returned_value,
        destructor_pointer: destructor_pointer.get(),
        destructor_generation: destructor_generation.get(),
    })
}

fn panic_identity(caught: &std::thread::Result<()>) -> (usize, Option<u8>) {
    match caught {
        Err(payload) => (
            (&**payload as *const (dyn std::any::Any + Send)) as *const () as usize,
            payload.downcast_ref::<u8>().copied(),
        ),
        Ok(()) => (0, None),
    }
}
fn report_unwind_ledger(observation: &Observation<'_>, identity: ApplicationUnwindIdentity) {
    let r = observation.receipt();
    assert_eq!(RECEIPT.with(Cell::get).counter, 0);
    assert_eq!(MEASURING_HERE.with(Cell::get), 0);
    eprintln!("application unwind identity {identity:?} receipt {r:?}");
    for (label, witness) in [("FIRST", FIRST_GAP.with(Cell::get)), ("WORST", WORST_GAP.with(Cell::get))] {
        if let Some(w) = witness {
            eprintln!("{label} generation={} live={} gap={}", w.generation, w.live, w.gap);
            dump_event(label, 0, w.event);
        }
    }
    REQUESTS.with(|slot| { for e in &slot.borrow()[..r.count] { eprintln!("request {e:?}"); } });
    HISTORY.with(|history| HISTORY_COUNT.with(|count| {
        for (i, event) in history[..count.get()].iter().enumerate() { dump_event("event", i, event.get()); }
    }));
    eprintln!("unwind phase boundaries {:?}", UNWIND_PHASES.with(Cell::get));
}

// This evidence is emitted unconditionally after observation closes, including
// successful negative controls. A future runner must retain its complete stderr.
fn report_application_unwind(
    observation: &Observation<'_>,
    identity: ApplicationUnwindIdentity,
) -> Layout {
    report_unwind_ledger(observation, identity);
    let r = observation.receipt();
    assert_ne!(identity.sent, 0);
    assert_eq!(identity.returned, identity.sent, "catch returns the same pre-existing payload allocation");
    assert_eq!(identity.returned_value, Some(7));
    assert!(!r.invalid, "bounded observer must not overflow or lose events");
    assert_eq!(EVENT_COUNT.with(Cell::get), 0);
    assert_eq!(r.live, 0);
    assert!(r.count > 0, "actual native unwind driver must be physically observed");
    assert!(r.gap > 0, "unfunded application runtime requests must remain visible");
    assert_eq!(r.gap, r.peak, "no engine credit funds this application-only interval");
    assert!(!r.prepared, "no source-native constructor preparation ran");
    assert_eq!((r.native, r.text, r.prepared_at, r.diagnoses, r.candidates), (0, 0, 0, 0, 0));
    assert!(r.denial.is_none());
    assert!(!r.json_refusal);
    assert_eq!(observation.0.usage().used, r.baseline);
    HISTORY.with(|history| HISTORY_COUNT.with(|count| {
        assert_eq!(count.get(), r.count * 3 + 2, "every allocation, before/after free, and the pre-existing payload free is retained");
        for event in &history[..count.get()] {
            let event = event.get();
            assert_eq!(event.funding, r.baseline);
            assert!(!event.prepared); assert!(!event.denied);
            assert_eq!((event.native, event.text, event.prepared_at), (0, 0, 0));
        }
        let before = history[count.get() - 2].get();
        let after = history[count.get() - 1].get();
        assert_eq!((before.pointer, before.kind), (identity.sent, 1));
        assert_eq!((after.pointer, after.kind), (identity.sent, 2));
        let payload_layout = Layout::new::<u8>();
        assert_eq!((before.bytes, before.align), (payload_layout.size(), payload_layout.align()));
        assert_eq!((after.bytes, after.align), (before.bytes, before.align));
    }));
    assert_eq!((r.output, r.output_overlap, r.rendered_at, r.destroyed_at), (0, 0, 0, 0));
    let first = FIRST_GAP.with(Cell::get).expect("driver first funding witness");
    assert_eq!((first.generation, first.event.kind), (1, 0));
    REQUESTS.with(|slot| {
        let entries = slot.borrow();
        for e in &entries[..r.count] {
            assert_ne!(e.pointer, 0); assert!(e.generation > 0); assert_eq!(e.frees, 1);
            assert!(!e.prepared);
            assert_eq!((e.allocated, e.before_free, e.after_free), (r.baseline, r.baseline, r.baseline));
        }
        let driver = entries[0];
        assert_eq!(first.event.pointer, driver.pointer);
        Layout::from_size_align(driver.bytes, driver.align).unwrap()
    })
}

// Acceptance: isolate the actual catch/resume driver from all engine work and
// retain its complete unfunded physical lifetime plus payload identity. Decisive
// mutant: suppress the native driver request in the observer (count/gap fails).
// The real engine row cannot replace this independent calibration: it decodes
// and destroys an engine error, whereas this control has no engine requests.
#[test]
#[serial]
#[cfg(all(target_arch="x86_64",target_os="linux",target_env="gnu",target_pointer_width="64"))]
fn application_only_native_unwind_driver_control() {
    let writer = MemoryAccountant::writer_no_limit().unwrap(); let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap(); let original = writer.reserve_assembly_original(&funder).unwrap();
    let baseline = owner.usage().used;
    let (observation, identity) = application_unwind_window(&owner, None);
    let layout = report_application_unwind(&observation, identity);
    let proof = unwind_proof(&observation, identity, layout).expect("phase-separated actual driver control");
    assert_eq!(proof.engine_gap, 0);
    assert_eq!(proof.engine_requests, 0);
    assert_eq!((identity.destructor_pointer, identity.destructor_generation), (0, 0));
    assert_eq!(owner.usage().used, baseline);
    drop(observation); drop(original);
}

// Acceptance: a same-Layout destructor allocation during real unwinding remains
// a separate UNFUNDED generation. Decisive future classifier mutant: exempt all
// allocations during unwind, or merge/exempt requests merely by matching Layout.
// The ACTUAL classifier below must retain this engineDrop request while the
// genuine driver pointer is still live. It deliberately has no admission.
#[test]
#[serial]
#[cfg(all(target_arch="x86_64",target_os="linux",target_env="gnu",target_pointer_width="64"))]
fn application_unwind_same_layout_unfunded_destructor_control() {
    let writer = MemoryAccountant::writer_no_limit().unwrap(); let owner = writer.accountant();
    let funder = writer.prepare_source_funder().unwrap(); let original = writer.reserve_assembly_original(&funder).unwrap();
    let baseline = owner.usage().used;
    let (driver, identity) = application_unwind_window(&owner, None);
    let layout = report_application_unwind(&driver, identity);
    let control = unwind_proof(&driver, identity, layout).expect("actual control provenance");
    assert_eq!(control.engine_gap, 0);
    assert_eq!(control.engine_requests, 0);
    drop(driver);
    let (observation, identity) = application_unwind_window(&owner, Some(layout));
    let replay_layout = report_application_unwind(&observation, identity);
    assert_eq!(replay_layout, layout, "fresh actual native driver has the observed control Layout");
    let proof = unwind_proof(&observation, identity, layout).expect("negative has valid provenance, not valid funding");
    assert_eq!(proof.engine_requests, 1);
    assert_eq!(proof.engine_gap, layout.size(), "same-Layout EngineDrop remains unpaid");
    assert_eq!(proof.raw_gap, control.raw_gap + layout.size());
    assert_ne!(proof.driver_pointer, identity.destructor_pointer);
    assert_eq!(proof.engine_drop_while_driver_live, 1);
    assert_ne!(identity.destructor_pointer, 0);
    assert_ne!(identity.destructor_generation, proof.driver_generation);
    REQUESTS.with(|slot| {
        let entries = slot.borrow(); let driver = entries[0]; let destructor = entries[1];
        assert_ne!(driver.pointer, destructor.pointer, "driver remains live across destructor allocation");
        assert_eq!(destructor.pointer, identity.destructor_pointer);
        assert_eq!((destructor.bytes, destructor.align), (layout.size(), layout.align()));
        assert_eq!(observation.receipt().peak, driver.bytes + destructor.bytes);
    });
    assert_eq!(owner.usage().used, baseline);
    drop(observation); drop(original);
}

#[derive(Debug)]
struct UnwindProof {
    driver_pointer: usize,
    driver_generation: usize,
    raw_gap: usize,
    engine_gap: usize,
    engine_requests: usize,
    engine_drop_while_driver_live: usize,
}
struct ReplayRequest {
    request: Request,
    phase: ProofPhase,
    // 0: live, 1: before physical free, 2: after physical free.
    state: u8,
}

// Closed-window replay, NOT a change to sample() or all_freed(). The whole
// ledger is validated before a driver byte can be subtracted. This function
// returns Err on any unclassifiable trace; it never clears the raw gap.
// Layout is freshly obtained from the application-only control executing in
// this same native tuple, and is necessary but never sufficient provenance.
fn unwind_proof(
    observation: &Observation<'_>,
    identity: ApplicationUnwindIdentity,
    driver_layout: Layout,
) -> std::result::Result<UnwindProof, &'static str> {
    if RECEIPT.with(Cell::get).counter != 0 || MEASURING_HERE.with(Cell::get) != 0 {
        return Err("provenance requires closed producer and aggregate windows");
    }
    let r = observation.receipt();
    let phases = UNWIND_PHASES.with(Cell::get);
    let count = HISTORY_COUNT.with(Cell::get);
    if r.invalid || r.live != 0 || EVENT_COUNT.with(Cell::get) != 0 || phases.invalid
        || phases.phase != ProofPhase::DriverCatchCleanup
        || !(phases.initiate_at < phases.drop_at && phases.drop_at <= phases.cleanup_at && phases.cleanup_at < count)
        || identity.sent == 0 || identity.sent != identity.returned || identity.returned_value != Some(7)
        || observation.0.usage().used != r.baseline {
        return Err("invalid observation, phases, payload identity, or final physical/funding balance");
    }
    // Small metadata on the stack; variable replay storage is allocated only
    // AFTER every observation is closed. Never borrow REQUESTS in a callback.
    let mut replay: Vec<ReplayRequest> = Vec::with_capacity(r.count);
    let mut driver_index = None;
    let mut payload_state = 0;
    let mut payload_before = 0;
    let mut raw_live = 0usize;
    let mut raw_peak = 0usize;
    let mut raw_gap = 0usize;
    let mut engine_requests = 0usize;
    let mut engine_drop_while_driver_live = 0usize;
    // First pass validates all phase boundaries, pointers, generations, Layouts,
    // exact one-free histories, and funding-before-physical-free ordering.
    for index in 0..count {
        let event = HISTORY.with(|history| history[index].get());
        let phase = if index < phases.initiate_at { ProofPhase::EngineProduce }
            else if index < phases.drop_at { ProofPhase::DriverInitiate }
            else if index < phases.cleanup_at { ProofPhase::EngineDrop }
            else { ProofPhase::DriverCatchCleanup };
        if event.proof_phase != phase || event.pointer == 0 || event.bytes == 0
            || Layout::from_size_align(event.bytes, event.align).is_err()
            || event.funding < r.baseline || event.kind > 2 {
            return Err("unclassifiable physical event or phase");
        }
        if event.kind == 0 {
            if event.pointer == identity.sent || replay.iter().any(|e| e.request.pointer == event.pointer && e.state != 2) {
                return Err("allocation aliases a live generation or the pre-existing payload");
            }
            match phase {
                ProofPhase::DriverInitiate => {
                    if driver_index.is_some() || (event.bytes, event.align) != (driver_layout.size(), driver_layout.align()) {
                        return Err("actual driver pattern differs from single-lifecycle control");
                    }
                    driver_index = Some(replay.len());
                }
                ProofPhase::EngineProduce | ProofPhase::EngineDrop => {
                    engine_requests += 1;
                    if phase == ProofPhase::EngineDrop {
                        let Some(driver) = driver_index else { return Err("engine destructor before actual driver allocation"); };
                        if replay[driver].state != 0 { return Err("driver no longer live at engine destruction"); }
                        engine_drop_while_driver_live += 1;
                    }
                }
                ProofPhase::DriverCatchCleanup => return Err("new cleanup allocation is not the control lifecycle"),
            }
            replay.push(ReplayRequest { request: Request {
                generation: replay.len() + 1, pointer: event.pointer, bytes: event.bytes, align: event.align,
                allocated: event.funding, before_free: 0, after_free: 0, frees: 0, prepared: event.prepared,
            }, phase, state: 0 });
            raw_live = raw_live.checked_add(event.bytes).ok_or("live byte overflow")?;
            raw_peak = raw_peak.max(raw_live);
        } else if let Some(e) = replay.iter_mut().rev().find(|e| e.request.pointer == event.pointer && e.state != 2) {
            if (e.request.bytes, e.request.align) != (event.bytes, event.align) {
                return Err("free Layout differs from original allocation");
            }
            let valid_phase = match e.phase {
                ProofPhase::DriverInitiate => phase == ProofPhase::DriverCatchCleanup,
                ProofPhase::EngineProduce => matches!(phase, ProofPhase::EngineProduce | ProofPhase::EngineDrop),
                ProofPhase::EngineDrop => phase == ProofPhase::EngineDrop,
                ProofPhase::DriverCatchCleanup => false,
            };
            if !valid_phase { return Err("engine/user destruction in a driver-only interval"); }
            if event.kind == 1 {
                if e.state != 0 { return Err("duplicate before-free event"); }
                e.request.before_free = event.funding;
                e.state = 1;
            } else {
                if e.state != 1 || e.request.before_free != event.funding {
                    return Err("after-free missing its original funded before-free sample");
                }
                e.request.after_free = event.funding;
                e.request.frees = 1;
                e.state = 2;
            }
        } else {
            // Only the known application Box predates this proof window.
            // All other untracked frees invalidate provenance, rather than
            // being waved away as application or runtime housekeeping.
            if event.pointer != identity.sent || phase != ProofPhase::DriverCatchCleanup
                || (event.bytes, event.align) != (Layout::new::<u8>().size(), Layout::new::<u8>().align())
                || event.kind != payload_state + 1 || payload_state >= 2 {
                return Err("unknown pointer, repeated free, or payload freed outside catch cleanup");
            }
            let Some(driver) = driver_index else { return Err("payload cleanup before driver initiation"); };
            if replay[driver].state != 2 { return Err("payload cleanup precedes complete driver destruction"); }
            if event.kind == 1 { payload_before = event.funding; }
            else if payload_before != event.funding { return Err("payload funding changed across physical free"); }
            payload_state += 1;
        }
        // Identical raw sampling semantics: after-free is sampled BEFORE live
        // retirement, just as the original observer does. No raw event is lost.
        raw_gap = raw_gap.max(raw_live.saturating_sub(event.funding - r.baseline));
        if event.kind == 2 && event.pointer != identity.sent {
            raw_live = raw_live.checked_sub(event.bytes).ok_or("invalid live retirement")?;
        }
    }
    let driver_index = driver_index.ok_or("actual driver allocation missing")?;
    if raw_live != 0 || payload_state != 2 || replay.len() != r.count
        || raw_peak != r.peak || raw_gap != r.gap {
        return Err("raw replay does not reconcile with the complete physical ledger");
    }
    let reconciled = REQUESTS.with(|slot| {
        let entries = slot.borrow();
        replay.iter().zip(entries.iter()).all(|(a, b)| {
            let a = &a.request;
            (a.generation, a.pointer, a.bytes, a.align, a.allocated, a.before_free, a.after_free, a.frees, a.prepared)
                == (b.generation, b.pointer, b.bytes, b.align, b.allocated, b.before_free, b.after_free, b.frees, b.prepared)
        })
    });
    if !reconciled || replay.iter().any(|e| e.state != 2 || e.request.frees != 1) {
        return Err("generation ledger mismatch or not exactly one complete physical free");
    }
    let driver = &replay[driver_index].request;
    // Only now is this actual, fully validated pointer separately identified.
    // Repeat physical sampling while subtracting its bytes ONLY when LIVE.
    let mut live = 0usize;
    let mut driver_live = false;
    let mut engine_gap = 0usize;
    for index in 0..count {
        let event = HISTORY.with(|history| history[index].get());
        if event.kind == 0 {
            live += event.bytes;
            // An address used by an earlier engine generation may have been
            // recycled. Address equality alone is never driver provenance.
            if event.pointer == driver.pointer && event.proof_phase == ProofPhase::DriverInitiate {
                driver_live = true;
            }
        }
        let driver_bytes = if driver_live { driver.bytes } else { 0 };
        engine_gap = engine_gap.max((live - driver_bytes).saturating_sub(event.funding - r.baseline));
        if event.kind == 2 && event.pointer != identity.sent {
            live -= event.bytes;
            if event.pointer == driver.pointer { driver_live = false; }
        }
    }
    let proof = UnwindProof { driver_pointer: driver.pointer, driver_generation: driver.generation,
        raw_gap, engine_gap, engine_requests, engine_drop_while_driver_live };
    eprintln!("unwind provenance {proof:?}; raw requests/events remain unchanged");
    Ok(proof)
}
