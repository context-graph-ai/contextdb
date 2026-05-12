//! Shared process-wide tracing subscriber for acceptance tests.
//!
//! Three suites need to count engine `tracing` events at different test windows:
//! - §30 (`trigger_same_db_progress.rs`): t30_17 asserts exactly one warn on
//!   deadlock-guard timeout; t30_18 asserts zero warns on healthy contention.
//! - §29 (`trigger_concurrency_panic_freedom.rs`): t37_* siblings assert zero
//!   deadlock-guard warns on healthy and cross-DB paths; one warn on the
//!   deadlock-guard sibling.
//! - §31 (`event_bus.rs`): t5_33 asserts sink callback panics are emitted as
//!   structured tracing events.
//!
//! Earlier suites consumed `set_global_default` via separate `OnceLock`-gated
//! installers. The first install won; the second silently failed; the loser's
//! counter never incremented. This module owns the single global subscriber
//! and dispatches events into suite-specific counters via per-suite enable
//! flags.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Mutex, OnceLock};

pub static T30_WARN_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static T30_COUNT_ENABLED: AtomicBool = AtomicBool::new(false);
pub static T30_LAST_WARN_FIELDS: OnceLock<Mutex<Option<String>>> = OnceLock::new();

pub static T37_TRACING_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static T37_COUNT_ENABLED: AtomicBool = AtomicBool::new(false);

pub static T5_EVENT_BUS_PANIC_COUNT: AtomicUsize = AtomicUsize::new(0);
pub static T5_EVENT_BUS_COUNT_ENABLED: AtomicBool = AtomicBool::new(false);
pub static T5_EVENT_BUS_LAST_PANIC_FIELDS: OnceLock<Mutex<Option<String>>> = OnceLock::new();

static SUBSCRIBER_INIT: OnceLock<()> = OnceLock::new();

pub fn install_global_subscriber() {
    SUBSCRIBER_INIT.get_or_init(|| {
        use tracing::subscriber::set_global_default;
        use tracing_subscriber::Registry;
        use tracing_subscriber::layer::SubscriberExt;

        struct CountingLayer;
        impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountingLayer {
            fn on_event(
                &self,
                event: &tracing::Event<'_>,
                _ctx: tracing_subscriber::layer::Context<'_, S>,
            ) {
                if T30_COUNT_ENABLED.load(AtomicOrdering::SeqCst)
                    && *event.metadata().level() == tracing::Level::WARN
                    && event.metadata().target().starts_with("contextdb_engine")
                {
                    let mut visitor = StringFieldVisitor(String::new());
                    event.record(&mut visitor);
                    if visitor
                        .0
                        .contains("trigger callback wait exceeded deadlock guard")
                    {
                        T30_WARN_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                        let cell = T30_LAST_WARN_FIELDS.get_or_init(|| Mutex::new(None));
                        *cell.lock().unwrap() = Some(visitor.0);
                    }
                }
                if T37_COUNT_ENABLED.load(AtomicOrdering::SeqCst)
                    && *event.metadata().level() == tracing::Level::WARN
                    && event.metadata().target().starts_with("contextdb_engine")
                {
                    let mut visitor = StringFieldVisitor(String::new());
                    event.record(&mut visitor);
                    if visitor
                        .0
                        .contains("trigger callback wait exceeded deadlock guard")
                    {
                        T37_TRACING_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                    }
                }
                if T5_EVENT_BUS_COUNT_ENABLED.load(AtomicOrdering::SeqCst)
                    && *event.metadata().level() == tracing::Level::WARN
                    && event.metadata().target().starts_with("contextdb_engine")
                {
                    let mut visitor = EventBusPanicFieldVisitor::default();
                    event.record(&mut visitor);
                    if visitor.has_sink
                        && visitor.has_entry_id
                        && visitor.has_panic_message
                        && visitor
                            .rendered
                            .contains("scoped-handle sink panic on first event")
                    {
                        T5_EVENT_BUS_PANIC_COUNT.fetch_add(1, AtomicOrdering::SeqCst);
                        let cell = T5_EVENT_BUS_LAST_PANIC_FIELDS.get_or_init(|| Mutex::new(None));
                        *cell.lock().unwrap() = Some(visitor.rendered);
                    }
                }
            }
        }

        struct StringFieldVisitor(String);
        impl tracing::field::Visit for StringFieldVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let _ = write!(&mut self.0, "{}={:?} ", field.name(), value);
            }
        }

        #[derive(Default)]
        struct EventBusPanicFieldVisitor {
            rendered: String,
            has_sink: bool,
            has_entry_id: bool,
            has_panic_message: bool,
        }

        impl tracing::field::Visit for EventBusPanicFieldVisitor {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                match field.name() {
                    "sink" => self.has_sink = true,
                    "entry_id" => self.has_entry_id = true,
                    "panic_message" => self.has_panic_message = true,
                    _ => {}
                }
                let _ = write!(&mut self.rendered, "{}={:?} ", field.name(), value);
            }
        }

        let _ = set_global_default(Registry::default().with(CountingLayer));
    });
}

pub fn t30_reset_warn_counters() {
    T30_WARN_COUNT.store(0, AtomicOrdering::SeqCst);
    T30_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
    if let Some(cell) = T30_LAST_WARN_FIELDS.get() {
        *cell.lock().unwrap() = None;
    }
}

pub fn t37_reset() {
    T37_TRACING_COUNT.store(0, AtomicOrdering::SeqCst);
    T37_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
}

pub fn t5_event_bus_reset() {
    T5_EVENT_BUS_PANIC_COUNT.store(0, AtomicOrdering::SeqCst);
    T5_EVENT_BUS_COUNT_ENABLED.store(false, AtomicOrdering::SeqCst);
    if let Some(cell) = T5_EVENT_BUS_LAST_PANIC_FIELDS.get() {
        *cell.lock().unwrap() = None;
    }
}
