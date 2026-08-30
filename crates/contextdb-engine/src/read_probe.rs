//! Subsystem probes a reading route is judged by.
//!
//! A reading route promises that opening a store for reading starts nothing:
//! no writer, no worker, no media repository, no service. That promise is only
//! worth its words if the counting happens where the thing actually starts, so
//! every note below sits at the real start site in the subsystem that owns it.
//! The reading route reads these numbers; it never maintains them, and it
//! cannot certify itself.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// One read operation and the cancellation token it actually ran with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ObservedCancellation {
    pub(crate) operation: u8,
    pub(crate) identity: u64,
    pub(crate) initially_cancelled: bool,
}

pub(crate) const CANCELLATION_EXECUTE: u8 = 0;
pub(crate) const CANCELLATION_CURSOR_OPEN: u8 = 1;
pub(crate) const CANCELLATION_CURSOR_FETCH: u8 = 2;

static CANCELLATIONS: Mutex<Vec<ObservedCancellation>> = Mutex::new(Vec::new());

/// Record the token one operation began with, at the moment it began.
pub(crate) fn note_cancellation(
    operation: u8,
    cancellation: &contextdb_core::read_contract::OwnerReadCancellation,
) {
    CANCELLATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(ObservedCancellation {
            operation,
            identity: cancellation.identity(),
            initially_cancelled: cancellation.is_cancelled(),
        });
}

pub(crate) fn observed_cancellations() -> Vec<ObservedCancellation> {
    CANCELLATIONS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone()
}

macro_rules! probes {
    ($($field:ident => $note:ident),+ $(,)?) => {
        #[derive(Default)]
        struct SubsystemProbes {
            $($field: AtomicU64,)+
        }

        static PROBES: SubsystemProbes = SubsystemProbes {
            $($field: AtomicU64::new(0),)+
        };

        $(
            #[allow(dead_code)]
            pub(crate) fn $note() {
                PROBES.$field.fetch_add(1, Ordering::Relaxed);
            }
        )+

        /// Begin a fresh observation window.
        pub(crate) fn reset() {
            $(PROBES.$field.store(0, Ordering::Relaxed);)+
            CANCELLATIONS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clear();
        }

        /// What every probe has counted since the window began.
        pub(crate) fn observed() -> ObservedProbes {
            ObservedProbes {
                $($field: PROBES.$field.load(Ordering::Relaxed),)+
            }
        }

        /// A point-in-time reading of every probe.
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        pub(crate) struct ObservedProbes {
            $(pub(crate) $field: u64,)+
        }
    };
}

probes! {
    persistence_read_only_opens => note_persistence_read_only_open,
    persistence_source_accesses => note_persistence_source_access,
    persistence_release_receipts => note_persistence_release_receipt,
    persistence_writer_open_attempts => note_persistence_writer_open_attempt,
    persistence_companion_mutations => note_persistence_companion_mutation,
    post_release_source_accesses => note_post_release_source_access,
    active_cursors => note_cursor_opened,
    closed_cursors => note_cursor_closed,
    retained_cursor_bytes => note_cursor_bytes_retained_unit,
    released_cursor_bytes => note_cursor_bytes_released_unit,
    cursor_refusals => note_cursor_refusal,
    bounded_source_touches => note_bounded_source_touch,
    writable_database_opens => note_writable_database_open,
    persistence_writer_opens => note_persistence_writer_open,
    plugin_starts => note_plugin_start,
    sync_client_starts => note_sync_client_start,
    cron_worker_starts => note_cron_worker_start,
    maintenance_worker_starts => note_maintenance_worker_start,
    event_delivery_starts => note_event_delivery_start,
    trigger_callback_starts => note_trigger_callback_start,
    background_worker_starts => note_background_worker_start,
    blob_repository_opens => note_blob_repository_open,
    owner_service_starts => note_owner_service_start,
}

// The source-item counter of the read this thread is running, when the caller
// asked to be told about its own read.
//
// The process-wide count answers "what has this process done", which is a
// different question from "what has THIS read done" -- and a proof that reads
// the process-wide number while other reads are running is measuring its
// neighbours. A session installs its own counter for exactly the span of one
// operation; the kernel bumps whichever counter is in force on the thread it
// is running on.
#[cfg(feature = "test-seams")]
thread_local! {
    static SESSION_SOURCE_TOUCHES: std::cell::RefCell<Option<Arc<AtomicU64>>> =
        const { std::cell::RefCell::new(None) };
}

/// Put this read's counter in force on this thread, answering with whatever
/// was there before so the caller can put it back.
#[cfg(feature = "test-seams")]
pub(crate) fn install_session_source_counter(
    counter: Option<Arc<AtomicU64>>,
) -> Option<Arc<AtomicU64>> {
    SESSION_SOURCE_TOUCHES.with(|slot| slot.replace(counter))
}

/// Count one real source item against the read this thread is running.
#[cfg(feature = "test-seams")]
pub(crate) fn note_session_source_touch() {
    SESSION_SOURCE_TOUCHES.with(|slot| {
        if let Some(counter) = slot.borrow().as_ref() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    });
}

/// Charge, then release, the bytes one cursor actually holds.
pub(crate) fn note_cursor_bytes_charged(bytes: u64) {
    PROBES
        .retained_cursor_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

pub(crate) fn note_cursor_bytes_returned(bytes: u64) {
    PROBES
        .retained_cursor_bytes
        .fetch_sub(bytes, Ordering::Relaxed);
    PROBES
        .released_cursor_bytes
        .fetch_add(bytes, Ordering::Relaxed);
}

/// Where each serving owner in THIS process publishes the counter its kernel
/// charges for work asked for over its channel.
///
/// A reader on the owner route asks a question in one thread and has it
/// answered by the owner's own thread, so the counter the reader installed on
/// its thread never sees the items the owner finished for it. The owner
/// publishes one counter per channel and charges every request it serves
/// there against it; a reader that dialled that channel in the same process
/// reads the same counter. The number is therefore the CHANNEL's completed
/// items, not one reader's: two readers on one channel see each other's work,
/// which is exactly what "the owner did this much for its channel" means.
/// Nothing outside this process can read it, so a cross-process reader still
/// sees only its own zero.
#[cfg(feature = "test-seams")]
static OWNER_ROUTE_SOURCE_COUNTERS: Mutex<
    Option<
        std::collections::BTreeMap<contextdb_core::read_contract::ChannelAddress, Arc<AtomicU64>>,
    >,
> = Mutex::new(None);

/// Publish the counter an owner charges its channel's work against.
#[cfg(feature = "test-seams")]
pub(crate) fn publish_owner_route_source_counter(
    address: contextdb_core::read_contract::ChannelAddress,
    counter: Arc<AtomicU64>,
) {
    OWNER_ROUTE_SOURCE_COUNTERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get_or_insert_with(std::collections::BTreeMap::new)
        .insert(address, counter);
}

/// Take an owner's counter down with the owner itself, so a later owner at the
/// same address never inherits a departed one's number.
#[cfg(feature = "test-seams")]
pub(crate) fn withdraw_owner_route_source_counter(
    address: contextdb_core::read_contract::ChannelAddress,
) {
    if let Some(published) = OWNER_ROUTE_SOURCE_COUNTERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_mut()
    {
        let _departed = published.remove(&address);
    }
}

/// The counter the owner at this channel charges, when that owner is in this
/// process.
#[cfg(feature = "test-seams")]
pub(crate) fn owner_route_source_counter(
    address: contextdb_core::read_contract::ChannelAddress,
) -> Option<Arc<AtomicU64>> {
    OWNER_ROUTE_SOURCE_COUNTERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .and_then(|published| published.get(&address).cloned())
}
