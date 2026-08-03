//! Per-peer transfer receipts: what actually moved, with whom, and which way.
//!
//! An embedding application that pays for bandwidth needs to answer "how much
//! did this node move, and to whom" without instrumenting the transport
//! itself. These counters answer exactly that, at all four boundaries — sync
//! push, sync pull, blob serve, blob fetch.
//!
//! Three properties are deliberate:
//!
//! * **Payload only.** `payload_bytes` counts the row payload or the blob's
//!   own bytes. Message framing, transport headers, retransmits and
//!   encryption overhead are all EXCLUDED, so the number is a floor on what
//!   crossed the wire, never a billing figure for it.
//! * **Authenticated peers only.** A receipt is keyed by the peer's
//!   transport-authenticated node id. An exchange over a transport that
//!   authenticates nobody produces NO receipt rather than an unkeyed one that
//!   would quietly aggregate every anonymous peer together — and it never adds
//!   to another peer's counters either.
//! * **In memory only.** Counters are monotonic for the lifetime of the
//!   `SyncClient` / `SyncServer` / `BlobStore` that owns them. The engine
//!   persists none of this, so a fresh handle starts from zero.

pub use contextdb_engine::transfer_receipts::{
    TransferCounters, TransferDirection, TransferPlane, TransferReceipt,
};

use std::collections::HashMap;
use std::sync::Mutex;

type ReceiptKey = (String, TransferPlane, TransferDirection);

/// The in-memory accumulator behind `transfer_receipts()`.
#[derive(Default)]
pub(crate) struct TransferLedger {
    entries: Mutex<HashMap<ReceiptKey, TransferCounters>>,
}

impl TransferLedger {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Add one exchange's counters against `peer`. An exchange whose peer the
    /// transport did not authenticate (`None`) is dropped entirely — it must
    /// not land on any other peer's key.
    pub(crate) fn record(
        &self,
        peer: Option<&str>,
        plane: TransferPlane,
        direction: TransferDirection,
        items: u64,
        payload_bytes: u64,
    ) {
        let Some(peer) = peer else {
            return;
        };
        let mut entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        let counters = entries
            .entry((peer.to_string(), plane, direction))
            .or_default();
        counters.items = counters.items.saturating_add(items);
        counters.payload_bytes = counters.payload_bytes.saturating_add(payload_bytes);
    }

    pub(crate) fn receipts(&self) -> Vec<TransferReceipt> {
        let entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        let mut receipts = entries
            .iter()
            .map(
                |((peer_node_id, plane, direction), counters)| TransferReceipt {
                    peer_node_id: peer_node_id.clone(),
                    plane: *plane,
                    direction: *direction,
                    counters: counters.clone(),
                },
            )
            .collect::<Vec<_>>();
        receipts.sort_by(|left, right| {
            left.peer_node_id
                .cmp(&right.peer_node_id)
                .then_with(|| format!("{:?}", left.plane).cmp(&format!("{:?}", right.plane)))
                .then_with(|| {
                    format!("{:?}", left.direction).cmp(&format!("{:?}", right.direction))
                })
        });
        receipts
    }
}
