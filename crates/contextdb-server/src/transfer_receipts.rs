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
//!   `SyncClient` / `SyncServer` / `BlobService` that owns them. The engine
//!   persists none of this, so a fresh handle starts from zero.

use std::collections::HashMap;
use std::sync::Mutex;

/// Which data plane a transfer crossed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransferPlane {
    /// The sync plane: changesets between an edge and its hub.
    Sync,
    /// The blob plane: content-addressed media between two nodes.
    Blob,
}

/// Which way the bytes went, from the perspective of the node holding the
/// receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransferDirection {
    Sent,
    Received,
}

/// What MOVED — not what was kept. `items` counts rows (sync) or blobs (blob
/// plane) that crossed the wire: the ones this end put on it for `Sent`, and the
/// ones it took off it for `Received`. A received row that the conflict policy
/// then skipped still moved, so it is still counted; these are transfer
/// counters, and an application asking "what did this node ship" is asking about
/// the wire, not about the outcome of applying it.
///
/// `payload_bytes` counts the payload those same items carried, with transport
/// and encryption overhead excluded (see the module docs). Both figures are
/// always drawn from the same row set, so they can be divided into each other.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TransferCounters {
    pub items: u64,
    pub payload_bytes: u64,
}

/// One peer, one plane, one direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferReceipt {
    /// The peer's transport-authenticated node id. Non-optional on purpose:
    /// receipts exist only for peers the transport actually authenticated.
    pub peer_node_id: String,
    pub plane: TransferPlane,
    pub direction: TransferDirection,
    pub counters: TransferCounters,
}

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
