//! The sync transport: moves changesets between an embedding `SyncClient`
//! edge and a hub-side `SyncServer`, plus (behind the `iroh` feature) the
//! blob data plane for content-addressed media. This crate carries no
//! reconciliation policy of its own beyond the shipped conflict rules — it
//! is a mover, not a judge.
//!
//! # Overview
//!
//! An edge builds a [`SyncClient`] bound to one tenant and dials a hub by its
//! enrollment ticket (the hub's cryptographic identity); the hub runs a
//! [`SyncServer`] for that same tenant, and the two exchange changesets in
//! both directions. Content-addressed media moves node-to-node through the
//! `BlobStore` when the `iroh` feature is enabled. See each type's own
//! documentation for constructor signatures and worked usage — the client and
//! server are built with a database handle, an endpoint, and a `TenantId`.

#[cfg(feature = "iroh")]
pub mod blob_resolver {
    pub use contextdb_engine::blob_store::{
        BlobFetchPolicy, BlobStore, ClaimRefreshHook, ResolveError,
    };
}
pub mod chunking;
pub use contextdb_engine::error;
pub mod exit_codes;
pub use contextdb_engine::identity;
pub use contextdb_engine::protocol;
pub use contextdb_engine::subjects;
pub mod sync_client {
    pub use contextdb_engine::sync_client::*;
}
pub mod sync_plugin;
pub mod sync_server {
    pub use contextdb_engine::sync_server::*;
}
pub mod transfer_receipts {
    pub use contextdb_engine::transfer_receipts::*;
}
pub mod transport {
    pub use contextdb_engine::transport::*;
}
pub mod work_ledger;

#[cfg(feature = "iroh")]
pub use blob_resolver::{BlobStore, ResolveError};
pub use contextdb_engine::FabricIdentity;
pub use sync_client::SyncClient;
#[doc(hidden)]
pub use sync_client::{acceptance_stamped_push_batches_for_test, split_changeset_for_test};
pub use sync_plugin::SyncPlugin;
pub use sync_server::SyncServer;
/// Re-export the async runtime this crate is built on, so a consumer that drives the shipped async
/// worker/claim/push surface can spin a runtime without taking its own direct `tokio` dependency.
pub use tokio;
pub use transfer_receipts::{TransferCounters, TransferDirection, TransferPlane, TransferReceipt};
#[cfg(any(test, feature = "test-seams"))]
pub use transport::in_process::InProcessBroker;
// Transport-neutral peer endpoint surface for embedding consumers (e.g.
// a downstream fabric runtime): a consumer names `PeerEndpoint` / `PeerEndpointSpec`
// and calls `peer_bind_spec` / `peer_dial_spec` without ever spelling the
// concrete transport's own name or its URI scheme literal. The alias and
// builders are established inside transport/mod.rs, a file physically under
// the adapter boundary; this is crate-root wiring only.
#[cfg(feature = "iroh")]
pub use transport::{PeerEndpoint, PeerEndpointSpec, peer_bind_spec, peer_dial_spec};
