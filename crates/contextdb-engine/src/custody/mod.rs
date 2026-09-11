//! Owned delivery metadata and canonical commitments shared by storage and wire.
pub(crate) mod authority;
pub(crate) mod canonical;
// Borrowed inspection of canonical bodies serves only the test seams.
#[cfg(feature = "test-seams")]
pub(crate) mod decoder;
pub(crate) mod durability;
#[cfg(feature = "test-seams")]
pub(crate) mod fixtures;
pub(crate) mod preparation;
pub(crate) mod records;
// Declared authority, authenticated binding, and metadata inspection.
pub(crate) mod policy;

pub(crate) mod inspection;

pub(crate) mod delivery;

pub(crate) mod incarnation;

pub(crate) mod erasure;

// Indexed metadata persistence seam.
pub(crate) mod store;

// Private bounded image evidence.
pub(crate) mod checkpoint;

// Explicit outbound membership.
pub(crate) mod outbound;
