//! Owned delivery metadata and canonical commitments shared by storage and wire.
pub(crate) mod authority;
pub(crate) mod canonical;
pub(crate) mod decoder;
pub(crate) mod durability;
#[cfg(feature = "test-seams")]
pub(crate) mod fixtures;
pub(crate) mod preparation;
pub(crate) mod records;
// Statements 1–6/19.
pub(crate) mod policy;

pub(crate) mod inspection;

pub(crate) mod delivery;

pub(crate) mod incarnation;

pub(crate) mod erasure;

// Statements 7/11/13/15/17: indexed metadata persistence seam.
pub(crate) mod store;

// Statements 11/13/15/17: private bounded image evidence.
pub(crate) mod checkpoint;

// Statements 9/10/14: explicit outbound membership.
pub(crate) mod outbound;
