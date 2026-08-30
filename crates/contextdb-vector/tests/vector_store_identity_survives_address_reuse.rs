//! One store is one store, however many have come and gone before it.
//!
//! The re-entrancy guards inside the vector store ask a question about a
//! STORE: "is this store's maintenance lock already held on this thread?" An
//! identity that can repeat makes that question unanswerable. A memory address
//! is exactly such an identity: a dropped store frees its address for the next
//! one, so two stores that never met can carry the same name, and the guard can
//! decide a lock is held when it is not -- skipping the very serialisation it
//! exists to provide, on a store that has never been touched.
//!
//! So a store's identity is a number assigned when it is built and never handed
//! out again. Nothing here waits for an address collision to happen by chance:
//! it builds and drops stores in a long loop, each one taking its maintenance
//! lock and giving it back, and reads each store's identity before the next one
//! moves in. Every identity must be different.

#![cfg(feature = "test-seams")]

use contextdb_core::{VectorIndexRef, VectorQuantization};
use contextdb_vector::VectorStore;
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

/// Long enough that an address freed by a dropped store is handed out again
/// many times over. Holding a thousand stores alive at once would never reuse
/// an address, so each store is dropped before the next is built.
const STORES: usize = 1_000;

fn index_named(ordinal: usize) -> VectorIndexRef {
    VectorIndexRef::new("docs", format!("embedding_{ordinal}"))
}

#[test]
fn a_thousand_stores_built_and_dropped_in_turn_each_carry_a_different_identity() {
    let mut identities = Vec::with_capacity(STORES);

    for ordinal in 0..STORES {
        let store = VectorStore::new(Arc::new(OnceLock::new()));
        // Registering takes the index's maintenance lock and pushes this
        // store onto the thread's held-locks stack, then gives both back --
        // the traffic the guards are about.
        store.register_index(index_named(ordinal), 3, VectorQuantization::F32);
        // A second index on the SAME store, so the stack holds more than one
        // entry for one store before it unwinds.
        store.register_index(index_named(ordinal + STORES), 3, VectorQuantization::F32);
        identities.push(store.store_identity_for_test());
        // Dropped here, freeing its address for the store built next turn.
        drop(store);
    }

    let distinct: HashSet<u64> = identities.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        STORES,
        "each store must carry an identity no other store has ever carried; \
         {} of {STORES} were distinct, so at least two stores answered to the \
         same name",
        distinct.len(),
    );

    // Two stores alive at once, after a long run of dropped ones, each taking
    // its own lock: if a name left behind by a dropped store were handed to a
    // live one, this is where a lock held by the first would be mistaken for
    // one held by the second.
    let first = VectorStore::new(Arc::new(OnceLock::new()));
    let second = VectorStore::new(Arc::new(OnceLock::new()));
    let shared = VectorIndexRef::new("docs", "embedding");
    first.register_index(shared.clone(), 3, VectorQuantization::F32);
    second.register_index(shared, 3, VectorQuantization::F32);
    assert_ne!(
        first.store_identity_for_test(),
        second.store_identity_for_test(),
        "two stores alive at the same time must not answer to the same name",
    );
}
