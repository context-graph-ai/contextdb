//! One database handle is one database handle, however many have come and gone
//! before it.
//!
//! Several thread-local registers inside the engine ask a question about a
//! HANDLE: is the rows-examined scope on this thread's stack this statement's,
//! is the cron or trigger callback running this handle's, is this handle's
//! operation already open, does this handle hold the write-control bypass. An
//! identity that can repeat makes those questions unanswerable, and a memory
//! address is exactly such an identity -- a dropped handle frees its address
//! for the next one, so two handles that never coexisted carry the same name.
//! The sharpest cost is the rows-examined key: a collision mixes one
//! database's examined rows into another's trace, and the figure an operator
//! reads to judge a query is then partly somebody else's.
//!
//! So a handle's identity is a number assigned when it is built and never
//! handed out again. Nothing here waits for an address collision to happen by
//! chance: it opens and drops handles in a long loop, reading each one's
//! identity before the next moves into the space it left. Every identity must
//! be different.

#![cfg(feature = "test-seams")]

use contextdb_engine::Database;
use std::collections::HashSet;

/// Long enough that an address freed by a dropped handle is handed out again
/// many times over. Holding a thousand handles alive at once would never reuse
/// an address, so each handle is dropped before the next is opened.
const HANDLES: usize = 1_000;

#[test]
fn a_thousand_handles_opened_and_dropped_in_turn_each_carry_a_different_identity() {
    let mut identities = Vec::with_capacity(HANDLES);

    for _ in 0..HANDLES {
        let database = Database::open_memory();
        identities.push(database.database_identity_for_test());
        // Dropped here, freeing its address for the handle opened next turn.
        drop(database);
    }

    let distinct: HashSet<u64> = identities.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        HANDLES,
        "each handle must carry an identity no other handle has ever carried; \
         {} of {HANDLES} were distinct, so at least two handles answered to the \
         same name",
        distinct.len(),
    );

    // Two handles alive at once, after a long procession of dropped ones: if a
    // name left behind by a dropped handle were handed to a live one, this is
    // where one handle's bookkeeping would be read as the other's.
    let first = Database::open_memory();
    let second = Database::open_memory();
    assert_ne!(
        first.database_identity_for_test(),
        second.database_identity_for_test(),
        "two handles alive at the same time must not answer to the same name",
    );
}
