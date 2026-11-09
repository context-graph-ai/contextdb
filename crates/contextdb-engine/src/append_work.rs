//! What one durable write costs the content-identity proof, family by family.
//!
//! The instrument an append repair is graded on, and the only place outside the
//! proof module that reads it. Three observations are published together
//! because a claim supported by one of them alone is not a claim about the
//! append:
//!
//! * the per-family vector — coalesced mutations, underlying node reads and
//!   writes, and the bytes those moved — attributed to the family whose tree
//!   the walk was inside;
//! * the `Status` split by the namespace literal its logical key opens with,
//!   beside the copies `config_mutations` and `schema_mutations` produced, so a
//!   `Status` member that is an operational record can be told from one that
//!   duplicates a fact another family already holds;
//! * the length of the page lists the commit itself had to write, from the
//!   storage engine, and which family's walk copied each page;
//! * where the wall time of the write went — the write-set preparation before
//!   the transaction opens, the rows/index postings/logs, the proof
//!   maintenance, and the storage commit split into the five regions
//!   `commit_inner` runs.
//!
//! The spans are the only timing this module carries and they are diagnostic:
//! a deterministic work count grades a repair, a span says where to look. The
//! consumer benchmark reads them to attribute the part of an append that its
//! own statement timing cannot see.
//!
//! Every counter is per thread and covers work performed on the observing
//! thread. Compiled only with `test-seams`.

use crate::persistence::content_identity;

pub use crate::persistence::{AppendStageNanos, IndexPostingTouch, IndexPrefixCountTouch};
pub use content_identity::{DirectoryBindingWork, FamilyWork, IdentitySpans};
pub use redb::commit_work::{
    CommitPagesForTest, CommitSpansForTest, TAG_COMMIT_BOOKKEEPING, TAG_UNTAGGED,
    reset_commit_pages_for_test, take_commit_pages_for_test, take_commit_spans_for_test,
    take_freed_pages_by_tag,
};

/// Forget every observation on this thread: the per-family vector, the `Status`
/// namespace split, the fan-out counts, the untagged totals, the commit's page
/// lists and every stage span.
pub fn reset() {
    content_identity::reset_work_for_test();
    // Clears the storage engine's page tags and commit spans as well.
    content_identity::reset_family_work_for_test();
    crate::persistence::reset_index_prefix_counts_for_test();
    reset_commit_pages_for_test();
}

/// Where the wall time of the writes in this window went. `content_identity`
/// covers proof maintenance and `commit` covers the storage commit, so
/// `take_commit_spans` splits the latter further.
pub fn take_append_stages() -> AppendStageNanos {
    crate::persistence::take_append_stages_for_test()
}

/// The five regions of the storage commit, in the order it runs them.
pub fn take_commit_spans() -> CommitSpansForTest {
    take_commit_spans_for_test()
}

/// Every authenticated index-prefix count this window's writes maintained, in
/// the order they were maintained. Each one becomes one `Status` member in the
/// `rel-index-count-v2` namespace.
pub fn take_index_prefix_counts() -> Vec<IndexPrefixCountTouch> {
    crate::persistence::take_index_prefix_counts_for_test()
}

/// Every ordered index posting this window's writes maintained, by the index
/// that owns it. Each one becomes one `SchemaAndIndexes` member.
pub fn take_index_postings() -> Vec<IndexPostingTouch> {
    crate::persistence::take_index_postings_for_test()
}

/// Per-family work since the last reset, as `(family tag, family name, counts)`.
pub fn take_family_work() -> Vec<(u8, &'static str, FamilyWork)> {
    content_identity::take_family_work_for_test()
}

/// Every page the storage engine had to copy in this window, split by the
/// content-identity family whose walk caused it.
///
/// `by_family` plus `outside_every_family` is every copy the storage engine
/// made, which is what the commits of the same window listed as freed. A caller
/// that finds the two disagreeing is looking at an incomplete instrument, not
/// at a cheaper append.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageCopies {
    pub by_family: Vec<(u8, &'static str, u64)>,
    /// Copies made by the write itself outside every proof walk: the rows, the
    /// index postings and the logs.
    pub outside_every_family: u64,
    /// Copies the commit's own system-table bookkeeping made. They land in the
    /// NEXT commit's freed list, so they are reported beside the total rather
    /// than inside it.
    pub during_the_commit: u64,
}

impl PageCopies {
    pub fn inside_a_family(&self) -> u64 {
        self.by_family.iter().map(|(_, _, pages)| pages).sum()
    }

    /// What the commits of this window listed as freed.
    pub fn total(&self) -> u64 {
        self.inside_a_family() + self.outside_every_family
    }
}

/// Where the content-identity stage's own time went: reading and decoding
/// stored chunks, hashing the chunks the walk builds, encoding a finished chunk
/// and storing it, and the coalescing, grouping and sorting that turns a
/// commit's mutations into one edit list per family. The four partition the
/// stage.
pub fn take_identity_spans() -> IdentitySpans {
    content_identity::take_identity_spans_for_test()
}

/// Chunk rewrites and copied pages by the LEVEL of the chunk rewritten, as
/// `(level, rewrites, pages copied)`. Level 0 holds the leaves, which are the
/// great majority of a family's chunks; the levels above it are few and are
/// where one edit's path converges.
pub fn take_writes_by_level() -> Vec<(u8, u64, u64)> {
    content_identity::take_writes_by_level_for_test()
}

/// The page copies of this window. Read once: taking them resets the counts.
pub fn take_page_copies() -> PageCopies {
    let mut copies = PageCopies::default();
    for (tag, pages) in take_freed_pages_by_tag() {
        if tag == TAG_UNTAGGED {
            copies.outside_every_family += pages;
        } else if tag == TAG_COMMIT_BOOKKEEPING {
            copies.during_the_commit += pages;
        } else {
            copies
                .by_family
                .push((tag, content_identity::family_name_for_tag(tag), pages));
        }
    }
    copies
}

/// Coalesced `Status` members since the last reset, by the namespace literal
/// their key opens with. A member written by `config_mutations` or
/// `schema_mutations` carries a bare name rather than a namespace and is
/// reported under `<not-namespaced>`.
pub fn take_status_namespaces() -> Vec<(String, u64)> {
    content_identity::take_status_namespaces_for_test()
}

/// The untagged totals the proof module already counted: node reads, node
/// writes, node removals, bytes read, bytes written. The per-family vector must
/// add up to these.
pub fn take_untagged_totals() -> (u64, u64, u64, u64, u64) {
    let (reads, writes, removes) = content_identity::take_work_for_test();
    let (bytes_read, bytes_written) = content_identity::take_work_bytes_for_test();
    (reads, writes, removes, bytes_read, bytes_written)
}

/// How many copied records the most recent commit on this thread hashed to
/// bind the compact vector directory and the custody index to the committed
/// family roots. Reading it does not reset it; the next commit replaces it.
/// A commit that published no root leaves the previous value in place.
pub fn last_directory_binding_work() -> DirectoryBindingWork {
    content_identity::last_directory_binding_work_for_test()
}

type CoalescedMutationObserver = fn(&'static str, &[u8], bool);

thread_local! {
    static COALESCED_MUTATION_OBSERVER: std::cell::Cell<Option<CoalescedMutationObserver>> = const { std::cell::Cell::new(None) };
}

/// Observe the borrowed final members of successful content-identity applies.
/// The caller must discard observations if its enclosing commit fails. Nested
/// calls and unwinding restore the previous observer. The emitter allocates no
/// storage and retains neither keys nor callback results.
#[doc(hidden)]
pub fn with_coalesced_mutation_observer_for_test<R>(
    observer: fn(&'static str, &[u8], bool),
    action: impl FnOnce() -> R,
) -> R {
    struct Restore(Option<CoalescedMutationObserver>);
    impl Drop for Restore {
        fn drop(&mut self) {
            COALESCED_MUTATION_OBSERVER.with(|slot| slot.set(self.0));
        }
    }
    let _restore = Restore(COALESCED_MUTATION_OBSERVER.with(|slot| slot.replace(Some(observer))));
    action()
}

pub(crate) fn observe_coalesced_mutation(family: &'static str, key: &[u8], present: bool) {
    COALESCED_MUTATION_OBSERVER.with(|slot| {
        if let Some(observer) = slot.get() {
            observer(family, key, present);
        }
    });
}

pub(crate) fn coalesced_mutation_observer_is_active() -> bool {
    COALESCED_MUTATION_OBSERVER.with(|slot| slot.get().is_some())
}
