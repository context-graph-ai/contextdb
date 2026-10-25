# contextdb-redb maintenance

The `test-seams`-only `commit_work::observe_root_work_for_test` callback observes catalog
enumeration, directory lookups, named-root finalization, checksum extent visits and directory
publication boundaries. Its scoped table attribution and fallible callback let independent
tests inject a caught admission failure without setting the backend storage-failed flag.
Capture begin/end events in `transactions.rs` distinguish one named capture from later
mutations/restages. `tree_store/btree_base.rs` observes the exact hashed slice length, separately
from checksum extent visits. The callback and its calls in these files,
`tree_store/table_tree.rs` and `tree_store/btree.rs` are absent
from ordinary builds; they alter no production root publication or storage policy.
Header begin/end observations in `tree_store/page_store/page_manager.rs` distinguish
both logical header preparations from backend writes that buffering may coalesce, so
process-crash fixtures can cover each boundary without changing write or sync ordering.

Direct read-only opens can install retained cache-preparation admission under
`contextdb-internal`. The actual counter, arena, stripe, head-array and write-map
control layouts are admitted before allocation. Weakless arena/write-map owners
use concrete Rust layouts instead of estimating an `Arc` header; fallible boxed
array construction cleans partially initialized arrays. The preparation receipt
travels with native backings until physical free, including detached cache aliases.
Read-only handles can retain the caller's original fixed page reserve in the
existing occupancy owners. Ordinary writer preparation retains its temporary
handoff semantics. These hooks choose no memory limit or funding policy; the
engine supplies the original source invoice. Compilation and runtime verification
of this ownership extension remain outstanding.

The `test-seams` cache receipt now borrows the actual counter, write-control,
shared arena, entry/free-slot arrays, stripe array and head-array pointers and
Layouts. Its preparation identity is the live native receipt already retained
by the counter. A cached-header alias exposes the actual original counter for
physical-free tests. These observations admit no bytes and choose no policy.
The flushed-header test prices the live file's native write-control instead of
constructing an unrelated `Arc`; the existing allocator/free watches observe
that constructor and its final native control release. Runtime proof is owed.

This existing storage fork is based on the **redb 4.1.0 crates.io package**, checksum
`8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`, upstream revision
`6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`. The `MIT OR Apache-2.0` license files are unchanged.

The maintained distribution is published as **`contextdb-redb` 4.1.0**. Its library remains named
`redb`, so the retained upstream integration tests and ContextDB callers keep their compile-time
identity, while the engine's packaged Cargo metadata resolves `contextdb-redb` rather than upstream
`redb`. The package identity, description, repository, and library alias are ContextDB maintenance
metadata. They are intentionally outside the source-only upstream diff below.

The `contextdb-internal` feature gates the additional E5 registered-reader opener, exact external
read on read-only handles, nonblocking external-root refresh admission, and provider-aware creation entry points. The engine enables this hidden
integration surface; the default fork API does not expose these methods. It also gates reject-only
byte-table inspection, its lazy opener, and a root-pinned opaque-entry cursor; these preserve the transaction and committed root.
The internal byte-table `Table::insert_zeroed_reserve` forwards the existing length-only
zero source directly to the borrowed mutable destination, with the same size validation
and replacement semantics as `insert_reserve`, avoiding its value-sized temporary buffer.
Callers must abort their transaction if filling a reserved destination fails.
The internal nonallocating `scratch_backend_allocation_size() -> usize` reports
`size_of::<TemporaryScratch>()` for the concrete `Box` payload returned by the
same-directory scratch factory. The private backend remains a `Mutex<File>`;
the helper constructs no backend and reports no file-content or OS-resource bytes.

[upstream-4.1.0.patch](upstream-4.1.0.patch) remains the complete source-only delta against that
exact package. It contains the changed source files listed below; fork package metadata, integration-test
registration, and this note are separate from the source diff.
`crates/contextdb-core/tests/redb_fork_patch_matches_its_sources.rs::the_redb_fork_patch_produces_the_sources_it_names`
binds every recorded source hunk to the maintained file it produces.

The internal byte-key range sibling uses the existing native range navigation and
complete-node checksum kernel. It validates byte keys without requiring UTF-8;
the text-key sibling retains UTF-8 validation and the same exclusive bounds.
The engine owns the table names, membership encoding and admission policy.

The private cache reserve owner retains the engine's original fixed invoice in
both real occupancy controls, including across close and recycled handle
construction. Every cache page or dirty alias retains those controls. Last-release
guards free page and counter allocations before releasing that same parent;
allocator state and cache index/map backing are destroyed before the final cache
control owner. The internal `CacheReserveOwner` and its synchronous recycle scope
share actual old/new generation occupancy, so retired pages cannot disappear from
the replacement handle's capacity accounting. No second fixed reserve is invoiced. A numeric cache capacity without that
actual installed parent cannot discharge a genuine source-page receipt; such
pages keep their real source charge through physical free or a later funded
handoff, including on standalone/custom backends.

A distinct internal cache-preparation scope receives the actual index/arena,
stripe/head array, write-buffer Arc control, stripe-lock scratch, allocator-window,
slot-vector and allocator-index Layouts before replacement allocation. The engine
keeps that real aggregate invoice across rollback/unwind or until the original
fixed reserve takes custody. Preparation has its own TLS slot and never calls the
source-page admission callback. Declared startup retains its existing retained-charge rule; runtime handle
recycling uses ordinary native writer admission. Undeclared cache residue
retains process-ceiling admission. The scope
uses no allocation per quote and no global allocation/accounting registry.

The feature-internal engine source-credit callback now supplies a concrete
`OwnedReadPageCharge`, prepaid with the actual `Layout<SourcePageCharge>` from
its original accountant before allocation. Its single-owner drop function moves
the genuine receipt to the stack, deallocates that exact control, then drops the
receipt and refunds. Page/counter free guards retain that owner outside their
physical free too. Allocator null returns its actual Layout through the existing
prepaid native refusal marker; it allocates no error Box. The original opaque
`ReadPageCharge` alias and all legacy callback APIs remain compatible and
explicitly unqualified for their own Box-control funding. No legacy Box is
promoted to the concrete owner. These changes are source-only and require
independent allocator/unwind proof before physical-funding qualification.

Native scan values retain only the captured original admission. Each distinct
active thread entry owns a separate exact-layout node prepaid from that original
reader child through the existing native credit protocol. Scan-value construction
and cloning allocate no counter. The thread-local scan slot is only a scalar tail
pointer, with no retained vector capacity. Links and guard counts are thread-bound;
only immutable owned admission crosses threads. Nested `scan_reads` and
`with_scan_reads` helpers retain the same active node without allocation. The last
guard unlinks its exact node, including an interior node, and physically frees it
before settling its original invoice. Native construction refuses missing original
admission; node entry preserves actual admission and allocator-null causes. Public
legacy `new`/`default`/`enter` signatures remain infallible and explicitly unqualified
for native funding; unscoped legacy scans keep their live 16 KiB fallback allowance.

These are source mechanisms, UNCOMPILED / RUNTIME_UNRUN. Initial bootstrap before
the engine installs this preparation/fixed owner, direct-read Vecs, growing dirty
map storage and legacy non-retaining/unscoped backing remain unqualified. The
fork does not invent a payer for those producers or arbitrary backend errors.
Allocator observation and independent tests must still prove every exact quote,
physical-free/refund order, alias/recycle overlap and refusal/unwind path. Existing
private cache tests still require the independently owned real-handle adoption.

| Changed source | Reason |
|---|---|
| `src/native_storage_admission.rs`, `src/lib.rs` | The test-seams-only pending-name observer borrows the real combined allocation Layout and native invoice after name construction. It admits or retains nothing and lets the engine allocator witness bind the exact original invoice to physical key teardown. |
| `src/read_admission.rs` | Feature-internal captured native admission and prepaid thread-bound per-entry linked scope nodes, using the existing original native credit ownership protocol. Same-node nested helper guards allocate nothing; exact last-owner unlink/free precedes refund. Preserve the public legacy signatures and the unscoped fallback allowance; declared scans retain their working set under the original reader admission. Upstream proposal remains unsubmitted under the existing thread-scoped read-admission proposal. |
| `src/read_admission.rs`, `src/lib.rs`, `src/tree_store/page_store/page_allocation.rs`, `src/tree_store/bounded_bytes.rs` | Feature-internal concrete movable source-credit owner and a distinct internal legacy/owned carrier. Preserve authentic credit through actual control deallocation, source-page/counter teardown and cache handoff. Keep legacy Box wrappers and the public backend trait unchanged; exact actual Layout prepayment remains with the engine factory. Upstream proposal remains unsubmitted. |
| `src/tree_store/page_store/cache_owner_contract_rows.rs` (added), `src/tree_store/page_store/lru_cache.rs`, `src/tree_store/page_store/cached_file.rs`, `src/tree_store/page_store/base.rs`, `src/tree_store/btree_base.rs`, `src/tree_store/page_store/page_manager.rs`, `src/db.rs` | Independent rows extend the existing immutable-page lifetime test with exact cached-identity displacement, a no-invalidation pinned negative, multiple clones, source-paid windows/unwind, retained dirty and COW aliases, flush/cancellation, refusal and real backend error/unwind. The existing test allocator observes one actual backing pointer and its real counter immediately before and after System deallocation. A test-seams-only borrowed guard adapter finds its actual Cached identity and file coordinate, calls existing exact invalidation and requires that identity absent. Ordinary holds, eviction policy and all byte caps stay unchanged. Private Arc-specific tests adopt actual PageAllocation handles and Layout-derived prices/full capacity; there is no Arc compatibility shim. These source tests remain uncompiled and unrun; fixed writer-reserve close/recycle custody is not certified. |
| `src/lib.rs`, `src/db.rs`, `src/read_admission.rs`, `src/native_storage_admission.rs`, `src/tree_store/mod.rs`, `src/tree_store/page_store/{mod,page_allocation,page_manager,cached_file,lru_cache,allocator_pager,region}.rs` | Feature-internal original cache-reserve custody and shared real occupancy across handle recycling; exact preallocation quotes for index/window/control reconstruction, separate from source-page admission. Physical cache/allocator destruction precedes final parent refund. Private allocator-window settlement propagates existing typed storage errors rather than boxing an admission refusal as I/O. No cache capacity, pinned-victim rule, public backend API, default or storage format changes. Upstream proposal remains unsubmitted. |
| `src/tree_store/page_store/page_allocation.rs` (added), `src/tree_store/page_store/cached_file.rs`, `src/tree_store/page_store/mod.rs`, `src/read_admission.rs` | Concrete weakless page allocation coallocates reference control, checksum, original payer and bytes. Every read/write alias retains that one allocation. External final-release guards physically free backing before releasing the genuine opaque source receipt or cache occupancy; separately admitted weakless occupancy controls use the same ordering. Write COW prepays complete old/new overlap, dirty sources stay in their map across fallible preparation, and flush transfers occupancy in place. Pinned read victims remain ineligible. Actual Layout replaces the old independent Arc estimates; private tests use the real page handle and Layout-derived capacity while preserving numeric caps, full-capacity pressure and strict assertions. The original fixed reserve now has physical-owner custody; initial bootstrap, non-retaining/unscoped map storage and over-cap allocations remain unqualified pending the documented integration proof. |
| `src/native_storage_admission.rs`, `src/error.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/table_tree_base.rs`, `src/tree_store/mod.rs` | Internal native errors retain separate admission-source ownership, actual allocator-null site/Layout and original I/O. The existing owned admission can lend its producing synchronized failure through a nonallocating callback; earlier pending-directory payers are not confused with current TLS. Feature-internal Native/PreparedRead transport crosses legacy storage wrappers without a post-denial refusal Box. Prepared descriptor errors retain independently admitted requested-name bytes beside their original paid descriptor page, render stored names/width/alignment, and share the validator alignment constant. Default backend methods and storage format are unchanged. |
| `src/transactions.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/btree.rs`, `src/tree_store/table_tree_base.rs` | Hidden internal borrowed directory and numeric-key/byte-value successor reads retain the captured user root, authenticate table types, and use complete-node checksums and native admitted navigation. The engine owns source coverage and all table names; unknown coverage cannot certify erasure. |
| `src/transactions.rs`, `src/tree_store/mod.rs`, `src/tree_store/btree.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/table_tree_base.rs`, `src/tree_store/bounded_bytes.rs`, `src/tree_store/page_store/cached_file.rs`, `src/native_storage_admission.rs` | Mechanical strict-lint cleanup: name the existing native key/value guard tuple and root-hook function types, mark the builder return as must-use, place the unchanged test module last, use checked integer conversions for page sizes and stripe indices, and normalize documentation, inclusive ranges and statement punctuation. Storage layout and operation ordering are unchanged. |
| `src/lib.rs`, `src/tree_store/mod.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/random_state_override.rs` | Under `test-seams`, let an explicitly scoped, thread-local cloned `RandomState` control pending table-update map construction; the scope restores a nested predecessor on normal return or unwind. Without `test-seams`, `TableTreeMut` still constructs `RandomState::new()` directly. This is fixture-only hash control, with no production hash policy or ownership change. |
| `src/table.rs`, `src/tree_store/bounded_bytes.rs` | Carry the authenticated writable table loan into both materialized page guards. Read-only cursors own their retained transaction guard and keep static materialized results; writable cursors remain borrowed. No unsafe lifetime conversion or runtime mutation check is used. |
| `src/table.rs`, `src/tree_store/bounded_bytes.rs`, `src/tree_store/page_store/mod.rs`, `src/tree_store/page_store/xxh3.rs` | Add an internal authenticated byte cursor over the actual borrowed write table. The streaming checksum helper is visible within `tree_store`, so its byte cursor sibling can call it. Oversized committed singleton leaves verify the unchanged seed-zero checksum through source windows; current dirty pages retain same-write custody. Borrowed key/value windows expose no mutable alias. Navigation arrays admit their replacement before growth and retain their page-charge receipt until destruction. Complete selected-page materialization retains ordinary source admission. No key/record cap, table policy or default feature is added. |
| `src/transactions.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/btree.rs`, `src/tree_store/page_store/page_manager.rs`, `src/tree_store/page_store/cached_file.rs`, `src/native_storage_admission.rs`, `src/lib.rs` | Internal `prepare_commit_with_read` installs the caller-owned admission before verified sealed-root construction. Prepared page copies inspect dirty write-buffer bytes first, then clean cache, then the checked backend, with distinct prepaid backing and controls; existing dirty/cache ownership is unchanged. Scalar byte lookup borrows names and descriptor fields, funds navigation storage, and retains malformed descriptor pages in the private error. Root/pin/seal/close/physical-flush publication semantics remain in the shared consuming implementation. Errors remain scalar or retain their original producer value; this seam does not certify producer diagnostic funding or an engine consumer integration. Upstream proposal: owned admission for prepared reads and callback-generic failure transport (not submitted). |
| `src/tree_store/page_store/page_manager.rs`, `src/tree_store/page_store/region.rs`, `src/db.rs` | An externally registered read-only handle builds no allocator: `Allocators::empty()` replaces the region tracker and per-region buddy allocators that upstream builds from the header layout, because such a handle never allocates, frees or asks whether a page is allocated. A hidden `ReadOnlyDatabase::committed_snapshot_for_registration` reports the committed root identity and applies exactly the native read-only open's clean-state predicates (header recovery flag, header length against the file, two-phase commit, an allocator-state table current for the last commit) without decoding any region payload: the allocator-state `TransactionId` is located through the existing fixed-key windows, so no region bitmap or tracker value is read. While it runs, every page and window read on that handle is charged in base pages before it is made against `ADMISSION_PAGE_BUDGET` (110, header included), and an over-budget, out-of-extent or empty read is refused as corruption without reading. All are gated by `contextdb-internal`. A hidden `ReadOnlyDatabase::writer_crashed` tells a registered reader to take a crashed writer's root through `ReadOnlyDatabase::crashed_snapshot_for_registration` instead. `TransactionTracker::has_external_snapshot` also admits any root while the provider supplies the retain-everything entry (transaction zero, no root): that entry keeps every page, so a root registered during a provider scan that could not see it is protected all the same. Upstream proposal (not submitted): an allocator-free read-only snapshot open that reports the committed root and the native clean-state verdict, as part of the external-root retention proposal below. |
| `src/transactions.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/btree.rs` | Add a feature-gated, hidden byte-key range sibling over the same captured transaction and admitted navigation. Validate the byte-table header, share the existing range descent, and retain text-key UTF-8 checks through an explicit private validation flag. |
| `src/error.rs`, `src/read_admission.rs`, `src/lib.rs`, `src/transactions.rs` | Retain opaque caller admission refusals as a non-I/O storage variant, including relocation and typed hidden scoped/charging entry points. Existing I/O admission interfaces adapt actual I/O unchanged; the engine alone interprets the payload and owns memory policy. |
| `src/db/allocator_metadata_test_seams.rs` (added), `src/tree_store/page_store/bitmap.rs`, `src/tree_store/page_store/buddy_allocator.rs`, `src/tree_store/page_store/region.rs` | Page both region-tracker and buddy bitmap words through fixed scratch windows for file-backed writers. Region views and descriptor arrays are reconstructed from scalar geometry, with only one active region retained; the previous descriptor owner drops before its successor is constructed. Bitmap reads mask stale tail bits after shrink and visit only words in the current geometry. `BtreeBitmap::has_unset` answers through the maintained fullness summary (the same one-word-per-level descent `alloc` takes) instead of sweeping leaf words, so finding a region's highest free order after each allocation reads a bounded number of words whatever the region's page count. Each `BtreeBitmap` keeps its one-word root summary resident once read (a fixed 16 bytes per bitmap, forgotten on any mutation and never remembered from a poisoned read), so an allocation's per-order free checks do not load the root window of an order it did not change. `BuddyAllocator::alloc_lowest` reads each order's lowest free block to choose the lowest address and allocates only the block it returns, instead of allocating and freeing one block per higher order to compare positions. `RegionTracker::mark_full` stops at the first order already marked full (full marks are upward-closed), so an allocation's tracker update reads only the orders it changes. The existing `test-seams` accessor delegates to the production resident-byte count; opaque backends without scratch keep the resident allocator. A bitmap's hash covers its leaf level only: the summary levels above it may stay optimistic in a paged allocator, so `check_integrity` compares the pages allocators hold, not how they cache fullness. |
| `src/tree_store/page_store/allocator_pager.rs` (added) | Keep a window set of 4096-byte windows sized from the writer's page-cache share (one byte in `CACHE_SHARE_DIVISOR`=32 of it, never fewer than the 32 windows every pager holds), found through a fixed open-addressed identity index (linear probing, backward-shift deletion, at most half full) allocated once per admitted window count so evict/insert churn never allocates, plus fixed control state; the whole admitted set is allocated when the share is admitted (at open and at startup settlement), never by a later read or write, and its bytes are carved out of the page cache's share so the owner's single cache charge covers them. Lowering the share spills and releases the surplus. A spilled window reloads with one scratch read of its 4104-byte record into a stack buffer; eviction is a clock over the windows: a window used since the hand last passed is spared once, a clean unused window is reused before a dirty unused one, and a dirty window nobody uses is spilled rather than pinning its slot; spill dirty windows to non-durable arithmetic slots beside the database and lazily load saved or virtual bitmap words. Detaching a replaced saved tree preserves the virtual initializer for future regions. A scratch I/O fault is sticky and preserves its original cause. The allocator operation envelope is `W * P + H + C`, with W the admitted window count (32 under any share up to 4 MiB), P=4096 plus index bytes per window, H=4096 bytes of fixed hashing scratch, and C<32 KiB for window tags, descriptors, and control; it follows the declared budget and is independent of region count, and under 256 KiB at the 32-window floor. B-tree source pages and OS page cache are accounted separately. |
| `src/db.rs` | Expose exact fixed cache-index allocation bytes on writable and read-only handles for the enclosing owner charge, and the constant per-stripe lock-control bytes within them (`cache_stripe_control_bytes`, through `page_manager.rs` and `cached_file.rs`) so the engine can classify that fixed part as open residue.  Expose bounded compaction steps, live-reader observations, a retained statistics snapshot, existing-only `Builder::open_with_backend`, and caller-coordinated `Builder::open_read_only_with_backend`; admit authenticated external snapshot roots before open housekeeping and every writer allocation/reclaim boundary, with exact-root reads and retained-page observations; offer contextdb-internal external-root admission that refuses an occupied refresh mutex without waiting and a structured helper that distinguishes this busy mutex from provider I/O refusals; let a descriptor-backed read-only database begin checksum-verifying reads at its captured root or at an exact external root whose registration the caller retains; open registered external readers without consulting a live writer's allocator state or assuming a captured header matches the file length during writer growth; attach the external provider during creation as well as reopen; configure caller-owned deferred-free extent and entry budgets on the builder so open, normal and close durable commits inherit the same bound; drain eligible deferred frees through repeated bounded commits when an idle caller explicitly requests close-time convergence; prepare a committed root for a graceful read-only handoff while the writer handle remains alive (`Database::prepare_read_only_handoff`), refuse later writes with `DatabaseClosed`, and skip its later Drop housekeeping commit; the waiting-writer and instance-scoped actual table-write observers, a one-shot simulated I/O fault at an eligible deferred-free cleanup commit, and a read-only diagnostic counting entries and extents in both complete deferred-free trees, are test-only under `test-seams`; the refresh-lock callback is test-only under test/test-seams. |
| `src/lib.rs` | Export the read admission entry points (`with_read_memory_admission`, `with_read_page_charge`, `ReadPageCharge`), the compaction cursor/progress, statistics snapshot, exact read-transaction root identity, and authenticated external-snapshot retention types; export actual table-write observation types only under `test-seams`; export hidden bounded byte cursor/handle types only under `contextdb-internal`. |
| `src/transaction_tracker.rs` | Compile the private transaction-count lookup only with `test-seams`, matching its sole native-reader-count observation caller; normal transaction tracking is unchanged. |
| `src/transactions.rs`, `src/tree_store/table_tree.rs`, `src/transaction_tracker.rs`, `src/db.rs`, `src/lib.rs` | Hidden `contextdb-internal` consuming prepared publication: seal actual user-root checksums before closing allocation custody; inspect only scalar facts through an opaque view; register a fixed-size inactive exact-id/root pin independent of the write guard; close once only after preparation, then use the existing commit-only finalizer and physical publishing path. Activate the pin at the existing final-flush marker; cap durable reclamation at the current write id; preserve inactive-pin abort and exact active-pin external admission even if the live header remains stale after cleanup failure. No physical header/flush sequence or file format change. Source candidate requires independent fault, allocation-lifetime and runtime qualification; upstream proposal not submitted. |
| `src/transactions.rs` | Carry resumable compaction progress across finite page batches, commit relocation frees, and retain statistics snapshot ownership; keep deferred-free extents behind the oldest authenticated external transaction until that root releases, without a page-set lookup on each commit; reclaim each free tree from an initial fair share of the caller budget, assigning the odd unit to the oldest durable free-list key across restart and lending unused capacity to the other tree without exceeding either total cap, trimming only one copy-on-write page-list value at a time and retaining the remainder durably; offer an on-demand root-page diagnostic separate from admission; expose a contextdb-internal lazy byte-table opener under the same read transaction and root; expose the exact transaction/user-tree root identity, bounded per-table root fingerprints (`table_root_fingerprint` on read and write transactions), and a same-transaction callback after a relocation batch (`compact_step_with_hook`) so a caller can bind the changed fingerprint before commit; propagate checksum-verifying reads from retained user roots into their tables; attach the instance observer to user-table namespaces and successful table deletion under `test-seams`; report the commit's two page-list lengths and the five commit regions' durations under `test-seams`; on Unix under `test-seams`, let an explicitly armed reclaim transaction signal and block after deferred-free and system-tree mutations are staged, immediately before durable publication, so an integration test can terminate that exact transaction. |
| `src/transaction_tracker.rs` | Let maintenance yield when a foreground writer is waiting; authenticate the saved root page in bounded work and retain only root identities as a transaction floor; admit an exact root at or after the oldest retained native/external transaction floor, including a provisional client handoff hold; serialize provider refreshes without rebuilding page unions, with a contextdb-internal try-lock admission that refuses a busy refresh using a private typed I/O cause; offer full root-page inventories only to explicit diagnostics; own the instance-scoped test observer slot and the database-lifetime deferred-free budget; hold refresh coordination across a test callback only under test/test-seams; under `test-seams` only, read the existing native transaction ownership map count for one actual transaction id without allocation, storage access, or retaining a root. |
| `src/table.rs` | Observe successful actual serialized table insert/reserve/remove operations under `test-seams`; no expected artifact set or planner counters. Offer contextdb-internal byte lookup and prefix inspection that materialize possible matches through ordinary complete-page reads, and a read-only opaque-entry cursor bound to the same captured root. |
| `src/table_write_observer.rs` (added) | Provide instance-scoped RAII callback installation/restoration, including nested scopes and unwinding; retain actual key bytes and serialized value lengths. |
| `src/commit_work.rs` (added) | Under `test-seams` only: count allocated extents in the freed and allocated commit lists and the extents/entries actually reclaimed by bounded cleanup; the legacy `*_pages` counter names do not count physical pages. Tag every copy-on-write freed page with the caller-declared region it happened in, and time the five regions of `commit_inner`. |
| `src/tree_store/btree_mutator.rs` | Hold a partially emptied leaf as the page memory type readers receive (`PageMemory`); report each copy-on-write page an insert, update or delete makes unreachable under `test-seams`. Inspect committed oversized singleton headers before the existing insert-before split and no-merge deletion paths; also reuse insert-after when the old separator fits a physical branch page. Carry its admitted key window through branch construction and any separator promotion; preserve the old extent and its checksum without faulting its unrelated value. |
| `src/multimap_table.rs` | Make subtree roots available to the bounded page traversal; report each copy-on-write page a multimap value removal makes unreachable under `test-seams`. |
| `src/tree_store/btree.rs` | Enumerate a bounded batch of tree pages from a continuation position; authenticate the saved root page at external admission, then verify retained child pages against parent checksums as they are faulted; start a range from the root page the tree already holds instead of reading it again; allow the contextdb-internal byte opener to retain the root header lazily while ordinary get/first/last/range keep full-page semantics; report each copy-on-write page the mutator makes unreachable under `test-seams`. Reserve allocator-table values through a length-only zero source and return their borrowed B-tree destination, avoiding any value-sized temporary owner. |
| `src/tree_store/bounded_bytes.rs` (added), `src/tree_store/mod.rs` | Reuse immutable byte-node computations only after layout validation, retaining expected captured-root/parent checksum comparison on every visit, including memo hits. Contextdb-internal reject-only inspection of committed oversized singleton byte leaves, incremental key comparison, lazy exact lookup, prefix/value-prefix iteration, and a transaction/root-pinned opaque-entry cursor. The cursor uses admitted parent navigation to continue after a caller refuses complete materialization of an oversized singleton; navigation errors remain terminal, every complete node reached by bounded lookup, prefix/value-prefix iteration or cursor navigation is layout-validated before offsets reach the ordinary accessors, branch cycles are rejected on every bounded traversal path, and key comparisons can stream borrowed fragments without a whole-key allocation. Possible matches use complete source-page admission and the existing decoder; checksum-verifying retained roots always keep complete-page verification. Tests cover typed corruption without panic for malformed leaf and branch roots and circular branch references through ordinary bounded lookup, prefix iteration and cursor navigation, matching refusal, zero backend reads on refused windows, charge release, prefix exclusion, both singleton insertion orders with giant values, separator-window refusal, giant-separator fallback, neighboring deletion, singleton refusal in every sibling position, terminal navigation refusal, streamed key-window charge release, abort, reopen, integrity and pinned snapshots. |
| `src/tree_store/btree_iters.rs` | Carry root and parent checksums through retained range traversal so each newly faulted page is verified without scanning the whole tree; accept a root page the caller already holds and has verified (`new_with_root_page`), so both ends of a range descend from one copy — a root larger than the page cache, such as a single leaf holding one oversized value, was otherwise read and held twice. |
| `src/tree_store/table_tree.rs` | Advance a finite compaction cursor past a protected allocator source without reading its payload. Enumerate and relocate the selected page batch, expose consistent statistics, the read transaction's user-tree root and its read hint; authenticate just the root page for bounded external admission; pop a deferred-free list tail in place without a scratch collection; report actual user-directory inserts/removals, and each copy-on-write page the table tree makes unreachable, under `test-seams`. |
| `src/tree_store/btree_base.rs` | Let an access guard hold a read page as `PageMemory`, keeping any charge the page carries alive while the guard is; allow crate-private mutation of an existing copy-on-write value so partial free-list cleanup needs no temporary page vector. |
| `src/tree_store/table_tree_base.rs` | Forward bounded page traversal for the table root. |
| `src/tree_store/page_store/cached_file.rs` | The private native clean read prepays fresh byte/header owners before cache locks, copies exact-length original warm read-cache bytes under the stripe read lock, and otherwise uses the backend without consulting dirty write cells. Its paid native page never borrows or adopts a legacy cell and never probes the write buffer. Use one shared fixed read-entry arena across all cache stripes; an exhausted read index returns an uncached page without allocation. The write buffer retains its original growable map and releases its capacity at each flush. Rebuild indexes under startup exclusion, preserving held entries, and expose exact index allocation bytes separately from the page cap.  Memoize only successful variable-width byte-node checksum computations in one object-local mutex on each immutable CachedPage/ChargedPage, leaving Plain bytes uncached; count the complete concrete page Layout, including checksum and alignment, in the unchanged native cache share, preadmit those controls independently of the existing directory/window allowance, and preserve charges through eviction until the last holder drops. Admit page/cache-entry bytes before allocation and observe successful backend reads; hand readers a `PageMemory` that, for a page the read cache does not keep, carries the charge its admission returned until the last holder drops the page; count a cached page against the cache budget until the page itself is dropped (`CachedPage`), so a page evicted while a reader still holds it stays counted, eviction frees only pages no reader holds, and a miss is cached only when its count fits the budget; keep caller quota refusals recoverable while real backend I/O failures still poison the handle; read private allocator and contextdb-internal bounded windows with admission before allocation and backend I/O, copying cached or buffered sources without installing a partial cache page; on resize invalidate only cached pages that reach past the new end, so a truncating compaction step keeps the valid working set instead of dropping the whole read cache; under one-pass scan reads a miss may use free cache room only within the scan's 16 KiB growth allowance; past it a miss first evicts as many bytes as it needs and enters the cache at lowest priority only if that eviction made room, otherwise it is returned uncached (hits are still served), so an exhaustive scan grows the cache by at most the allowance and replaces its own pages before the warm working set. Size the fixed read arena with one extra slot for the sole sub-page entry, the 320-byte database header at offset zero. Construction and startup settlement share overflow-safe sizing and retain the held-entry floor. Debug assertions at every native cache admission enforce the header exception; the fixed arena's actual bytes remain charged by the existing index observation. Rust formatting is normalized without changing expressions or assertions. The byte-node checksum memo is compiled only for its `contextdb-internal` and test callers (its slot stays in every build so page layout and admitted charge do not vary), and page-size conversions are checked. |
| `src/tree_store/page_store/lru_cache.rs` | Insert a page at the front of the eviction order without a second chance (`insert_lowest_priority`), so a page a one-pass scan installs is the next one evicted unless it is read again, and a scan replaces its own pages rather than the warm working set. Keep pinned read entries linked during a bounded two-pass CLOCK victim search, so only reclaimable entries leave the map. Maintain per-stripe eviction order and collision chains with fixed u32 links in one shared entry arena; allocate arena slots, a free stack and stripe heads once, return slots on eviction and clear, and count their exact allocations outside the page cap. Retain held entries and their second chances when rebuilding indexes for a settled startup cap. The arena resize helpers (`arena_capacity`, `transfer_to`) are compiled only for their `contextdb-internal` caller. |
| `src/tree_store/page_store/mod.rs` | Re-export `PageMemory` to the tree code that holds read pages and register the allocator pager. |
| `src/tree_store/page_store/page_manager.rs` | Forward the exact fixed cache-index allocation to the database owner. Give a paged allocator its window allotment of the cache share at open and at startup cache settlement, settling the page cache on the remainder and returning one reserve that covers both.  Leave the allocator layout and recovery state unchanged when a caller refuses projected file growth before resize; allow caller-retained external readers to use file geometry while a live writer has marked the current header dirty, without recovery or allocator writes, and without asserting that a concurrently growing file still matches the captured header length; publish the clean committed header before a graceful read-only handoff without closing the writer backend; validate byte-window ranges against their allocation extent before storage access. Choose paged allocators when the backend supplies scratch, locate saved fixed-key allocator values through header/key windows and read only requested bitmap header/word windows (including the region tracker), preserving full retained-root checksum authentication; lazily read saved allocator words, preserve repair and growth ordering, and refuse publication or clean Drop after a sticky scratch fault. Before explicit integrity repair, align an aborted writer's free tail to the committed header geometry. |
| `src/tree_store/page_store/file_backend/optimized.rs`, `src/tree_store/page_store/file_backend/fallback.rs` | Supply same-directory scratch for normal path-based file opens on every supported platform. File-only opens recover the path from Linux/Android, macOS, and Windows handles; other targets treat pathless raw handles as opaque backends with optional backing `None`, preserving raw `File` opens. Known paths always request same-directory scratch and propagate scratch errors. The scratch directory entry is removed immediately after creation, so process death leaves no orphan. |
| `src/db.rs`, `src/lib.rs` | Add a defaulted optional scratch-backend hook, export the non-durable scratch trait and same-directory factory, and expose read-only resident-byte and region-count observations. Writable file opens use scratch when offered; opaque and in-memory backends remain fully resident. Unit tests retain exact debug allocation identities for relocation checks; normal paged writers omit that unbounded inventory. |
| `src/transactions.rs` | Keep a large committed allocator table reachable while ordinary commits use its lazy source, and materialize that source before quick repair replaces it. An ordinary commit can retire a small source within the fixed 32-window capacity (never the larger admitted set, so an ordinary commit never reads a large saved bitmap), then remove its stale system table and restore the upstream page footprint; a source measured too large keeps that verdict until the layout or the window set changes, so later commits do not rebuild a regional view to re-measure it. Finite compaction admits the encoded extent size before source-page acquisition or destination allocation, and protects the retained allocator table instead of materializing every region before admission: its table cursor advances without reading protected payload pages, while moving the system master tree preserves that child root. Quick repair and explicit whole-file compaction detach the source before replacing it. |
| `src/tree_store/btree.rs`, `src/tree_store/btree_base.rs`, `src/tree_store/btree_mutator.rs` | Locate fixed-key byte values without loading their payloads for lazy allocator windows; no public lookup semantics change, and verified roots still authenticate complete nodes. Reserve allocator values from a length-only zero source, initialize the destination directly, and preserve arbitrary byte-value insertion and the v3 serialized node layout. Replacement accounting is separate from retaining the discarded old value. |
| `src/db.rs`, `src/tree_store/page_store/page_manager.rs` | Every durable commit (`commit_inner`, `commit_inner_publishing`) hands all of its buffered pages to the file (`write_barrier`) before the first header write, so the header always leaves the write buffer last: a process that dies at any point leaves a header whose commit is complete in the file it names. Same writes, same single sync. The contextdb-internal `ReadOnlyDatabase::crashed_snapshot_for_registration` gives a registered reader the root of a store whose writer died, at the healthy capture's fixed `ADMISSION_PAGE_BUDGET`: it rereads the header (charged), runs repair's own `pick_primary_for_repair` on that copy, and verifies only the chosen slot's user and system root pages, falling back to the other slot on a checksum mismatch exactly as repair does and returning any other failure as it is -- no allocator state, no tree walk, no write. Its caller-supplied checkpoint runs before the header and before every root read, and its error stops the capture there, before that read, and is returned as it is, never as a checksum mismatch. The caller vouches that the writer died on the same boot, on that file, on a local filesystem; a machine crash can tear a commit below its roots, so a store left by one is refused and repaired by a writer (recorded limit), and media damage below verified roots surfaces as `Corrupted` when the reader reaches it. `Builder::open_read_only_allocator_free_with_backend` opens a read-only handle that builds and loads no allocator state and refuses a crashed store with `RepairAborted`, like every other read-only open. |
| `src/tree_store/page_store/layout.rs` | Reuse the rounding remainder, preserving arithmetic while satisfying current strict lints. |
| `src/tree_store/page_store/base.rs` | Release the debug page-reference and dirty-page tracking allocations when their last live page is dropped, retaining the existing reference-count assertions. Keep all debug-only mutable-page tracking statements inside one `cfg(debug_assertions)` block so release builds retain no debug-only references.  Hold a read page as `PageMemory` so a charge it carries lives exactly as long as the page; validate serialized external page identities before constructing allocation extents, including the encoded allocation order; distinguish checksum-verifying retained page faults from ordinary cache hints. |
| `src/read_admission.rs` (added) | Scope read admission and backend-read observation callbacks to the active thread using RAII guards; a charging admission (`with_read_page_charge`) returns a per-page charge that the page carries. One-pass scan reads (`scan_reads` guard, `with_scan_reads`) mark this thread's reads so, past a 16 KiB growth allowance (`ScanAllowance`, shared by nested scopes and re-entered across one statement's resumed steps), a cache miss replaces evictable cached bytes instead of growing the read cache. |
| `src/open_with_backend_region_override_tests.rs` (added) | Verify existing-storage backend open ignores a creation-only region-size override. |
| `src/table_registry.rs` (added), `src/transactions.rs`, `src/table.rs`, `src/lib.rs`, `Cargo.toml` | The write transaction's open-table registry stays std's hash table and randomly seeded `RandomState` (the `hashbrown` crate std wraps, with `allocator-api2`), and gains an allocator a caller can fund: a fallible reserve refused with no credit reports the exact `Layout` it would allocate without changing the registry, a reserve paid with exactly that credit performs it, an empty replacement array can be prepared ahead and installed later without allocating, and a monitor counts any array allocation made outside a funded reserve. Keys and a table handle's name are `Box<str>`. The hidden `contextdb-internal` methods are `open_table_registry_{len,bytes,cost_to_reserve,reserve,cost_to_hold,prepare,install,monitor}`; `tests/open_table_registry_allocations.rs` witnesses every registry allocation by address against a counting global allocator. The engine owns the funding policy. |
| `src/tree_store/table_tree.rs` | Restage an already staged table root in place instead of allocating a key the map drops again; expose `pending_table_update_contains` (internal) and its size (test seam). |
| `src/tree_store/page_store/page_manager.rs`, `src/db.rs` | A contextdb-internal `Database::set_growth_step_ratio` (with its getter `growth_step_ratio`) caps every later `grow` at that share of the current usable size, never below twice the allocation's own size (upstream's guarantee of an aligned block for the request); a ratio that is not positive keeps upstream's growth. The ratio is kept exactly, as its binary mantissa and shift packed into one word, and the byte step is `floor(usable x ratio)` computed in 128-bit integers, so the cap never exceeds the caller's declaration at any store size. Upstream doubles a single-region file and fills out a short trailing region, so the first write after a compaction took a 2.16 GB store to 4 GiB, larger than before it was compacted. The engine derives the ratio from the store's declared `STORAGE_COMPACTION STOP_RATIO` and reapplies it to a recycled handle; a declared disk limit still refuses growth past it through the quota retry below. Guard: `tests/growth_after_compaction_contract.rs::a_compacted_file_grows_by_the_declared_ratio_not_by_doubling`; engine: `storage_compaction_is_declarable_and_unmeasured_fragmentation_reads_unknown::the_first_write_after_compaction_grows_the_file_by_the_stop_ratio_not_double`. Upstream proposal (not submitted): a builder-level growth step bound. |
| `src/tree_store/page_store/page_manager.rs` | When storage refuses `grow`'s preferred size with `QuotaExceeded`, retry once at upstream's own minimum for the allocation (`usable + 2 × required`, which guarantees its aligned block), and refuse only if that is refused too; a quota refusal still leaves the layout and recovery state unchanged. |

The allocator metadata seam is compiled only under `test-seams`; it delegates to the production resident-byte count. A nested, thread-local `observe_allocator_region_loads_for_test` counter counts region views materialized during its scope. Writable startup uses lazy saved-word lookup, so that counter does not imply a whole-table load. The read-only `allocator_regions_for_test` accessor reads the current scalar region count under the state mutex; `Builder::set_region_size` remains a `test-seams` fixture constructor.
`Database::native_read_transaction_count_for_test` exposes that existing ownership count only under `test-seams`; reader lifetime tests release other holders of that transaction before checking zero, while the writer retains its successor root.
The production accessor reads the fixed retained allocator envelope after open or file growth. Serialization computes lengths arithmetically and writes directly into borrowed reserved B-tree values. A 4096-byte stack window bounds hashing scratch. Length-only zero reservations initialize only the allocated B-tree destination, with no value-sized source buffer or anonymous mapping. The leaf builder stores borrowed byte sources or zero lengths and writes either into its destination; ordinary insertions retain their previous-value behavior. Private reservations discard replaced values without making a value-sized copy on the uncommitted in-place path. Source and destination B-tree pages retain their existing data-page ownership.

Read-cache CLOCK eviction leaves externally held entries linked and selects only pages whose
cache reference is their sole owner. A bounded two-pass search clears second chances and terminates
when all entries are pinned. Skipping a pinned entry does not count an eviction or release its credit;
uniform-page misses therefore replace one reclaimable page without creating a later cache hole when
an iterator releases its path. The page representation, admission and final-holder charging stay unchanged.

The five upstream integration targets are copied **unchanged**, including redb's own backward
compatibility tests: `backward_compatibility`, `basic_tests`, `integration_tests`, `multimap_tests`,
and `multithreading_tests`. The upstream standalone `Cargo.lock` and test dependencies are retained.
Package verification copies the fork to a disposable input and verifies this lockfile's digest is
unchanged; cold dependency resolution never imports workspace patch entries into the fork lock.
The compatibility suite's `redb =2.6.0` dependency verifies storage-file interoperability; it does
not enable any older ContextDB sync protocol. Upstream unit tests and doctests remain enabled. Fork-local `open_with_backend_contract` tests
cover existing-file refusal without writes, successful reopen, and configured repair callbacks; the
private region-override unit test verifies parity with pathname-based `open`.

Run the complete storage suite from the product repository:

```bash
cargo test --manifest-path crates/contextdb-redb/Cargo.toml --locked
```

CI, nightly, release dry runs, and the contributor gate run this command in addition to the
workspace tests. CI and release dry runs also format and lint this excluded crate, then build an
unpacked `contextdb-engine` package against the unpacked `contextdb-redb` package; that final gate
rejects any resolution back to a workspace path. The fork remains excluded from the workspace, so a
workspace-only gate cannot cover it.

To reproduce the retained implementation patch, obtain and verify the exact package above, unpack
it outside the repository, and set `REDB_UPSTREAM` to its `redb-4.1.0` directory. From the product
repository, the following prints the same source-only unified diff:

```bash
python3 - "$REDB_UPSTREAM" <<'PY'
import difflib
import pathlib
import sys
upstream = pathlib.Path(sys.argv[1])
fork = pathlib.Path('crates/contextdb-redb')
paths = sorted({p.relative_to(upstream) for p in (upstream / 'src').rglob('*') if p.is_file()}
               | {p.relative_to(fork) for p in (fork / 'src').rglob('*') if p.is_file()})
for path in paths:
    before = (upstream / path).read_text().splitlines(True) if (upstream / path).exists() else []
    after = (fork / path).read_text().splitlines(True) if (fork / path).exists() else []
    sys.stdout.writelines(difflib.unified_diff(before, after,
        fromfile=f'a/{path}' if (upstream / path).exists() else '/dev/null',
        tofile=f'b/{path}' if (fork / path).exists() else '/dev/null'))
PY
```

An upgrade starts with the new upstream package identity and its unmodified test suite/lockfile.
Rebase the retained source diff, discard changes upstream has absorbed, regenerate the patch, then
update the `contextdb-redb` package version and engine dependency together. Run upstream tests and
the ContextDB compaction, read-admission, crash/restart, resource, formatter, linter, and packaged
engine proofs before changing the pin. Do not update the upstream version without updating these
records.

Propose fallible fixed-window allocator metadata backing upstream as a storage primitive. The
backend may provide a same-directory, disposable scratch handle; absent one, the prior resident
allocator remains available. Pager windows cover both buddy and region-tracker bitmaps, lazy reads
preserve the v3 on-disk format, and any scratch I/O fault prevents a clean publication. ContextDB's
writer-path forwarding stays outside that proposal. This proposal has not been submitted upstream.

Propose bounded resumable compaction upstream as a small change with page-budget, interruption,
reader-overlap, waiting-writer yielding, and recovery tests. The `write_wait_observer` API in this
fork is test support for that internal yield mechanism; it is not part of the upstream proposal.
Propose the retained statistics snapshot separately where it can serve ordinary redb consumers.
Propose a small upstream graceful read-only handoff primitive separately: `prepare_read_only_handoff`
finishes the existing quick-repair allocator-state commit and publishes the clean header while the
writer still owns the file, then suppresses only the redundant Drop housekeeping commit; its
fork-local unit test `prepared_read_only_handoff_keeps_reads_and_refuses_later_writes` in
`src/db.rs` proves reads continue and later writes are refused. An upstream equivalent would
remove this fork-local method;
ContextDB still owns the companion lock and data-descriptor downgrade policy.
Propose the scoped read-admission and backend-read observation hooks separately with nested-guard
and error-path tests. Keep ContextDB policy out of those proposals. None of these proposals has been
submitted upstream.
Propose the quota-refused growth retry upstream on its own: any backend that refuses a resize for
quota (a disk-limited file, a fixed-size device) otherwise refuses an allocation that would fit,
because the preferred growth at least doubles the file. The change is one retry inside `grow` and
the test `a_growth_refused_for_quota_retries_at_the_size_the_allocation_needs`. Not submitted
upstream.

The commit-work counters answer how much of a write's cost is the storage engine copying pages on
the way to a durable commit: the lengths of the two allocation-extent lists a commit writes, a per-page tag the
caller sets so a copy can be attributed to the part of its own workload that caused it, and the
durations of the five regions `commit_inner` runs. The commit's own system-table bookkeeping sets
its own tag, so the tagged copies of a window add up to exactly the freed list its commit carried. They are absent from normal builds, hold no
ContextDB policy, and are test support rather than an upstream proposal.

The table-write observer records logical serialized key/value traffic at real storage mutation
points, including the user-table directory. It does not claim physical page/fsync byte accounting.
Schema replay assertions choose their expected artifacts in engine tests; the storage observer
contains no catalog, sync, graph, vector, or relational artifact-name whitelist. It is absent from
normal builds and is test support, not a new storage policy or an upstream proposal.

Propose `Builder::open_with_backend` separately as the existing-storage counterpart of
`create_with_backend`: initialization is disabled, configured cache/page sizes and repair callback
are preserved, and a creation-only region-size override is ignored just as in `open`. It lets a
consumer retain physical file ownership while replacing allocator/cache handles. File locking and
backend lifetime remain the caller's responsibility; this API grants no lock-bypass capability.
This additive API proposal has not been submitted upstream.

Propose `Builder::open_read_only_with_backend` and the exact read-transaction root identity as a
separate additive capability for consumers that already own and coordinate a physical storage
handle. The backend API does not bypass locking by itself; its caller remains responsible for
stable read access. This proposal has not been submitted upstream.

Propose authenticated external-root retention separately as an additive storage primitive. The
caller supplies the complete trusted root set before open housekeeping and every writer boundary;
redb authenticates each saved root page, holds the oldest transaction as a deferred-free floor,
and releases that floor when the supplied set shrinks. The floor can temporarily defer unrelated
pages; it avoids a database-wide page inventory during cursor admission or ordinary commits.
Closing the final hold makes deferred extents eligible, but does not synchronously drain them.
The caller configures a per-durable-commit extent/entry budget on `Builder`; the database tracker
owns it through open/close housekeeping and ordinary transactions, with an optional transaction
setter. ContextDB passes a 64-allocated-extent cap and a 64-free-list-entry cap. The two
free trees initially share those caps equally; the extra unit goes to the older first pending durable
key (data wins ties), so restart and aborted transaction identifiers cannot reset fairness. Unused
capacity is lent to the other tree, with the combined work still under both caps. Each visited entry contributes one unit, including
empty entries; each removed extent contributes one extent unit. An extent can span multiple physical
pages, so this count does not bound physical pages or bytes reclaimed in a commit. The separate
append-time and memory acceptance gates remain necessary. No backlog-sized collection is
allocated. The storage cache owns the current leaf and its copy-on-write replacement; uncached
source pages use the existing scoped page admission/charge hook. There is no cleanup scratch
allocation outside those existing page owners. A partial list retains its prefix under the same
durable key, atomically with the allocator changes; ordinary pre-commit abort preserves the
backlog, and later commits or reopen resume from that prefix. Reclamation itself runs only
inside commit, after `commit()` has disabled the ordinary abort path; invoking that private
stage and then aborting is not a supported transaction operation. Idle storage retains pending work until another durable commit. ContextDB requests an idle close drain, which advances the durable continuation through repeated bounded commits when no reader or savepoint pins older pages.
Default upstream callers retain effectively unlimited reclamation. Explicit whole-file
`Database::compact` overrides the bound only for its own transactions, preserving its full-drain
semantics while later ordinary commits inherit the configured limit. The upstream nondurable
helper is unchanged; ContextDB's ordinary persistence commits use the durable path.
`transactions.rs::bounded_reclamation_tests` proves the cap, lending of an idle tree's share,
abort/reopen continuation, both-tree progress through repeated reopen with a one-unit budget,
explicit compaction with a finite
builder budget, and eventual emptiness of both entire free trees (including later housekeeping
entries). `idle_writer_drains_eligible_deferred_pages_with_bounded_commits` proves the explicit
idle drain reaches that state after a reader releases its hold. `cursor_survives_writer_handover.rs::closing_a_cursor_keeps_the_next_small_append_cleanup_bounded`
measures reclaimed extents/entries and post-commit bookkeeping copy-on-write counts after
unrelated churn behind an owner cursor. Its thread-scoped allocator oracle measures the peak
requested bytes of new live heap allocations during the small append, including transient
vectors and copy-on-write buffers; pre-existing allocations and other threads are excluded.
The engine proof also observes the entry and extent counts of both complete durable free trees
through the production-dead `Database::deferred_free_backlog_for_test` diagnostic. It takes the
writer guard, walks read-only system tables, and aborts without draining or publishing changes.
The proof closes and reopens with pending work, then drives ordinary empty durable storage
commits through an engine test wrapper until both trees are empty, checking the existing extent
and entry caps on every drive and preserving the appended row across restart.
Allocation headers retain their own sample counters through deallocation, so overlapping
lifetimes cannot contaminate a later sample. The separate source-page charge peak/total counts
read-cache-miss admissions only; cached and writer-buffer pages are outside that metric.
The caller owns process authentication and lifecycle. The primitive does not authenticate
registrations, define a disk budget, or activate a lazy reader route. This proposal has not been
submitted upstream.

`Database::begin_external_read_nonblocking` is hidden behind `contextdb-internal`. It uses the
same root authentication and retention admission as `begin_external_read`, but returns an I/O
`WouldBlock` refusal carrying a private typed cause if another provider refresh owns the serialization
mutex. The hidden `Database::is_external_snapshot_refresh_busy` helper recognizes only that cause,
so callers can retry busy refresh coordination without retrying indeterminate provider inventories.
Ordinary reads and external-root admission retain their existing behavior. The caller supplies nonblocking provider
coordination when it requires bounded admission; the primitive does not alter provider policy.
`src/db.rs::test::nonblocking_external_read_refuses_before_refresh_release` holds that mutex through
the refusal assertion, proves the provider is not called, then proves admission succeeds after
release. The engine's `src/persistence/owner_cursor_admission_tests.rs` proves that busy refresh
and persistence ownership guards produce typed cursor refusals before their locks release.

Propose retained read-only transactions as a separate additive capability layered on the exact
user-root identity. A retained transaction first proves that its descriptor-backed database still
names the requested captured root, then verifies only the root-to-record paths it faults. It reads
no historical system-tree pages and performs no whole-tree checksum pass. The caller remains
responsible for retaining every user page reachable from that root and authenticating that hold.

Propose per-table root fingerprints (`table_root_fingerprint`) and the relocation-batch callback
(`compact_step_with_hook`, which `compact_step` now calls with an empty callback) as additive
storage primitives. The fingerprint copies Redb's existing finalized B-tree root header from table
metadata without faulting payload pages. The callback runs after relocation and before its
commit so a caller can bind a changed physical root in its own table atomically. The caller
defines what that binding means; Redb does not know ContextDB's vector directory.

The hidden byte-table primitive rejects unrelated oversized singleton leaves through small,
admitted windows. It uses the unchanged key/value layout and transaction root; windows never
enter the full-page cache and own their admission charge until release. Key comparisons stream
through bounded windows. Value-prefix filtering only excludes definite mismatches: possible
matches fault the complete extent and use ordinary decoding. Retained roots requiring checksum
verification take the ordinary complete-page path even for rejection. The mutator moves
its existing insert-before and singleton no-merge checks ahead of complete page acquisition;
insert-after also moves ahead when the existing separator fits a physical branch page. That
separator remains a charged window through branch allocation and any promotion by a parent
split. Equal keys, large separators and uncommitted pages use ordinary complete-page reads.
Allocation, publication, checksums, deferred frees and abort follow the existing paths. This
contextdb-internal mechanism carries no route, catalog, memory-limit or admission policy.

The read-only `cursor_bounded` interface separates navigation from selected record
materialization. Its opaque handles retain the captured transaction guard and borrow
the table. A committed oversized singleton can be identified through validated,
admitted header windows, after which only an already admitted parent supplies the
next sibling. The cursor never skips unreadable or malformed navigation. A caller
can refuse a handle's complete page without poisoning that saved navigation state;
redb does not classify the refusal or decide whether the caller should continue.
`key_matches` and `key_matches_parts` are reject-only comparisons, never descriptor,
mutation, or checksum authority. They use at most 512-byte charged windows and do
not assemble a whole canonical key. `materialize` uses ordinary complete-page
admission and decoding. Checksum-verifying retained roots continue to require
complete-page admission and checksum verification during navigation. Ordinary
open, lookup, and iterator behavior is unchanged.

The hidden `Database::settle_startup_cache_budget` entry point lets the engine settle the native
writer cache share after reading the persisted memory limit and before publishing its first reader
root. It flushes the write buffer before lowering capacity, evicts cache references through the
existing eviction path, and returns a reserve covering both capacity and any pages still held by
readers. Those pages retain their bytes and live-count owner until their final holder drops. The
caller excludes concurrent operations during this startup step; ordinary limit transitions keep
existing behavior. This additive internal proposal has not been submitted upstream.

Idle-writer shutdown drains only deferred-free entries older than the ordinary
commit's reader fence. It probes the oldest key in each free tree instead of
refusing all cleanup whenever a reader exists. Protected entries and savepoints
remain deferred; each cleanup commit keeps the builder's page and entry caps.

Storage relocation exposes a bounded scalar cursor checkpoint and a successor-cursor hook.
The engine records that successor in the same native transaction as relocation and restores
it before publishing writer roots. Failed relocation or checkpoint encoding publishes neither
the cursor nor moved pages; reader fences and per-batch page limits are unchanged.

The hidden contextdb-internal `WriteTransaction::table_exists` performs a typed table-directory
probe without flushing pending root updates or finalizing dirty payload checksums. It distinguishes
an existing empty table from an absent family. The engine caches this existence result per table
while maintaining row heads; ordinary `open_table` type validation and exact row-body checks remain.
The finalized root fingerprint API is unchanged and still supplies durable root bindings.

The one-pass scan seam carries an operation-owned cache-growth allowance across
resumed reads and nested scopes; once that allowance is exhausted, misses replace
evictable pages at lowest priority rather than filling unused cache capacity. This
keeps exhaustive reads from displacing the useful working set or allocating a
second scan cache. Propose the storage-neutral RAII scan scope, shared allowance
and lowest-priority insertion upstream against redb 4.1.0; an upstream equivalent
would let `ScanAllowance`, `scan_reads`, `with_scan_reads` and their cache hooks
shrink out of the fork. This proposal has not been submitted upstream.

Saved region-tracker payloads remain source-backed on writable reopen: only
order/height geometry is resident, while tracker words and regional bitmap words
share `AllocatorPager`'s budget-sized window set. A tracker lookup reads one saved layer
window; geometry masks padding past the actual region count. Successful allocation
marks the used region's now-full orders immediately, retaining fast startup
selection without a decoded tracker directory. Before detaching a saved source,
tracker words also spill through those windows; the small-source bound includes
the tracker encoding. The resident observation keeps its fixed control allowance
and reports any larger actual descriptor owner rather than hiding it. These
changes extend the existing upstream proposal for source-backed fixed-window
allocator metadata against redb 4.1.0.

Tracker marking skips already-equal leaf states. Source-only cache misses reuse
clean windows before spilling dirty words, so serializing saved geometry does
not force dirty allocator words out while a clean window is available. These
rules stay inside the same fixed window owner and add no resident directory.

The engine can install a process-local snapshot provider beside authenticated
external registrations. New local roots pass the same native-pin and checksum
admission as external roots. This lets engine descriptors retain exact user roots
without a native read transaction, and lets a replacement handle register those
roots before allocator housekeeping. The provider owns lifetimes; redb owns page
retention. Active storage views remain fenced by the engine during replacement.
Retention statistics (`external_snapshot_retention_stats`) count only authenticated external
registrations; process-local roots are pinned but excluded from that diagnostic.

Allocator-window upstream proposal: add a private fixed-key value-location/read-window primitive usable by saved allocator bitmaps, retaining ordinary full-checksum behavior on verified roots. The saved allocator directory can then stay lazy both in resident bytes and in startup backend reads without changing its format.

Cache-control ownership: removing an LRU key also removes its queue position, so
reinserting that key cannot turn a stale position into another live second
chance. Removed, evicted and cleared entries release unused map and queue
capacity; controls follow the current cached working set rather than prior
commit peaks.
This corrects unbounded queue history with a fixed live page set while preserving
page ownership and the second-chance eviction rule. Upstream proposal: make key
removal retire its eviction position and make control capacity follow retained
entries, so an admitted cache bounds directory allocations as well as payloads.

Immutable byte-node checksum reuse stores computed content, never authentication.
`bounded_bytes::verify` keeps the existing root/parent expected-checksum comparison
on every verified traversal. Complete-node layout validation precedes hashing and
offset access. Only the variable-width `(None, None)` checksum interpretation is
memoized. Every newly constructed immutable page owner starts empty; Plain
write-buffer/unowned bytes compute normally. The slot mutex holds only immutable
hash computation, with no admission, backend, cache, or page-manager callback.
External disk mutation retains the existing cache visibility boundary: a resident
immutable object represents its previously loaded bytes; a fresh fault sees the
changed file. Tests modify a task-created file on disk, prove fresh typed checksum
refusal, and prove a memoized resident page refuses the changed expected checksum
that a corrupt/new root or parent would supply.

Immutable cache owners now include their actual inline controls in the existing
native capacity: payload plus the byte Arc's two counters plus the aligned Arc
header and `size_of::<CachedPage>()`. Reader admission separately covers the larger
of the complete CachedPage/ChargedPage Arc owners, the byte Arc header, and the
pre-existing 128-byte directory/window allowance; that allowance does not fund the
memo. Scan-growth, admission, eviction, startup reserve and cache metrics use the
same cached total; final-owner Drop releases that total. ChargedPage retains its
reader admission through its final Arc holder. Shared pre-existing counters,
caller-defined charge implementations and cache-directory geometry keep their
existing owners. No new share, budget, default, format or public table API is added.
Upstream proposal: retain byte-layout-specific computed checksums on immutable
page owners, compare expectations on every traversal, and charge inline controls
within the existing cache limit. This proposal has not been submitted upstream.

### Retained guard accounting quote

Fork-only `#[doc(hidden)] AccessGuard::retained_page_bytes()` reports the entire
backing page length, or the owned value Vec capacity. ContextDB's invocation-local
verified writer chunk cache uses it to admit and retain a writer lease for each
cached guard until eviction or invocation end. A value slice length omits other
bytes in the pinned page and cannot serve this quote. The accessor adds no IO,
allocation, mutation, or validation bypass; guard/value semantics stay unchanged.
Changed source: `src/tree_store/btree_base.rs` (maintained upstream delta).

The internal normal-table lifecycle hook is optional and scoped to one write transaction.
Before open/delete/rename/relocation it permits caller policy to reject the old root;
after a handle closes or bounded relocation finishes, the caller sees the finalized
root header. A close-hook failure is retained and refuses commit before the transaction
is marked completed, so abort preserves the old committed state. Normal-table root
lookup distinguishes absent from empty without payload or catalog enumeration. A rename
validates the old and destination names before mutation, then reports removal/creation.
The engine owns historical witness classification, representation and publication.
Compaction reports only its already selected table, with no changed-table collection.
`TableRootEvent` and the hidden root/hook methods are gated by `contextdb-internal`.
The function pointer and first-error mutex are a fixed `TableRootHookControl` budget
term per write transaction; roots and witness records have fixed stack representations.
No hook inventory survives across transactions or grows with retained history.

`Database::begin_verified_read` (internal feature only)
captures the ordinary native transaction guard with `PageHint::Verified`.
The guard and retained-root lifetime are unchanged; every consumed page verifies
its parent/root checksum, without scanning unvisited pages or retaining another
reader object. `transactions.rs` shares construction through `new_with_hint`;
ordinary upstream reads continue using `Clean`.


## Internal exact byte-read companion (source qualification pending)

`src/native_storage_admission.rs` adds a paid owned receipt interface, fixed
`Option` TLS scope with unwind restoration, exact fresh buffers and an all-owner
shared page header without Weak. Its lower implementation depends only on std.
`ReadTransaction::get_native_byte_value` traverses the captured directory,
validates borrowed byte-table types/widths/alignments, and performs an iterative
exact lookup with admitted ancestor storage. It validates every node offset,
ordered key and child encoding, checks cycles and authenticates complete pages.
`page_manager.rs` checks extent geometry and reads native immutable PageMemory;
this path does not construct PageImpl or insert a debug reference-map entry.
`cached_file.rs` admits fresh byte backing and its shared header on the original
native scope before any cache lock. A clean committed read copies an exact-length
read-cache hit under its stripe read lock; a miss uses the unchanged backend read.
It never probes the write buffer, clones a cache owner, inserts a native page or
falls through after refusal. Native pages stay operation-funded and do not enter
the legacy fixed arena; Entry/index layout is unchanged. Copied pages retain the
same caller snapshot, page identity and complete checksum verification; physical
buffer/header destruction still precedes release of their original credits.

This is an additive non-default internal seam, not a qualified production read
route. Writer/read kernel failure construction, native cache adoption with a
retained fixed reserve, engine error adaptation, typed row decoding, snapshot
factory/publication and allocator/runtime proofs remain open. Generic APIs,
legacy admission function bodies, file format and all upstream tests are
unchanged. No compiler or runtime result is asserted by this source record.

The hidden `contextdb-internal` `ReadTransaction::get_native_text_value` sibling
reads the existing text-key/byte-value descriptor from the same committed
user-directory root. It shares the byte descriptor validator's width, alignment,
root and type checks, selecting exactly the existing `&str` key identity; UTF-8
text keys use the same lexicographic native descent and complete-page checksum
validation. No default table API or byte-lookup signature/body changes. Private
native buffer/control null allocations return an inline out-of-memory I/O fault;
the engine keeps these unqualified, while original admission refusals remain
scalar. The exact fork delta is recorded in `upstream-4.1.0.patch`.


## Internal committed text-prefix range companion (source qualification pending)

The hidden, non-default `ReadTransaction::next_native_text_range` sibling seeks
one variable text key at or above a raw lower endpoint (exclusive when requested)
and strictly below an optional raw upper endpoint. It uses THIS retained transaction's
user-directory root and the existing true `&str`/`&[u8]` descriptor validator; raw
endpoints permit a UTF-8 prefix successor that is not itself valid UTF-8.
`src/transactions.rs` and `src/tree_store/table_tree.rs` forward the operation;
`src/tree_store/btree.rs` adds a monotone boundary descent and the next sibling's
left descent, sharing the existing node validator and complete-page checksums.
Navigation identities and sibling headers use admitted `NativeStorageBuffer`
backing; `src/native_storage_admission.rs` adds only its nonallocating pop.
Both returned byte guards share the actual prepaid complete immutable leaf page.
A checkpoint callback runs before the directory lookup and each range-page read.
Admission is installed by the original owner; checkpoint/backend/bootstrap/cache
faults stay unqualified. No ordinary table iterator, owned metadata decoder,
fixed relational-key reinterpretation or source-record serialization is used.
The engine owns prefix/resume policy and advances only after callback/checkpoint
success; default APIs, formats and the previous exact lookup bodies are unchanged.
Compiler, runtime, races and outside-family review remain unrun. The maintained
patch reproduces every current src byte, including this companion.


## Maintained allocator free-page totals (S9)

`src/tree_store/page_store/region.rs` keeps one fixed `FreeTotals` aggregate in `Allocators`:
free pages, region pages, and a trust flag, 24 bytes inside `size_of::<Allocators>()`. The
resident envelope stays independent of the region count.

**How the totals move.** `src/tree_store/page_store/page_manager.rs` applies a net delta only
after an operation succeeds:
- a successful allocation subtracts 2^order;
- a free or a rollback free adds 2^order;
- `Allocators::resize_to` moves both totals by the change in region pages: growth adds free pages,
  and a trim removes trailing free pages only.

Buddy split and coalesce never move them. Repair, a poisoned pager, a failed or out-of-range
delta, or a missing or invalid saved value makes the totals untrusted. Untrusted totals are never
published (`Database::allocator_free_totals` returns `None`) or saved.

**Persistence.**
- A new `AllocatorStateKey::FreeTotals` (tag 6, after `TransactionId`) holds 24 bytes:
  `[u32 format=1][u32 regions][u64 region pages][u64 free pages]`.
- It is reserved in `reserve_allocator_state` and written in `try_save_allocator_state` from the
  same snapshot as the region bitmaps. Untrusted totals leave the reserved zeros (format 0), which
  every load rejects. That save is not retried.
- The saved value precedes the commit's `try_shrink`. A load therefore maps it onto the committed
  geometry: the saved regions must equal the committed regions or exceed them by one, the
  committed pages must not exceed the saved pages, and the trimmed pages must fit within the
  saved free pages. The adopted totals are `free - trimmed` over the committed pages.
- Anything else, and every full repair (`end_repair`), runs a checked recount of every region's
  bitmap. The recount publishes trust only if it completed with a healthy pager; otherwise it
  returns the storage error.

**Geometry by arithmetic.** Region pages come from layout arithmetic: full regions × full-region
pages, plus the trailing region. The old page count in `resize_to` is (regions − 1) × full pages
plus the last region's length. No open, save, trim or resize visits a region to compute them. A
test-seams counter of geometry region visits proves this.

**Lowest-first placement.** `TransactionalMemory::allocate` takes the lowest free pages
(`allocate_helper(.., lowest = true, ..)`), as relocation already did. First-fit-by-order could
put a small block at the very end of a grown file, because growth from an odd length leaves
buddy fragments at both ends of the new space. One page there pins the tail: no trim, and the
free space before it reads as reclaimable, so a compacted store restarted automatic compaction on
every reopen. The quick-repair close's allocator-state save is an ordinary allocation, so it now
lands low too and the close's trim returns the end of the file.

**Accessors.** Production gets one hidden accessor, `Database::allocator_free_totals`. It
returns `(free, region pages, trailing free pages)`. The trailing run comes from the last
region's `trailing_free_pages()`, the same bounded walk over buddy blocks that `try_shrink` does
on every shrinking commit.

The test-seams accessors are:
- an independent recount;
- a distrust seam;
- a thread-local rebuild counter;
- a thread-local hook that runs as a rebuild begins, so a test can inject a storage fault inside
  the count;
- a one-shot save mode, `FreeTotalsSaveForTest`: omit the key, write chosen bytes, or fail before
  or after the encode.

**Tests.** The proofs are in `tests/allocator_residency_contract.rs`:
- `allocator_free_totals_track_the_bitmaps_and_survive_trimming_reopen`
- `allocator_free_totals_survive_an_intra_region_trim_at_close`
- `allocator_free_totals_survive_region_removing_trims_at_close_and_in_ordinary_commits`
- `allocator_free_totals_reject_absent_or_malformed_saved_values`
- `allocator_free_totals_after_an_interrupted_save_are_rebuilt_by_repair`
- `allocator_free_total_rebuild_fault_during_repair_refuses_the_open`
- `allocator_free_total_geometry_costs_the_same_at_one_and_many_regions`
- `allocator_untrusted_free_totals_are_not_saved_and_rebuild_once_on_reopen`
- `allocator_free_totals_after_poison_are_withheld_and_rebuilt_by_repair`
- `allocator_free_total_rebuild_fault_refuses_the_open_and_publishes_nothing`

The patch sections for the changed files were regenerated in place; the fork's other
sections are unchanged. Compiler and runtime results are recorded in the S9 handover, not here.

## Quota-refused growth retries at the allocation's size

`grow` asks storage for at least twice the file (`max(2 × usable, usable + 2 × required)`), or a
whole region once regions are full. A backend that enforces a size limit refuses that with
`QuotaExceeded` even when the allocation itself would fit: ContextDB's writer file refuses any
resize past `DISK_LIMIT`, so a store more than 100 KiB under its limit refused a 16 KiB insert.
`grow` now retries once at `usable + 2 × required`, upstream's own lower bound, which keeps an
aligned block of the requested order available after growth. If that retry is refused too, the
error returns as before: the layout and the recovery state are unchanged, and the writer can
continue once capacity is released. Without a quota refusal, growth is exactly upstream's.

Test: `a_growth_refused_for_quota_retries_at_the_size_the_allocation_needs` in
`tests/allocator_residency_contract.rs` caps a store's growth and checks that later writes use the
headroom instead of stopping at the first preferred growth. The engine's
`disk_04_insert_rejected_when_over_disk_budget` covers the same promise through `DISK_LIMIT`.

## Normal-table entry count without opening (S8)

`WriteTransaction::normal_table_len` (contextdb-internal, doc-hidden) returns a normal table's
entry count from its directory header, or `None` when the table is absent. It is the same
`length` that `Table::len()` reads, but it does not open the table. The engine uses it to ask
whether a declared relation exists before creating it at declaration time, and to compare a
relation's current entry count with one commit's inserts for the retention-clock first-insert
check. Opening a relation to ask either question would fire the contextdb historical-root hook,
which re-witnesses that table. The proof that a one-table commit touches no other relation is the
engine test `a_one_table_commit_leaves_every_other_tables_relation_untouched`.

Only the `src/transactions.rs` section of the patch was regenerated.

## Later external snapshots under an earlier retained read

An admitted read at transaction F already keeps every page retired at or after F
allocated. Provider refresh therefore admits authenticated snapshots whose transaction
is at or after the existing native/external floor, even if the new snapshot's own
native capture has ended. Roots below every existing floor, or introduced without any
retained floor after writer admission, remain refused. Root authentication and the
committed-transaction upper bound still apply before publication. Computing the floor
uses the existing identity set and native read map, without a page inventory or new
retained allocation. A stack guard increments the oldest native floor's existing
map reference until authentication and external-root installation finish. Thus a
concurrent final native release cannot open a reclamation gap. The guard releases
on success, refusal and unwind, with no new map entry or persistent state.

The engine's `owner_cursor_provider_contention_retains_and_admits` includes separate
rows with only a native floor and only an external floor. Both publish a later real
snapshot after an empty commit and drop its native capture before another owner's
admission. The fork's `external_root_validation_and_prewriter_registration_order_fail_closed`
keeps the forged-root and unprotected historical-root controls and releases the
last native reader during an actual uncached authentication read, asserting that
its transaction floor survives until external installation and then releases. Upstream proposal
(not submitted): validate later external snapshot admission against the oldest already
retained transaction rather than requiring an exact-id native reader.

## Internal retained native read capture and registry funding

`Database::begin_native_read` and its read-only sibling are hidden behind
`contextdb-internal`. Their `NativeReadView` captures the committed transaction id
and user-directory root together while holding tracker state continuously through
page-manager capture and floor registration. A stack guard owns rollback before
admitted shared-guard construction outside the tracker lock; cloning the view
retains that same guard without allocating. Public default read APIs are unchanged.

The one live-reader registry uses the existing hashbrown/allocator-api2 dependency.
Its allocator combines the exact map layout and invoice header, reserves before
allocation, and keeps the original invoice until after physical free. Growth funds
the complete replacement before releasing the previous backing. Native use of
legacy unqualified backing first admits a fresh replacement, preserving counts and
minimum floors. The final registration detaches the empty map without retaining
reader-funded capacity. Savepoints, non-durable ancestors and refresh pins share
that registry and keep their existing retention meanings.

Changed sources: `src/db.rs`, `src/transactions.rs`, `src/transaction_tracker.rs`,
`src/native_storage_admission.rs`, `src/lib.rs`, `src/tree_store/btree.rs`,
`src/tree_store/table_tree.rs`, `src/tree_store/table_tree_base.rs`, and
`src/tree_store/page_store/page_manager.rs`. Authenticated byte/u8 lookup and table
fingerprints all descend from the captured root without ordinary TableTree handles.
The maintained patch is regenerated from the checksum-verified redb 4.1.0 package
using the command above, rather than replacing sections from another source tree.
Upstream proposal (not submitted): paired committed-root capture with a fallible,
allocation-owned reader registry and a lightweight retained authenticated read view.
Compilation and execution receipts belong to the source qualification record.

Publication control reads borrow an existing registered transaction slot without
replacing its backing; a new slot still uses the fully admitted native capture.
The tracker lock spans root capture and count retention. Guard destruction
releases the count before its own backing is freed and original credit settles.
NativeReadView also provides a hidden text-key exact lookup using the same
captured typed descriptor and checksum descent as byte/u8 controls.
Changed sources for these internal additions: `src/db.rs`,
`src/transaction_tracker.rs`, `src/transactions.rs`, `src/tree_store/table_tree.rs`.
The engine keeps ordinary Error-producing reader adapters on their original
admission route; that route is explicitly unqualified for allocation-free
refusal. Native-capable readers retain scalar admission and exact-layout proof.
The complete source patch was regenerated against the verified 4.1.0 archive.

Named root capture finalizes only its pending table and leaves the directory
unpublished. Pure lifecycle probes and length reads overlay pending headers;
commit publishes each surviving changed definition once. Pending buckets, names
and bounded flush scratch use the existing native owned receipts, including
allocation overlap and physical-free-before-refund ordering. Embedding writer
scopes supply the original writer epoch; standalone redb retains its ordinary
unbudgeted allocation mode. A sticky pending failure and transaction capture
failure prevent both commit paths from publishing after a caught partial error.
Compaction reads pending headers and drops source guards before freeing moved
uncommitted pages. Savepoint creation retains its clean-transaction requirement
and flushes pending definitions before capturing the savepoint boundary.


## Internal native payer composition

The hidden `native_storage_active` query reads the existing native TLS only.
The engine's handle boundary preserves a caller-installed admission, and bridges
an existing decode reader child through that child's admission when no native
scope exists. Only an otherwise unscoped internal caller selects the pinned
attach-time writer. Owned publications explicitly install their original writer
before entering that boundary; pending maps retain the admission captured at
construction, including their scope/control invoices, through physical free.
The private native module is compiled for standalone pending-map allocation;
its externally reachable integration re-exports remain gated by
`contextdb-internal`. With no installed admission, standalone pending backing
keeps its ordinary process allocator mode. Writer pending-directory refusals use
the existing scalar reservation kernel with writer bookkeeping labels.
The complete source patch reconstructs all 47 maintained source paths; package,
registration and this maintenance note remain outside that source-only patch.


The hidden directory successor borrows the user-directory record without building
an owned table-name list. The numeric-key successor supports the existing u64-key
cleanup owner using numeric order, fixed-width node validation and byte values.
Both keep native page/control admission and cancellation boundaries. The complete
source patch was regenerated from the locally checksum-verified redb 4.1.0 archive.
These additions have no compilation or runtime qualification in this source packet.


## Internal native producer provenance and exact prepared publication

The existing `NativeColdReadError` distinguishes an actual allocator-null with
its producer site and complete admitted `Layout` from a budget marker and from
an unchanged genuine `io::Error`. Buffer, shared-control, reader-registry and
pending-directory allocators tag their actual null returns. Capacity failures
without an observed allocator call keep an explicitly unqualified I/O fallback;
I/O `OutOfMemory` is never used to infer budget or null provenance.

Admission markers retain the original `NativeStorageAdmission`, including when
pending-directory backing was admitted by an earlier scope than the one running
commit. Its optional failure inspector borrows that same synchronized typed
source during one callback; no raw owner address escapes, new registry is
introduced, or error Box is constructed after denial. Foreign owners without
that inspector retain the marker and remain unqualified. The internal feature
adds hidden `StorageError::Native` and `Error::{Native, PreparedRead}` transport;
ordinary default-feature error variants and backend methods are unchanged.

Prepared descriptor failure now owns a requested-name copy admitted before the
lookup/validation plus the original complete descriptor page. Both allocations
keep independent original receipts through physical free. Formatting reads those
bytes without owned `TypeName` decoding or String conversion and preserves the
normal table validator's diagnostic precedence. The shared alignment constant
still has the existing value and is an invariant, not a consumer policy.

The engine's real exact queue-publication consumer now uses
`prepare_commit_with_read`. It owns abort custody before diagnostic bootstrap,
keeps its prepaid primary outside retained native scopes, and installs the same
prepared scope across consuming commit and retirement. Exact identity, complete
marker, schema, source-link settlement and durable-root settlement still come
from the sealed consuming write. The five real engine phases remain at their
existing call boundaries; retaining the prepared scope adds no phase marker.

This is source work only: no compilation, native execution or test ran in this
packet. The engine's legacy prepared Storage wrapper remains unqualified, as do
opaque I/O/custom error backing, legacy Error-only conversions and foreign
cleanup/unwind payloads. No whole-route qualification or ordinary-host activation
is established. File/coordination/witness/saved-abandonment origins remain separate
work. Required independent proofs include actual budget/null/real-I/O producers,
zero post-refusal allocation, exact durable outcomes and last-holder/unwind frees.

The source-only patch was regenerated from the locally checksum-verified redb
4.1.0 archive, and offline reconstruction compared every resulting source byte.
The unchanged `redb_fork_patch_matches_its_sources` audit remains the source-hunk
binding; no expected checksum was changed to excuse unrelated drift. Upstream
proposal (not submitted): retain original native admission ownership on scalar
failure transport and defer descriptor diagnostic ownership to its producing page.


## Concrete weakless legacy page allocation (source work, run 898)

The page control, checksum mutex, original payer and initialized bytes occupy one
allocation quoted by `Layout::new::<PageBlock>().extend(Layout::array::<u8>(len)?)`
followed by `pad_to_align`. Reads preserve the separate 128-byte directory/window
allowance. Cached, Charged, Plain, write-buffer and writable handles share the
same backing owner; no byte Arc or Weak escape exists. Mutability requires the
actual strong count to be one. A write with other readers allocates a complete
new owner before releasing the old one; checksum invalidation precedes mutation.
Exact invalidation, resize and replacement unlink only the selected map owner.
The pinned-victim guard and the scan allowance are preserved.

Final Release/Acquire release extracts the real source receipt or occupancy
payment into an external free guard, destroys the emptied header and checksum,
frees the complete allocation, then drops that payer. Null/backend errors and
unwind release only an actually created allocation; write preparation keeps its
old dirty map entry until all fallible preparation completes. Occupancy controls
are themselves concrete weakless allocations, admitted with their actual Layout,
counted separately in index observation, and refunded after their own free.
The native owner/error provenance remains the separate run-887 protocol; native
cache copies only change their borrow of the new legacy backing representation.

This closes the page/occupancy-control release ordering in source, not the entire
engine cache funding route. The engine still owns/refunds the fixed outer writer
reserve at database close; it is not moved into these counters by this patch.
The existing write policy can retain more physical backing than the fixed share
when old aliases cannot be reclaimed or one incoming extent exceeds the share.
In charging mode that new backing keeps its genuine source receipt beside its
occupancy tally, through write and flush, until it fits the fixed reserve or is
physically freed. The tally is not a second source invoice. The close edge and
overflow under non-retaining/unscoped admissions, as well as standard write-map,
control/index/bootstrap allocations and native copies, still require their own
source funding proof. They are
implementation blockers to whole-cache qualification, not claims of paid memory.

All existing test text is unchanged in this production patch. An independent
author must adopt private Arc constructor/count/pointer/type spellings and derive
representation-specific prices/full-page counts from the actual Layout while
preserving resource caps, full-capacity workloads, pinned policies and assertions.
The separately authorized engine exact-identity invalidation setup is not changed
here. There is no compilation or runtime result in this source packet.
The complete upstream patch is regenerated and reconstructed offline from the
checksum-verified 4.1.0 archive, including every inherited run-887 source change.


Compiler integration: the two cached mutable-page copy paths borrow the
`ValueRef<PageAllocation>` when passing its byte slice to `copy_from_slice`.
This preserves page ownership, admission, copy contents and cache behavior.
The complete source patch is regenerated against the pinned upstream archive.

## Independent reserve/source precondition tests (source work, run 915)

The preserved header/full-page test now independently constructs the concrete
write-buffer Arc Layout and observes its actual allocation and free. Its exact
index equality includes that real control; all original assertions, the 4 MiB
and 64 MiB capacities, populations and stripe pressure remain. The test-seams-only
`CachePageForTest`/`CacheWriteForTest` wrappers lend genuine production page and
write-map/control aliases from the installed engine cache. They expose backing
identity and immutable bytes, create no parent or receipt, and return the checked
out write before close/flush. `cache_controls_for_test` observes the actual two
occupancy controls so engine tests can prove old/new handle identity and summed
occupancy through the real recycler. These seams are absent from ordinary builds.
The engine tests inspect genuine fixed-writer/process-residue invoices at actual
System deallocation, close, final alias release and same-cap re-admission.
Everything remains UNCOMPILED / RUNTIME_UNRUN. The inherited source-charge Box
control refund before its own physical free remains a known production defect;
the new independent assertion exposes it and no production repair is included.


## Integrated concrete source-credit observation (source work)

The independent source-control test retains its historical selector and every
original assertion. With the concrete prepaid owner integrated, its test-only
observer arms the existing allocator immediately before construction using the
actual monomorphized SourcePageCharge Layout and original accountant counter.
The allocator binds its real returned pointer and samples that same counter
before and after System.dealloc. The test arms before the read and additionally
requires the selected control to remain physically live until value release.
No Box is promoted to the concrete owner, and no erased-pointer ABI is assumed.
The historical early-refund defect described above is repaired in the integrated
source by the concrete free-before-refund owner; its physical proof is UNRUN.
The recorded-refusal replacement of a successful callback remains unresolved;
its original successful-return assertions are deliberately retained.
Compiler integration still borrows both mutable-page cells at copy_from_slice.
The generated complete fork delta includes those fixes and all independent
seams. Parsing and offline reconstruction are not compilation or runtime proof.

The standalone `cached_file` cache-byte-budget test module imports the standard-library
`Mutex` and `AtomicUsize` types used by its existing native write-cache control
layout observations. This is a compile-only test import repair; its fixtures,
assertions, expected bytes, production cache behavior and budgets are unchanged.

## Current page-payment observation for independent recycle proofs

The existing test-seams-only page alias now reports its CURRENT PagePayer
category, original CounterBlock identity and complete payment bytes under the
actual payer transition mutex. It neither constructs nor retains a payer and
changes no admission, transfer or refund. The independent engine fixture checks
each distinct C/D/F/G backing against the original controls and exact native
Layout, retaining the valid read lower bound plus per-counter and aggregate
coverage. A SourceCounted observation is explicitly unqualified until its actual
source invoice/lifetime is independently proved; capacity is never a receipt.
Runtime handoff observations remain owed. The complete source patch and metadata
overlay reconstruct all maintained bytes/modes from the pinned cached archive.

The `contextdb-internal` + `test-seams` scan observations report the actual
shared block, linked node, concrete Layout and original receipt identity. The
remaining-byte and current-tail methods inspect the real scan state; the debit
adapter calls the same production `take_scan_growth` used by the page cache.
These methods allocate nothing and retain no owner. They are absent without the
test feature and do not change ordinary scan behavior. Independent engine tests
extend the existing System-backed allocator oracle with a scalar injected-null
sample; no second allocator or heap ledger is added. Source parsing and patch
reconstruction do not certify compilation, physical funding or runtime behavior:
these tests remain UNCOMPILED / RUNTIME_UNRUN. The upstream proposal for the
existing thread-scoped admission remains unsubmitted.

| `src/db.rs`, `src/lib.rs`, `src/native_storage_admission.rs`, `src/tree_store/page_store/cached_file.rs`, `src/tree_store/page_store/page_manager.rs` | Feature-internal native backend capability is explicitly selected at original engine WriterFileView construction before trait erasure. Inline Legacy/Native dispatch retains one original concrete backend Box and uses the same database/memory/cache funnel; foreign, wrapped, read-only and scratch sources keep their legacy provenance. Finite owner-closed, projected-growth-poison and disk-growth-refusal causes bypass custom I/O payload construction; opaque OS errors move unchanged. Quota retry and non-sticky refusal accept both original opaque quota errors and native scalar disk refusals without changing sizes, retry count, recovery or publication. The engine funds its actual diagnostic block before the producer and retains the original source through physical free; this does not certify bootstrap, region/pager round trips, scratch or general OS Display funding. Source-only: compilation and independent runtime proofs remain owed. Upstream proposal (not submitted): an optional typed backend-failure construction capability preserving the existing StorageBackend contract. |


## Independent WriterFileView error producer observation

`src/tree_store/page_store/cached_file.rs` exposes
`CheckedWriterBackendForTest` only with both `test-seams` and
`contextdb-internal`. It holds the actual `CheckedBackend`, dispatches its real
len/read/write/resize/sync operations, and accepts the existing native or legacy
backend constructor. It produces no errors itself and allocates no observer.
The original backend Box construction remains outside the diagnostic funding
claim. Re-exports in the two tree-store module files and `src/lib.rs` have the
same feature guards. Ordinary compiled source and every existing test body are
unchanged. The independent engine test module
`persistence/writer_native_io_contract_tests.rs` uses real WriterFileView owners,
OS calls, poisoned growth mutexes and quota refusals. Source is uncompiled;
runtime, strict linting, and mutation validation remain unrun. The complete
source delta is regenerated in `upstream-4.1.0.patch`.


## Request cache retirement

A failed engine read retires only cache misses created by that request and no
longer held outside the cache. An inline monotone request identity tags the
original complete page allocation; hits never change the existing tag. The
engine commits successful request scopes and asks the existing statement cleanup
boundary to retire failed scopes. `src/read_admission.rs`, `src/lib.rs`,
`src/tree_store/page_store/page_allocation.rs`,
`src/tree_store/page_store/cached_file.rs`,
`src/tree_store/page_store/page_manager.rs` and `src/db.rs` implement this
feature-internal primitive. No reader alias, allocation, limit, backend read or
wire change is introduced by retirement. Actual page drops settle the existing
occupancy receipts after physical free. Upstream proposal: an operation-scoped
cache insertion identity with selective idle-page retirement after refusal.
The source patch is regenerated from every current src file against the pinned,
checksum-verified archive; no prior source hunks are removed by reconstruction.

The test-seams reader-registry census records the successfully allocated combined registry Layout and the producing scope’s actual receipt Layout, filtered by its original accountant identity. It does not reserve storage, change native-credit ownership, infer payment from occupancy, or count an unallocated capacity. TransactionTracker lends the live allocator observation under its state lock; Database forwards that observation without opening another read transaction.

## Reader working sets and warm native pages

Declared scans use the existing shared read cache rather than a separate
operation page index or a fixed tiny scan-growth allowance. A working set that
fits that cache reads each page once; a larger stream recycles unpinned pages.
No operation owns a growing collection of every visited page. The previous
operation slot-array implementation is removed, so there is no `ScanCache::Table`
allocation and no operation index retained by a cached page. Native scan values
retain only their original admission; thread-bound nodes remain independently prepaid.

Every page carries its original source invoice until physical free or transfer
to a genuine fixed cache reserve. Insertion checks read/write occupancy against
`max_cache_size`; `CachePayment::reserve` only records occupancy. Its tally is
not another source invoice. Transfer requires the actual installed fixed reserve.
Ending a statement releases its own handles; shared and detached holders can
outlive it without refunding still-live backing. Cache counter controls keep an
inline release generation under their existing exact-layout funding arrangement.
A last external page release publishes to its retained original control, never
through a freed page pointer. A previously unsuccessful eviction is reused only
until that generation changes, and the clock resumes after its actual last stripe.

Native clean reads install natively admitted immutable pages in that same shared
cache and reuse them across operations. A legacy hit is copied under native
admission before publication, preserving separate provenance. The native page
uses the complete weakless page allocation and its original native receipt;
moving the receipt into single-owner custody needs no allocation or new retain.
The valid allocation Layout leaves the high length bit available to record
native construction without increasing the header. Byte slices and final-free
Layouts always use the masked actual length. Prepared reads retain their separate
dirty-source copying route and do not install pages. Ordinary reads without a
legacy callback use their installed native admission before allocation. No
persisted format, declared memory limit, cache share or query route changes.

| Changed source | Reason |
|---|---|
| `src/read_admission.rs`, `src/lib.rs`, `src/tree_store/mod.rs` | Declared-scan detection under the original caller admission; remove the redundant operation page index. Upstream proposal: retain scan working sets within the admitted shared cache rather than impose an unrelated scan-growth constant. |
| `src/tree_store/page_store/cached_file.rs`, `src/tree_store/page_store/page_allocation.rs`, `src/native_storage_admission.rs` | Shared warm-page retention for declared and native clean reads; retain original native invoices through physical free. Preserve caller capacity checks and genuine fixed-reserve transfer. Continue the clock from the last visited stripe and cache an unsuccessful victim search only until an actual external holder releases a page. |


## Minimum-version compatible strict linting

`src/atomic_compat.rs` implements the same conditional compare/exchange loop on
AtomicUsize and AtomicU64 without requiring the renamed API from newer compilers.
Original success/failure ordering and overflow/refusal behavior are preserved.
Fixed-width byte iteration uses array chunks with the same ignored remainder;
nonnull pointer conversions preserve the actual allocated Layout alignment.
Inline typed native errors and const constructors retain narrow documented lint
exceptions where changing representation or const behavior would change their
contract. Unused private native-page construction helpers are removed. Test
modules follow production items, and passive observers require their live guard.
The opaque native byte-key fixture keeps its intentional big-endian lexical order;
its assertions and test bodies are unchanged.

| Changed source | Reason |
|---|---|
| `src/atomic_compat.rs` (added), `src/lib.rs`, `src/db.rs`, `src/read_admission.rs`, `src/native_storage_admission.rs`, `src/tree_store/page_store/page_allocation.rs` | Preserve conditional atomic update semantics on the minimum supported Rust version, preserve typed error custody and actual allocation-pointer provenance, and remove unused private construction methods. |
| `src/transaction_tracker.rs`, `src/transactions.rs`, `src/tree_store/{bounded_bytes,btree,table_tree,random_state_override}.rs`, `src/tree_store/page_store/{allocator_pager,cache_owner_contract_rows,cached_file,lru_cache,page_manager}.rs` | Equivalent fixed-array iteration, const-safe invariants, explicit absent-versus-empty root semantics, allocation-free observer capability requirements, feature-accurate passive accessors, and source formatting. |


## Reader cache capacity changes

A read-only handle may prepay index slots for its maximum selectable page
capacity, then use `set_read_cache_capacity` to change the selected page capacity
without replacing the index or reopening its snapshot. Increasing page capacity
still requires the caller's original reserve admission before publication.
`with_read_cache_capacity` serializes capacity publication and caller settlement
with cache insertion. The settlement callback may inspect atomic occupancy and
must not read or modify cached pages.

Shrinking retires cache entries in CLOCK order. A detached reader alias keeps the
same physical page, payment and original reserve owner. The optional inline
`CacheReserveOwner::set_read_settlement` callback runs after page backing is
freed; the original source owner reads current atomic occupancy under its funding
lock before refunding excess. The fixed reserve therefore covers the selected
capacity or live physical occupancy, whichever is larger. Counter custody keeps
the callback context alive through its final call. Static reserves skip the
callback mutex, and ordinary pressure eviction keeps its bounded victim walk.
`read_cache_index_bytes` reports the prepaid index and native controls even when
page capacity is zero; `read_cache_held_bytes` includes detached physical holders.

## Advisory shared-read-cache residency

With `contextdb-internal`, `NativeReadView`, `ReadOnlyDatabase`, and `Database`
expose `read_cache_resident_fraction(&[[u8; 8]]) -> f64`. Inputs are the existing
opaque eight-byte page identifiers from root identities or table-root headers.
Each identifier contributes one observation, including repeated identifiers;
invalid identifiers count as absent and empty input returns zero. The fraction is
an advisory observation under concurrent reads and eviction, never a snapshot
validity check. Only shared read-cache entries count, excluding dirty write cells
and detached holders.

The observer performs one indexed lookup per requested identifier, holding only
that cache stripe and entry's read locks. It allocates nothing, performs no I/O,
and leaves the CLOCK second-chance bit unchanged. Native root/page admission is
not required to inspect this existing state. No cache capacity, page invoice,
route default, or persisted format changes.

| Changed source | Reason |
|---|---|
| `src/db.rs`, `src/transactions.rs`, `src/tree_store/page_store/{page_manager,cached_file,lru_cache}.rs` | Production planner access to an allocation-free per-page residency estimate; validate opaque page geometry and inspect exact cached extents without changing eviction priority. |


## Native scan state without a growth counter

Declared native scans use the shared cache capacity and allocate no shared growth
counter. `ScanAllowance::try_new_native` only retains the original admission;
`try_enter_native` still admits the complete concrete linked-node Layout before
allocation and reinstates that captured admission on the target thread. The last
entry unlinks and physically frees its node before settling its original invoice.
Legacy unscoped scans retain their live finite allowance. Counter-only observers
and the private shared-payload getter are removed with the obsolete object.

| Changed source | Reason |
|---|---|
| `src/read_admission.rs`, `src/native_storage_admission.rs` | Remove the unused native growth-counter allocation and its test-only observers; retain original-admission custody, funded thread-local nodes, and the live legacy allowance. |
