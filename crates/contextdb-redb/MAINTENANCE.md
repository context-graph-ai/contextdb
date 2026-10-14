# contextdb-redb maintenance

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

| Changed source | Reason |
|---|---|
| `src/transactions.rs`, `src/tree_store/table_tree.rs`, `src/tree_store/btree.rs` | Add a feature-gated, hidden byte-key range sibling over the same captured transaction and admitted navigation. Validate the byte-table header, share the existing range descent, and retain text-key UTF-8 checks through an explicit private validation flag. |
| `src/error.rs`, `src/read_admission.rs`, `src/lib.rs`, `src/transactions.rs` | Retain opaque caller admission refusals as a non-I/O storage variant, including relocation and typed hidden scoped/charging entry points. Existing I/O admission interfaces adapt actual I/O unchanged; the engine alone interprets the payload and owns memory policy. |
| `src/db/allocator_metadata_test_seams.rs` (added), `src/tree_store/page_store/bitmap.rs`, `src/tree_store/page_store/buddy_allocator.rs`, `src/tree_store/page_store/region.rs` | Page both region-tracker and buddy bitmap words through fixed scratch windows for file-backed writers. Region views and descriptor arrays are reconstructed from scalar geometry, with only one active region retained; the previous descriptor owner drops before its successor is constructed. Bitmap reads mask stale tail bits after shrink and visit only words in the current geometry. `BtreeBitmap::has_unset` answers through the maintained fullness summary (the same one-word-per-level descent `alloc` takes) instead of sweeping leaf words, so finding a region's highest free order after each allocation reads a bounded number of words whatever the region's page count. Each `BtreeBitmap` keeps its one-word root summary resident once read (a fixed 16 bytes per bitmap, forgotten on any mutation and never remembered from a poisoned read), so an allocation's per-order free checks do not load the root window of an order it did not change. `BuddyAllocator::alloc_lowest` reads each order's lowest free block to choose the lowest address and allocates only the block it returns, instead of allocating and freeing one block per higher order to compare positions. `RegionTracker::mark_full` stops at the first order already marked full (full marks are upward-closed), so an allocation's tracker update reads only the orders it changes. The existing `test-seams` accessor delegates to the production resident-byte count; opaque backends without scratch keep the resident allocator. |
| `src/tree_store/page_store/allocator_pager.rs` (added) | Keep a window set of 4096-byte windows sized from the writer's page-cache share (one byte in `CACHE_SHARE_DIVISOR`=32 of it, never fewer than the 32 windows every pager holds), found through a fixed open-addressed identity index (linear probing, backward-shift deletion, at most half full) allocated once per admitted window count so evict/insert churn never allocates, plus fixed control state; the whole admitted set is allocated when the share is admitted (at open and at startup settlement), never by a later read or write, and its bytes are carved out of the page cache's share so the owner's single cache charge covers them. Lowering the share spills and releases the surplus. A spilled window reloads with one scratch read of its 4104-byte record into a stack buffer; eviction is a clock over the windows: a window used since the hand last passed is spared once, a clean unused window is reused before a dirty unused one, and a dirty window nobody uses is spilled rather than pinning its slot; spill dirty windows to non-durable arithmetic slots beside the database and lazily load saved or virtual bitmap words. Detaching a replaced saved tree preserves the virtual initializer for future regions. A scratch I/O fault is sticky and preserves its original cause. The allocator operation envelope is `W * P + H + C`, with W the admitted window count (32 under any share up to 4 MiB), P=4096 plus index bytes per window, H=4096 bytes of fixed hashing scratch, and C<32 KiB for window tags, descriptors, and control; it follows the declared budget and is independent of region count, and under 256 KiB at the 32-window floor. B-tree source pages and OS page cache are accounted separately. |
| `src/db.rs` | Expose exact fixed cache-index allocation bytes on writable and read-only handles for the enclosing owner charge, and the constant per-stripe lock-control bytes within them (`cache_stripe_control_bytes`, through `page_manager.rs` and `cached_file.rs`) so the engine can classify that fixed part as open residue.  Expose bounded compaction steps, live-reader observations, a retained statistics snapshot, existing-only `Builder::open_with_backend`, and caller-coordinated `Builder::open_read_only_with_backend`; admit authenticated external snapshot roots before open housekeeping and every writer allocation/reclaim boundary, with exact-root reads and retained-page observations; offer contextdb-internal external-root admission that refuses an occupied refresh mutex without waiting and a structured helper that distinguishes this busy mutex from provider I/O refusals; let a descriptor-backed read-only database begin checksum-verifying reads at its captured root or at an exact external root whose registration the caller retains; open registered external readers without consulting a live writer's allocator state or assuming a captured header matches the file length during writer growth; attach the external provider during creation as well as reopen; configure caller-owned deferred-free extent and entry budgets on the builder so open, normal and close durable commits inherit the same bound; drain eligible deferred frees through repeated bounded commits when an idle caller explicitly requests close-time convergence; prepare a committed root for a graceful read-only handoff while the writer handle remains alive (`Database::prepare_read_only_handoff`), refuse later writes with `DatabaseClosed`, and skip its later Drop housekeeping commit; the waiting-writer and instance-scoped actual table-write observers, a one-shot simulated I/O fault at an eligible deferred-free cleanup commit, and a read-only diagnostic counting entries and extents in both complete deferred-free trees, are test-only under `test-seams`; the refresh-lock callback is test-only under test/test-seams. |
| `src/lib.rs` | Export the read admission entry points (`with_read_memory_admission`, `with_read_page_charge`, `ReadPageCharge`), the compaction cursor/progress, statistics snapshot, exact read-transaction root identity, and authenticated external-snapshot retention types; export actual table-write observation types only under `test-seams`; export hidden bounded byte cursor/handle types only under `contextdb-internal`. |
| `src/transactions.rs` | Carry resumable compaction progress across finite page batches, commit relocation frees, and retain statistics snapshot ownership; keep deferred-free extents behind the oldest authenticated external transaction until that root releases, without a page-set lookup on each commit; reclaim each free tree from an initial fair share of the caller budget, assigning the odd unit to the oldest durable free-list key across restart and lending unused capacity to the other tree without exceeding either total cap, trimming only one copy-on-write page-list value at a time and retaining the remainder durably; offer an on-demand root-page diagnostic separate from admission; expose a contextdb-internal lazy byte-table opener under the same read transaction and root; expose the exact transaction/user-tree root identity, bounded per-table root fingerprints (`table_root_fingerprint` on read and write transactions), and a same-transaction callback after a relocation batch (`compact_step_with_hook`) so a caller can bind the changed fingerprint before commit; propagate checksum-verifying reads from retained user roots into their tables; attach the instance observer to user-table namespaces and successful table deletion under `test-seams`; report the commit's two page-list lengths and the five commit regions' durations under `test-seams`; on Unix under `test-seams`, let an explicitly armed reclaim transaction signal and block after deferred-free and system-tree mutations are staged, immediately before durable publication, so an integration test can terminate that exact transaction. |
| `src/transaction_tracker.rs` | Let maintenance yield when a foreground writer is waiting; authenticate the saved root page in bounded work and retain only root identities as a transaction floor; admit an exact root while a local read pin or a provisional client handoff hold protects it; serialize provider refreshes without rebuilding page unions, with a contextdb-internal try-lock admission that refuses a busy refresh using a private typed I/O cause; offer full root-page inventories only to explicit diagnostics; own the instance-scoped test observer slot and the database-lifetime deferred-free budget; hold refresh coordination across a test callback only under test/test-seams; under `test-seams` only, read the existing native transaction ownership map count for one actual transaction id without allocation, storage access, or retaining a root. |
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
| `src/tree_store/page_store/cached_file.rs` | The private native clean read prepays fresh byte/header owners before cache locks, copies exact-length original warm read-cache bytes under the stripe read lock, and otherwise uses the backend without consulting dirty write cells. Its paid native page never borrows or adopts a legacy cell and never probes the write buffer. Use one shared fixed read-entry arena across all cache stripes; an exhausted read index returns an uncached page without allocation. The write buffer retains its original growable map and releases its capacity at each flush. Rebuild indexes under startup exclusion, preserving held entries, and expose exact index allocation bytes separately from the page cap.  Memoize only successful variable-width byte-node checksum computations in one object-local mutex on each immutable CachedPage/ChargedPage, leaving Plain bytes uncached; explicitly count immutable wrapper, slot, alignment and Arc controls in the unchanged native cache share, preadmit those controls independently of the existing directory/window allowance, and preserve charges through eviction until the last holder drops. Admit page/cache-entry bytes before allocation and observe successful backend reads; hand readers a `PageMemory` that, for a page the read cache does not keep, carries the charge its admission returned until the last holder drops the page; count a cached page against the cache budget until the page itself is dropped (`CachedPage`), so a page evicted while a reader still holds it stays counted, eviction frees only pages no reader holds, and a miss is cached only when its count fits the budget; keep caller quota refusals recoverable while real backend I/O failures still poison the handle; read private allocator and contextdb-internal bounded windows with admission before allocation and backend I/O, copying cached or buffered sources without installing a partial cache page; on resize invalidate only cached pages that reach past the new end, so a truncating compaction step keeps the valid working set instead of dropping the whole read cache; under one-pass scan reads a miss may use free cache room only within the scan's 16 KiB growth allowance; past it a miss first evicts as many bytes as it needs and enters the cache at lowest priority only if that eviction made room, otherwise it is returned uncached (hits are still served), so an exhaustive scan grows the cache by at most the allowance and replaces its own pages before the warm working set. Size the fixed read arena with one extra slot for the sole sub-page entry, the 320-byte database header at offset zero. Construction and startup settlement share overflow-safe sizing and retain the held-entry floor. Debug assertions at every native cache admission enforce the header exception; the fixed arena's actual bytes remain charged by the existing index observation. Rust formatting is normalized without changing expressions or assertions. The byte-node checksum memo is compiled only for its `contextdb-internal` and test callers (its slot stays in every build so page layout and admitted charge do not vary), and page-size conversions are checked. |
| `src/tree_store/page_store/lru_cache.rs` | Insert a page at the front of the eviction order without a second chance (`insert_lowest_priority`), so a page a one-pass scan installs is the next one evicted unless it is read again, and a scan replaces its own pages rather than the warm working set. Keep pinned read entries linked during a bounded two-pass CLOCK victim search, so only reclaimable entries leave the map. Maintain per-stripe eviction order and collision chains with fixed u32 links in one shared entry arena; allocate arena slots, a free stack and stripe heads once, return slots on eviction and clear, and count their exact allocations outside the page cap. Retain held entries and their second chances when rebuilding indexes for a settled startup cap. The arena resize helpers (`arena_capacity`, `transfer_to`) are compiled only for their `contextdb-internal` caller. |
| `src/tree_store/page_store/mod.rs` | Re-export `PageMemory` to the tree code that holds read pages and register the allocator pager. |
| `src/tree_store/page_store/page_manager.rs` | Forward the exact fixed cache-index allocation to the database owner. Give a paged allocator its window allotment of the cache share at open and at startup cache settlement, settling the page cache on the remainder and returning one reserve that covers both.  Leave the allocator layout and recovery state unchanged when a caller refuses projected file growth before resize; allow caller-retained external readers to use file geometry while a live writer has marked the current header dirty, without recovery or allocator writes, and without asserting that a concurrently growing file still matches the captured header length; publish the clean committed header before a graceful read-only handoff without closing the writer backend; validate byte-window ranges against their allocation extent before storage access. Choose paged allocators when the backend supplies scratch, locate saved fixed-key allocator values through header/key windows and read only requested bitmap header/word windows (including the region tracker), preserving full retained-root checksum authentication; lazily read saved allocator words, preserve repair and growth ordering, and refuse publication or clean Drop after a sticky scratch fault. Before explicit integrity repair, align an aborted writer's free tail to the committed header geometry. |
| `src/tree_store/page_store/file_backend/optimized.rs`, `src/tree_store/page_store/file_backend/fallback.rs` | Supply same-directory scratch for normal path-based file opens on every supported platform. File-only opens recover the path from Linux/Android, macOS, and Windows handles; other targets treat pathless raw handles as opaque backends with optional backing `None`, preserving raw `File` opens. Known paths always request same-directory scratch and propagate scratch errors. The scratch directory entry is removed immediately after creation, so process death leaves no orphan. |
| `src/db.rs`, `src/lib.rs` | Add a defaulted optional scratch-backend hook, export the non-durable scratch trait and same-directory factory, and expose read-only resident-byte and region-count observations. Writable file opens use scratch when offered; opaque and in-memory backends remain fully resident. Unit tests retain exact debug allocation identities for relocation checks; normal paged writers omit that unbounded inventory. |
| `src/transactions.rs` | Keep a large committed allocator table reachable while ordinary commits use its lazy source, and materialize that source before quick repair replaces it. An ordinary commit can retire a small source within the fixed 32-window capacity (never the larger admitted set, so an ordinary commit never reads a large saved bitmap), then remove its stale system table and restore the upstream page footprint; a source measured too large keeps that verdict until the layout or the window set changes, so later commits do not rebuild a regional view to re-measure it. Finite compaction admits the encoded extent size before source-page acquisition or destination allocation, and protects the retained allocator table instead of materializing every region before admission: its table cursor advances without reading protected payload pages, while moving the system master tree preserves that child root. Quick repair and explicit whole-file compaction detach the source before replacing it. |
| `src/tree_store/btree.rs`, `src/tree_store/btree_base.rs`, `src/tree_store/btree_mutator.rs` | Locate fixed-key byte values without loading their payloads for lazy allocator windows; no public lookup semantics change, and verified roots still authenticate complete nodes. Reserve allocator values from a length-only zero source, initialize the destination directly, and preserve arbitrary byte-value insertion and the v3 serialized node layout. Replacement accounting is separate from retaining the discarded old value. |
| `src/tree_store/page_store/layout.rs` | Reuse the rounding remainder, preserving arithmetic while satisfying current strict lints. |
| `src/tree_store/page_store/base.rs` | Release the debug page-reference and dirty-page tracking allocations when their last live page is dropped, retaining the existing reference-count assertions. Keep all debug-only mutable-page tracking statements inside one `cfg(debug_assertions)` block so release builds retain no debug-only references.  Hold a read page as `PageMemory` so a charge it carries lives exactly as long as the page; validate serialized external page identities before constructing allocation extents, including the encoded allocation order; distinguish checksum-verifying retained page faults from ordinary cache hints. |
| `src/read_admission.rs` (added) | Scope read admission and backend-read observation callbacks to the active thread using RAII guards; a charging admission (`with_read_page_charge`) returns a per-page charge that the page carries. One-pass scan reads (`scan_reads` guard, `with_scan_reads`) mark this thread's reads so, past a 16 KiB growth allowance (`ScanAllowance`, shared by nested scopes and re-entered across one statement's resumed steps), a cache miss replaces evictable cached bytes instead of growing the read cache. |
| `src/open_with_backend_region_override_tests.rs` (added) | Verify existing-storage backend open ignores a creation-only region-size override. |
| `src/table_registry.rs` (added), `src/transactions.rs`, `src/table.rs`, `src/lib.rs`, `Cargo.toml` | The write transaction's open-table registry stays std's hash table and randomly seeded `RandomState` (the `hashbrown` crate std wraps, with `allocator-api2`), and gains an allocator a caller can fund: a fallible reserve refused with no credit reports the exact `Layout` it would allocate without changing the registry, a reserve paid with exactly that credit performs it, an empty replacement array can be prepared ahead and installed later without allocating, and a monitor counts any array allocation made outside a funded reserve. Keys and a table handle's name are `Box<str>`. The hidden `contextdb-internal` methods are `open_table_registry_{len,bytes,cost_to_reserve,reserve,cost_to_hold,prepare,install,monitor}`; `tests/open_table_registry_allocations.rs` witnesses every registry allocation by address against a counting global allocator. The engine owns the funding policy. |
| `src/tree_store/table_tree.rs` | Restage an already staged table root in place instead of allocating a key the map drops again; expose `pending_table_update_contains` (internal) and its size (test seam). |

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

CDB-141 source revision: `Database::begin_verified_read` (internal feature only)
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
