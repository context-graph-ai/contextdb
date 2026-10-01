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

[upstream-4.1.0.patch](upstream-4.1.0.patch) remains the complete source-only delta against that
exact package. It contains the changed source files listed below; fork package metadata, integration-test
registration, and this note are separate from the source diff.
`crates/contextdb-core/tests/redb_fork_patch_matches_its_sources.rs::the_redb_fork_patch_produces_the_sources_it_names`
binds every recorded source hunk to the maintained file it produces.

| Changed source | Reason |
|---|---|
| `src/db/allocator_metadata_test_seams.rs` (added), `src/tree_store/page_store/bitmap.rs`, `src/tree_store/page_store/buddy_allocator.rs`, `src/tree_store/page_store/region.rs` | Under `test-seams` only, observe actual retained vector capacities for buddy bitmap/tree metadata and region bookkeeping without allocation or storage reads. `Database::allocator_metadata_bytes_for_test` reports heap payload bytes as a lower bound, excluding heap allocator overhead, fixed inline control state, commit/debug page sets, serialization/open buffers, and transient reallocation overlap; it supports the engine's retained-residency assertion without claiming complete memory reconciliation. |
| `src/db.rs` | Expose bounded compaction steps, live-reader observations, a retained statistics snapshot, existing-only `Builder::open_with_backend`, and caller-coordinated `Builder::open_read_only_with_backend`; admit authenticated external snapshot roots before open housekeeping and every writer allocation/reclaim boundary, with exact-root reads and retained-page observations; offer contextdb-internal external-root admission that refuses an occupied refresh mutex without waiting and a structured helper that distinguishes this busy mutex from provider I/O refusals; let a descriptor-backed read-only database begin checksum-verifying reads at its captured root or at an exact external root whose registration the caller retains; open registered external readers without consulting a live writer's allocator state or assuming a captured header matches the file length during writer growth; attach the external provider during creation as well as reopen; configure caller-owned deferred-free extent and entry budgets on the builder so open, normal and close durable commits inherit the same bound; drain eligible deferred frees through repeated bounded commits when an idle caller explicitly requests close-time convergence; prepare a committed root for a graceful read-only handoff while the writer handle remains alive (`Database::prepare_read_only_handoff`), refuse later writes with `DatabaseClosed`, and skip its later Drop housekeeping commit; the waiting-writer and instance-scoped actual table-write observers, a one-shot simulated I/O fault at an eligible deferred-free cleanup commit, and a read-only diagnostic counting entries and extents in both complete deferred-free trees, are test-only under `test-seams`; the refresh-lock callback is test-only under test/test-seams. |
| `src/lib.rs` | Export the read admission entry points (`with_read_memory_admission`, `with_read_page_charge`, `ReadPageCharge`), the compaction cursor/progress, statistics snapshot, exact read-transaction root identity, and authenticated external-snapshot retention types; export actual table-write observation types only under `test-seams`; export hidden bounded byte cursor/handle types only under `contextdb-internal`. |
| `src/transactions.rs` | Carry resumable compaction progress across finite page batches, commit relocation frees, and retain statistics snapshot ownership; keep deferred-free extents behind the oldest authenticated external transaction until that root releases, without a page-set lookup on each commit; reclaim each free tree from an initial fair share of the caller budget, assigning the odd unit to the oldest durable free-list key across restart and lending unused capacity to the other tree without exceeding either total cap, trimming only one copy-on-write page-list value at a time and retaining the remainder durably; offer an on-demand root-page diagnostic separate from admission; expose a contextdb-internal lazy byte-table opener under the same read transaction and root; expose the exact transaction/user-tree root identity, bounded per-table root fingerprints (`table_root_fingerprint` on read and write transactions), and a same-transaction callback after a relocation batch (`compact_step_with_hook`) so a caller can bind the changed fingerprint before commit; propagate checksum-verifying reads from retained user roots into their tables; attach the instance observer to user-table namespaces and successful table deletion under `test-seams`; report the commit's two page-list lengths and the five commit regions' durations under `test-seams`. |
| `src/transaction_tracker.rs` | Let maintenance yield when a foreground writer is waiting; authenticate the saved root page in bounded work and retain only root identities as a transaction floor; admit an exact root while a local read pin or a provisional client handoff hold protects it; serialize provider refreshes without rebuilding page unions, with a contextdb-internal try-lock admission that refuses a busy refresh using a private typed I/O cause; offer full root-page inventories only to explicit diagnostics; own the instance-scoped test observer slot and the database-lifetime deferred-free budget; hold refresh coordination across a test callback only under test/test-seams. |
| `src/table.rs` | Observe successful actual serialized table insert/reserve/remove operations under `test-seams`; no expected artifact set or planner counters. Offer contextdb-internal byte lookup and prefix inspection that materialize possible matches through ordinary complete-page reads, and a read-only opaque-entry cursor bound to the same captured root. |
| `src/table_write_observer.rs` (added) | Provide instance-scoped RAII callback installation/restoration, including nested scopes and unwinding; retain actual key bytes and serialized value lengths. |
| `src/commit_work.rs` (added) | Under `test-seams` only: count allocated extents in the freed and allocated commit lists and the extents/entries actually reclaimed by bounded cleanup; the legacy `*_pages` counter names do not count physical pages. Tag every copy-on-write freed page with the caller-declared region it happened in, and time the five regions of `commit_inner`. |
| `src/tree_store/btree_mutator.rs` | Hold a partially emptied leaf as the page memory type readers receive (`PageMemory`); report each copy-on-write page an insert, update or delete makes unreachable under `test-seams`. Inspect committed oversized singleton headers before the existing insert-before split and no-merge deletion paths; also reuse insert-after when the old separator fits a physical branch page. Carry its admitted key window through branch construction and any separator promotion; preserve the old extent and its checksum without faulting its unrelated value. |
| `src/multimap_table.rs` | Make subtree roots available to the bounded page traversal; report each copy-on-write page a multimap value removal makes unreachable under `test-seams`. |
| `src/tree_store/btree.rs` | Enumerate a bounded batch of tree pages from a continuation position; authenticate the saved root page at external admission, then verify retained child pages against parent checksums as they are faulted; start a range from the root page the tree already holds instead of reading it again; allow the contextdb-internal byte opener to retain the root header lazily while ordinary get/first/last/range keep full-page semantics; report each copy-on-write page the mutator makes unreachable under `test-seams`. |
| `src/tree_store/bounded_bytes.rs` (added), `src/tree_store/mod.rs` | Contextdb-internal reject-only inspection of committed oversized singleton byte leaves, incremental key comparison, lazy exact lookup, prefix/value-prefix iteration, and a transaction/root-pinned opaque-entry cursor. The cursor uses admitted parent navigation to continue after a caller refuses complete materialization of an oversized singleton; navigation errors remain terminal, every complete node reached by bounded lookup, prefix/value-prefix iteration or cursor navigation is layout-validated before offsets reach the ordinary accessors, branch cycles are rejected on every bounded traversal path, and key comparisons can stream borrowed fragments without a whole-key allocation. Possible matches use complete source-page admission and the existing decoder; checksum-verifying retained roots always keep complete-page verification. Tests cover typed corruption without panic for malformed leaf and branch roots and circular branch references through ordinary bounded lookup, prefix iteration and cursor navigation, matching refusal, zero backend reads on refused windows, charge release, prefix exclusion, both singleton insertion orders with giant values, separator-window refusal, giant-separator fallback, neighboring deletion, singleton refusal in every sibling position, terminal navigation refusal, streamed key-window charge release, abort, reopen, integrity and pinned snapshots. |
| `src/tree_store/btree_iters.rs` | Carry root and parent checksums through retained range traversal so each newly faulted page is verified without scanning the whole tree; accept a root page the caller already holds and has verified (`new_with_root_page`), so both ends of a range descend from one copy — a root larger than the page cache, such as a single leaf holding one oversized value, was otherwise read and held twice. |
| `src/tree_store/table_tree.rs` | Enumerate and relocate the selected page batch, expose consistent statistics, the read transaction's user-tree root and its read hint; authenticate just the root page for bounded external admission; pop a deferred-free list tail in place without a scratch collection; report actual user-directory inserts/removals, and each copy-on-write page the table tree makes unreachable, under `test-seams`. |
| `src/tree_store/btree_base.rs` | Let an access guard hold a read page as `PageMemory`, keeping any charge the page carries alive while the guard is; allow crate-private mutation of an existing copy-on-write value so partial free-list cleanup needs no temporary page vector. |
| `src/tree_store/table_tree_base.rs` | Forward bounded page traversal for the table root. |
| `src/tree_store/page_store/cached_file.rs` | Admit page/cache-entry bytes before allocation and observe successful backend reads; hand readers a `PageMemory` that, for a page the read cache does not keep, carries the charge its admission returned until the last holder drops the page; count a cached page against the cache budget until the page itself is dropped (`CachedPage`), so a page evicted while a reader still holds it stays counted, eviction frees only pages no reader holds, and a miss is cached only when its count fits the budget; keep caller quota refusals recoverable while real backend I/O failures still poison the handle; read contextdb-internal bounded windows with admission before allocation and backend I/O, copying cached or buffered sources without installing a partial cache page. |
| `src/tree_store/page_store/mod.rs` | Re-export `PageMemory` to the tree code that holds read pages. |
| `src/tree_store/page_store/page_manager.rs` | Leave the allocator layout and recovery state unchanged when a caller refuses projected file growth before resize; allow caller-retained external readers to use file geometry while a live writer has marked the current header dirty, without recovery or allocator writes, and without asserting that a concurrently growing file still matches the captured header length; publish the clean committed header before a graceful read-only handoff without closing the writer backend; validate byte-window ranges against their allocation extent before storage access. |
| `src/tree_store/page_store/layout.rs` | Reuse the rounding remainder, preserving arithmetic while satisfying current strict lints. |
| `src/tree_store/page_store/base.rs` | Hold a read page as `PageMemory` so a charge it carries lives exactly as long as the page; validate serialized external page identities before constructing allocation extents, including the encoded allocation order; distinguish checksum-verifying retained page faults from ordinary cache hints. |
| `src/read_admission.rs` (added) | Scope read admission and backend-read observation callbacks to the active thread using RAII guards; a charging admission (`with_read_page_charge`) returns a per-page charge that the page carries. |
| `src/open_with_backend_region_override_tests.rs` (added) | Verify existing-storage backend open ignores a creation-only region-size override. |

The allocator metadata seam is compiled only under `test-seams`. Its module hook in `src/db.rs`
and state-mutex accessor in `src/tree_store/page_store/page_manager.rs` do not affect ordinary
builds. The observer reads the current retained owner capacities after open or file growth;
it does not observe allocations made and released during open, serialization, or `Vec` resize.
The independent allocation oracle must cover those transient owners.

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
