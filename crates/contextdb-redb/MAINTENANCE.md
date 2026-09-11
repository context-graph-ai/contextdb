# contextdb-redb maintenance

This existing storage fork is based on the **redb 4.1.0 crates.io package**, checksum
`8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`, upstream revision
`6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`. The `MIT OR Apache-2.0` license files are unchanged.

The maintained distribution is published as **`contextdb-redb` 4.1.0**. Its library remains named
`redb`, so the retained upstream integration tests and ContextDB callers keep their compile-time
identity, while the engine's packaged Cargo metadata resolves `contextdb-redb` rather than upstream
`redb`. The package identity, description, repository, and library alias are ContextDB maintenance
metadata. They are intentionally outside the source-only upstream diff below.

[upstream-4.1.0.patch](upstream-4.1.0.patch) remains the complete source-only delta against that
exact package. It contains these eleven files; fork package metadata, test registration, and this note
are separate from the implementation diff.

| Changed source | Reason |
|---|---|
| `src/db.rs` | Expose bounded compaction steps, live-reader observations, and a retained statistics snapshot; the waiting-writer observer is test-only under `test-seams`. |
| `src/lib.rs` | Export the compaction cursor/progress and statistics snapshot types. |
| `src/transactions.rs` | Carry resumable compaction progress across finite page batches, commit relocation frees, and retain statistics snapshot ownership. |
| `src/transaction_tracker.rs` | Let maintenance yield when a foreground writer is waiting. |
| `src/multimap_table.rs` | Make subtree roots available to the bounded page traversal. |
| `src/tree_store/btree.rs` | Enumerate a bounded batch of tree pages from a continuation position. |
| `src/tree_store/table_tree.rs` | Enumerate and relocate the selected page batch and expose consistent statistics. |
| `src/tree_store/table_tree_base.rs` | Forward bounded page traversal for the table root. |
| `src/tree_store/page_store/cached_file.rs` | Admit page/cache-entry bytes before allocation and observe successful backend reads. |
| `src/tree_store/page_store/layout.rs` | Reuse the rounding remainder, preserving arithmetic while satisfying current strict lints. |
| `src/read_admission.rs` (added) | Scope read admission and backend-read observation callbacks to the active thread using RAII guards. |

The five upstream integration targets are copied **unchanged**, including redb's own backward
compatibility tests: `backward_compatibility`, `basic_tests`, `integration_tests`, `multimap_tests`,
and `multithreading_tests`. The upstream standalone `Cargo.lock` and test dependencies are retained.
Package verification copies the fork to a disposable input and verifies this lockfile's digest is
unchanged; cold dependency resolution never imports workspace patch entries into the fork lock.
The compatibility suite's `redb =2.6.0` dependency verifies storage-file interoperability; it does
not enable any older ContextDB sync protocol. Upstream unit tests and doctests remain enabled.

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
Propose the scoped read-admission and backend-read observation hooks separately with nested-guard
and error-path tests. Keep ContextDB policy out of those proposals. None of these proposals has been
submitted upstream.
