# Download ContextDB binaries

Development binary delivery is prepared in [`build.yml`](../.github/workflows/build.yml).
No hosted pilot, native archive smoke or rolling development release has been
verified for this workflow yet. Do not assume a download exists because this
page describes it. Stable tagged releases and the existing Cargo/container
installation channels remain separate; see [installation](../README.md#install)
and [getting started](getting-started.md).

## Development downloads

After maintainer-authorized hosted qualification and publication, the rolling
`dev-latest` **prerelease** contains:

- `contextdb-x86_64-unknown-linux-gnu.tar.gz`: `contextdb`, `contextdb-server`, `LICENSE`.
- `contextdb-x86_64-unknown-linux-gnu.tar.gz.sha256`: archive checksum.
- `contextdb-x86_64-unknown-linux-gnu.json`: full source SHA, lockfile and tracked
  manifest hashes, pinned toolchain identity, compiler/Cargo versions, target,
  feature/profile configuration, runner image, workflow run/attempt and smoke receipt.

Initial download coverage is Linux x86_64. Other release targets await native
jobs that smoke their exact archives. This describes development download
coverage; it does not change ContextDB's OS or product support promises.

The permanent [latest development download](https://github.com/context-graph-ai/contextdb/releases/tag/dev-latest)
uses the stable filenames above. For direct downloads, use the permanent
[archive](https://github.com/context-graph-ai/contextdb/releases/download/dev-latest/contextdb-x86_64-unknown-linux-gnu.tar.gz),
[checksum](https://github.com/context-graph-ai/contextdb/releases/download/dev-latest/contextdb-x86_64-unknown-linux-gnu.tar.gz.sha256)
and [JSON receipt](https://github.com/context-graph-ai/contextdb/releases/download/dev-latest/contextdb-x86_64-unknown-linux-gnu.json)
URLs. These links remain the same across builds; their existence still awaits
hosted publication.

Once those assets exist, download all three together to an empty directory:

```bash
set -e
gh release download dev-latest --repo context-graph-ai/contextdb \
  --pattern 'contextdb-x86_64-unknown-linux-gnu.*'
sha256sum -c contextdb-x86_64-unknown-linux-gnu.tar.gz.sha256
python3 - <<'PYTHON'
import hashlib
import json
from pathlib import Path

base = 'contextdb-x86_64-unknown-linux-gnu'
receipt = json.loads(Path(base + '.json').read_text())
actual = hashlib.sha256(Path(base + '.tar.gz').read_bytes()).hexdigest()
if receipt['sha256'] != actual:
    raise SystemExit('Mixed download: receipt does not match archive; reject all three files')
print(json.dumps(receipt, indent=2))
PYTHON
tar -tzf contextdb-x86_64-unknown-linux-gnu.tar.gz
mkdir unpacked
tar -xzf contextdb-x86_64-unknown-linux-gnu.tar.gz -C unpacked
./unpacked/contextdb --version
./unpacked/contextdb-server --version
```

Inspect the JSON `source_sha` against the revision you intended to use. The
rolling name is mutable; save the archive, checksum and receipt together if you
need to reproduce a specific build. The checksum sidecar check and the JSON
receipt hash cross-check against the downloaded archive must both pass before unpacking. Reject the entire set on
either failure and download all three files again into an empty directory. These
checks detect corruption and mixed rolling downloads; no cryptographic
provenance attestation is claimed.
Development builds can change incompatibly and are not stable releases.

## How a development build qualifies

`ci.yml` owns correctness. Its `development-binaries` job calls `build.yml`
from the same commit only after successful `PR checks` on a canonical-repository
**push to `dev`**. The guard verifies the current CI run ID, attempt, workflow
identity, source SHA and successful aggregate through the Actions API. Overall
CI can still be in progress; waiting for its completion would wait on delivery
itself. Source, workflow and event SHAs must equal the current `dev` tip.
Automatic delivery is independent of the default branch and needs no change on
`main` when `dev` carries the caller and reusable workflow.

A manual dispatch must select `dev` and supply its full current 40-character SHA.
It requires exact-source same-repository push CI with successful `PR checks` on
the latest attempt of the selected run. If that run failed overall, only a
delivery failure is permitted; failures in any required or unrelated job refuse
delivery. This lets maintainers retry delivery after its own failure without
bypassing correctness. See the [manual retry command](../.github/CI.md#development-binary-delivery-buildyml).
The workflow must be present on the default branch for GitHub's manual dispatch
UI to discover it; automatic reuse from `dev` works independently of that UI.

The read-only build checks out that SHA without persisted credentials, uses
`Cargo.lock` with `--locked`, and builds the two named release binaries with
package-default features. It unpacks the archive into a fresh directory, runs
both binaries' `--version` and `--help`, and checks the CLI computes
`SELECT 1 + 1;` as JSON in `:memory:`. It then runs the complete existing
`installed-release-durable-sync-smoke.sh` against those exact unpacked CLI/server
bytes, including durable files and ticketed Iroh sync. The required
`contextdb-smoke-driver` verifier is built separately with the existing
`production-smoke-driver` feature into a separate target directory, after the
published binaries; it is never shipped and cannot replace the default-feature
server bytes. Every build or smoke failure blocks artifact upload. Successful
archives are retained as workflow artifacts for 14 days.

Only the separate publisher has `contents: write`. It rechecks source and CI,
validates the exact asset set, source/config/dependency identities and checksums,
then finds the channel by authenticated release listing across all pages,
including drafts. Duplicate channel releases or listing errors fail closed.
A first publication creates a hidden draft; an interrupted first upload retries
that same draft. `dev-latest` is created once at the **current default branch's
head**, whether the default is `main` or `dev`, and is never moved or forced.
The tag's commit is the bootstrap point, **not the binaries' source**. Use the
JSON receipt and release title/notes for the exact built source commit.

Only the publisher serializes, after qualification and build succeed, with
`queue: max` and cancellation disabled. The queue holds at most **100 pending
runs**; GitHub cancels new arrivals beyond that capacity. Processing order is
not guaranteed. The publisher checks the current `dev` tip on entry and directly
before every stable-name mutation; a superseded publisher skips successfully
with a summary. Job-level `queue: max` acceptance remains pending the first
hosted run. If GitHub rejects it, stop; do not silently substitute another queue.

Each publication first uploads a new write-once run-named group:
`contextdb-x86_64-unknown-linux-gnu-<sha12>-r<run_id>a<run_attempt>.tar.gz`,
with matching `.tar.gz.sha256` and `.json` sidecars. Here `sha12` is the first
12 characters of the full source SHA; run ID and attempt are in the receipt.
Any existing member name fails closed before mutation, even if its bytes match.
Run copies are never replaced or clobbered. Only after the entire group's actual
downloaded bytes pass digest verification may stable names be replaced: archive,
checksum, JSON receipt last. Stable uploads are also downloaded and digest-checked
before title/notes update and draft publication. Successful publication retains
the newest three complete owned run groups, ordered by their last member's
creation time, plus every group whose receipt equals the current stable receipt,
even if older. Only complete older unprotected groups may be pruned. Stable names,
incomplete groups and unrelated operator assets are preserved; failed publication
never prunes.

Per-asset replacement is not transactional: an interruption or advancing `dev`
can leave mixed stable assets. Reject mismatches using **both** checks above.
If stable verification fails, use the newest complete run-named set. Release
notes name the last completed publication's group; an interrupted publication
can leave a newer complete group visible among the release assets before notes
update. Choose all three files with exactly the same source/run/attempt prefix
and download only those exact names into an empty directory. Rename each by
removing `-<sha12>-r<run_id>a<run_attempt>` so the checksum sidecar's archive
filename matches, then run the same checksum and receipt hash checks above before
unpacking. Require the receipt's full `source_sha`, `run_id` and `run_attempt`
to match the chosen group. Run copies preserve the original archive and sidecar
bytes; they are not cryptographic provenance attestations. An interruption during
a retry preserves earlier fallback groups even if stable was already mixed.
A fresh successful delivery repairs the rolling stable names; the stationary tag
never identifies that repair's source.

Publication remains a separate maintainer-authorized hosted rollout. This local
workflow preparation neither creates downloads nor authorizes a remote dispatch,
release, tag, push or settings change. Stable `v*` tagging, crates publication,
container publication and manual performance qualification keep their existing
paths.

## Offline workflow controls

```bash
python3 scripts/ci-build-tests.py
python3 scripts/ci-required-tests.py
bash -n scripts/ci-build-package.sh
actionlint .github/workflows/build.yml
shellcheck scripts/ci-build-package.sh
```

The Python suite uses synthetic metadata and archives. Its passes prove guard
behavior only; native compilation, packaged smoke and hosted publication still
require actual runs. Existing source compilation defects are not bypassed by
this delivery path.

actionlint 1.7.12 rejects the documented job-level `queue: max` key in
`build.yml`. That parser error is not a lint pass, and it is not a workflow
defect to suppress: required CI does not run actionlint and instead uses the
exact strict offline semantic controls above (`ci-build-tests.py` plus
`ci-required-tests.py`). Those controls check the known queue, concurrency and
routing shapes; they do not validate every GitHub expression context. Hosted
acceptance of `queue: max` remains pending the first hosted run.
