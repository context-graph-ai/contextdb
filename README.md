# macOS reader authentication proof

This tooling-only branch runs on pushes to ci-macos-reader-auth-proof. The workflow
and verifier are reviewed independently from the engine candidate. Only the committed
candidate_sha in scripts/macos-reader-proof.json selects the engine fix; null fails.
The verifier checks out itself at the workflow event SHA and fetches baseline and
candidate objects explicitly from the public HTTPS repository into isolated paths.
The exact SHA-256 checked test fixture is applied dynamically to each clean revision.
A pre-applied or incompatible fixture fails. Candidate scripts are never invoked.
Cargo builds and runs candidate engine code under the ordinary test trust boundary.

The baseline must compile, list exactly one mapped case, then fail at ordinary self-pin
admission with the authenticated registry diagnostic. The candidate must pass that
same case. Both halves are attempted; fetch/build/oracle/cleanup failures fail overall.
Commands, actual host/toolchain, verifier/workflow/candidate identities, source hashes,
separate paths, exits, elapsed times, and JUnit are uploaded as focused evidence.

Before accepting a result, the CI manager must verify run head and workflow SHA against
an independent review of the tooling commit, compare verifier source hashes to those
reviewed bytes (including the filled config), and verify candidate SHA against the
BS-approved immutable PUBLIC repair head using an externally delivered approval receipt. Green PR jobs or candidate-supplied
verifiers are not accepted. Source hashes are attestations to compare, not self-certification.
Publication requires explicit BS branch concurrence; no secrets, settings changes,
production merges, general CI gate waiver, or broad matrix run is part of this tool.

The evidence consumer requires --bs-approval-receipt pointing outside the downloaded
artifact (resolved symlinks included). The caller must obtain the actual manager-delivered
BS receipt independently; never construct it from candidate artifacts or API metadata.
The receipt is a JSON object with exactly these fixed-format fields:

- schema: "bs-public-repair-approval-v1"
- issuer: "BS"
- repository: "context-graph-ai/contextdb"
- approval_status: "approved"
- review_status: "reviewed"
- public_status: "public"
- approved_public_sha: the approved full lowercase 40-hex public repair SHA

The receipt SHA must equal both --candidate and artifact identity.fixed. Missing,
malformed, wrong-repository, unapproved, unreviewed, private, or mismatched receipts
fail before acceptance. These fields do not authenticate the issuer or provide
cryptographic provenance. Independent delivery and public reachability checks remain
the caller's responsibility. No real approval receipt or repair SHA is supplied here;
candidate_sha remains null and native proof remains unrun.

After independent review of tooling T and its filled five-file manifest, obtain the
actual receipt and public candidate metadata independently. Invoke the consumer as:

```bash
python3 "$ARTIFACT/verify-evidence.py" --repository repository.json --run run.json \
  --candidate-commit approved-public-candidate.json \
  --artifact downloaded-proof --reviewed-manifest reviewed-source-manifest.json \
  --verifier "$REVIEWED_T" --candidate "$BS_PUBLIC_REPAIR_SHA" \
  --bs-approval-receipt "$BS_APPROVAL_RECEIPT"
```

BS_APPROVAL_RECEIPT must name the independently delivered receipt outside downloaded-proof.
The reviewed manifest must include this README's final bytes and the filled config.
