# Runtime-filter facet identity includes its relation owner

Q01 on `dfdb0d84` (the physical phase repair already present) still fails
verification. The bounded error context identifies facet
`395fc3d53298a339b083d82eb1b97a01`, winner owner group 11, physical expression
146, and a deferred RF declaration with 18 anchors:
11, 69, 86, 96, 112, 126, 132, 138, 143, 147, 148, 158, 168, 169, 174, 180,
186, 194. Raw evidence: `c0/q01-facet-context-r1.json` in the private archive.
These IDs describe this one invocation only; none is a code selector.

The RF declaration fingerprint used the logical expression key and operator
but not the owning relation group. Repeated structure in distinct contextual
groups acquired the same capability identity. Declaration union then combined
their independent anchors into an oversized optional region. A previously
priced candidate still referred to that now-deferred identity.

Both initial and transformed RF declarations now include the canonical
relation owner at creation. A rewrite within that same owner can still retain
its inherited facet; group merge still recanonicalizes declarations without
renaming archived artifact identities. Required exact-scope facets retain
their existing identity and union semantics. No verifier is bypassed and no
old winner receives a fabricated ownership certificate.

The minimal fixture gives identical logical/operator fingerprints to two
distinct owners. Their RF identities and admitted regions must be distinct,
including a composite-region ceiling of one; rediscovery for the same owner
must have the same identity. Full optimizer suite: **1345 pass / 0 fail**,
`c0/rf-facet-owner-tests.log`. Release Q01/Q23 retest is required before
claiming their SQL failures closed. This slice does not certify arbitrary
facet retirement or the broader C2/C4 gates.
