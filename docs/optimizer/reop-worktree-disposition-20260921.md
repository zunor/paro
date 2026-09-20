# re-op disposition — 2026-09-21

This receipt covers the EXPLAIN (COMPILE) transport-ownership repair and the
semantic cleanup of the re-op checkout. It is not a C2, F2, latency, or parity
claim.

## Active baseline

`re-op` is at `1b54b432` (`docs(protocol): record bounded terminal ownership`)
with an empty index and no active tracked or untracked experiment delta. The
production transport repair is `76cc9e0c` (`fix(protocol): bound cancelled
terminal output`); the current commit adds only its evidence and disposition
documents. The pre-transport source is recoverable at
`refs/codex/recovery/reop-before-transport` (`bd43b308`); the post-transport,
pre-parking point is recoverable at
`refs/codex/recovery/reop-after-transport-before-park` (`7f0f5251`).

## Parked mixed-worktree material

The original tracked WIP patch is retained at
`/private/tmp/paro-reop-transport-recovery.us6Zni/unstaged.patch`.
It is 111,260 bytes with SHA-256
`6588a2df89ab0e7fefa03d4ba1baa4449db3bc9e5996423478d85da103c0c2a3`.
The original index was empty; the empty staged patch is retained at
`staged.patch` with SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

The 83 untracked historical files were moved, not deleted, to
`/private/tmp/paro-reop-transport-recovery.us6Zni/parked-evidence-20260921/`.
Their original manifest is `untracked.sha256` (SHA-256
`0fc146428863a7f581458db7865fffb463596eaf1fcdf240e8f6296580f796e0`), and
all 83 hashes were rechecked after the move. The parked material is not added
to the production commit.

The tracked WIP is classified as follows:

| Material | Disposition | Reason |
| --- | --- | --- |
| Resident-contract removal/rebind changes in transformation and settlement | Parked in the patch | The freeze-time rebind correctness repair remains admitted; its removal has no independent proof. |
| Pre-Memo CTE normalization, normalization proofs, and harness mode | Parked in the patch | One unadmitted behavior experiment; no fragments were promoted separately. |
| Quality preflight/index traversal changes | Parked in the patch | Requires its own semantic and work-equivalence evidence. |
| Native-deferral assertions and staging adjustments | Parked in the patch | Requires an independent production-entry contract and regression gate. |
| Projection, aggregate, and join shape/value debug logs | Parked in the patch | Temporary probes are not a long-term diagnostic interface. |
| Checkpoint reader and benchmark environment propagation | Parked in the patch | Historical/experiment tooling is not made a second production semantic source. |
| Engine diagnostic counter relocation | Restored to the admitted baseline | The four counters remain present; only the unverified relocation was withdrawn. |

## Other worktrees and space

The following paths were not modified or deleted. Sizes are `du -sk` KiB at
the time of this receipt; all are owned by the current user account. Recovery
is by the recorded worktree path and HEAD, or by the branch/ref shown.

| Path | Size | State / HEAD | Recovery and disposition |
| --- | ---: | --- | --- |
| `/Users/linjunhong/workspace/paro` | 3,666,704 KiB | active `re-op` / `7f0f5251`, clean | Daily baseline; retain. |
| `/private/tmp/paro-convergence-20260920` | 164,775,480 KiB | clean branch `codex/optimizer-convergence-20260920` / `7f0f5251` | Existing clean validation tree; retain until transport evidence is independently archived, then a user-approved target-only cleanup is possible. |
| `/private/tmp/paro-convergence-c2-control-20260920` | 1,800,188 KiB | clean branch / `f450d93c` | C2 control evidence tree; retain. |
| `/private/tmp/paro-convergence-f2-parent` | 46,895,452 KiB | clean detached / `22d39fda` | Not an ancestor of re-op; retain as unique F2 material. |
| `/private/tmp/paro-convergence-f2-probe` | 46,895,792 KiB | clean detached / `d3097038` | Not an ancestor of re-op; retain as unique F2 material. |
| `/private/tmp/paro-q11-chain-control.SbtqO3` | 5,553,196 KiB | dirty detached / `99ef4f31` | Concurrent experiment; untouched, restore only through its owner. |
| `/private/tmp/paro-q11-chain-control-clean` | 1,790,904 KiB | clean detached / `99ef4f31` | Clean comparison tree; retain. |
| `/private/tmp/paro-struct-clean` | 30,035,048 KiB | dirty detached / `d3097038` | Concurrent experiment; untouched, restore only through its owner. |
| `/private/tmp/paro-nchain-clean` | 785,500 KiB | clean detached / `d4557653` | Historical evidence tree; retain. |
| `/private/tmp/paro-reop-transport-recovery.us6Zni` | 218,608 KiB | recovery archive | Contains patches, manifests, and parked evidence; retain until the user approves archival cleanup. |
| `/private/tmp/paro-transport-regress-final.U6jewj` | 9,840 KiB | fresh validation data only | Rebuildable SQL-regress data; retained for this receipt, no source or unique evidence. |
| `/private/tmp/paro-transport-regress-final2.J2BVi0` | 9,840 KiB | fresh validation data only | Rebuildable SQL-regress data; retained for this receipt, no source or unique evidence. |

The preserved inventory (`worktrees.json`) contains 42 registered
`ResidualNotWorktree` directories and 7 `Missing` registrations, 49 entries
in those two categories and 65 entries including the 16 live worktrees. They
were not pruned because a registration may be the only recovery pointer for
old evidence. The largest reclaim candidates are target caches under the
clean validation trees above; they are rebuildable in principle, but no
deletion was performed in this turn. Source trees, unique evidence, and
worktree registrations require a separate explicit cleanup decision.

The 15-file tracked WIP also has a durable recovery copy at
`/Users/linjunhong/paro-convergence-archive/20260921/reop-transport-recovery/`.
Its base is `c4a79abd`; `unstaged.patch` has SHA-256
`6588a2df89ab0e7fefa03d4ba1baa4449db3bc9e5996423478d85da103c0c2a3`, and the
empty staged patch has SHA-256
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. The
tracked portion is additionally reconstructible from
`refs/codex/recovery/reop-wip-tracked-20260921`; the parked 83-file evidence
manifest remains outside production Git. The temporary originals and other
worktrees were not deleted.
