# Replacement cohort: artifact identity failure

The original P1/C1/C2/P2 schedule finished, but final binary auditing rejected
P2: the shared target's `release/parod` still had the control binary SHA256
`4bb7edfca32192625ab61e5dd967a1be2b239d57e46e33c63020672c22d5079b`,
despite the main-checkout Cargo invocation reporting a fresh build and its
source attestation naming probe `5b83c533`. Its search counts also match control.
P1's probe binary was
`f0dd39fb6dac14093a3587cf6926b8102e8c96e7e2e565922484c74ce0a5243a`.
Thus successful `cargo build` plus hashing a shared top-level executable is
insufficient build provenance when alternating checkout roots. Both source
identities and all original observations survive; the original cohort is
Incomplete/Incomparable for the scheduled two-pair intervention claim. Do not
pool mislabeled P2 with probe or selectively replace its slow/fast samples.

One unrelated setup invocation also used the SQL directory as the CSV source;
it failed on missing schema.sql before any server, oracle or measured attempt
was created. Its error is retained separately; it contributed no samples.

## New complete cohort (registered before collection)

EvidenceId: aggregate-contracts-ab-v2-20260924. Run all four batches again;
reuse no timings from v1. Same claims, inputs, settings, thresholds, sample
counts and query order as registration.md. Seeds are 2026092421/2026092422.

Use the **same newly owned comparison checkout path for both versions**:
probe `5b83c533` and control `e37ae93f`. Keep re-op itself unchanged. Before
each version transition, verify the comparison checkout is clean, select the
exact detached revision, and invalidate only the release outputs for
paro-optimizer, paro-execution and paro-server. Cargo then rebuilds their
dependents; third-party dependencies and the shared target remain retained.
No query measurement overlaps a build. This is artifact preparation, not a
runtime performance intervention.

Prebuild through the maintained build_benchmark_server API and save a copy of
each source's binary outside the repository. Check binary digest immediately
before each collector invocation and after each completed cell against that
source's first rebuilt image. Same-source batches must have the same binary
hash; the two intervention arms must not have the same binary. A mismatch
stops the new cohort. Do not repair a completed sample's source label.

The measured source/harness stay unchanged; this registration fixes the
orchestration and checks, not SQL, optimizer policy or the benchmark timer.
Retain v1 separately and archive v2 as the only complete eligible comparison.
Total archive limit remains 16MiB for both cohorts combined. Temporary binaries
and build/server logs stay outside Git. Remove only this task's temporary
comparison checkout after collection; keep existing historical worktrees.
