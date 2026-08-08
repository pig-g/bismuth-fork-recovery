# Implementation Plan

## Goal

Deliver a standalone `fork_recovery.py` that can be downloaded into, or pointed at, an unmodified `bismuthfoundation/Bismuth` base directory. With the node stopped, it discovers the last local/canonical common ancestor and can remove only the divergent local tail without changing node code, consensus, block format, or wire protocol.

## Repository boundary

- Project: `pig-g/bismuth-fork-recovery`
- Bismuth remains an unmodified integration target.
- Runtime deliverable: one import-safe Python file.
- Runtime dependencies: Python standard library plus the normal Bismuth node environment.
- Never depend on fork-only `labs/mainnet-interaction`, `bismuthclient`, or `requirements-mainnet.txt`.

## Operator flow

1. Completely stop `node.py`.
2. Run the tool without `--apply`.
3. Validate Bismuth base/config/database paths.
4. Reject active port, write locks, WAL/SHM, or SQLite integrity failures.
5. In automatic mode, query multiple peers for block hashes using upstream `rpcconnections.Connection` and `api_getblockfromheight`; explicit/manual mode is local-only unless peer verification is requested.
6. In automatic mode, find the last common ancestor using exponential bracketing plus binary search; in explicit mode, resolve the operator's exact retained target locally.
7. Require exactly one physical reward row at the retained ancestor in both ledger and hyper and require both hashes to match; automatic and peer-verified explicit modes also require canonical peer evidence, then print the plan.
8. On `--apply`, require an exact boundary-and-hash confirmation phrase.
9. Reserve the configured node port; reject matching/ambiguous `node.py` processes.
10. Attach ledger, hyper, and index in fixed order, read their current journal modes, and fsync a `journal_guard` manifest with exact DB identities before the first mode change.
11. Force DELETE journals, FULL synchronous mode, and acquire one `BEGIN EXCLUSIVE` transaction without an implicit integrity rescan in the lock helper.
12. Re-run the process check and perform the local tip and `A`/`A+1` boundary validation under exclusion; perform the single full pre-mutation integrity cycle only when `--integrity-check` is set.
13. Export the exact rollback tail and a bounded retained window, then fsync a `prepared` manifest with database identities, schema signatures, and targeted/bounded-retained fingerprints.
14. Persist `committing`, then apply direct idempotent deletes matching upstream `rollback_under`, token, and alias boundaries.
15. Validate the resulting tip and absence of rollback-range rows inside the transaction, perform the full post-mutation integrity cycle only when `--integrity-check` is set, then commit last and immediately reacquire exclusion.
16. Revalidate committed logical postconditions and metadata without a duplicate full-page scan, then durably mark `restoring` while exclusion is still held.
17. Restore and verify all original journal modes, reacquire write exclusion, and revalidate metadata/postconditions without another duplicate full-page scan.
18. Only then mark the bundle `complete`, remove active markers, and tell the operator to restart the unchanged node.
19. On explicit `--resume`, restore a guard-only interruption without touching rows, or validate the full bundle before peer planning, classify every table PRE/POST/UNKNOWN, complete PRE or mixed PRE/POST states, and reject UNKNOWN.

## Fail-closed rules

- Whenever peer verification is active, require at least two agreeing hashes and a strict majority of selected peers.
- In automatic or peer-verified explicit mode, a tie, timeout-driven quorum failure, malformed response, or no retained ancestor aborts.
- Operator-selected rollback modes resolve to a validated retained target; they never accept a raw SQL deletion boundary.
- Common ancestor `A` is preserved; deletion starts at `A + 1`.
- Explicit `--rollback-to H` preserves `H`; `--rollback-blocks N` snapshots local tip `T`, preserves `T-N`, and deletes the exact suffix. These are peer-independent local operator actions by default; `--verify-peers` or explicit peer options opt into canonical target verification. From the retained target through the tip, both databases must have one contiguous, integer-height, duplicate-free reward-block row per height with identical hashes, checked during planning and again under apply exclusion.
- Automatic mode requires `local_hash(A+1) != canonical_hash(A+1)`. Explicit mode intentionally permits deletion of a canonical suffix, but revalidates the retained target and unchanged local tip under exclusion before mutation.
- Default is dry-run.
- Apply never starts/stops/restarts the node.
- Full operations use `journal_guard` → `prepared` → `committing` → `restoring` → `complete`; a guard-only interruption ends in idempotently resumable `journal_restored` after mode verification and before active-marker cleanup. Completion is recorded only after DB commit, original journal-mode restoration, reacquired exclusion, and final revalidation.
- Explicit bundles canonicalize selection mode, rollback request, and optional peer policy into a format-3 recovery-intent digest independently bound in the root active marker before the prepared manifest is installed. Local-manual bundles bind a null policy and resume without peers. Peer-verified all-PRE resumes re-resolve/deduplicate endpoints and refresh target quorum under exclusion; mixed/POST crash recovery follows the already-confirmed immutable plan without a network dependency.
- Resume never infers success from the ledger tip alone and never creates a new plan over a pending operation.
- No private keys, wallet data, tokens, or credentials enter logs or manifests.

## Test plan

- Peer-file parsing and explicit peer policy.
- Read-only local tip/hash queries.
- Multi-peer hash majority and split/no-quorum behavior.
- Exponential/binary common-ancestor search and no-ancestor refusal.
- Active writer and WAL/SHM refusal.
- Apply-lifetime port reservation and selected-root process/cwd checks.
- Competing-writer exclusion for all three attached databases.
- Injected failure rolls back ledger, hyper, and index together.
- Resume of PRE/POST states and refusal of UNKNOWN, changed schema, identity, archive, or retained history.
- Exact confirmation requirement.
- Recovery bundle deletion-boundary export.
- Default dry-run leaves databases unchanged.
- Apply preserves `A`, removes `A+1` and above, and completes the manifest.
- Clean Foundation upstream comparison plus direct-atomic integration against temporary production-schema SQLite fixtures.
- CI on Python 3.9–3.13 plus a clean upstream integration job.

## Out of scope

- Running-node automation.
- Automatic process signaling or restart.
- Consensus/checkpoint changes.
- New P2P commands.
- Automatic restoration of deleted rows from a bundle.
- Full bootstrap/archive fallback automation.
- Destructive tests against real operator/mainnet databases.
