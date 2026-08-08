# Bismuth Fork Recovery

Experimental, operator-run recovery tool for a stopped [Bismuth](https://github.com/bismuthfoundation/Bismuth) node that needs divergent-chain recovery or an intentional canonical suffix rewind.

By default, the tool finds the last block hash shared by the local ledger and multiple canonical peers. It can also intentionally remove a canonical suffix down to an operator-selected height or by a selected block count. Every mode shows a dry-run plan and—only after `--apply` plus exact confirmation—deletes the selected tail from the offline databases using the same row boundaries as Bismuth's rollback methods.

## Safety contract

- **Dry-run is the default.**
- The Bismuth node must be completely stopped.
- A listening node port, matching `node.py` process, SQLite write lock, failed `quick_check`, or WAL/SHM sidecar aborts recovery.
- Whenever peer verification is used, a single peer is never sufficient. Each queried height requires at least two agreeing hashes.
- In automatic mode, split or insufficient peer evidence aborts without mutation. Explicit/manual rollback is local-only by default; peer verification is optional.
- Before a plan is printed, ledger and hyper must each contain exactly one physical reward row at the retained target and must agree on its hash. Automatic mode and peer-verified explicit mode also require that hash to match canonical evidence.
- Apply rechecks the unchanged local tip and retained target under exclusion. Automatic mode also rechecks the first divergent block; explicit mode intentionally permits that deleted block to be canonical.
- Height `A` is preserved; deletion begins at `A + 1`.
- Apply reserves the configured node port and keeps the maintenance guard active through finalization.
- Ledger, hyper, and index are attached to one connection in `journal_mode=DELETE`, with `synchronous=FULL` and one `BEGIN EXCLUSIVE` transaction.
- A checksummed, tail-only recovery bundle and durable `prepared` manifest are fsynced before mutation.
- Original journal modes are fsynced in a `journal_guard` manifest before the first `PRAGMA journal_mode` change and restored before completion.
- Interrupted `journal_guard`/`journal_restored`/`prepared`/`committing`/`restoring` operations can be explicitly resumed; unknown database state fails closed.
- The tool never starts, stops, signals, or restarts `node.py`.
- It never reads wallet files or private keys.

This is an offline maintenance tool, not a consensus or protocol change. If no common ancestor is retained, SQLite is damaged, or post-validation fails, stop and use the normal archive/bootstrap or full-sync fallback.

## Requirements

- Python 3.9+
- An unmodified Foundation Bismuth checkout
- The normal Bismuth node environment

For a fresh Bismuth environment, follow upstream's full node installation sequence. `simple-crypt` is a manual prerequisite and is not installed by `requirements-node.txt` itself:

```bash
python3 -m pip install simple-crypt --no-deps
python3 -m pip install -r requirements-node.txt
```

The recovery tool adds **no runtime third-party dependency**. It uses Python's standard library and the checkout's existing `options.py` and `rpcconnections.py`.

## Download and run from the Bismuth base directory

```bash
cd /path/to/Bismuth
curl -fLO https://raw.githubusercontent.com/pig-g/bismuth-fork-recovery/main/fork_recovery.py

# Safe discovery only; no DB mutation.
python3 fork_recovery.py
```

You can instead keep this repository separate:

```bash
git clone https://github.com/pig-g/bismuth-fork-recovery.git
cd /path/to/Bismuth
python3 /path/to/bismuth-fork-recovery/fork_recovery.py \
  --bismuth-dir .
```

The default invocation mirrors Foundation upstream: it loads the Bismuth base directory's `config.txt`. If the conventional `config_custom.txt` exists in that directory, it is applied automatically as an override, just as upstream does.

Only specify `--config-custom` when the node itself uses an explicitly selected custom file, especially one with a different name or path. Use the same file for both commands so the recovery tool selects the same database paths and node port:

```bash
python3 node.py --config-custom my_node_config.txt
python3 fork_recovery.py --config-custom my_node_config.txt
```

The base directory is validated using upstream sentinel files. Config-relative database paths and relative custom-config paths are resolved against that base directory.

## Peer policy

Automatic fork recovery loads peers from the checkout's `suggested_peers.txt`; a strict majority with a minimum of two votes is required. Explicit/manual rollback does not contact peers unless verification is requested. To verify an explicit target against the default peer file, add `--verify-peers`. For operator-selected trusted peers:

```bash
python3 fork_recovery.py \
  --peer 203.0.113.10:5658 \
  --peer 203.0.113.11:5658 \
  --required-votes 2
```

Explicit `--peer` values replace the peer file. In an explicit rollback, `--peer`, a non-default `--peer-file`, `--required-votes`, or `--verify-peers` opts into peer verification. A bounded per-peer timeout defaults to 10 seconds and can be changed with `--peer-timeout`.

## Apply recovery

First inspect the dry-run plan. Then rerun with `--apply`:

```bash
python3 fork_recovery.py --apply
```

### Explicit rollback modes

The default command performs automatic fork recovery. To retain an exact block height and delete everything after it:

```bash
# Retain height 1000; delete heights 1001 through the current local tip.
python3 fork_recovery.py --rollback-to 1000
```

To remove an exact number of blocks from the current local tip:

```bash
# If the local tip is T, retain T-100 and delete T-99 through T.
python3 fork_recovery.py --rollback-blocks 100
```

These options are mutually exclusive with each other and with `--resume`. `--rollback-blocks` must be positive and must leave at least height 1 retained; `--rollback-to` must be between height 1 and the current local tip. From the retained target through the local tip, ledger and hyper must contain the same contiguous, integer-height, duplicate-free reward-block interval, including matching hashes at every height, so `COUNT` always means exactly that many blocks. The local tip and interval are rechecked under the apply lock, so a tip or row change after planning aborts rather than shifting the requested boundary.

Explicit rollback is a local operator action and does not require peer access. Ledger and hyper must each contain exactly one reward-bearing row at the retained target and must agree on the same hash. To add canonical target verification, use `--verify-peers` or explicit peer options; verification then uses the same strict quorum rules as automatic mode. Unlike automatic fork recovery, the first deleted block is allowed to be canonical. After restart, the node can download that suffix again when canonical peers still serve the required range and normal synchronization permits it; this is not a guarantee under every peer-availability or checkpoint condition.

**Automatic mode pins the peers to follow with `--peer`.** Automatic fork recovery ***without*** pinning qualifies the pool by actual reachability at the local tip: it probes every pool peer (in parallel), keeps only the peers that respond, requires at least 5 responsive peers, and then requires a strict majority of **those reachable peers** (`required_votes = reachable//2 + 1`). This avoids the old behavior of requiring a majority of the whole pool file (which lists mostly-dormant/dead bootstrap nodes and made consensus impractical when few peers were up). Peer hash queries for a height run concurrently, and a peer that fails is cached so it is not re-queried (and re-timed-out) on later ancestor-search heights. Passing one or more `--peer HOST:PORT` (which also replaces the pool file as the peer source) pins the trusted set: the default then becomes that **all pinned peers must agree**, so a single `--peer` reconciles the local chain to that one explicitly trusted peer (`required_votes = 1`). The ancestor search, exclusive-lock rollback, and fail-closed guarantees are unchanged; only who is trusted changes. Pass `--required-votes N` to override the threshold explicitly.

Both commands above are dry-runs. After inspecting the exact target and deletion range, add `--apply` to perform the operation:

```bash
python3 fork_recovery.py --rollback-to 1000 --apply
python3 fork_recovery.py --rollback-blocks 100 --apply
```

The tool prints an exact confirmation phrase such as:

```text
ROLLBACK 901-1000 TO 900 <full-common-ancestor-hash>
```

Nothing is changed unless the phrase matches exactly. Before mutation, the tool writes:

```text
recovery_bundles/fork_recovery_<UTC timestamp>/
├── ACTIVE                 # present only while an operation is pending
├── manifest.json
└── tail.json.gz
```

While an operation is pending, the Bismuth base directory also contains `.fork_recovery_active.json`, a durable pointer to that exact bundle. A new dry-run or apply refuses to plan over this marker; use `--resume`. Both active markers are removed only after validated completion.

The durable state sequence is `journal_guard` → `prepared` → `committing` → `restoring` → `complete`. The guard records exact database identities and original journal modes before the first mode change. The full manifest also records boundary and hashes, archive digest, schema signatures, and targeted/retained row fingerprints. After the database commit, the tool durably enters `restoring`, restores and verifies all original journal modes, reacquires write exclusion, and revalidates the database contents. Only then can it write `complete` and remove active markers.

If a process interruption leaves an active manifest in `journal_guard`, `journal_restored`, `prepared`, `committing`, or `restoring`, keep the node stopped and resume the exact operation instead of creating a new plan. `journal_restored` means mode restoration was durably recorded but active-marker cleanup may still need to finish:

```bash
python3 fork_recovery.py \
  --apply \
  --resume recovery_bundles/fork_recovery_<UTC timestamp>
```

Resume requires the root `.fork_recovery_active.json` marker, an exact match between its bundle path and `--resume`, a matching marker/manifest operation ID, and an exact `RESUME <operation-id>` confirmation. A `journal_guard`-only resume restores journal modes and exits without changing blockchain rows; the operator must rerun a dry-run. A full resume verifies the tail digest, exact database paths/inodes, schemas, retained contents, ancestor, and original journal modes. Each targeted table must be exactly PRE (matches the archive) or POST (no rollback rows remain). PRE/POST mixtures are completed with idempotent deletes; any UNKNOWN state aborts without mutation.

Every explicit rollback bundle binds its selection mode, rollback request, and optional peer policy into a recovery-intent digest that is independently recorded by the root active marker before the prepared manifest is installed; resume rejects either a manifest self-digest mismatch or a root/manifest intent mismatch. A local-manual bundle records a null peer policy and resumes without network access. A peer-verified bundle re-resolves its saved policy and rejects textual or resolved endpoint aliases before counting votes. If every targeted table in a peer-verified bundle is still PRE, resume refreshes target quorum under exclusion before the first resumed delete; once any table is POST, recovery follows the immutable bundle plan deterministically without a network dependency.

Apply binds a bounded window of retained rows just below the boundary plus the archived tail into the manifest, then performs the rollback as one atomic SQLite transaction. **By default the tool skips the whole-DB `PRAGMA quick_check` scan**: a tail delete never touches retained pages, so integrity of the retained data is guaranteed by the exclusive locks, the ancestor/tip hash checks, the journal guard, and normal WAL/journal atomicity, and a full scan would cost minutes on a large observer DB. Add `--integrity-check` to also run a full pre/post SQLite b-tree integrity scan (3 databases x 2 cycles) to additionally detect pre-existing external corruption. Commit/relock, journal restoration, and finalization repeat exact logical, metadata, and bounded fingerprint checks without scanning every database page.

If the stopped node left SQLite WAL sidecars (`-wal`/`-shm`), pass `--checkpoint-wal` to fold them into their main DBs (`PRAGMA wal_checkpoint(TRUNCATE)`) before the offline check runs. This only acts once the node is confirmed stopped (port closed, no node process) and fails closed if another connection holds a database busy; it is ignored on `--resume`, where sidecars are intentional. Without it, leftover sidecars abort the run.

After success, restart the unchanged node yourself:

```bash
python3 node.py
```

If the stopped node used an explicit custom config, include the same `--config-custom <file>` when planning, applying, resuming, and restarting it.

## What is modified

The direct transaction reproduces the row predicates of upstream rollback methods with boundary `A + 1`:

```text
transactions: block_height >= A+1 or block_height <= -(A+1)
misc/tokens/aliases: block_height >= A+1
```

That affects only rollback-range rows in:

- ledger `transactions` and `misc`
- hyperblock `transactions` and `misc`
- index `tokens` and `aliases`

It does not modify Bismuth source, peer files, configuration, wallet data, consensus rules, or wire protocol.

## Development and verification

```bash
python3 -m pip install pytest
python3 -m pytest tests/ -q
```

To run the real upstream `DbHandler` integration test:

```bash
git clone https://github.com/bismuthfoundation/Bismuth.git /tmp/Bismuth
BISMUTH_SOURCE_DIR=/tmp/Bismuth python3 -m pytest tests/ -q
```

The integration test uses temporary synthetic SQLite databases. It never applies recovery to a live or mainnet database.

## Status and limitations

- Compatibility is tested against a clean Foundation upstream checkout.
- Network evidence uses the existing public `api_getblockfromheight` RPC.
- The recovery bundle supports deterministic completion of an interrupted delete; it is not an automatic row-restore command.
- There has been no destructive test against a real mainnet operator database.
- No portable userspace tool can universally prove that every possible writer is stopped. Port reservation, process/cwd checks, sidecar refusal, and SQLite exclusion are complementary safeguards.
- Atomicity cannot be guaranteed across SQLite files and `manifest.json`, nor across arbitrary storage under power loss. The durable manifest and PRE/POST/UNKNOWN resume classification detect and safely resolve the expected interruption states.
- Always preserve normal operator backups and use small, controlled recovery rehearsals first.

## License

GPL-3.0-only. See [LICENSE](LICENSE).
