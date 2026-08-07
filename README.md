# Bismuth Fork Recovery

Experimental, operator-run recovery tool for a stopped [Bismuth](https://github.com/bismuthfoundation/Bismuth) node that is stranded on a divergent chain below the node's automatic rollback checkpoint.

The tool finds the last block hash shared by the local ledger and multiple canonical peers, shows a dry-run plan, and—only after `--apply` plus exact confirmation—deletes the divergent tail from the offline databases using the same row boundaries as Bismuth's rollback methods.

## Safety contract

- **Dry-run is the default.**
- The Bismuth node must be completely stopped.
- A listening node port, matching `node.py` process, SQLite write lock, failed `quick_check`, or WAL/SHM sidecar aborts recovery.
- A single peer is never sufficient. Each queried height requires at least two agreeing hashes.
- Split or insufficient peer evidence aborts without mutation.
- Before a plan is printed, ledger and hyper must each contain exactly one physical reward row at the retained ancestor, and both hashes must match canonical evidence.
- Apply rechecks the local tip and both sides of the ancestor boundary.
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
python3 fork_recovery.py --config-custom config_custom.txt
```

You can instead keep this repository separate:

```bash
git clone https://github.com/pig-g/bismuth-fork-recovery.git
cd /path/to/Bismuth
python3 /path/to/bismuth-fork-recovery/fork_recovery.py \
  --bismuth-dir . \
  --config-custom config_custom.txt
```

The base directory is validated using upstream sentinel files. Config-relative database paths are resolved against that base directory.

## Peer policy

By default, peers are loaded from the checkout's `suggested_peers.txt`; a strict majority with a minimum of two votes is required. For operator-selected trusted peers:

```bash
python3 fork_recovery.py \
  --peer 203.0.113.10:5658 \
  --peer 203.0.113.11:5658 \
  --required-votes 2
```

Explicit `--peer` values replace the peer file. A bounded per-peer timeout defaults to 10 seconds and can be changed with `--peer-timeout`.

## Apply recovery

First inspect the dry-run plan. Then rerun with `--apply`:

```bash
python3 fork_recovery.py \
  --config-custom config_custom.txt \
  --apply
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
  --config-custom config_custom.txt \
  --apply \
  --resume recovery_bundles/fork_recovery_<UTC timestamp>
```

Resume requires the root `.fork_recovery_active.json` marker, an exact match between its bundle path and `--resume`, a matching marker/manifest operation ID, and an exact `RESUME <operation-id>` confirmation. A `journal_guard`-only resume restores journal modes and exits without changing blockchain rows; the operator must rerun a dry-run. A full resume verifies the tail digest, exact database paths/inodes, schemas, retained contents, ancestor, and original journal modes. Each targeted table must be exactly PRE (matches the archive) or POST (no rollback rows remain). PRE/POST mixtures are completed with idempotent deletes; any UNKNOWN state aborts without mutation.

After success, restart the unchanged node yourself:

```bash
python3 node.py --config-custom config_custom.txt
```

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
