#!/usr/bin/env python3
"""Offline automatic and explicit rollback for Bismuth node databases."""

from __future__ import annotations

import argparse
import errno
import gzip
import hashlib
import importlib.util
import json
import logging
import os
import shlex
import socket
import sqlite3
import subprocess
import sys
import threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, NamedTuple

ROOT_ACTIVE_MARKER = ".fork_recovery_active.json"
DATABASE_NAMES = ("ledger", "hyper", "index")
# When False (the default) the full SQLite structural scan (PRAGMA quick_check)
# is skipped. A tail rollback is a single atomic SQLite transaction that never
# touches retained pages, so a whole-DB page scan is not required for the
# rollback itself: retained-data safety comes from the exclusive locks, the
# ancestor/tip hash checks, the journal guard and normal WAL/journal atomicity.
# Pass --integrity-check to also run the full pre/post b-tree integrity scan
# (detects pre-existing external corruption) at the cost of minutes on a large DB.
INTEGRITY_CHECK = False
SQLITE_SCHEMAS = ("main", "hyperdb", "indexdb")


class CanonicalHashEvidence(NamedTuple):
    height: int
    selected_hash: str
    votes: dict[str, str | None]
    errors: dict[str, str]
    required_votes: int


class DbPaths(NamedTuple):
    ledger: Path
    hyper: Path
    index: Path


class LockedDatabases:
    def __init__(
        self,
        connection: sqlite3.Connection,
        original_journal_modes: tuple[str, str, str],
    ) -> None:
        self.connection = connection
        self.original_journal_modes = original_journal_modes
        self.committed_and_relocked = False

    def commit_and_relock(self) -> None:
        if self.committed_and_relocked:
            raise RuntimeError("database transaction was already committed")
        self.connection.commit()
        try:
            self.connection.execute("BEGIN EXCLUSIVE")
        except sqlite3.OperationalError as exc:
            raise RuntimeError(
                "database changed while reacquiring post-commit exclusion"
            ) from exc
        self.committed_and_relocked = True


class RecoveryPlan(NamedTuple):
    local_tip_height: int
    local_tip_hash: str
    ancestor_height: int
    ancestor_hash: str
    first_delete_height: int
    rollback_blocks: int


def load_peers(peer_file: Path, explicit_peers: list[str]) -> list[tuple[str, int]]:
    """Load explicit HOST:PORT peers or an upstream peers JSON dictionary."""
    if explicit_peers:
        peers = []
        for value in explicit_peers:
            host, separator, port = value.rpartition(":")
            if not separator or not host:
                raise ValueError(f"invalid peer, expected HOST:PORT: {value}")
            peers.append((host, int(port)))
    else:
        data = json.loads(Path(peer_file).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise TypeError(f"peer file must contain a JSON object: {peer_file}")
        peers = [(str(host), int(port)) for host, port in data.items()]

    return resolve_peer_endpoints(peers)


def resolve_peer_endpoints(peers: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """Resolve peers to unique IPv4 endpoints, rejecting textual and DNS aliases."""

    seen_labels: set[tuple[str, int]] = set()
    seen_endpoints: set[tuple[str, int]] = set()
    resolved_peers: list[tuple[str, int]] = []
    for host, port in peers:
        host = host.strip().rstrip(".")
        if not host:
            raise ValueError("peer host is empty")
        if not 1 <= port <= 65535:
            raise ValueError(f"invalid peer port: {host}:{port}")
        label = (host.casefold(), port)
        if label in seen_labels:
            raise ValueError(f"duplicate peer endpoint: {host}:{port}")
        seen_labels.add(label)
        try:
            addresses = {
                result[4][0]
                for result in socket.getaddrinfo(
                    host, port, family=socket.AF_INET, type=socket.SOCK_STREAM
                )
            }
        except socket.gaierror as exc:
            raise ValueError(f"could not resolve peer {host}:{port}: {exc}") from exc
        if len(addresses) != 1:
            raise ValueError(
                f"peer must resolve to exactly one IPv4 address: {host}:{port} -> {sorted(addresses)}"
            )
        endpoint = (addresses.pop(), port)
        if endpoint in seen_endpoints:
            raise ValueError(f"duplicate resolved peer endpoint: {endpoint[0]}:{port}")
        seen_endpoints.add(endpoint)
        resolved_peers.append(endpoint)
    return resolved_peers


def compute_required_votes(
    *,
    peers_pinned: bool,
    required_votes: int | None,
    peer_count: int,
) -> int:
    """Resolve the canonical-agreement vote threshold for a peer set.

    ``peers_pinned`` is True when the operator explicitly pinned the peers to
    follow (via ``--peer``): the default is then that ALL pinned peers must
    agree, which allows following a single explicitly trusted peer (1 vote).
    Without pinning, canonical evidence defaults to a strict majority of the
    whole pool (>= 2), which can be impractical when only a few peers in the
    pool are actually reachable.
    """
    if required_votes is not None:
        votes = required_votes
    elif peers_pinned:
        votes = peer_count
    else:
        votes = max(2, peer_count // 2 + 1)
    if not peers_pinned and votes < 2:
        raise ValueError("at least two canonical hash votes are required")
    if votes < 1 or votes > peer_count:
        raise ValueError(
            f"required votes ({votes}) exceeds peer count ({peer_count})"
        )
    return votes


def probe_reachable_peers(
    connection_factory,
    peers: list[tuple[str, int]],
    height: int,
    timeout: float,
) -> list[tuple[str, int]]:
    """Return the peers that actually respond with a block hash at ``height``.

    Used by unpinned automatic pool mode to base its consensus quorum on peers
    that are genuinely reachable, rather than on the whole pool file (which may
    list mostly-dormant or dead bootstap nodes). A responsive peer is one that
    returns a parseable block hash within ``timeout``.
    """
    reachable: list[tuple[str, int]] = []
    results = _query_peers_parallel(connection_factory, peers, height, timeout)
    by_label = {f'{peer[0]}:{peer[1]}': peer for peer in peers}
    for label, (value, _error) in results.items():
        if value is not None:
            reachable.append(by_label[label])
    return reachable


def resolve_db_paths(
    bismuth_root: Path,
    config: object,
    index_override: str | None,
) -> DbPaths:
    """Resolve configured database paths relative to the Bismuth checkout."""
    root = Path(bismuth_root).resolve()

    def resolve(value: str) -> Path:
        candidate = Path(value).expanduser()
        return (candidate if candidate.is_absolute() else root / candidate).resolve()

    return DbPaths(
        resolve(str(config.ledger_path)),
        resolve(str(config.hyper_path)),
        resolve(index_override or "static/index.db"),
    )


def assert_databases_offline(
    paths: DbPaths,
    allow_recovery_sidecars: bool = False,
    check_integrity: bool = True,
) -> None:
    """Fail closed unless all databases are present, clean, and write-unlocked."""
    for path in paths:
        if not path.is_file():
            raise RuntimeError(f"database does not exist: {path}")
        sidecars = [Path(f"{path}-wal"), Path(f"{path}-shm")]
        existing_sidecars = [str(sidecar) for sidecar in sidecars if sidecar.exists()]
        if existing_sidecars and not allow_recovery_sidecars:
            raise RuntimeError(
                "SQLite WAL/SHM sidecar present; cleanly stop the node first: "
                + ", ".join(existing_sidecars)
            )

        connection = sqlite3.connect(path, timeout=0.0)
        try:
            if check_integrity:
                check = connection.execute("PRAGMA quick_check").fetchone()
                if check != ("ok",):
                    raise RuntimeError(f"SQLite quick_check failed for {path}: {check}")
            connection.execute("BEGIN IMMEDIATE")
            connection.rollback()
        except sqlite3.OperationalError as exc:
            raise RuntimeError(f"database is write-locked: {path}") from exc
        finally:
            connection.close()


def checkpoint_wal(paths: DbPaths) -> None:
    """Fold SQLite WAL sidecars into their main DBs (safe offline finalization).

    Opens each database and, for those in journal_mode=WAL, runs
    ``PRAGMA wal_checkpoint(TRUNCATE)`` so committed frames are written into the
    main DB and the ``-wal``/``-shm`` sidecars are removed. Non-WAL databases
    (e.g. the index in ``journal_mode=delete``) are left untouched.

    Must only be called after the node is verified stopped (its port closed and
    no node process running). Fails closed if another connection is holding a
    database busy, so a leftover sidecar can never be dropped while data may
    still be in flight.
    """
    for path in (paths.ledger, paths.hyper, paths.index):
        if not path.is_file():
            continue
        connection = sqlite3.connect(str(path), timeout=30)
        try:
            mode = (
                str(connection.execute("PRAGMA journal_mode").fetchone()[0]).casefold()
            )
            if mode != "wal":
                continue
            row = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if row is not None and row[0]:
                raise RuntimeError(
                    "wal_checkpoint for %s is busy; another process is using the database"
                    % path
                )
        finally:
            connection.close()
        # A TRUNCATE checkpoint folds every committed frame into the main DB, so
        # any -wal/-shm that survive the close are empty and safe to remove. Some
        # SQLite builds leave a stale -shm behind; removing it (only after the
        # checkpoint succeeded and was not busy) keeps the offline precondition
        # clean without risking in-flight data.
        for suffix in ("-wal", "-shm"):
            sidecar = Path(f"{path}{suffix}")
            if sidecar.exists():
                sidecar.unlink()


@contextmanager
def hold_database_locks(
    paths: DbPaths,
    before_journal_change=None,
    restore_journal_modes: tuple[str, str, str] | None = None,
    allow_recovery_sidecars: bool = False,
):
    ordered = (paths.ledger, paths.hyper, paths.index)
    devices = {os.stat(path).st_dev for path in ordered}
    inodes = {(os.stat(path).st_dev, os.stat(path).st_ino) for path in ordered}
    if len(devices) != 1 or len(inodes) != 3:
        raise RuntimeError(
            "atomic recovery requires three distinct databases on one filesystem"
        )
    for path in ordered:
        for suffix in ("-wal", "-shm"):
            if Path(f"{path}{suffix}").exists() and not allow_recovery_sidecars:
                raise RuntimeError(f"refusing database with WAL/SHM sidecar: {path}{suffix}")
    connection = sqlite3.connect(paths.ledger, timeout=0, isolation_level=None)
    original_modes: tuple[str, str, str] | None = None
    try:
        connection.execute("ATTACH DATABASE ? AS hyperdb", (str(paths.hyper),))
        connection.execute("ATTACH DATABASE ? AS indexdb", (str(paths.index),))
        schemas = SQLITE_SCHEMAS
        modes = tuple(
            str(connection.execute(f"PRAGMA {schema}.journal_mode").fetchone()[0]).casefold()
            for schema in schemas
        )
        if any(mode not in {"delete", "wal"} for mode in modes):
            raise RuntimeError(f"unsupported SQLite journal modes: {modes}")
        if restore_journal_modes is not None and any(
            mode not in {"delete", "wal"} for mode in restore_journal_modes
        ):
            raise RuntimeError("invalid journal restoration target")
        original_modes = restore_journal_modes or modes
        if before_journal_change is not None:
            before_journal_change(modes)
        for schema in schemas:
            mode = connection.execute(
                f"PRAGMA {schema}.journal_mode=DELETE"
            ).fetchone()[0]
            if str(mode).casefold() != "delete":
                raise RuntimeError(f"could not set {schema} journal_mode=DELETE")
            connection.execute(f"PRAGMA {schema}.synchronous=FULL")
        connection.execute("BEGIN EXCLUSIVE")
        locked = LockedDatabases(connection, original_modes)
        yield locked
        if not locked.committed_and_relocked:
            locked.commit_and_relock()
    except sqlite3.OperationalError as exc:
        connection.rollback()
        raise RuntimeError("could not maintain atomic exclusive database transaction") from exc
    except BaseException:
        connection.rollback()
        raise
    finally:
        if connection.in_transaction:
            connection.rollback()
        if original_modes is not None:
            for schema, mode in zip(SQLITE_SCHEMAS, original_modes):
                connection.execute(f"PRAGMA {schema}.journal_mode={mode.upper()}")
        connection.close()


def journal_mode_mapping(modes: tuple[str, str, str]) -> dict[str, str]:
    return dict(zip(DATABASE_NAMES, modes))


def parse_original_journal_modes(manifest: dict[str, object]) -> tuple[str, str, str]:
    value = manifest.get("original_journal_modes")
    if not isinstance(value, dict) or set(value) != set(DATABASE_NAMES):
        raise TypeError("original journal modes are missing or malformed")
    modes = tuple(value[name] for name in DATABASE_NAMES)
    if any(not isinstance(mode, str) or mode not in {"delete", "wal"} for mode in modes):
        raise TypeError("original journal modes are invalid")
    return modes  # type: ignore[return-value]


def restore_original_journal_modes(
    paths: DbPaths, original_modes: tuple[str, str, str]
) -> None:
    connection = sqlite3.connect(paths.ledger, timeout=0, isolation_level=None)
    try:
        connection.execute("ATTACH DATABASE ? AS hyperdb", (str(paths.hyper),))
        connection.execute("ATTACH DATABASE ? AS indexdb", (str(paths.index),))
        for schema, expected in zip(SQLITE_SCHEMAS, original_modes):
            actual = str(
                connection.execute(
                    f"PRAGMA {schema}.journal_mode={expected.upper()}"
                ).fetchone()[0]
            ).casefold()
            if actual != expected:
                raise RuntimeError(
                    f"could not restore {schema} journal mode to {expected}"
                )
        verified = tuple(
            str(connection.execute(f"PRAGMA {schema}.journal_mode").fetchone()[0]).casefold()
            for schema in SQLITE_SCHEMAS
        )
        if verified != original_modes:
            raise RuntimeError(
                f"journal mode restoration verification failed: {verified}"
            )
    finally:
        connection.close()


@contextmanager
def hold_restored_database_locks(
    paths: DbPaths, expected_modes: tuple[str, str, str]
):
    connection = sqlite3.connect(paths.ledger, timeout=0, isolation_level=None)
    try:
        connection.execute("ATTACH DATABASE ? AS hyperdb", (str(paths.hyper),))
        connection.execute("ATTACH DATABASE ? AS indexdb", (str(paths.index),))
        actual_modes = tuple(
            str(connection.execute(f"PRAGMA {schema}.journal_mode").fetchone()[0]).casefold()
            for schema in SQLITE_SCHEMAS
        )
        if actual_modes != expected_modes:
            raise RuntimeError(
                f"journal modes changed before finalization: {actual_modes}"
            )
        connection.execute("BEGIN EXCLUSIVE")
        connection.execute("UPDATE main.transactions SET block_height=block_height WHERE 0")
        connection.execute(
            "UPDATE hyperdb.transactions SET block_height=block_height WHERE 0"
        )
        connection.execute("UPDATE indexdb.tokens SET block_height=block_height WHERE 0")
        yield connection
    except sqlite3.OperationalError as exc:
        raise RuntimeError("could not lock restored databases for finalization") from exc
    finally:
        if connection.in_transaction:
            connection.rollback()
        connection.close()


def apply_atomic_rollbacks(
    connection: sqlite3.Connection, first_delete_height: int
) -> None:
    if first_delete_height < 1:
        raise ValueError("first delete height must be positive")
    parameters = (first_delete_height, -first_delete_height)
    for schema in ("main", "hyperdb"):
        connection.execute(
            f"DELETE FROM {schema}.transactions "
            "WHERE block_height >= ? OR block_height <= ?",
            parameters,
        )
        connection.execute(
            f"DELETE FROM {schema}.misc WHERE block_height >= ?",
            (first_delete_height,),
        )
    connection.execute(
        "DELETE FROM indexdb.tokens WHERE block_height >= ?",
        (first_delete_height,),
    )
    connection.execute(
        "DELETE FROM indexdb.aliases WHERE block_height >= ?",
        (first_delete_height,),
    )


@contextmanager
def open_local_ledger_readonly(path: Path):
    """Open an existing SQLite ledger without creating or writing it."""
    resolved = Path(path).resolve()
    connection = sqlite3.connect(f"file:{resolved}?mode=ro&immutable=1", uri=True)
    try:
        yield connection
    finally:
        connection.close()


def local_tip(conn: sqlite3.Connection) -> tuple[int, str]:
    """Return the highest positive reward block height and hash."""
    row = conn.execute(
        "SELECT MAX(block_height) FROM transactions "
        "WHERE block_height > 0 AND reward != 0 "
    ).fetchone()
    if row is None or row[0] is None:
        raise ValueError("ledger contains no positive reward block")
    height = int(row[0])
    return height, unique_local_reward_hash(conn, height)


def unique_local_reward_hash(conn: sqlite3.Connection, height: int) -> str:
    rows = conn.execute(
        "SELECT block_hash FROM transactions "
        "WHERE block_height = ? AND reward != 0",
        (height,),
    ).fetchall()
    if len(rows) != 1 or not isinstance(rows[0][0], str):
        raise ValueError(f"ambiguous reward rows at local height {height}")
    return rows[0][0]


def assert_contiguous_reward_suffix(
    connection: sqlite3.Connection,
    table: str,
    retained_height: int,
    tip_height: int,
) -> tuple[tuple[int, str], ...]:
    if table not in {"transactions", "main.transactions", "hyperdb.transactions"}:
        raise ValueError(f"unsupported transaction table: {table}")
    actual = connection.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT block_height), "
        f"MIN(block_height), MAX(block_height) FROM {table} "
        "WHERE block_height >= ? AND block_height <= ? AND reward != 0",
        (retained_height, tip_height),
    ).fetchone()
    expected_count = tip_height - retained_height + 1
    expected = (
        expected_count,
        expected_count,
        retained_height,
        tip_height,
    )
    if actual != expected:
        raise RuntimeError(
            "explicit rollback requires a contiguous reward-block suffix: "
            f"{table} shape {actual}, expected {expected}"
        )
    rows = connection.execute(
        f"SELECT block_height, block_hash FROM {table} "
        "WHERE block_height >= ? AND block_height <= ? AND reward != 0 "
        "ORDER BY block_height",
        (retained_height, tip_height),
    ).fetchall()
    if any(type(height) is not int for height, _block_hash in rows):
        raise RuntimeError(
            "explicit rollback requires a contiguous integer reward-block interval: "
            f"{table}"
        )
    return tuple((height, validate_sha224_hash(block_hash)) for height, block_hash in rows)


def assert_explicit_reward_suffix(
    paths: DbPaths,
    ledger: sqlite3.Connection,
    retained_height: int,
    tip_height: int,
) -> None:
    ledger_suffix = assert_contiguous_reward_suffix(
        ledger, "transactions", retained_height, tip_height
    )
    with open_local_ledger_readonly(paths.hyper) as hyper:
        hyper_suffix = assert_contiguous_reward_suffix(
            hyper, "transactions", retained_height, tip_height
        )
    if ledger_suffix != hyper_suffix:
        raise RuntimeError("ledger and hyper explicit rollback suffixes disagree")


def validate_sha224_hash(value: object) -> str:
    if not isinstance(value, str) or len(value) != 56:
        raise ValueError("block hash is not a 56-character SHA-224 hex string")
    normalized = value.casefold()
    if any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError("block hash is not a 56-character SHA-224 hex string")
    return normalized


def _block_hash_from_response(response: object, height: int) -> str:
    if not isinstance(response, dict):
        raise TypeError("block response is not a JSON object")
    block = response.get(str(height), response.get(height))
    if not isinstance(block, dict) or not block.get("block_hash"):
        raise ValueError(f"peer returned no block hash at height {height}")
    return validate_sha224_hash(block["block_hash"])


def _query_peers_parallel(
    connection_factory,
    peers: list[tuple[str, int]],
    height: int,
    timeout: float,
) -> dict[str, tuple[str | None, str | None]]:
    """Query all peers for one height concurrently.

    Returns ``{label: (hash_or_None, error_or_None)}``. Each peer runs in its
    own worker (each still bounded by the per-peer deadline inside
    :func:`query_peer_hash_with_deadline`), so one slow or dead peer cannot
    stall the others: the whole pass finishes in roughly one timeout instead of
    ``len(peers) * timeout``.
    """
    results: dict[str, tuple[str | None, str | None]] = {}
    lock = threading.Lock()

    def work(peer: tuple[str, int]) -> None:
        label = f"{peer[0]}:{peer[1]}"
        try:
            value = query_peer_hash_with_deadline(
                connection_factory, peer, height, timeout
            )
        except Exception as exc:  # noqa: BLE001 - isolate untrusted peer failure
            with lock:
                results[label] = (None, str(exc))
        else:
            with lock:
                results[label] = (value, None)

    workers = max(1, min(len(peers), 12))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        list(executor.map(work, peers))
    return results


def canonical_hash_at(
    connection_factory,
    peers: list[tuple[str, int]],
    height: int,
    required_votes: int,
    query_timeout: float = 10.0,
    dead_peers: set[str] | None = None,
) -> CanonicalHashEvidence:
    """Query peers for one height in parallel and require a hash vote threshold.

    Peers already recorded in ``dead_peers`` (by label) are skipped without
    being queried again, preventing a known-dead peer from re-consuming the
    full timeout on every ancestor-search height.
    """
    votes: dict[str, str | None] = {}
    errors: dict[str, str] = {}
    to_query = peers
    if dead_peers:
        to_query = [
            peer for peer in peers if f"{peer[0]}:{peer[1]}" not in dead_peers
        ]
        for peer in peers:
            label = f"{peer[0]}:{peer[1]}"
            if label in dead_peers:
                votes[label] = None
                errors[label] = "peer unreachable (cached)"
    results = _query_peers_parallel(
        connection_factory, to_query, height, query_timeout
    )
    for label, (value, error) in results.items():
        votes[label] = value
        if error is not None:
            errors[label] = error
            if dead_peers is not None:
                dead_peers.add(label)

    counts = Counter(value for value in votes.values() if value is not None)
    if not counts:
        raise ValueError(f"no peer supplied a block hash at height {height}")
    ranked = counts.most_common()
    selected_hash, selected_votes = ranked[0]
    tied = len(ranked) > 1 and ranked[1][1] == selected_votes
    if tied or selected_votes < required_votes:
        raise ValueError(
            f"insufficient canonical hash agreement at height {height}: "
            f"{selected_votes}/{required_votes}"
        )
    return CanonicalHashEvidence(height, selected_hash, votes, errors, required_votes)


def query_peer_hash_with_deadline(
    connection_factory,
    peer: tuple[str, int],
    height: int,
    timeout: float,
) -> str:
    if timeout <= 0:
        raise ValueError("peer timeout must be positive")
    finished = threading.Event()
    state: dict[str, object] = {"connection": None}

    def query() -> None:
        try:
            connection = connection_factory(peer)
            state["connection"] = connection
            response = connection.command("api_getblockfromheight", [height])
            state["hash"] = _block_hash_from_response(response, height)
        except Exception as exc:  # noqa: BLE001 - report untrusted peer failure
            state["error"] = exc
        finally:
            connection = state.get("connection")
            if connection is not None:
                try:
                    connection.close()
                except Exception as close_exc:  # noqa: BLE001 - preserve diagnostics
                    state["close_error"] = close_exc
            finished.set()

    threading.Thread(target=query, daemon=True).start()
    if not finished.wait(timeout):
        connection = state.get("connection")
        if connection is not None:
            try:
                connection.close()
            except Exception as close_exc:  # noqa: BLE001 - preserve diagnostics
                state["close_error"] = close_exc
        raise TimeoutError(f"peer query exceeded {timeout} seconds")
    if "error" in state:
        raise state["error"]  # type: ignore[misc]
    if "hash" not in state:
        raise RuntimeError("peer query ended without a hash or error")
    return str(state["hash"])


def find_common_ancestor(
    local_hash_at_fn,
    canonical_hash_at_fn,
    local_tip_height: int,
) -> tuple[int, str]:
    """Find the last shared block with exponential bracketing and binary search."""
    if local_tip_height < 1:
        raise ValueError("local ledger has no searchable positive height")
    cache: dict[int, tuple[str, str]] = {}

    def matches(height: int) -> bool:
        if height not in cache:
            cache[height] = (
                str(local_hash_at_fn(height)),
                str(canonical_hash_at_fn(height)),
            )
        return cache[height][0] == cache[height][1]

    if matches(local_tip_height):
        return local_tip_height, cache[local_tip_height][0]

    mismatch_height = local_tip_height
    step = 1
    while True:
        probe = max(1, local_tip_height - step)
        if matches(probe):
            match_height = probe
            break
        mismatch_height = probe
        if probe == 1:
            raise ValueError("no common ancestor found in local ledger history")
        step *= 2

    while match_height + 1 < mismatch_height:
        middle = (match_height + mismatch_height) // 2
        if matches(middle):
            match_height = middle
        else:
            mismatch_height = middle

    return match_height, cache[match_height][0]


def apply_existing_rollbacks(handler, ancestor_height: int, node_stub: object) -> None:
    """Apply the same persistent rollback sequence used by Bismuth node.py."""
    delete_from = ancestor_height + 1
    try:
        handler.rollback_under(delete_from)
        handler.tokens_rollback(node_stub, delete_from)
        handler.aliases_rollback(node_stub, delete_from)
    finally:
        handler.close()


def confirmation_phrase(plan: RecoveryPlan) -> str:
    return (
        f"ROLLBACK {plan.first_delete_height}-{plan.local_tip_height} "
        f"TO {plan.ancestor_height} {plan.ancestor_hash}"
    )


def confirm_apply(plan: RecoveryPlan, input_fn=input) -> None:
    expected = confirmation_phrase(plan)
    actual = input_fn(f"Type exactly '{expected}' to continue: ")
    if actual != expected:
        raise RuntimeError("confirmation did not match; no database changes made")


def _export_rows(
    path: Path,
    table: str,
    query: str,
    params: tuple[int, ...],
) -> dict[str, object]:
    with open_local_ledger_readonly(path) as connection:
        columns = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM pragma_table_info(?)", (table,)
            )
        ]
        if not columns:
            raise RuntimeError(f"required table {table!r} is missing from {path}")
        rows = connection.execute(query, params).fetchall()
    return {"columns": columns, "rows": rows}


def _export_attached_rows(
    connection: sqlite3.Connection,
    schema: str,
    table: str,
    query: str,
    params: tuple[int, ...],
) -> dict[str, object]:
    pragma_queries = {
        ("main", "transactions"): "PRAGMA main.table_info(transactions)",
        ("main", "misc"): "PRAGMA main.table_info(misc)",
        ("hyperdb", "transactions"): "PRAGMA hyperdb.table_info(transactions)",
        ("hyperdb", "misc"): "PRAGMA hyperdb.table_info(misc)",
        ("indexdb", "tokens"): "PRAGMA indexdb.table_info(tokens)",
        ("indexdb", "aliases"): "PRAGMA indexdb.table_info(aliases)",
    }
    pragma = pragma_queries.get((schema, table))
    if pragma is None:
        raise ValueError(f"unsupported recovery table: {schema}.{table}")
    columns = [row[1] for row in connection.execute(pragma)]
    if not columns:
        raise RuntimeError(f"required table is missing: {schema}.{table}")
    rows = connection.execute(query, params).fetchall()
    return {"columns": columns, "rows": rows}


# Rollback only ever mutates the tail [boundary, tip]. The retained rows
# strictly below boundary are protected by the exclusive DB locks, the
# ancestor/tip hash checks and normal SQLite journaling — so verifying the
# whole retained database with a full read + JSON + sha256 pass is redundant
# and is the dominant cost on large observer DBs. We verify only a small
# bounded window of retained rows just below the boundary. This keeps a
# rollback of a very large DB O(window) instead of O(full DB), while still
# catching boundary-region tampering (see test_cli_resume_rejects_changed_retained_history).
RETAINED_WINDOW = 256

# Minimum number of independently responsive peers required before automatic
# (unpinned) pool-mode reconciliation will run. With fewer reachable peers the
# majority-of-connected consensus rests on too few independent sources; the
# operator should pin a peer with --peer instead.
POOL_MIN_PEERS = 5


def recovery_table_specs(boundary: int):
    low = boundary - RETAINED_WINDOW
    return (
        (
            "ledger.transactions",
            "ledger",
            "main",
            "transactions",
            "SELECT * FROM main.transactions WHERE block_height >= ? AND block_height < ? AND block_height > ? ORDER BY rowid",
            (low, boundary, -boundary),
            "SELECT * FROM transactions WHERE block_height >= ? AND block_height < ? AND block_height > ? ORDER BY rowid",
        ),
        (
            "ledger.misc",
            "ledger",
            "main",
            "misc",
            "SELECT * FROM main.misc WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
            (low, boundary),
            "SELECT * FROM misc WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
        ),
        (
            "hyper.transactions",
            "hyper",
            "hyperdb",
            "transactions",
            "SELECT * FROM hyperdb.transactions WHERE block_height >= ? AND block_height < ? AND block_height > ? ORDER BY rowid",
            (low, boundary, -boundary),
            "SELECT * FROM transactions WHERE block_height >= ? AND block_height < ? AND block_height > ? ORDER BY rowid",
        ),
        (
            "hyper.misc",
            "hyper",
            "hyperdb",
            "misc",
            "SELECT * FROM hyperdb.misc WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
            (low, boundary),
            "SELECT * FROM misc WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
        ),
        (
            "index.tokens",
            "index",
            "indexdb",
            "tokens",
            "SELECT * FROM indexdb.tokens WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
            (low, boundary),
            "SELECT * FROM tokens WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
        ),
        (
            "index.aliases",
            "index",
            "indexdb",
            "aliases",
            "SELECT * FROM indexdb.aliases WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
            (low, boundary),
            "SELECT * FROM aliases WHERE block_height >= ? AND block_height < ? ORDER BY rowid",
        ),
    )


def rows_fingerprint(rows) -> dict[str, object]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        encoded = json.dumps(
            list(row), separators=(",", ":"), default=str
        ).encode("utf-8")
        digest.update(encoded)
        digest.update(b"\n")
        count += 1
    return {"count": count, "sha256": digest.hexdigest()}


def schema_fingerprint(rows) -> str:
    return str(rows_fingerprint(rows)["sha256"])


def build_table_manifest(
    paths: DbPaths,
    boundary: int,
    tail: dict[str, object],
    locked_connection: sqlite3.Connection | None,
) -> dict[str, object]:
    path_by_name = dict(zip(paths._fields, paths))
    result: dict[str, object] = {}
    for (
        label,
        database,
        schema,
        table,
        locked_query,
        params,
        standalone_query,
    ) in recovery_table_specs(boundary):
        archived = tail[database][table]
        targeted = rows_fingerprint(archived["rows"])
        if locked_connection is not None:
            retained = rows_fingerprint(
                locked_connection.execute(locked_query, params)
            )
            schema_query = {
                ("main", "transactions"): "PRAGMA main.table_info(transactions)",
                ("main", "misc"): "PRAGMA main.table_info(misc)",
                ("hyperdb", "transactions"): "PRAGMA hyperdb.table_info(transactions)",
                ("hyperdb", "misc"): "PRAGMA hyperdb.table_info(misc)",
                ("indexdb", "tokens"): "PRAGMA indexdb.table_info(tokens)",
                ("indexdb", "aliases"): "PRAGMA indexdb.table_info(aliases)",
            }[(schema, table)]
            schema_digest = schema_fingerprint(
                locked_connection.execute(schema_query)
            )
        else:
            with open_local_ledger_readonly(path_by_name[database]) as connection:
                retained = rows_fingerprint(
                    connection.execute(standalone_query, params)
                )
                schema_digest = schema_fingerprint(
                    connection.execute("SELECT * FROM pragma_table_info(?)", (table,))
                )
        result[label] = {
            "schema_sha256": schema_digest,
            "targeted": targeted,
            "retained": retained,
        }
    return result


def serialize_evidence(
    evidence_by_height: dict[int, CanonicalHashEvidence],
) -> dict[str, object]:
    return {
        str(height): {
            "selected_hash": evidence.selected_hash,
            "required_votes": evidence.required_votes,
            "votes": evidence.votes,
            "errors": evidence.errors,
        }
        for height, evidence in sorted(evidence_by_height.items())
    }


def database_identities(paths: DbPaths) -> dict[str, dict[str, object]]:
    return {
        name: {
            "path": str(path),
            "st_dev": os.stat(path).st_dev,
            "st_ino": os.stat(path).st_ino,
        }
        for name, path in zip(DATABASE_NAMES, paths)
    }


def read_standalone_journal_modes(paths: DbPaths) -> tuple[str, str, str]:
    modes = []
    for path in paths:
        connection = sqlite3.connect(path)
        try:
            modes.append(
                str(connection.execute("PRAGMA journal_mode").fetchone()[0]).casefold()
            )
        finally:
            connection.close()
    result = tuple(modes)
    if len(result) != 3 or any(mode not in {"delete", "wal"} for mode in result):
        raise RuntimeError(f"unsupported SQLite journal modes: {result}")
    return result  # type: ignore[return-value]


def write_journal_guard(
    root: Path,
    bundle_dir: Path,
    paths: DbPaths,
    operation_id: str,
    original_modes: tuple[str, str, str],
) -> None:
    bundle = Path(bundle_dir)
    bundle.parent.mkdir(parents=True, exist_ok=True)
    fsync_directory(bundle.parent.parent)
    bundle.mkdir(exist_ok=False)
    fsync_directory(bundle.parent)
    manifest = {
        "format": 2,
        "operation_id": operation_id,
        "status": "journal_guard",
        "databases": database_identities(paths),
        "original_journal_modes": journal_mode_mapping(original_modes),
    }
    write_manifest_atomic(bundle / "manifest.json", manifest)
    write_root_active_marker(root, bundle)


def recovery_intent_digest(
    selection_mode: str,
    rollback_request: dict[str, int] | None,
    peer_policy: dict[str, object] | None,
) -> str:
    payload = {
        "selection_mode": selection_mode,
        "rollback_request": rollback_request,
        "peer_policy": peer_policy,
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def validate_manifest_recovery_intent(manifest: dict[str, object]) -> str | None:
    manifest_format = manifest.get("format")
    if manifest_format == 2:
        if manifest.get("selection_mode") == "explicit":
            raise RuntimeError(
                "legacy recovery manifest cannot contain unbound explicit recovery intent"
            )
        return None
    if manifest_format != 3:
        raise RuntimeError("unsupported recovery manifest format")
    selection_mode = manifest.get("selection_mode")
    rollback_request = manifest.get("rollback_request")
    peer_policy = manifest.get("peer_policy")
    if not isinstance(selection_mode, str):
        raise TypeError("recovery manifest selection mode is malformed")
    expected = recovery_intent_digest(
        selection_mode,
        rollback_request if isinstance(rollback_request, dict) else None,
        peer_policy if isinstance(peer_policy, dict) else None,
    )
    if manifest.get("recovery_intent_sha256") != expected:
        raise RuntimeError("recovery manifest recovery intent digest mismatch")
    return expected


def write_recovery_bundle(
    paths: DbPaths,
    plan: RecoveryPlan,
    bundle_dir: Path,
    evidence_by_height: dict[int, CanonicalHashEvidence] | None = None,
    locked_connection: sqlite3.Connection | None = None,
    operation_id: str | None = None,
    original_journal_modes: tuple[str, str, str] | None = None,
    selection_mode: str = "automatic",
    rollback_request: dict[str, int] | None = None,
    peer_policy: dict[str, object] | None = None,
) -> Path:
    """Export the exact rows targeted by rollback into a lightweight bundle."""
    bundle = Path(bundle_dir)
    if bundle.exists():
        guard = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        if (
            guard.get("status") != "journal_guard"
            or not operation_id
            or guard.get("operation_id") != operation_id
        ):
            raise RuntimeError("existing recovery bundle is not the active journal guard")
    else:
        bundle.mkdir(parents=True, exist_ok=False)
    operation_id = operation_id or str(uuid.uuid4())
    original_journal_modes = original_journal_modes or read_standalone_journal_modes(
        paths
    )
    boundary = plan.first_delete_height
    if locked_connection is None:
        export = lambda path, schema, table, locked_query, standalone_query, params: _export_rows(
            path, table, standalone_query, params
        )
    else:
        export = lambda path, schema, table, locked_query, standalone_query, params: _export_attached_rows(
            locked_connection, schema, table, locked_query, params
        )
    tail = {
        "ledger": {
            "transactions": export(
                paths.ledger,
                "main",
                "transactions",
                "SELECT * FROM main.transactions WHERE block_height >= ? OR block_height <= ?",
                "SELECT * FROM transactions WHERE block_height >= ? OR block_height <= ?",
                (boundary, -boundary),
            ),
            "misc": export(
                paths.ledger,
                "main",
                "misc",
                "SELECT * FROM main.misc WHERE block_height >= ?",
                "SELECT * FROM misc WHERE block_height >= ?",
                (boundary,),
            ),
        },
        "hyper": {
            "transactions": export(
                paths.hyper,
                "hyperdb",
                "transactions",
                "SELECT * FROM hyperdb.transactions WHERE block_height >= ? OR block_height <= ?",
                "SELECT * FROM transactions WHERE block_height >= ? OR block_height <= ?",
                (boundary, -boundary),
            ),
            "misc": export(
                paths.hyper,
                "hyperdb",
                "misc",
                "SELECT * FROM hyperdb.misc WHERE block_height >= ?",
                "SELECT * FROM misc WHERE block_height >= ?",
                (boundary,),
            ),
        },
        "index": {
            "tokens": export(
                paths.index,
                "indexdb",
                "tokens",
                "SELECT * FROM indexdb.tokens WHERE block_height >= ?",
                "SELECT * FROM tokens WHERE block_height >= ?",
                (boundary,),
            ),
            "aliases": export(
                paths.index,
                "indexdb",
                "aliases",
                "SELECT * FROM indexdb.aliases WHERE block_height >= ?",
                "SELECT * FROM aliases WHERE block_height >= ?",
                (boundary,),
            ),
        },
    }
    tail_path = bundle / "tail.json.gz"
    with gzip.open(tail_path, "wt", encoding="utf-8") as handle:
        json.dump(tail, handle, separators=(",", ":"), default=str)
    with tail_path.open("rb") as handle:
        os.fsync(handle.fileno())
    digest = hashlib.sha256(tail_path.read_bytes()).hexdigest()
    table_manifest = build_table_manifest(
        paths, boundary, tail, locked_connection
    )
    intent_digest = recovery_intent_digest(
        selection_mode, rollback_request, peer_policy
    )
    manifest = {
        "format": 3,
        "operation_id": operation_id,
        "status": "prepared",
        "selection_mode": selection_mode,
        "rollback_request": rollback_request,
        "peer_policy": peer_policy,
        "recovery_intent_sha256": intent_digest,
        **plan._asdict(),
        "databases": database_identities(paths),
        "original_journal_modes": journal_mode_mapping(original_journal_modes),
        "canonical_evidence": serialize_evidence(evidence_by_height or {}),
        "tables": table_manifest,
        "tail_file": tail_path.name,
        "tail_sha256": digest,
    }
    manifest_path = bundle / "manifest.json"
    write_manifest_atomic(manifest_path, manifest)
    fsync_directory(bundle)
    return bundle


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_manifest_atomic(manifest_path: Path, manifest: dict[str, object]) -> None:
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with temporary.open("rb") as handle:
        os.fsync(handle.fileno())
    temporary.replace(manifest_path)
    fsync_directory(manifest_path.parent)


def mark_recovery_committing(bundle_dir: Path) -> None:
    manifest_path = Path(bundle_dir) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "prepared":
        raise RuntimeError("recovery manifest is not in prepared state")
    manifest["status"] = "committing"
    manifest["committing_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_manifest_atomic(manifest_path, manifest)
    marker = Path(bundle_dir) / "ACTIVE"
    marker.write_text(str(manifest["operation_id"]) + "\n", encoding="utf-8")
    with marker.open("rb") as handle:
        os.fsync(handle.fileno())
    fsync_directory(Path(bundle_dir))


def mark_recovery_restoring(bundle_dir: Path) -> None:
    manifest_path = Path(bundle_dir) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "committing":
        raise RuntimeError("recovery manifest is not in committing state")
    manifest["status"] = "restoring"
    manifest["restoring_at_utc"] = datetime.now(timezone.utc).isoformat()
    write_manifest_atomic(manifest_path, manifest)


def write_root_active_marker(
    root: Path, bundle_dir: Path, recovery_intent_sha256: str | None = None
) -> None:
    manifest = json.loads(
        (Path(bundle_dir) / "manifest.json").read_text(encoding="utf-8")
    )
    marker = {
        "operation_id": manifest.get("operation_id"),
        "bundle": str(Path(bundle_dir).resolve()),
    }
    intent_digest = recovery_intent_sha256 or manifest.get("recovery_intent_sha256")
    if intent_digest is not None:
        if not isinstance(intent_digest, str) or len(intent_digest) != 64:
            raise RuntimeError("recovery intent digest is malformed")
        marker["recovery_intent_sha256"] = intent_digest
    write_manifest_atomic(Path(root) / ROOT_ACTIVE_MARKER, marker)


def read_root_active_marker(root: Path) -> dict[str, object] | None:
    marker_path = Path(root) / ROOT_ACTIVE_MARKER
    if not marker_path.exists():
        return None
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    if (
        not isinstance(marker, dict)
        or not isinstance(marker.get("operation_id"), str)
        or not isinstance(marker.get("bundle"), str)
    ):
        raise TypeError("root recovery marker is malformed")
    return marker


def complete_recovery_bundle(bundle_dir: Path, root: Path | None = None) -> None:
    manifest_path = Path(bundle_dir) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") not in {"restoring", "complete"}:
        raise RuntimeError("recovery manifest is not in restoring state")
    if manifest.get("status") == "restoring":
        manifest["status"] = "complete"
        manifest["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
        write_manifest_atomic(manifest_path, manifest)
    marker = Path(bundle_dir) / "ACTIVE"
    if marker.exists():
        marker.unlink()
        fsync_directory(Path(bundle_dir))
    if root is not None:
        root_marker = Path(root) / ROOT_ACTIVE_MARKER
        if root_marker.exists():
            root_marker.unlink()
            fsync_directory(Path(root))


def assert_post_recovery(paths: DbPaths, plan: RecoveryPlan) -> None:
    if INTEGRITY_CHECK:
        for path in paths:
            with open_local_ledger_readonly(path) as connection:
                check = connection.execute("PRAGMA quick_check").fetchone()
                if check != ("ok",):
                    raise RuntimeError(
                        f"post-recovery quick_check failed for {path}: {check}"
                    )

    with open_local_ledger_readonly(paths.ledger) as ledger:
        tip = local_tip(ledger)
        if tip != (plan.ancestor_height, plan.ancestor_hash):
            raise RuntimeError(
                f"post-recovery ledger tip mismatch: {tip}, expected "
                f"{(plan.ancestor_height, plan.ancestor_hash)}"
            )
    boundary = plan.first_delete_height
    checks = (
        (paths.ledger, "transactions", "SELECT COUNT(*) FROM transactions WHERE block_height >= ? OR block_height <= ?", (boundary, -boundary)),
        (paths.ledger, "misc", "SELECT COUNT(*) FROM misc WHERE block_height >= ?", (boundary,)),
        (paths.hyper, "transactions", "SELECT COUNT(*) FROM transactions WHERE block_height >= ? OR block_height <= ?", (boundary, -boundary)),
        (paths.hyper, "misc", "SELECT COUNT(*) FROM misc WHERE block_height >= ?", (boundary,)),
        (paths.index, "tokens", "SELECT COUNT(*) FROM tokens WHERE block_height >= ?", (boundary,)),
        (paths.index, "aliases", "SELECT COUNT(*) FROM aliases WHERE block_height >= ?", (boundary,)),
    )
    for path, table, query, params in checks:
        with open_local_ledger_readonly(path) as connection:
            remaining = connection.execute(query, params).fetchone()[0]
        if remaining:
            raise RuntimeError(
                f"post-recovery validation found {remaining} rollback rows in {path}:{table}"
            )


def assert_post_recovery_locked(
    connection: sqlite3.Connection,
    plan: RecoveryPlan,
    check_integrity: bool = True,
) -> None:
    if check_integrity and INTEGRITY_CHECK:
        for schema in ("main", "hyperdb", "indexdb"):
            check = connection.execute(f"PRAGMA {schema}.quick_check").fetchone()
            if check != ("ok",):
                raise RuntimeError(
                    f"post-recovery quick_check failed for {schema}: {check}"
                )
    expected_tip = (plan.ancestor_height, plan.ancestor_hash)
    tip = attached_tip(connection, "main")
    if tip != expected_tip:
        raise RuntimeError(
            f"post-recovery ledger tip mismatch: {tip}, expected "
            f"{expected_tip}"
        )
    hyper_tip = attached_tip(connection, "hyperdb")
    if hyper_tip != expected_tip:
        raise RuntimeError(
            f"post-recovery hyper tip mismatch: {hyper_tip}, expected {expected_tip}"
        )
    boundary = plan.first_delete_height
    checks = (
        (
            "main.transactions",
            "SELECT COUNT(*) FROM main.transactions WHERE block_height >= ? OR block_height <= ?",
            (boundary, -boundary),
        ),
        (
            "main.misc",
            "SELECT COUNT(*) FROM main.misc WHERE block_height >= ?",
            (boundary,),
        ),
        (
            "hyperdb.transactions",
            "SELECT COUNT(*) FROM hyperdb.transactions WHERE block_height >= ? OR block_height <= ?",
            (boundary, -boundary),
        ),
        (
            "hyperdb.misc",
            "SELECT COUNT(*) FROM hyperdb.misc WHERE block_height >= ?",
            (boundary,),
        ),
        (
            "indexdb.tokens",
            "SELECT COUNT(*) FROM indexdb.tokens WHERE block_height >= ?",
            (boundary,),
        ),
        (
            "indexdb.aliases",
            "SELECT COUNT(*) FROM indexdb.aliases WHERE block_height >= ?",
            (boundary,),
        ),
    )
    for table, query, params in checks:
        remaining = connection.execute(query, params).fetchone()[0]
        if remaining:
            raise RuntimeError(
                f"post-recovery validation found {remaining} rollback rows in {table}"
            )


def attached_tip(connection: sqlite3.Connection, schema: str) -> tuple[int, str]:
    queries = {
        "main": (
            "SELECT MAX(block_height) FROM main.transactions WHERE reward != 0",
            (
                "SELECT block_hash FROM main.transactions "
                "WHERE block_height = ? AND reward != 0"
            ),
        ),
        "hyperdb": (
            "SELECT MAX(block_height) FROM hyperdb.transactions WHERE reward != 0",
            (
                "SELECT block_hash FROM hyperdb.transactions "
                "WHERE block_height = ? AND reward != 0"
            ),
        ),
    }
    try:
        height_query, hash_query = queries[schema]
    except KeyError as exc:
        raise ValueError(f"unsupported transaction schema: {schema}") from exc
    height = connection.execute(height_query).fetchone()[0]
    if height is None:
        raise RuntimeError(f"{schema} has no reward-bearing blocks")
    hashes = connection.execute(hash_query, (height,)).fetchall()
    if len(hashes) != 1 or not isinstance(hashes[0][0], str):
        raise RuntimeError(f"ambiguous reward-bearing block at {schema} height {height}")
    return int(height), hashes[0][0]


def attached_hash_at(
    connection: sqlite3.Connection, schema: str, height: int
) -> str:
    queries = {
        "main": (
            "SELECT block_hash FROM main.transactions "
            "WHERE block_height = ? AND reward != 0"
        ),
        "hyperdb": (
            "SELECT block_hash FROM hyperdb.transactions "
            "WHERE block_height = ? AND reward != 0"
        ),
    }
    try:
        query = queries[schema]
    except KeyError as exc:
        raise ValueError(f"unsupported transaction schema: {schema}") from exc
    hashes = connection.execute(query, (height,)).fetchall()
    if len(hashes) != 1 or not isinstance(hashes[0][0], str):
        raise RuntimeError(f"ambiguous reward-bearing block at {schema} height {height}")
    return hashes[0][0]


def assert_locked_integrity(connection: sqlite3.Connection) -> None:
    if not INTEGRITY_CHECK:
        return
    quick_checks = {
        "main": "PRAGMA main.quick_check",
        "hyperdb": "PRAGMA hyperdb.quick_check",
        "indexdb": "PRAGMA indexdb.quick_check",
    }
    for schema, query in quick_checks.items():
        check = connection.execute(query).fetchone()
        if check != ("ok",):
            raise RuntimeError(f"locked quick_check failed for {schema}: {check}")


def assert_locked_preconditions(
    connection: sqlite3.Connection,
    expected_tip: tuple[int, str],
    retained_ancestor: tuple[int, str] | None = None,
) -> None:
    assert_locked_integrity(connection)
    ledger_tip = attached_tip(connection, "main")
    if ledger_tip != expected_tip:
        raise RuntimeError(
            f"locked ledger tip changed: {ledger_tip}; expected {expected_tip}"
        )
    hyper_tip = attached_tip(connection, "hyperdb")
    if hyper_tip != expected_tip:
        raise RuntimeError("inconsistent database tail: ledger and hyper tips do not match")
    if retained_ancestor is not None:
        ancestor_height, ancestor_hash = retained_ancestor
        if attached_hash_at(connection, "main", ancestor_height) != ancestor_hash:
            raise RuntimeError("locked ledger ancestor hash changed")
        if attached_hash_at(connection, "hyperdb", ancestor_height) != ancestor_hash:
            raise RuntimeError("locked hyper ancestor hash changed")
    tip_height = expected_tip[0]
    checks = (
        (
            "main.transactions",
            "SELECT COUNT(*) FROM main.transactions WHERE block_height > ? OR block_height < ?",
            (tip_height, -tip_height),
        ),
        (
            "main.misc",
            "SELECT COUNT(*) FROM main.misc WHERE block_height > ?",
            (tip_height,),
        ),
        (
            "hyperdb.transactions",
            "SELECT COUNT(*) FROM hyperdb.transactions WHERE block_height > ? OR block_height < ?",
            (tip_height, -tip_height),
        ),
        (
            "hyperdb.misc",
            "SELECT COUNT(*) FROM hyperdb.misc WHERE block_height > ?",
            (tip_height,),
        ),
        (
            "indexdb.tokens",
            "SELECT COUNT(*) FROM indexdb.tokens WHERE block_height > ?",
            (tip_height,),
        ),
        (
            "indexdb.aliases",
            "SELECT COUNT(*) FROM indexdb.aliases WHERE block_height > ?",
            (tip_height,),
        ),
    )
    for table, query, params in checks:
        count = connection.execute(query, params).fetchone()[0]
        if count:
            raise RuntimeError(
                f"inconsistent database tail: {count} rows beyond local tip in {table}"
            )


def load_resume_bundle(
    bundle_dir: Path, paths: DbPaths
) -> tuple[dict[str, object], RecoveryPlan, dict[str, object]]:
    bundle = Path(bundle_dir).resolve()
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") not in {2, 3} or manifest.get("status") not in {
        "prepared",
        "committing",
        "restoring",
        "complete",
    }:
        raise RuntimeError("bundle is not a resumable recovery operation")
    validate_manifest_recovery_intent(manifest)
    tail_path = bundle / str(manifest.get("tail_file"))
    actual_digest = hashlib.sha256(tail_path.read_bytes()).hexdigest()
    if actual_digest != manifest.get("tail_sha256"):
        raise RuntimeError("recovery tail archive digest mismatch")
    database_manifest = manifest.get("databases")
    if not isinstance(database_manifest, dict):
        raise TypeError("recovery manifest database identities are missing")
    for name, path in zip(paths._fields, paths):
        identity = database_manifest.get(name)
        stat = os.stat(path)
        if not isinstance(identity, dict) or identity != {
            "path": str(path),
            "st_dev": stat.st_dev,
            "st_ino": stat.st_ino,
        }:
            raise RuntimeError(f"recovery database identity changed: {name}")
    plan = RecoveryPlan(
        *(manifest[field] for field in RecoveryPlan._fields)  # type: ignore[arg-type]
    )
    validate_resume_plan_metadata(manifest, plan)
    with gzip.open(tail_path, "rt", encoding="utf-8") as handle:
        tail = json.load(handle)
    if not isinstance(tail, dict):
        raise TypeError("recovery tail archive is malformed")
    parse_original_journal_modes(manifest)
    return manifest, plan, tail


def validate_resume_plan_metadata(
    manifest: dict[str, object], plan: RecoveryPlan
) -> None:
    integer_fields = (
        plan.local_tip_height,
        plan.ancestor_height,
        plan.first_delete_height,
        plan.rollback_blocks,
    )
    if any(type(value) is not int for value in integer_fields):
        raise TypeError("recovery plan heights are malformed")
    if (
        plan.ancestor_height < 1
        or plan.local_tip_height < plan.ancestor_height
        or plan.first_delete_height != plan.ancestor_height + 1
        or plan.rollback_blocks != plan.local_tip_height - plan.ancestor_height
    ):
        raise RuntimeError("recovery plan boundary arithmetic is inconsistent")

    has_mode = "selection_mode" in manifest
    has_request = "rollback_request" in manifest
    if not has_mode and not has_request:
        return
    if not has_mode or not has_request:
        raise RuntimeError("recovery rollback selection metadata is incomplete")
    mode = manifest["selection_mode"]
    request = manifest["rollback_request"]
    if mode == "automatic":
        if request is not None:
            raise RuntimeError("automatic recovery has an unexpected rollback request")
        return
    if mode != "explicit" or not isinstance(request, dict) or len(request) != 1:
        raise RuntimeError("recovery rollback selection metadata is malformed")
    if "blocks" in request:
        blocks = request["blocks"]
        matches = type(blocks) is int and blocks > 0 and blocks == plan.rollback_blocks
    elif "to_height" in request:
        height = request["to_height"]
        matches = type(height) is int and height == plan.ancestor_height
    else:
        matches = False
    if not matches:
        raise RuntimeError("rollback request does not match recovery plan")
    if manifest.get("peer_policy") is None:
        if manifest.get("canonical_evidence") != {}:
            raise RuntimeError(
                "local manual recovery cannot contain canonical peer evidence"
            )
        return
    parse_explicit_peer_policy(manifest, plan)


def parse_explicit_peer_policy(
    manifest: dict[str, object], plan: RecoveryPlan | None = None
) -> tuple[list[tuple[str, int]], int, float]:
    policy = manifest.get("peer_policy")
    if not isinstance(policy, dict):
        raise TypeError("explicit recovery peer policy is missing")
    raw_peers = policy.get("peers")
    required_votes = policy.get("required_votes")
    query_timeout = policy.get("query_timeout")
    if not isinstance(raw_peers, list):
        raise TypeError("explicit recovery peer policy is malformed")
    peers: list[tuple[str, int]] = []
    for raw_peer in raw_peers:
        if (
            not isinstance(raw_peer, list)
            or len(raw_peer) != 2
            or not isinstance(raw_peer[0], str)
            or not raw_peer[0]
            or type(raw_peer[1]) is not int
            or not 1 <= raw_peer[1] <= 65535
        ):
            raise RuntimeError("explicit recovery peer policy is malformed")
        peers.append((raw_peer[0], raw_peer[1]))
    labels = [f"{host}:{port}" for host, port in peers]
    if len(set(labels)) != len(labels):
        raise RuntimeError("explicit recovery peer policy contains duplicate peers")
    if (
        type(required_votes) is not int
        or required_votes < 2
        or required_votes > len(peers)
        or isinstance(query_timeout, bool)
        or not isinstance(query_timeout, (int, float))
        or not 0 < float(query_timeout) <= 300
    ):
        raise RuntimeError("explicit recovery peer policy is malformed")
    if plan is not None:
        all_evidence = manifest.get("canonical_evidence")
        evidence = (
            all_evidence.get(str(plan.ancestor_height))
            if isinstance(all_evidence, dict)
            else None
        )
        if not isinstance(evidence, dict):
            raise RuntimeError("explicit recovery target evidence is missing")
        votes = evidence.get("votes")
        errors = evidence.get("errors")
        if not isinstance(votes, dict) or not isinstance(errors, dict):
            raise RuntimeError("explicit recovery target evidence is malformed")
        recorded_labels = set(votes) | set(errors)
        if (
            recorded_labels != set(labels)
            or evidence.get("required_votes") != required_votes
            or validate_sha224_hash(evidence.get("selected_hash"))
            != plan.ancestor_hash
        ):
            raise RuntimeError("explicit recovery peer policy does not match target evidence")
    return resolve_peer_endpoints(peers), required_votes, float(query_timeout)


def classify_resume_tables(
    connection: sqlite3.Connection, tail: dict[str, object], boundary: int
) -> dict[str, str]:
    specs = (
        (
            "ledger.transactions",
            "SELECT * FROM main.transactions WHERE block_height >= ? OR block_height <= ?",
            (boundary, -boundary),
            "ledger",
            "transactions",
        ),
        (
            "ledger.misc",
            "SELECT * FROM main.misc WHERE block_height >= ?",
            (boundary,),
            "ledger",
            "misc",
        ),
        (
            "hyper.transactions",
            "SELECT * FROM hyperdb.transactions WHERE block_height >= ? OR block_height <= ?",
            (boundary, -boundary),
            "hyper",
            "transactions",
        ),
        (
            "hyper.misc",
            "SELECT * FROM hyperdb.misc WHERE block_height >= ?",
            (boundary,),
            "hyper",
            "misc",
        ),
        (
            "index.tokens",
            "SELECT * FROM indexdb.tokens WHERE block_height >= ?",
            (boundary,),
            "index",
            "tokens",
        ),
        (
            "index.aliases",
            "SELECT * FROM indexdb.aliases WHERE block_height >= ?",
            (boundary,),
            "index",
            "aliases",
        ),
    )
    states: dict[str, str] = {}
    for label, query, params, database, table in specs:
        archived = tail.get(database)
        if not isinstance(archived, dict) or not isinstance(archived.get(table), dict):
            raise TypeError(f"recovery tail archive is missing {label}")
        expected_rows = archived[table].get("rows")
        if not isinstance(expected_rows, list):
            raise TypeError(f"recovery tail rows are malformed for {label}")
        current_rows = [list(row) for row in connection.execute(query, params)]
        if current_rows == expected_rows:
            states[label] = "PRE"
        elif not current_rows:
            states[label] = "POST"
        else:
            states[label] = "UNKNOWN"
    return states


def verify_resume_table_metadata(
    connection: sqlite3.Connection,
    manifest: dict[str, object],
    tail: dict[str, object],
    boundary: int,
) -> None:
    table_manifest = manifest.get("tables")
    if not isinstance(table_manifest, dict):
        raise TypeError("recovery table metadata is missing")
    schema_queries = {
        ("main", "transactions"): "PRAGMA main.table_info(transactions)",
        ("main", "misc"): "PRAGMA main.table_info(misc)",
        ("hyperdb", "transactions"): "PRAGMA hyperdb.table_info(transactions)",
        ("hyperdb", "misc"): "PRAGMA hyperdb.table_info(misc)",
        ("indexdb", "tokens"): "PRAGMA indexdb.table_info(tokens)",
        ("indexdb", "aliases"): "PRAGMA indexdb.table_info(aliases)",
    }
    for (
        label,
        database,
        schema,
        table,
        retained_query,
        params,
        _standalone_query,
    ) in recovery_table_specs(boundary):
        expected = table_manifest.get(label)
        archived = tail.get(database)
        if not isinstance(expected, dict) or not isinstance(archived, dict):
            raise TypeError(f"recovery metadata is malformed for {label}")
        archived_table = archived.get(table)
        if not isinstance(archived_table, dict):
            raise TypeError(f"recovery tail archive is missing {label}")
        actual = {
            "schema_sha256": schema_fingerprint(
                connection.execute(schema_queries[(schema, table)])
            ),
            "targeted": rows_fingerprint(archived_table.get("rows", ())),
            "retained": rows_fingerprint(
                connection.execute(retained_query, params)
            ),
        }
        if actual != expected:
            raise RuntimeError(f"retained database content changed: {label}")


def finalize_restored_recovery(
    root: Path,
    paths: DbPaths,
    bundle_dir: Path,
    manifest: dict[str, object],
    plan: RecoveryPlan,
    tail: dict[str, object],
) -> None:
    original_modes = parse_original_journal_modes(manifest)
    restore_original_journal_modes(paths, original_modes)
    assert_no_node_processes(root)
    with hold_restored_database_locks(paths, original_modes) as connection:
        verify_resume_table_metadata(
            connection, manifest, tail, plan.first_delete_height
        )
        assert_post_recovery_locked(connection, plan, check_integrity=False)
        complete_recovery_bundle(bundle_dir, root)


def resume_journal_guard(
    root: Path, config: object, paths: DbPaths, bundle_dir: Path
) -> int:
    manifest_path = Path(bundle_dir) / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("format") != 2 or manifest.get("status") not in {
        "journal_guard",
        "journal_restored",
    }:
        raise RuntimeError("bundle is not a journal-guard recovery operation")
    if manifest.get("databases") != database_identities(paths):
        raise RuntimeError("journal guard database identity changed")
    original_modes = parse_original_journal_modes(manifest)
    operation_id = str(manifest.get("operation_id"))
    expected = f"RESUME {operation_id}"
    if input(f"Type exactly '{expected}' to continue: ") != expected:
        raise RuntimeError("resume confirmation did not match; no changes made")
    assert_no_node_processes(root)
    with reserve_node_port(int(config.port)):
        assert_no_node_processes(root)
        restore_original_journal_modes(paths, original_modes)
        with hold_restored_database_locks(paths, original_modes):
            if manifest["status"] == "journal_guard":
                manifest["status"] = "journal_restored"
                manifest["restored_at_utc"] = datetime.now(timezone.utc).isoformat()
                write_manifest_atomic(manifest_path, manifest)
            root_marker = Path(root) / ROOT_ACTIVE_MARKER
            if root_marker.exists():
                root_marker.unlink()
                fsync_directory(Path(root))
    print("JOURNAL MODES RESTORED; no blockchain rows were changed")
    print("Rerun the recovery dry-run before applying a new operation.")
    return 0


def resume_recovery(
    root: Path,
    config: object,
    paths: DbPaths,
    bundle_dir: Path,
    explicit_canonical_hash: Callable[[int], str] | None = None,
) -> int:
    manifest, plan, tail = load_resume_bundle(bundle_dir, paths)
    operation_id = str(manifest.get("operation_id"))
    expected = f"RESUME {operation_id}"
    if input(f"Type exactly '{expected}' to continue: ") != expected:
        raise RuntimeError("resume confirmation did not match; no changes made")
    assert_no_node_processes(root)
    with reserve_node_port(int(config.port)):
        assert_no_node_processes(root)
        if manifest["status"] in {"restoring", "complete"}:
            finalize_restored_recovery(
                root, paths, bundle_dir, manifest, plan, tail
            )
            print("RECOVERY RESUME COMPLETE")
            print("Restart the existing Bismuth node to resynchronize the canonical tail.")
            return 0
        original_modes = parse_original_journal_modes(manifest)
        with hold_database_locks(
            paths,
            restore_journal_modes=original_modes,
            allow_recovery_sidecars=True,
        ) as locked:
            assert_no_node_processes(root)
            connection = locked.connection
            assert_locked_integrity(connection)
            verify_resume_table_metadata(
                connection, manifest, tail, plan.first_delete_height
            )
            if attached_hash_at(connection, "main", plan.ancestor_height) != plan.ancestor_hash:
                raise RuntimeError("retained ledger ancestor changed; refusing resume")
            if attached_hash_at(connection, "hyperdb", plan.ancestor_height) != plan.ancestor_hash:
                raise RuntimeError("retained hyper ancestor changed; refusing resume")
            states = classify_resume_tables(
                connection, tail, plan.first_delete_height
            )
            unknown = [label for label, state in states.items() if state == "UNKNOWN"]
            if unknown:
                raise RuntimeError(
                    "resume encountered unknown database state: " + ", ".join(unknown)
                )
            if manifest["status"] == "complete" and any(
                state != "POST" for state in states.values()
            ):
                raise RuntimeError(
                    "complete manifest contradicts current database state"
                )
            if (
                manifest.get("selection_mode") == "explicit"
                and manifest.get("peer_policy") is not None
                and all(state == "PRE" for state in states.values())
            ):
                if explicit_canonical_hash is None:
                    raise RuntimeError(
                        "explicit PRE resume requires canonical peer revalidation"
                    )
                if explicit_canonical_hash(plan.ancestor_height) != plan.ancestor_hash:
                    raise RuntimeError(
                        "explicit resume target no longer matches canonical peer evidence"
                    )
            write_root_active_marker(root, bundle_dir)
            if manifest["status"] == "prepared":
                mark_recovery_committing(bundle_dir)
            if any(state == "PRE" for state in states.values()):
                apply_atomic_rollbacks(connection, plan.first_delete_height)
            assert_post_recovery_locked(connection, plan)
            locked.commit_and_relock()
            verify_resume_table_metadata(
                connection, manifest, tail, plan.first_delete_height
            )
            assert_post_recovery_locked(connection, plan, check_integrity=False)
            mark_recovery_restoring(bundle_dir)
        restoring_manifest = json.loads(
            (Path(bundle_dir) / "manifest.json").read_text(encoding="utf-8")
        )
        finalize_restored_recovery(
            root, paths, bundle_dir, restoring_manifest, plan, tail
        )
    print("RECOVERY RESUME COMPLETE")
    print("Restart the existing Bismuth node to resynchronize the canonical tail.")
    return 0


def make_node_stub() -> object:
    logger = logging.getLogger("bismuth-fork-recovery")
    return SimpleNamespace(logger=SimpleNamespace(app_log=logger))


def validate_bismuth_root(path: Path, require_peer_support: bool = True) -> Path:
    root = Path(path).expanduser().resolve()
    required = [
        "node.py",
        "options.py",
        "dbhandler.py",
        "config.txt",
    ]
    if require_peer_support:
        required.append("rpcconnections.py")
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise RuntimeError(
            f"not a compatible Bismuth base directory ({root}); missing: "
            + ", ".join(missing)
        )
    return root


def import_upstream_module(root: Path, module_name: str):
    if not module_name.isidentifier():
        raise ValueError(f"invalid upstream module name: {module_name}")
    module_file = (root / f"{module_name}.py").resolve()
    if module_file.parent != root or not module_file.is_file():
        raise RuntimeError(
            f"required upstream module is missing from Bismuth root: {module_file}"
        )
    root_text = str(root)
    sys.path[:] = [entry for entry in sys.path if entry != root_text]
    sys.path.insert(0, root_text)
    spec = importlib.util.spec_from_file_location(module_name, module_file)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load upstream module spec: {module_file}")
    module = importlib.util.module_from_spec(spec)
    previous = sys.modules.pop(module_name, None)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(module_name, None)
        if previous is not None:
            sys.modules[module_name] = previous
        raise
    return module


def load_node_config(root: Path, custom_config: str | None):
    options_module = import_upstream_module(root, "options")
    config = options_module.Get()
    custom_path = None
    if custom_config:
        candidate = Path(custom_config).expanduser()
        custom_path = str(
            (candidate if candidate.is_absolute() else root / candidate).resolve()
        )
    elif (root / "config_custom.txt").is_file():
        custom_path = str((root / "config_custom.txt").resolve())
    config.read(config_file=str(root / "config.txt"), custom_config_file=custom_path)
    return config


def assert_node_port_closed(port: int) -> None:
    with socket.socket() as probe:
        probe.settimeout(0.2)
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            raise RuntimeError(
                f"Bismuth node port {port} is accepting connections; stop the node first"
            )


@contextmanager
def reserve_node_port(port: int):
    if port == 0:
        raise ValueError("configured node port 0 cannot be reserved safely")
    if not 1 <= port <= 65535:
        raise ValueError(f"invalid configured node port: {port}")
    listeners: list[socket.socket] = []
    try:
        ipv4 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listeners.append(ipv4)
        ipv4.bind(("0.0.0.0", port))
        ipv4.listen(1)
        try:
            ipv6 = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            listeners.append(ipv6)
            ipv6.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
            ipv6.bind(("::", port))
            ipv6.listen(1)
        except OSError as exc:
            if exc.errno not in {errno.EAFNOSUPPORT, errno.EPROTONOSUPPORT}:
                raise
    except OSError as exc:
        for listener in reversed(listeners):
            listener.close()
        raise RuntimeError(
            f"could not reserve Bismuth node port {port}; stop the node and retry"
        ) from exc
    try:
        yield
    finally:
        for listener in reversed(listeners):
            listener.close()


def process_working_directory(pid: str) -> Path | None:
    proc_cwd = Path(f"/proc/{pid}/cwd")
    if proc_cwd.exists():
        try:
            return proc_cwd.resolve(strict=True)
        except OSError:
            return None
    try:
        result = subprocess.run(
            ["lsof", "-a", "-p", pid, "-d", "cwd", "-Fn"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        if line.startswith("n/"):
            return Path(line[1:]).resolve()
    return None


def assert_no_node_processes(
    root: Path,
    *,
    process_list: str | None = None,
    process_cwds: dict[str, Path] | None = None,
) -> None:
    if process_list is None:
        result = subprocess.run(
            ["ps", "-axo", "pid=,command="],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError("could not inspect running processes; refusing apply")
        process_list = result.stdout
    root_node = (root / "node.py").resolve()
    matches: list[str] = []
    for line in process_list.splitlines():
        fields = line.strip().split(maxsplit=1)
        if len(fields) != 2 or not fields[0].isdigit():
            continue
        pid, command = fields
        try:
            command_parts = shlex.split(command)
        except ValueError:
            command_parts = command.split()
        node_arguments = [part for part in command_parts if Path(part).name == "node.py"]
        matched = False
        for argument in node_arguments:
            candidate = Path(argument)
            if candidate.is_absolute():
                matched = candidate.resolve() == root_node
            else:
                cwd = (process_cwds or {}).get(pid)
                if cwd is None:
                    cwd = process_working_directory(pid)
                matched = cwd is None or (cwd / candidate).resolve() == root_node
            if matched:
                break
        if matched:
            matches.append(pid)
    if matches:
        rendered = ", ".join(f"PID {pid}" for pid in matches)
        raise RuntimeError(
            f"possible running Bismuth node.py process detected ({rendered}); stop it first"
        )


def timed_connection_factory(connection_class, timeout: float):
    """Apply a bounded timeout to upstream's otherwise fixed-timeout connection."""
    if timeout <= 0:
        raise ValueError("peer timeout must be positive")

    def factory(peer):
        previous = socket.getdefaulttimeout()
        socket.setdefaulttimeout(timeout)
        try:
            connection = connection_class(peer)
        finally:
            socket.setdefaulttimeout(previous)
        sock = getattr(connection, "sdef", None)
        if sock is not None:
            sock.settimeout(timeout)
        return connection

    return factory


def local_hash_at(conn: sqlite3.Connection, height: int) -> str:
    return unique_local_reward_hash(conn, height)


def assert_consistent_database_tip(
    paths: DbPaths, tip_height: int, tip_hash: str
) -> None:
    with open_local_ledger_readonly(paths.hyper) as hyper:
        hyper_height, hyper_hash = local_tip(hyper)
    if (hyper_height, hyper_hash) != (tip_height, tip_hash):
        raise RuntimeError(
            "inconsistent database tail: ledger and hyper tips do not match"
        )

    checks = (
        (
            paths.ledger,
            "ledger transactions",
            (
                "SELECT COUNT(*) FROM transactions "
                "WHERE block_height > ? OR block_height < ?"
            ),
            (tip_height, -tip_height),
        ),
        (
            paths.ledger,
            "ledger misc",
            "SELECT COUNT(*) FROM misc WHERE block_height > ?",
            (tip_height,),
        ),
        (
            paths.hyper,
            "hyper transactions",
            (
                "SELECT COUNT(*) FROM transactions "
                "WHERE block_height > ? OR block_height < ?"
            ),
            (tip_height, -tip_height),
        ),
        (
            paths.hyper,
            "hyper misc",
            "SELECT COUNT(*) FROM misc WHERE block_height > ?",
            (tip_height,),
        ),
        (
            paths.index,
            "tokens",
            "SELECT COUNT(*) FROM tokens WHERE block_height > ?",
            (tip_height,),
        ),
        (
            paths.index,
            "aliases",
            "SELECT COUNT(*) FROM aliases WHERE block_height > ?",
            (tip_height,),
        ),
    )
    for path, label, query, params in checks:
        with open_local_ledger_readonly(path) as connection:
            remaining = connection.execute(query, params).fetchone()[0]
        if remaining:
            raise RuntimeError(
                f"inconsistent database tail: {remaining} rows beyond local tip in {label}"
            )


def assert_hyper_retained_ancestor(
    paths: DbPaths, ancestor_height: int, ancestor_hash: str
) -> None:
    with open_local_ledger_readonly(paths.hyper) as hyper:
        hyper_hash = unique_local_reward_hash(hyper, ancestor_height)
    if hyper_hash != ancestor_hash:
        raise RuntimeError(
            "inconsistent database tail: hyper ancestor does not match ledger"
        )


def explicit_resume_canonical_query(
    root: Path, manifest: dict[str, object]
) -> Callable[[int], str]:
    peers, required_votes, query_timeout = parse_explicit_peer_policy(manifest)
    rpc_module = import_upstream_module(root, "rpcconnections")
    if hasattr(rpc_module, "LTIMEOUT"):
        rpc_module.LTIMEOUT = query_timeout
    connection_factory = timed_connection_factory(
        rpc_module.Connection, query_timeout
    )

    def canonical(height: int) -> str:
        return canonical_hash_at(
            connection_factory,
            peers,
            height,
            required_votes,
            query_timeout=query_timeout,
        ).selected_hash

    return canonical


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bismuth-dir", default=".")
    parser.add_argument("--config-custom")
    parser.add_argument("--index-db")
    parser.add_argument("--peer", action="append", default=[])
    parser.add_argument("--peer-file", default="suggested_peers.txt")
    parser.add_argument("--required-votes", type=int)
    parser.add_argument("--peer-timeout", type=float, default=10.0)
    parser.add_argument(
        "--verify-peers",
        action="store_true",
        help="opt in to canonical peer verification for an explicit manual rollback",
    )
    rollback_group = parser.add_mutually_exclusive_group()
    rollback_group.add_argument(
        "--rollback-to",
        type=int,
        metavar="HEIGHT",
        help="retain HEIGHT and delete every later local block",
    )
    rollback_group.add_argument(
        "--rollback-blocks",
        type=int,
        metavar="COUNT",
        help="delete exactly COUNT blocks from the current local tip",
    )
    rollback_group.add_argument(
        "--resume", metavar="BUNDLE", help="resume the exact active recovery bundle"
    )
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--bundle-dir")
    parser.add_argument(
        "--integrity-check",
        action="store_true",
        help=(
            "also run the full SQLite b-tree integrity scan (PRAGMA quick_check) "
            "before and after the rollback; costs minutes on a large DB and is "
            "off by default because a tail rollback is atomic"
        ),
    )
    parser.add_argument(
        "--checkpoint-wal",
        action="store_true",
        help=(
            "before running, fold leftover SQLite WAL sidecars into their main DBs "
            "(PRAGMA wal_checkpoint(TRUNCATE)); only acts when the node is already "
            "stopped, and is ignored on --resume where sidecars are intentional"
        ),
    )
    return parser


def run_cli(args: argparse.Namespace) -> int:
    global INTEGRITY_CHECK
    INTEGRITY_CHECK = bool(args.integrity_check)
    explicit_target = args.rollback_to is not None or args.rollback_blocks is not None
    explicit_peer_options = (
        args.verify_peers
        or bool(args.peer)
        or args.required_votes is not None
        or args.peer_file != "suggested_peers.txt"
    )
    peer_verification = not explicit_target or explicit_peer_options
    root = validate_bismuth_root(
        Path(args.bismuth_dir),
        require_peer_support=peer_verification and not args.resume,
    )
    config = load_node_config(root, args.config_custom)
    paths = resolve_db_paths(root, config, args.index_db)
    active_marker = read_root_active_marker(root)
    if active_marker is not None and not args.resume:
        raise RuntimeError(
            "pending recovery operation detected; rerun with --apply --resume "
            f"{active_marker['bundle']}"
        )
    resume_path: Path | None = None
    resume_manifest: dict[str, object] | None = None
    if args.resume:
        if not args.apply:
            raise RuntimeError("--resume requires --apply")
        if active_marker is None:
            raise RuntimeError("--resume requires the root active recovery marker")
        resume_path = Path(args.resume).expanduser()
        if not resume_path.is_absolute():
            resume_path = root / resume_path
        resume_path = resume_path.resolve()
        if resume_path != Path(str(active_marker["bundle"])):
            raise RuntimeError("--resume does not match the pending recovery operation")
        resume_manifest = json.loads(
            (resume_path / "manifest.json").read_text(encoding="utf-8")
        )
        manifest_operation_id = resume_manifest.get("operation_id")
        if (
            not isinstance(manifest_operation_id, str)
            or active_marker["operation_id"] != manifest_operation_id
        ):
            raise RuntimeError(
                "root active marker operation ID does not match the recovery manifest"
            )
        if resume_manifest.get("status") not in {"journal_guard", "journal_restored"}:
            intent_digest = validate_manifest_recovery_intent(resume_manifest)
            if (
                intent_digest is not None
                and active_marker.get("recovery_intent_sha256") != intent_digest
            ):
                raise RuntimeError(
                    "root active marker recovery intent does not match the recovery manifest"
                )
    assert_node_port_closed(int(config.port))
    if args.checkpoint_wal and not (args.resume and active_marker is not None):
        assert_no_node_processes(root)
        checkpoint_wal(paths)
    assert_databases_offline(
        paths,
        allow_recovery_sidecars=bool(args.resume and active_marker),
        check_integrity=False,
    )
    if resume_path is not None and resume_manifest is not None:
        if resume_manifest.get("status") in {"journal_guard", "journal_restored"}:
            return resume_journal_guard(root, config, paths, resume_path)
        explicit_canonical = None
        if (
            resume_manifest.get("selection_mode") == "explicit"
            and resume_manifest.get("peer_policy") is not None
        ):
            explicit_canonical = explicit_resume_canonical_query(root, resume_manifest)
        return resume_recovery(
            root,
            config,
            paths,
            resume_path,
            explicit_canonical_hash=explicit_canonical,
        )

    peers: list[tuple[str, int]] = []
    required_votes = 0
    connection_factory = None
    evidence_by_height: dict[int, CanonicalHashEvidence] = {}

    if peer_verification:
        peer_file = Path(args.peer_file).expanduser()
        if not peer_file.is_absolute():
            peer_file = root / peer_file
        peers = load_peers(peer_file, args.peer)
        # --peer pins the exact peers to follow (load_peers then ignores the
        # pool file).
        peers_pinned = bool(args.peer)
        pool_default_votes = (
            not peers_pinned and args.required_votes is None and not explicit_target
        )
        if pool_default_votes:
            # Unpinned pool mode: the quorum is the majority of the peers that
            # actually respond, computed after a reachability probe at the local
            # tip (below), not the majority of the whole pool file.
            required_votes = 0
        else:
            required_votes = compute_required_votes(
                peers_pinned=peers_pinned,
                required_votes=args.required_votes,
                peer_count=len(peers),
            )

        rpc_module = import_upstream_module(root, "rpcconnections")
        if hasattr(rpc_module, "LTIMEOUT"):
            rpc_module.LTIMEOUT = args.peer_timeout
        connection_factory = timed_connection_factory(
            rpc_module.Connection, args.peer_timeout
        )
        # Peers that fail stay cached so a known-dead peer is not re-queried.
        dead_peers: set[str] = set()

    def canonical(height: int) -> str:
        if connection_factory is None:
            raise RuntimeError("canonical peer verification is not enabled")
        evidence = canonical_hash_at(
            connection_factory,
            peers,
            height,
            required_votes,
            query_timeout=args.peer_timeout,
            dead_peers=dead_peers,
        )
        evidence_by_height[height] = evidence
        return evidence.selected_hash

    with open_local_ledger_readonly(paths.ledger) as ledger:
        tip_height, tip_hash = local_tip(ledger)
        assert_consistent_database_tip(paths, tip_height, tip_hash)
        if peer_verification and required_votes == 0:
            # Unpinned pool mode: qualify the pool by actual reachability at the
            # tip. Require POOL_MIN_PEERS, then quorum on the responsive subset.
            reachable = probe_reachable_peers(
                connection_factory, peers, tip_height, args.peer_timeout
            )
            if len(reachable) < POOL_MIN_PEERS:
                raise RuntimeError(
                    f"only {len(reachable)} of {len(peers)} pool peers reachable; "
                    f"automatic pool mode needs at least {POOL_MIN_PEERS} responsive "
                    "peers; pin a peer with --peer or pass --required-votes"
                )
            peers = reachable
            required_votes = compute_required_votes(
                peers_pinned=False, required_votes=None, peer_count=len(reachable)
            )
        if args.rollback_blocks is not None:
            if args.rollback_blocks < 1:
                raise ValueError("--rollback-blocks must be at least 1")
            ancestor_height = tip_height - args.rollback_blocks
            if ancestor_height < 1:
                raise ValueError(
                    "--rollback-blocks must leave at least block height 1 retained"
                )
        elif args.rollback_to is not None:
            ancestor_height = args.rollback_to
            if not 1 <= ancestor_height <= tip_height:
                raise ValueError(
                    f"--rollback-to must be between 1 and local tip {tip_height}"
                )
        else:
            ancestor_height = -1
        if explicit_target:
            assert_explicit_reward_suffix(
                paths, ledger, ancestor_height, tip_height
            )
            ancestor_hash = local_hash_at(ledger, ancestor_height)
            if peer_verification and canonical(ancestor_height) != ancestor_hash:
                raise RuntimeError(
                    "explicit rollback target does not match canonical peer evidence"
                )
        else:
            ancestor_height, ancestor_hash = find_common_ancestor(
                lambda height: local_hash_at(ledger, height),
                canonical,
                tip_height,
            )
    assert_hyper_retained_ancestor(paths, ancestor_height, ancestor_hash)

    plan = RecoveryPlan(
        tip_height,
        tip_hash,
        ancestor_height,
        ancestor_hash,
        ancestor_height + 1,
        tip_height - ancestor_height,
    )
    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"mode: {mode}")
    print(f"local tip: {plan.local_tip_height} {plan.local_tip_hash}")
    target_label = "rollback target" if explicit_target else "common ancestor"
    print(f"{target_label}: {plan.ancestor_height} {plan.ancestor_hash}")
    print(
        f"rollback: {plan.first_delete_height}-{plan.local_tip_height} "
        f"({plan.rollback_blocks} blocks)"
    )
    print(f"databases: ledger={paths.ledger} hyper={paths.hyper} index={paths.index}")
    if peer_verification:
        policy = (
            f"pinned peer policy: following {len(peers)} explicitly trusted peer(s), "
            "all must agree"
            if peers_pinned
            else f"canonical peer policy: {required_votes} agreeing votes from {len(peers)} peers"
        )
        print(policy)
    else:
        print("manual peer validation: disabled")
    for height, evidence in sorted(evidence_by_height.items(), reverse=True):
        print(f"  height {height}: selected={evidence.selected_hash}")
        for peer, vote in evidence.votes.items():
            status = vote if vote is not None else f"ERROR {evidence.errors.get(peer, 'no hash')}"
            print(f"    {peer}: {status}")
    if not args.apply:
        print("DRY RUN: no database changes made")
        return 0
    if plan.rollback_blocks == 0:
        if explicit_target:
            print("NO-OP: requested retained height is the current local tip")
        else:
            print("NO-OP: local tip already matches canonical peers")
        return 0

    confirm_apply(plan)
    if args.bundle_dir:
        bundle = Path(args.bundle_dir).expanduser()
        if not bundle.is_absolute():
            bundle = root / bundle
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        bundle = root / "recovery_bundles" / f"fork_recovery_{stamp}"
    operation_id = str(uuid.uuid4())
    assert_no_node_processes(root)
    with reserve_node_port(int(config.port)):
        assert_no_node_processes(root)
        with hold_database_locks(
            paths,
            before_journal_change=lambda modes: write_journal_guard(
                root, bundle, paths, operation_id, modes
            ),
        ) as locked:
            assert_no_node_processes(root)
            connection = locked.connection
            assert_locked_preconditions(
                connection,
                (plan.local_tip_height, plan.local_tip_hash),
                (plan.ancestor_height, plan.ancestor_hash),
            )
            current_tip = local_tip(connection)
            if current_tip != (plan.local_tip_height, plan.local_tip_hash):
                raise RuntimeError(
                    f"local tip changed after planning: {current_tip}; rerun dry-run"
                )
            current_ancestor_hash = local_hash_at(connection, plan.ancestor_height)
            if explicit_target:
                ledger_suffix = assert_contiguous_reward_suffix(
                    connection,
                    "main.transactions",
                    plan.ancestor_height,
                    plan.local_tip_height,
                )
                hyper_suffix = assert_contiguous_reward_suffix(
                    connection,
                    "hyperdb.transactions",
                    plan.ancestor_height,
                    plan.local_tip_height,
                )
                if ledger_suffix != hyper_suffix:
                    raise RuntimeError(
                        "ledger and hyper explicit rollback suffixes disagree"
                    )
            if peer_verification:
                canonical_ancestor_hash = canonical(plan.ancestor_height)
                if current_ancestor_hash != canonical_ancestor_hash:
                    raise RuntimeError(
                        "apply boundary failed: ancestor hash no longer agrees"
                    )
            if not explicit_target:
                first_divergent_hash = local_hash_at(
                    connection, plan.first_delete_height
                )
                canonical_first_hash = canonical(plan.first_delete_height)
                if first_divergent_hash == canonical_first_hash:
                    raise RuntimeError(
                        "apply boundary failed: first divergent block now agrees"
                    )
            if args.rollback_blocks is not None:
                rollback_request = {"blocks": args.rollback_blocks}
            elif args.rollback_to is not None:
                rollback_request = {"to_height": args.rollback_to}
            else:
                rollback_request = None
            selection_mode = "explicit" if explicit_target else "automatic"
            peer_policy = (
                {
                    "peers": [[host, port] for host, port in peers],
                    "required_votes": required_votes,
                    "query_timeout": args.peer_timeout,
                }
                if explicit_target and peer_verification
                else None
            )
            write_root_active_marker(
                root,
                bundle,
                recovery_intent_digest(
                    selection_mode, rollback_request, peer_policy
                ),
            )
            write_recovery_bundle(
                paths,
                plan,
                bundle,
                evidence_by_height,
                locked_connection=connection,
                operation_id=operation_id,
                original_journal_modes=locked.original_journal_modes,
                selection_mode=selection_mode,
                rollback_request=rollback_request,
                peer_policy=peer_policy,
            )
            print(f"recovery bundle: {bundle}")
            mark_recovery_committing(bundle)
            apply_atomic_rollbacks(connection, plan.first_delete_height)
            assert_post_recovery_locked(connection, plan)
            locked.commit_and_relock()
            committed_manifest, _committed_plan, committed_tail = load_resume_bundle(
                bundle, paths
            )
            verify_resume_table_metadata(
                connection,
                committed_manifest,
                committed_tail,
                plan.first_delete_height,
            )
            assert_post_recovery_locked(connection, plan, check_integrity=False)
            mark_recovery_restoring(bundle)
        restoring_manifest, restoring_plan, restoring_tail = load_resume_bundle(
            bundle, paths
        )
        finalize_restored_recovery(
            root,
            paths,
            bundle,
            restoring_manifest,
            restoring_plan,
            restoring_tail,
        )
    print("RECOVERY COMPLETE")
    print("Restart the existing Bismuth node to resynchronize the canonical tail.")
    return 0


def main(argv: list[str] | None = None) -> int:
    try:
        return run_cli(build_parser().parse_args(argv))
    except Exception as exc:  # noqa: BLE001 - fail-closed CLI boundary
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
