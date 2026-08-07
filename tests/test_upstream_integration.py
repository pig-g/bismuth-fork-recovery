import importlib
import importlib.util
import logging
import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = PROJECT_ROOT / "fork_recovery.py"


def load_tool():
    spec = importlib.util.spec_from_file_location("fork_recovery_integration", TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_real_schema_databases(root):
    ledger = root / "ledger.db"
    hyper = root / "hyper.db"
    index = root / "index.db"
    transaction_schema = (
        "CREATE TABLE transactions (block_height INTEGER, timestamp NUMERIC, "
        "address TEXT, recipient TEXT, amount NUMERIC, signature TEXT, "
        "public_key TEXT, block_hash TEXT, fee NUMERIC, reward NUMERIC, "
        "operation TEXT, openfield TEXT)"
    )
    for path in (ledger, hyper):
        conn = sqlite3.connect(path)
        conn.execute(transaction_schema)
        conn.execute("CREATE TABLE misc (block_height INTEGER, difficulty TEXT)")
        for height in range(1, 11):
            block_hash = f"shared-{height}" if height <= 5 else f"fork-{height}"
            conn.execute(
                "INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (height, height, "a", "b", 0, "sig", "pub", block_hash, 0, 1, "", ""),
            )
        conn.executemany("INSERT INTO misc VALUES (?, ?)", [(5, "1"), (6, "1")])
        conn.commit()
        conn.close()
    conn = sqlite3.connect(index)
    conn.execute("CREATE TABLE aliases (block_height INTEGER, address, alias)")
    conn.execute(
        "CREATE TABLE tokens (block_height INTEGER, timestamp, token, address, "
        "recipient, txid, amount INTEGER)"
    )
    conn.execute("INSERT INTO aliases VALUES (6, 'a', 'alias')")
    conn.execute("INSERT INTO tokens VALUES (6, 0, 'T', 'a', 'b', 'tx', 1)")
    conn.commit()
    conn.close()
    return ledger, hyper, index


def test_foundation_upstream_dbhandler_rolls_back_synthetic_tail(tmp_path):
    source = os.environ.get("BISMUTH_SOURCE_DIR")
    if not source:
        pytest.skip("set BISMUTH_SOURCE_DIR to a clean Foundation Bismuth checkout")
    source_path = Path(source).resolve()
    sys.path.insert(0, str(source_path))
    stale = {
        name: sys.modules.pop(name, None)
        for name in ("dbhandler", "essentials", "quantizer", "fork")
    }
    try:
        dbhandler = importlib.import_module("dbhandler")
        assert Path(dbhandler.__file__).resolve().parent == source_path
        tool = load_tool()
        ledger, hyper, index = create_real_schema_databases(tmp_path)
        logger = SimpleNamespace(app_log=logging.getLogger("upstream-integration"))
        node = SimpleNamespace(logger=logger)
        handler = dbhandler.DbHandler(
            str(index), str(ledger), str(hyper), False, str(ledger), logger
        )

        tool.apply_existing_rollbacks(handler, ancestor_height=5, node_stub=node)

        plan = tool.RecoveryPlan(10, "fork-10", 5, "shared-5", 6, 5)
        tool.assert_post_recovery(tool.DbPaths(ledger, hyper, index), plan)

        direct_root = tmp_path / "direct-atomic"
        direct_root.mkdir()
        direct_paths = tool.DbPaths(*create_real_schema_databases(direct_root))
        with tool.hold_database_locks(direct_paths) as locked:
            tool.apply_atomic_rollbacks(locked.connection, first_delete_height=6)
            tool.assert_post_recovery_locked(locked.connection, plan)
        tool.assert_post_recovery(direct_paths, plan)
    finally:
        for name in ("dbhandler", "essentials", "quantizer", "fork"):
            sys.modules.pop(name, None)
        for name, module in stale.items():
            if module is not None:
                sys.modules[name] = module
        sys.path.remove(str(source_path))
