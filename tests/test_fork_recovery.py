import gzip
import hashlib
import importlib.util
import json
import sqlite3
import subprocess
import sys
import threading
from contextlib import closing
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = PROJECT_ROOT / "fork_recovery.py"


def valid_hash(value):
    return hashlib.sha224(value.encode("utf-8")).hexdigest()


def load_tool():
    spec = importlib.util.spec_from_file_location("fork_recovery", TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_peers_reads_upstream_suggested_peers_format(tmp_path):
    peer_file = tmp_path / "suggested_peers.txt"
    peer_file.write_text(
        json.dumps({"192.0.2.10": "5658", "198.51.100.7": 5658}),
        encoding="utf-8",
    )

    tool = load_tool()

    assert tool.load_peers(peer_file, []) == [
        ("192.0.2.10", 5658),
        ("198.51.100.7", 5658),
    ]


def test_load_peers_rejects_duplicate_explicit_endpoint(tmp_path):
    tool = load_tool()

    with pytest.raises(ValueError, match="duplicate peer"):
        tool.load_peers(
            tmp_path / "unused.txt",
            ["192.0.2.10:5658", "192.0.2.10:5658"],
        )


def test_load_peers_rejects_dns_aliases_for_same_endpoint(tmp_path, monkeypatch):
    tool = load_tool()

    def same_address(host, port, **kwargs):
        return [(tool.socket.AF_INET, tool.socket.SOCK_STREAM, 6, "", ("192.0.2.10", port))]

    monkeypatch.setattr(tool.socket, "getaddrinfo", same_address)
    with pytest.raises(ValueError, match="duplicate resolved peer"):
        tool.load_peers(
            tmp_path / "unused.txt",
            ["node-a.example:5658", "node-b.example.:5658"],
        )


def test_local_tip_reads_highest_positive_reward_block(tmp_path):
    ledger = tmp_path / "ledger.db"
    conn = sqlite3.connect(ledger)
    conn.execute(
        "CREATE TABLE transactions (block_height INTEGER, reward TEXT, block_hash TEXT)"
    )
    conn.executemany(
        "INSERT INTO transactions VALUES (?, ?, ?)",
        [(4, "1", "hash-4"), (5, "0", "hash-5-tx"), (5, "1", "hash-5")],
    )
    conn.commit()
    conn.close()

    tool = load_tool()
    with tool.open_local_ledger_readonly(ledger) as readonly:
        assert tool.local_tip(readonly) == (5, "hash-5")


def test_local_tip_rejects_conflicting_reward_rows_at_tip(tmp_path):
    ledger = tmp_path / "ledger.db"
    conn = sqlite3.connect(ledger)
    conn.execute(
        "CREATE TABLE transactions (block_height INTEGER, reward TEXT, block_hash TEXT)"
    )
    conn.executemany(
        "INSERT INTO transactions VALUES (?, ?, ?)",
        [(2, "1", "hash-a"), (2, "1", "hash-b")],
    )
    conn.commit()
    conn.close()

    tool = load_tool()
    with (
        tool.open_local_ledger_readonly(ledger) as readonly,
        pytest.raises(ValueError, match="ambiguous reward rows"),
    ):
        tool.local_tip(readonly)


def test_canonical_hash_requires_majority_and_closes_connections():
    tool = load_tool()
    responses = {
        ("a", 5658): {"10": {"block_hash": valid_hash("canonical")}},
        ("b", 5658): {"10": {"block_hash": valid_hash("canonical")}},
        ("c", 5658): {"10": {"block_hash": valid_hash("fork")}},
    }
    created = []

    class FakeConnection:
        def __init__(self, peer):
            self.peer = peer
            self.closed = False
            created.append(self)

        def command(self, command, options):
            assert command == "api_getblockfromheight"
            assert options == [10]
            return responses[self.peer]

        def close(self):
            self.closed = True

    evidence = tool.canonical_hash_at(
        FakeConnection,
        list(responses),
        height=10,
        required_votes=2,
    )

    assert evidence.selected_hash == valid_hash("canonical")
    assert evidence.votes == {
        "a:5658": valid_hash("canonical"),
        "b:5658": valid_hash("canonical"),
        "c:5658": valid_hash("fork"),
    }
    assert all(connection.closed for connection in created)


def test_canonical_hash_rejects_non_sha224_response():
    tool = load_tool()

    class MalformedConnection:
        def __init__(self, peer):
            pass

        def command(self, command, options):
            return {str(options[0]): {"block_hash": 12345}}

        def close(self):
            pass

    with pytest.raises(ValueError, match="no peer supplied"):
        tool.canonical_hash_at(
            MalformedConnection,
            [("a", 5658), ("b", 5658)],
            height=10,
            required_votes=2,
        )


def test_canonical_hash_enforces_wall_clock_peer_deadline():
    tool = load_tool()

    class TricklingConnection:
        def __init__(self, peer):
            self.release = threading.Event()

        def command(self, command, options):
            self.release.wait()
            raise OSError("closed")

        def close(self):
            self.release.set()

    with pytest.raises(ValueError, match="no peer supplied"):
        tool.canonical_hash_at(
            TricklingConnection,
            [("a", 5658), ("b", 5658)],
            height=10,
            required_votes=2,
            query_timeout=0.01,
        )


def test_canonical_hash_rejects_split_votes():
    tool = load_tool()

    class SplitConnection:
        def __init__(self, peer):
            self.peer = peer

        def command(self, command, options):
            height = options[0]
            return {str(height): {"block_hash": valid_hash(self.peer[0])}}

        def close(self):
            pass

    with pytest.raises(ValueError, match="insufficient canonical hash agreement"):
        tool.canonical_hash_at(
            SplitConnection,
            [("hash-a", 5658), ("hash-b", 5658)],
            height=10,
            required_votes=2,
        )


def test_find_common_ancestor_returns_last_matching_height():
    tool = load_tool()
    local = {height: f"shared-{height}" for height in range(1, 6)}
    local.update({height: f"fork-{height}" for height in range(6, 11)})
    canonical = {height: f"shared-{height}" for height in range(1, 6)}
    canonical.update({height: f"main-{height}" for height in range(6, 13)})
    queried = []

    ancestor_height, ancestor_hash = tool.find_common_ancestor(
        local.__getitem__,
        lambda height: queried.append(height) or canonical[height],
        local_tip_height=10,
    )

    assert (ancestor_height, ancestor_hash) == (5, "shared-5")
    assert len(queried) < 10


def test_find_common_ancestor_rejects_chain_with_no_retained_match():
    tool = load_tool()

    with pytest.raises(ValueError, match="no common ancestor"):
        tool.find_common_ancestor(
            lambda height: f"local-{height}",
            lambda height: f"canonical-{height}",
            local_tip_height=10,
        )


def test_apply_existing_rollbacks_deletes_from_ancestor_plus_one():
    tool = load_tool()
    calls = []

    class FakeHandler:
        def rollback_under(self, height):
            calls.append(("ledger", height))

        def tokens_rollback(self, node, height):
            calls.append(("tokens", height))

        def aliases_rollback(self, node, height):
            calls.append(("aliases", height))

        def close(self):
            calls.append(("close", None))

    tool.apply_existing_rollbacks(FakeHandler(), ancestor_height=5, node_stub=object())

    assert calls == [
        ("ledger", 6),
        ("tokens", 6),
        ("aliases", 6),
        ("close", None),
    ]


def test_resolve_db_paths_uses_config_paths_and_node_index_default(tmp_path):
    tool = load_tool()

    class Config:
        ledger_path = "node-data/ledger.db"
        hyper_path = "node-data/hyper.db"

    paths = tool.resolve_db_paths(tmp_path, Config(), index_override=None)

    assert paths.ledger == (tmp_path / "node-data/ledger.db").resolve()
    assert paths.hyper == (tmp_path / "node-data/hyper.db").resolve()
    assert paths.index == (tmp_path / "static/index.db").resolve()


def test_load_node_config_finds_custom_config_inside_external_bismuth_root(tmp_path):
    tool = load_tool()
    root = tmp_path / "Bismuth"
    root.mkdir()
    (root / "config.txt").write_text("base=true\n", encoding="utf-8")
    custom = root / "config_custom.txt"
    custom.write_text("custom=true\n", encoding="utf-8")
    (root / "options.py").write_text(
        "class Get:\n"
        "    def read(self, config_file='config.txt', custom_config_file=None):\n"
        "        self.received_custom = custom_config_file\n",
        encoding="utf-8",
    )
    stale = sys.modules.pop("options", None)
    try:
        config = tool.load_node_config(root.resolve(), custom_config=None)
        assert config.received_custom == str(custom.resolve())
    finally:
        sys.modules.pop("options", None)
        if stale is not None:
            sys.modules["options"] = stale
        sys.path.remove(str(root.resolve()))


def test_import_upstream_module_validates_path_before_executing_shadow(tmp_path):
    tool = load_tool()
    root = tmp_path / "Bismuth"
    shadow = tmp_path / "shadow"
    root.mkdir()
    shadow.mkdir()
    marker = tmp_path / "shadow-executed"
    (root / "options.py").write_text("ORIGIN = 'safe'\n", encoding="utf-8")
    (shadow / "options.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('bad')\n",
        encoding="utf-8",
    )
    stale = sys.modules.pop("options", None)
    sys.path.insert(0, str(root.resolve()))
    sys.path.insert(0, str(shadow.resolve()))
    try:
        module = tool.import_upstream_module(root.resolve(), "options")
        assert module.ORIGIN == "safe"
        assert not marker.exists()
    finally:
        sys.modules.pop("options", None)
        if stale is not None:
            sys.modules["options"] = stale
        sys.path.remove(str(shadow.resolve()))
        sys.path.remove(str(root.resolve()))


def test_assert_databases_offline_rejects_active_writer(tmp_path):
    tool = load_tool()
    databases = []
    for name in ("ledger.db", "hyper.db", "index.db"):
        path = tmp_path / name
        sqlite3.connect(path).close()
        databases.append(path)
    lock = sqlite3.connect(databases[0])
    lock.execute("BEGIN IMMEDIATE")

    try:
        with pytest.raises(RuntimeError, match="write-locked"):
            tool.assert_databases_offline(tool.DbPaths(*databases))
    finally:
        lock.rollback()
        lock.close()


def test_assert_databases_offline_rejects_wal_sidecar(tmp_path):
    tool = load_tool()
    databases = []
    for name in ("ledger.db", "hyper.db", "index.db"):
        path = tmp_path / name
        sqlite3.connect(path).close()
        databases.append(path)
    Path(f"{databases[0]}-wal").write_bytes(b"stale")

    with pytest.raises(RuntimeError, match="WAL/SHM sidecar"):
        tool.assert_databases_offline(tool.DbPaths(*databases))


def test_hold_database_locks_blocks_all_competing_writers(tmp_path):
    tool = load_tool()
    databases = []
    for name in ("ledger.db", "hyper.db", "index.db"):
        path = tmp_path / name
        sqlite3.connect(path).close()
        databases.append(path)
    paths = tool.DbPaths(*databases)

    with tool.hold_database_locks(paths):
        for path in databases:
            competitor = sqlite3.connect(path, timeout=0)
            try:
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    competitor.execute("BEGIN IMMEDIATE")
            finally:
                competitor.close()

    for path in databases:
        with sqlite3.connect(path, timeout=0) as competitor:
            competitor.execute("BEGIN IMMEDIATE")


def test_atomic_rollback_failure_restores_all_three_databases(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )

    with (
        pytest.raises(RuntimeError, match="injected failure"),
        tool.hold_database_locks(paths) as locked,
    ):
        tool.apply_atomic_rollbacks(locked.connection, first_delete_height=6)
        raise RuntimeError("injected failure")

    for name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / name) as connection:
            maximum = connection.execute(
                "SELECT MAX(block_height) FROM transactions"
            ).fetchone()
            assert maximum == (10,)
    with sqlite3.connect(root / "static/index.db") as connection:
        assert connection.execute("SELECT MAX(block_height) FROM tokens").fetchone() == (6,)
        assert connection.execute("SELECT MAX(block_height) FROM aliases").fetchone() == (6,)


def test_commit_relocks_databases_through_manifest_finalization(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )

    with tool.hold_database_locks(paths) as locked:
        tool.apply_atomic_rollbacks(locked.connection, 6)
        tool.assert_post_recovery_locked(locked.connection, plan)
        locked.commit_and_relock()
        tool.assert_post_recovery_locked(locked.connection, plan)
        with (
            sqlite3.connect(paths.ledger, timeout=0) as competitor,
            pytest.raises(sqlite3.OperationalError),
        ):
            competitor.execute("BEGIN IMMEDIATE")


def test_locked_preconditions_reject_hyper_tip_change(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    with sqlite3.connect(paths.hyper) as connection:
        connection.execute("DELETE FROM transactions WHERE block_height = 10")
        connection.commit()

    with (
        tool.hold_database_locks(paths) as locked,
        pytest.raises(RuntimeError, match="ledger and hyper tips"),
    ):
        tool.assert_locked_preconditions(
            locked.connection,
            expected_tip=(10, valid_hash("fork-10")),
        )


def test_locked_preconditions_reject_hyper_ancestor_change(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    with sqlite3.connect(paths.hyper) as connection:
        connection.execute(
            "UPDATE transactions SET block_hash = ? WHERE block_height = 5",
            (valid_hash("hyper-divergent-5"),),
        )
        connection.commit()

    with (
        tool.hold_database_locks(paths) as locked,
        pytest.raises(RuntimeError, match="hyper ancestor"),
    ):
        tool.assert_locked_preconditions(
            locked.connection,
            expected_tip=(10, valid_hash("fork-10")),
            retained_ancestor=(5, valid_hash("shared-5")),
        )


@pytest.mark.parametrize("database_name", ["ledger", "hyper"])
@pytest.mark.parametrize("duplicate_reward", ["1", "-1"])
def test_locked_preconditions_reject_duplicate_same_hash_ancestor(
    tmp_path, database_name, duplicate_reward
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    with sqlite3.connect(getattr(paths, database_name)) as connection:
        connection.execute(
            "INSERT INTO transactions VALUES (?, ?, ?)",
            (5, duplicate_reward, valid_hash("shared-5")),
        )
        connection.commit()

    with (
        tool.hold_database_locks(paths) as locked,
        pytest.raises(RuntimeError, match="ambiguous reward-bearing block"),
    ):
        tool.assert_locked_preconditions(
            locked.connection,
            expected_tip=(10, valid_hash("fork-10")),
            retained_ancestor=(5, valid_hash("shared-5")),
        )


@pytest.mark.parametrize("schema", ["main", "hyperdb"])
@pytest.mark.parametrize("duplicate_reward", ["1", "-1"])
def test_locked_postvalidation_rejects_duplicate_same_hash_tip(
    tmp_path, schema, duplicate_reward
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )

    with (
        tool.hold_database_locks(paths) as locked,
        pytest.raises(RuntimeError, match="ambiguous reward-bearing block"),
    ):
        tool.apply_atomic_rollbacks(locked.connection, plan.first_delete_height)
        locked.connection.execute(
            f"INSERT INTO {schema}.transactions VALUES (?, ?, ?)",
            (5, duplicate_reward, valid_hash("shared-5")),
        )
        tool.assert_post_recovery_locked(locked.connection, plan)


def test_locked_postvalidation_checks_hyper_retained_tip(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    with (
        tool.hold_database_locks(paths) as locked,
        pytest.raises(RuntimeError, match="hyper tip mismatch"),
    ):
        tool.apply_atomic_rollbacks(locked.connection, 6)
        locked.connection.execute(
            "UPDATE hyperdb.transactions SET block_hash = ? WHERE block_height = 5",
            (valid_hash("hyper-divergent-5"),),
        )
        tool.assert_post_recovery_locked(locked.connection, plan)


def test_reserve_node_port_blocks_competing_listener():
    tool = load_tool()
    with closing(tool.socket.socket(tool.socket.AF_INET, tool.socket.SOCK_STREAM)) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    with (
        tool.reserve_node_port(port),
        closing(tool.socket.socket(tool.socket.AF_INET, tool.socket.SOCK_STREAM)) as competitor,
        pytest.raises(OSError),
    ):
        competitor.bind(("0.0.0.0", port))


def test_reserve_node_port_rejects_zero():
    tool = load_tool()
    with pytest.raises(ValueError, match="port 0"), tool.reserve_node_port(0):
        pass


def test_assert_no_node_processes_rejects_running_node(tmp_path):
    tool = load_tool()
    root = tmp_path / "Bismuth"
    root.mkdir()
    process_list = f"  123 python3 {root / 'node.py'} --config-custom config_custom.txt\n"

    with pytest.raises(RuntimeError, match="PID 123"):
        tool.assert_no_node_processes(root, process_list=process_list)


def test_assert_no_node_processes_ignores_recovery_tool(tmp_path):
    tool = load_tool()
    process_list = "  456 python3 /tmp/fork_recovery.py --apply\n"

    tool.assert_no_node_processes(tmp_path, process_list=process_list)


def test_assert_no_node_processes_ignores_other_absolute_bismuth_root(tmp_path):
    tool = load_tool()
    selected = tmp_path / "selected" / "Bismuth"
    selected.mkdir(parents=True)
    process_list = "  9181 python3 /srv/observer/Bismuth/node.py --config-custom observer.txt\n"

    tool.assert_no_node_processes(selected, process_list=process_list)


def test_assert_no_node_processes_uses_cwd_for_relative_node(tmp_path):
    tool = load_tool()
    selected = tmp_path / "selected" / "Bismuth"
    observer = tmp_path / "observer" / "Bismuth"
    selected.mkdir(parents=True)
    observer.mkdir(parents=True)
    process_list = "  9181 python3 node.py --config-custom observer.txt\n"

    tool.assert_no_node_processes(
        selected,
        process_list=process_list,
        process_cwds={"9181": observer},
    )


def test_confirm_apply_requires_exact_boundary_and_hash_phrase():
    tool = load_tool()
    plan = tool.RecoveryPlan(
        local_tip_height=10,
        local_tip_hash="fork-10",
        ancestor_height=5,
        ancestor_hash="shared-5",
        first_delete_height=6,
        rollback_blocks=5,
    )

    assert tool.confirmation_phrase(plan) == "ROLLBACK 6-10 TO 5 shared-5"
    with pytest.raises(RuntimeError, match="confirmation did not match"):
        tool.confirm_apply(plan, input_fn=lambda _: "yes")
    tool.confirm_apply(
        plan,
        input_fn=lambda _: "ROLLBACK 6-10 TO 5 shared-5",
    )


def test_write_recovery_bundle_exports_only_rows_that_will_be_deleted(tmp_path):
    tool = load_tool()
    ledger = tmp_path / "ledger.db"
    hyper = tmp_path / "hyper.db"
    index = tmp_path / "index.db"
    for path in (ledger, hyper):
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE transactions (block_height INTEGER, block_hash TEXT)")
        conn.execute("CREATE TABLE misc (block_height INTEGER, value TEXT)")
        conn.executemany(
            "INSERT INTO transactions VALUES (?, ?)",
            [(5, "keep"), (6, "delete"), (-6, "delete-negative")],
        )
        conn.executemany("INSERT INTO misc VALUES (?, ?)", [(5, "keep"), (6, "delete")])
        conn.commit()
        conn.close()
    conn = sqlite3.connect(index)
    conn.execute("CREATE TABLE tokens (block_height INTEGER, token TEXT)")
    conn.execute("CREATE TABLE aliases (block_height INTEGER, alias TEXT)")
    conn.executemany("INSERT INTO tokens VALUES (?, ?)", [(5, "keep"), (6, "delete")])
    conn.executemany("INSERT INTO aliases VALUES (?, ?)", [(5, "keep"), (6, "delete")])
    conn.commit()
    conn.close()
    plan = tool.RecoveryPlan(10, "fork-10", 5, "shared-5", 6, 5)
    bundle = tmp_path / "bundle"

    evidence = {
        10: tool.CanonicalHashEvidence(
            10,
            "main-10",
            {"peer-a:5658": "main-10", "peer-b:5658": "main-10"},
            {},
            2,
        )
    }
    tool.write_recovery_bundle(
        tool.DbPaths(ledger, hyper, index), plan, bundle, evidence
    )

    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    with gzip.open(bundle / "tail.json.gz", "rt", encoding="utf-8") as handle:
        tail = json.load(handle)
    assert manifest["status"] == "prepared"
    assert manifest["first_delete_height"] == 6
    assert manifest["canonical_evidence"]["10"]["votes"]["peer-a:5658"] == "main-10"
    assert [row[0] for row in tail["ledger"]["transactions"]["rows"]] == [6, -6]
    assert [row[0] for row in tail["index"]["tokens"]["rows"]] == [6]

    manifest["selection_mode"] = "explicit"
    manifest["rollback_request"] = {"blocks": 4}
    manifest["recovery_intent_sha256"] = tool.recovery_intent_digest(
        "explicit", manifest["rollback_request"], manifest["peer_policy"]
    )
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(RuntimeError, match="rollback request does not match recovery plan"):
        tool.load_resume_bundle(bundle, tool.DbPaths(ledger, hyper, index))


def test_explicit_resume_peer_policy_rejects_resolved_endpoint_aliases():
    tool = load_tool()
    manifest = {
        "peer_policy": {
            "peers": [["localhost", 5658], ["127.0.0.1", 5658]],
            "required_votes": 2,
            "query_timeout": 1.0,
        }
    }

    with pytest.raises(ValueError, match="duplicate resolved peer endpoint"):
        tool.parse_explicit_peer_policy(manifest)


def create_fake_bismuth_root(tmp_path):
    root = tmp_path / "Bismuth"
    static = root / "static"
    static.mkdir(parents=True)
    (root / "node.py").write_text("# sentinel\n", encoding="utf-8")
    (root / "config.txt").write_text("port=65534\n", encoding="utf-8")
    (root / "suggested_peers.txt").write_text(
        json.dumps({"192.0.2.10": 5658, "198.51.100.7": 5658}), encoding="utf-8"
    )
    (root / "options.py").write_text(
        "class Get:\n"
        "    def read(self, config_file='config.txt', custom_config_file=None):\n"
        "        self.ledger_path = 'static/ledger.db'\n"
        "        self.hyper_path = 'static/hyper.db'\n"
        "        self.port = '65534'\n",
        encoding="utf-8",
    )
    (root / "rpcconnections.py").write_text(
        "import hashlib\n"
        "class Connection:\n"
        "    def __init__(self, peer): self.peer = peer\n"
        "    def command(self, command, options):\n"
        "        height = int(options[0])\n"
        "        value = ('shared-' if height <= 5 else 'main-') + str(height)\n"
        "        value = hashlib.sha224(value.encode()).hexdigest()\n"
        "        return {str(height): {'block_hash': value}}\n"
        "    def close(self): pass\n",
        encoding="utf-8",
    )
    (root / "dbhandler.py").write_text(
        "import sqlite3\n"
        "class DbHandler:\n"
        "    def __init__(self, index_db, ledger_path, hyper_path, ram, ledger_ram_file, logger):\n"
        "        self.index_db, self.ledger_path, self.hyper_path = index_db, ledger_path, hyper_path\n"
        "    def rollback_under(self, height):\n"
        "        for path in (self.ledger_path, self.hyper_path):\n"
        "            conn = sqlite3.connect(path)\n"
        "            conn.execute('DELETE FROM transactions WHERE block_height >= ? OR block_height <= ?', (height, -height))\n"
        "            conn.execute('DELETE FROM misc WHERE block_height >= ?', (height,))\n"
        "            conn.commit(); conn.close()\n"
        "    def tokens_rollback(self, node, height):\n"
        "        conn = sqlite3.connect(self.index_db); conn.execute('DELETE FROM tokens WHERE block_height >= ?', (height,)); conn.commit(); conn.close()\n"
        "    def aliases_rollback(self, node, height):\n"
        "        conn = sqlite3.connect(self.index_db); conn.execute('DELETE FROM aliases WHERE block_height >= ?', (height,)); conn.commit(); conn.close()\n"
        "    def close(self): pass\n",
        encoding="utf-8",
    )
    for name in ("ledger.db", "hyper.db"):
        conn = sqlite3.connect(static / name)
        conn.execute(
            "CREATE TABLE transactions (block_height INTEGER, reward TEXT, block_hash TEXT)"
        )
        conn.execute("CREATE TABLE misc (block_height INTEGER, value TEXT)")
        conn.executemany(
            "INSERT INTO transactions VALUES (?, '1', ?)",
            [
                (
                    height,
                    valid_hash(
                        f"shared-{height}" if height <= 5 else f"fork-{height}"
                    ),
                )
                for height in range(1, 11)
            ],
        )
        conn.executemany("INSERT INTO misc VALUES (?, ?)", [(5, "keep"), (6, "delete")])
        conn.commit()
        conn.close()
    conn = sqlite3.connect(static / "index.db")
    conn.execute("CREATE TABLE tokens (block_height INTEGER)")
    conn.execute("CREATE TABLE aliases (block_height INTEGER)")
    conn.executemany("INSERT INTO tokens VALUES (?)", [(5,), (6,)])
    conn.executemany("INSERT INTO aliases VALUES (?)", [(5,), (6,)])
    conn.commit()
    conn.close()
    return root


def test_cli_defaults_to_dry_run_and_leaves_databases_unchanged(tmp_path):
    root = create_fake_bismuth_root(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout
    assert f"common ancestor: 5 {valid_hash('shared-5')}" in result.stdout
    with sqlite3.connect(root / "static/ledger.db") as conn:
        assert conn.execute("SELECT MAX(block_height) FROM transactions").fetchone() == (10,)


def test_cli_rollback_blocks_dry_run_retains_tip_minus_count(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout
    assert f"rollback target: 8 {valid_hash('main-8')}" in result.stdout
    assert "rollback: 9-10 (2 blocks)" in result.stdout
    with sqlite3.connect(root / "static/ledger.db") as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (10,)


def test_cli_explicit_rollback_does_not_require_peers_by_default(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    (root / "suggested_peers.txt").unlink()
    (root / "rpcconnections.py").unlink()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "manual peer validation: disabled" in result.stdout
    assert "rollback: 9-10 (2 blocks)" in result.stdout


def test_cli_explicit_apply_without_peers_records_local_manual_intent(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "manual-local-bundle"
    (root / "suggested_peers.txt").unlink()
    (root / "rpcconnections.py").unlink()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--rollback-blocks",
            "2",
            "--apply",
            "--bundle-dir",
            str(bundle),
        ],
        input=f"ROLLBACK 9-10 TO 8 {valid_hash('fork-8')}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["selection_mode"] == "explicit"
    assert manifest["rollback_request"] == {"blocks": 2}
    assert manifest["peer_policy"] is None
    assert manifest["canonical_evidence"] == {}


def test_cli_help_describes_automatic_and_explicit_rollback_modes():
    result = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--help"],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    assert "automatic and explicit rollback" in result.stdout


def test_cli_rollback_to_dry_run_retains_requested_height(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-to",
            "7",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "DRY RUN" in result.stdout
    assert f"rollback target: 7 {valid_hash('main-7')}" in result.stdout
    assert "rollback: 8-10 (3 blocks)" in result.stdout
    with sqlite3.connect(root / "static/ledger.db") as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (10,)


def test_cli_rollback_blocks_apply_deletes_exact_suffix(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "explicit-rollback-bundle"
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.execute(
                "INSERT INTO transactions VALUES (?, ?, ?)",
                (-9, "0", valid_hash("negative-9")),
            )
            connection.executemany(
                "INSERT INTO misc VALUES (?, ?)", [(8, "keep-8"), (9, "delete-9")]
            )
            connection.commit()
    with sqlite3.connect(root / "static/index.db") as connection:
        connection.executemany("INSERT INTO tokens VALUES (?)", [(8,), (9,)])
        connection.executemany("INSERT INTO aliases VALUES (?)", [(8,), (9,)])
        connection.commit()

    confirmation = f"ROLLBACK 9-10 TO 8 {valid_hash('main-8')}\n"
    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
            "--apply",
            "--bundle-dir",
            str(bundle),
        ],
        input=confirmation,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "RECOVERY COMPLETE" in result.stdout
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            assert connection.execute(
                "SELECT MAX(block_height) FROM transactions WHERE reward != 0"
            ).fetchone() == (8,)
            assert connection.execute(
                "SELECT COUNT(*) FROM transactions "
                "WHERE block_height >= 9 OR block_height <= -9"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT COUNT(*) FROM misc WHERE block_height >= 9"
            ).fetchone() == (0,)
            assert connection.execute(
                "SELECT COUNT(*) FROM misc WHERE block_height = 8"
            ).fetchone() == (1,)
    with sqlite3.connect(root / "static/index.db") as connection:
        for table in ("tokens", "aliases"):
            assert connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE block_height >= 9"
            ).fetchone() == (0,)
            assert connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE block_height = 8"
            ).fetchone() == (1,)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["selection_mode"] == "explicit"
    assert manifest["rollback_request"] == {"blocks": 2}
    assert manifest["ancestor_height"] == 8
    assert manifest["first_delete_height"] == 9
    assert manifest["rollback_blocks"] == 2


def test_cli_rollback_to_current_tip_apply_is_no_op(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "must-not-exist"
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-to",
            "10",
            "--apply",
            "--bundle-dir",
            str(bundle),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "NO-OP: requested retained height is the current local tip" in result.stdout
    assert not bundle.exists()
    assert not (root / ".fork_recovery_active.json").exists()
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            assert connection.execute(
                "SELECT MAX(block_height) FROM transactions WHERE reward != 0"
            ).fetchone() == (10,)


def test_cli_explicit_rollback_rejects_noncanonical_retained_target(tmp_path):
    root = create_fake_bismuth_root(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "explicit rollback target does not match canonical peer evidence" in result.stderr
    with sqlite3.connect(root / "static/ledger.db") as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (10,)


def test_cli_explicit_custom_peer_file_opts_into_target_verification(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    peer_file = root / "trusted_peers.txt"
    peer_file.write_text(
        json.dumps({"203.0.113.10": 5658, "203.0.113.11": 5658}),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--rollback-blocks",
            "2",
            "--peer-file",
            peer_file.name,
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "explicit rollback target does not match canonical peer evidence" in result.stderr


def test_cli_rollback_blocks_rejects_gapped_reward_suffix(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.execute(
                "DELETE FROM transactions WHERE block_height = 9 AND reward != 0"
            )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "explicit rollback requires a contiguous reward-block suffix" in result.stderr


def test_cli_explicit_rollback_rejects_ledger_hyper_suffix_hash_mismatch(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                block_hash = valid_hash(f"main-{height}")
                if database_name == "hyper.db" and height == 9:
                    block_hash = valid_hash("hyper-mismatch-9")
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (block_hash, height),
                )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "ledger and hyper explicit rollback suffixes disagree" in result.stderr


def test_cli_rollback_blocks_rejects_nonintegral_reward_height(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.execute(
                "UPDATE transactions SET block_height = 8.5 WHERE block_height = 9"
            )
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "3",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "contiguous integer reward-block interval" in result.stderr


def test_cli_explicit_rollback_rejects_real_retained_height(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    for database_name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / database_name) as connection:
            connection.execute("ALTER TABLE transactions RENAME TO transactions_typed")
            connection.execute(
                "CREATE TABLE transactions "
                "(block_height, reward TEXT, block_hash TEXT)"
            )
            connection.execute(
                "INSERT INTO transactions SELECT * FROM transactions_typed"
            )
            connection.execute("DROP TABLE transactions_typed")
            for height in range(6, 11):
                connection.execute(
                    "UPDATE transactions SET block_hash = ? WHERE block_height = ?",
                    (valid_hash(f"main-{height}"), height),
                )
            connection.execute(
                "UPDATE transactions SET block_height = CAST(8.0 AS REAL) "
                "WHERE block_height = 8"
            )
            assert connection.execute(
                "SELECT typeof(block_height) FROM transactions WHERE block_height = 8"
            ).fetchone() == ("real",)
            connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--rollback-blocks",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "contiguous integer reward-block interval" in result.stderr


def test_explicit_reward_suffix_rejects_duplicate_reward_height():
    tool = load_tool()
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(
            "CREATE TABLE transactions "
            "(block_height INTEGER, reward NUMERIC, block_hash TEXT)"
        )
        connection.executemany(
            "INSERT INTO transactions VALUES (?, ?, ?)",
            [
                (9, 1, valid_hash("main-9")),
                (9, -1, valid_hash("duplicate-9")),
                (10, 1, valid_hash("main-10")),
            ],
        )

        with pytest.raises(
            RuntimeError,
            match="explicit rollback requires a contiguous reward-block suffix",
        ):
            tool.assert_contiguous_reward_suffix(connection, "transactions", 8, 10)


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (("--rollback-blocks", "0"), "--rollback-blocks must be at least 1"),
        (
            ("--rollback-blocks", "10"),
            "--rollback-blocks must leave at least block height 1 retained",
        ),
        (("--rollback-to", "0"), "--rollback-to must be between 1 and local tip 10"),
        (("--rollback-to", "11"), "--rollback-to must be between 1 and local tip 10"),
    ],
)
def test_cli_explicit_rollback_rejects_invalid_range(tmp_path, arguments, message):
    root = create_fake_bismuth_root(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            *arguments,
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert message in result.stderr


def test_cli_explicit_rollback_options_are_mutually_exclusive(tmp_path):
    root = create_fake_bismuth_root(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--rollback-to",
            "5",
            "--rollback-blocks",
            "5",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "not allowed with argument" in result.stderr


def test_cli_explicit_rollback_option_is_mutually_exclusive_with_resume(tmp_path):
    root = create_fake_bismuth_root(tmp_path)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--rollback-blocks",
            "2",
            "--resume",
            str(tmp_path / "bundle"),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "not allowed with argument" in result.stderr


@pytest.mark.parametrize(
    "duplicate_hash",
    [valid_hash("shared-5"), valid_hash("conflicting-hyper-5")],
)
@pytest.mark.parametrize("duplicate_reward", ["1", "-1"])
@pytest.mark.parametrize("database_name", ["ledger.db", "hyper.db"])
def test_cli_dry_run_rejects_ambiguous_retained_ancestor(
    tmp_path, duplicate_hash, duplicate_reward, database_name
):
    root = create_fake_bismuth_root(tmp_path)
    with sqlite3.connect(root / "static" / database_name) as connection:
        connection.execute(
            "INSERT INTO transactions VALUES (?, ?, ?)",
            (5, duplicate_reward, duplicate_hash),
        )
        connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "ambiguous reward rows at local height 5" in result.stderr
    assert "DRY RUN" not in result.stdout
    with sqlite3.connect(root / "static/ledger.db") as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions WHERE reward > 0"
        ).fetchone() == (10,)


def test_cli_apply_preserves_ancestor_and_completes_bundle(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "recovery-bundle"

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--apply",
            "--bundle-dir",
            str(bundle),
        ],
        input=f"ROLLBACK 6-10 TO 5 {valid_hash('shared-5')}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "RECOVERY COMPLETE" in result.stdout
    for name in ("ledger.db", "hyper.db"):
        with sqlite3.connect(root / "static" / name) as conn:
            assert conn.execute("SELECT MAX(block_height) FROM transactions").fetchone() == (5,)
    with sqlite3.connect(root / "static/index.db") as conn:
        assert conn.execute("SELECT MAX(block_height) FROM tokens").fetchone() == (5,)
        assert conn.execute("SELECT MAX(block_height) FROM aliases").fetchone() == (5,)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"


def test_cli_apply_restores_original_journal_modes_before_complete(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "journal-mode-bundle"
    connection = sqlite3.connect(root / "static/hyper.db")
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    finally:
        connection.close()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
            "--apply",
            "--bundle-dir",
            str(bundle),
        ],
        input=f"ROLLBACK 6-10 TO 5 {valid_hash('shared-5')}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert manifest["original_journal_modes"]["hyper"] == "wal"
    connection = sqlite3.connect(root / "static/hyper.db")
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
    finally:
        connection.close()


def test_cli_resume_journal_guard_restores_modes_without_tail_mutation(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    bundle = tmp_path / "journal-guard-bundle"
    operation_id = "journal-guard-test"
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    finally:
        connection.close()
    tool.write_journal_guard(
        root, bundle, paths, operation_id, ("delete", "wal", "delete")
    )
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
    finally:
        connection.close()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {operation_id}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "no blockchain rows were changed" in result.stdout
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "journal_restored"
    assert not (root / tool.ROOT_ACTIVE_MARKER).exists()
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (10,)
    finally:
        connection.close()


def test_cli_resume_journal_restored_crash_window_cleans_root_marker(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    bundle = tmp_path / "journal-restored-bundle"
    operation_id = "journal-restored-test"
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    finally:
        connection.close()
    tool.write_journal_guard(
        root, bundle, paths, operation_id, ("delete", "wal", "delete")
    )
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["status"] = "journal_restored"
    manifest["restored_at_utc"] = "simulated-crash-window"
    tool.write_manifest_atomic(manifest_path, manifest)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {operation_id}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "no blockchain rows were changed" in result.stdout
    assert not (root / tool.ROOT_ACTIVE_MARKER).exists()
    completed = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert completed["status"] == "journal_restored"
    with sqlite3.connect(paths.hyper) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (10,)


def test_cli_resume_restoring_phase_repairs_partial_mode_restoration(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    finally:
        connection.close()
    bundle = tmp_path / "restoring-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)
    tool.mark_recovery_committing(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    original_modes = tool.parse_original_journal_modes(manifest)
    with tool.hold_database_locks(
        paths, restore_journal_modes=original_modes
    ) as locked:
        tool.apply_atomic_rollbacks(locked.connection, plan.first_delete_height)
        tool.assert_post_recovery_locked(locked.connection, plan)
        locked.commit_and_relock()
        tool.mark_recovery_restoring(bundle)
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode=DELETE").fetchone() == ("delete",)
    finally:
        connection.close()
    operation_id = manifest["operation_id"]

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {operation_id}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    completed = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert completed["status"] == "complete"
    connection = sqlite3.connect(paths.hyper)
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions"
        ).fetchone() == (5,)
    finally:
        connection.close()


def test_cli_rejects_false_noop_after_partial_apply(tmp_path):
    root = create_fake_bismuth_root(tmp_path)
    with sqlite3.connect(root / "static/ledger.db") as conn:
        conn.execute(
            "DELETE FROM transactions WHERE block_height >= 6 OR block_height <= -6"
        )
        conn.execute("DELETE FROM misc WHERE block_height >= 6")
        conn.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--required-votes",
            "2",
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "inconsistent database tail" in result.stderr


def test_cli_resume_committing_bundle_before_peer_planning(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "interrupted-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)
    tool.mark_recovery_committing(bundle)
    operation_id = json.loads(
        (bundle / "manifest.json").read_text(encoding="utf-8")
    )["operation_id"]

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {operation_id}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "RECOVERY RESUME COMPLETE" in result.stdout
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "complete"
    assert not (bundle / "ACTIVE").exists()


@pytest.mark.parametrize(
    ("peer_prefix", "downgrade_manifest", "expected_returncode", "expected_tip", "error"),
    [
        (
            "changed-",
            False,
            1,
            10,
            "explicit resume target no longer matches canonical peer evidence",
        ),
        ("shared-", False, 0, 5, None),
        ("changed-", True, 1, 10, "recovery intent"),
    ],
)
def test_cli_explicit_pre_resume_refreshes_retained_target_quorum(
    tmp_path,
    peer_prefix,
    downgrade_manifest,
    expected_returncode,
    expected_tip,
    error,
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "explicit-pre-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["selection_mode"] = "explicit"
    manifest["rollback_request"] = {"to_height": 5}
    manifest["peer_policy"] = {
        "peers": [["192.0.2.10", 5658], ["198.51.100.7", 5658]],
        "required_votes": 2,
        "query_timeout": 1.0,
    }
    manifest["canonical_evidence"] = {
        "5": {
            "selected_hash": valid_hash("shared-5"),
            "required_votes": 2,
            "votes": {
                "192.0.2.10:5658": valid_hash("shared-5"),
                "198.51.100.7:5658": valid_hash("shared-5"),
            },
            "errors": {},
        }
    }
    manifest["recovery_intent_sha256"] = tool.recovery_intent_digest(
        manifest["selection_mode"],
        manifest["rollback_request"],
        manifest["peer_policy"],
    )
    tool.write_manifest_atomic(manifest_path, manifest)
    tool.write_root_active_marker(root, bundle)
    if downgrade_manifest:
        manifest["selection_mode"] = "automatic"
        manifest["rollback_request"] = None
        manifest["peer_policy"] = None
        manifest["recovery_intent_sha256"] = tool.recovery_intent_digest(
            "automatic", None, None
        )
        tool.write_manifest_atomic(manifest_path, manifest)
    (root / "rpcconnections.py").write_text(
        "import hashlib\n"
        "class Connection:\n"
        "    def __init__(self, peer): self.peer = peer\n"
        "    def command(self, command, options):\n"
        "        height = int(options[0])\n"
        f"        value = hashlib.sha224(({peer_prefix!r} + str(height)).encode()).hexdigest()\n"
        "        return {str(height): {'block_hash': value}}\n"
        "    def close(self): pass\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {manifest['operation_id']}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == expected_returncode, result.stderr
    if expected_returncode:
        assert error in result.stderr
    else:
        assert "RECOVERY RESUME COMPLETE" in result.stdout
    for database in (paths.ledger, paths.hyper):
        with sqlite3.connect(database) as connection:
            assert connection.execute(
                "SELECT MAX(block_height) FROM transactions WHERE reward != 0"
            ).fetchone() == (expected_tip,)


def test_cli_local_manual_pre_resume_is_peer_independent(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        8,
        valid_hash("fork-8"),
        9,
        2,
    )
    bundle = tmp_path / "local-manual-pre-bundle"
    rollback_request = {"blocks": 2}
    intent_digest = tool.recovery_intent_digest(
        "explicit", rollback_request, None
    )
    tool.write_recovery_bundle(
        paths,
        plan,
        bundle,
        selection_mode="explicit",
        rollback_request=rollback_request,
        peer_policy=None,
    )
    tool.write_root_active_marker(root, bundle, intent_digest)
    (root / "suggested_peers.txt").unlink()
    (root / "rpcconnections.py").unlink()
    operation_id = json.loads(
        (bundle / "manifest.json").read_text(encoding="utf-8")
    )["operation_id"]

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {operation_id}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "RECOVERY RESUME COMPLETE" in result.stdout
    for database in (paths.ledger, paths.hyper):
        with sqlite3.connect(database) as connection:
            assert connection.execute(
                "SELECT MAX(block_height) FROM transactions WHERE reward != 0"
            ).fetchone() == (8,)


def test_resume_rewrite_preserves_bound_recovery_intent_on_crash(
    tmp_path, monkeypatch
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        8,
        valid_hash("fork-8"),
        9,
        2,
    )
    bundle = tmp_path / "resume-marker-crash-bundle"
    rollback_request = {"blocks": 2}
    intent_digest = tool.recovery_intent_digest(
        "explicit", rollback_request, None
    )
    tool.write_recovery_bundle(
        paths,
        plan,
        bundle,
        selection_mode="explicit",
        rollback_request=rollback_request,
        peer_policy=None,
    )
    tool.write_root_active_marker(root, bundle, intent_digest)
    operation_id = json.loads(
        (bundle / "manifest.json").read_text(encoding="utf-8")
    )["operation_id"]
    monkeypatch.setattr("builtins.input", lambda _prompt: f"RESUME {operation_id}")

    def stop_after_marker(_bundle):
        raise RuntimeError("simulated stop after marker rewrite")

    monkeypatch.setattr(tool, "mark_recovery_committing", stop_after_marker)
    with pytest.raises(RuntimeError, match="simulated stop after marker rewrite"):
        tool.resume_recovery(
            root,
            type("Config", (), {"port": 65534})(),
            paths,
            bundle,
        )

    marker = tool.read_root_active_marker(root)
    assert marker is not None
    assert marker["recovery_intent_sha256"] == intent_digest


def test_hold_database_locks_does_not_run_an_implicit_integrity_scan(
    tmp_path, monkeypatch
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    statements = []
    original_connect = tool.sqlite3.connect

    def recording_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(tool.sqlite3, "connect", recording_connect)
    with tool.hold_database_locks(paths):
        pass

    assert not any("quick_check" in statement.casefold() for statement in statements)


def test_offline_apply_preflight_can_defer_integrity_scan_to_exclusive_lock(
    tmp_path, monkeypatch
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    statements = []
    original_connect = tool.sqlite3.connect

    def recording_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(tool.sqlite3, "connect", recording_connect)
    tool.assert_databases_offline(paths, check_integrity=False)

    assert not any("quick_check" in statement.casefold() for statement in statements)
    assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 3


def test_post_recovery_revalidation_can_skip_repeated_full_integrity_scan(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    connection = sqlite3.connect(root / "static/ledger.db", isolation_level=None)
    try:
        connection.execute(
            "ATTACH DATABASE ? AS hyperdb", (str(root / "static/hyper.db"),)
        )
        connection.execute(
            "ATTACH DATABASE ? AS indexdb", (str(root / "static/index.db"),)
        )
        connection.execute("BEGIN EXCLUSIVE")
        tool.apply_atomic_rollbacks(connection, 9)
        statements = []
        connection.set_trace_callback(statements.append)
        tool.assert_post_recovery_locked(
            connection,
            tool.RecoveryPlan(
                10,
                valid_hash("fork-10"),
                8,
                valid_hash("fork-8"),
                9,
                2,
            ),
            check_integrity=False,
        )
    finally:
        connection.rollback()
        connection.close()

    assert not any("quick_check" in statement.casefold() for statement in statements)


def test_local_manual_apply_runs_one_pre_and_one_post_integrity_cycle(
    tmp_path, monkeypatch
):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    bundle = tmp_path / "integrity-budget-bundle"
    statements = []
    original_connect = tool.sqlite3.connect

    def recording_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(tool.sqlite3, "connect", recording_connect)
    monkeypatch.setattr(tool, "confirm_apply", lambda _plan: None)
    result = tool.run_cli(
        tool.build_parser().parse_args(
            [
                "--bismuth-dir",
                str(root),
                "--rollback-blocks",
                "2",
                "--apply",
                "--bundle-dir",
                str(bundle),
            ]
        )
    )

    assert result == 0
    quick_checks = [
        statement for statement in statements if "quick_check" in statement.casefold()
    ]
    assert len(quick_checks) == 6


def test_cli_resume_rejects_missing_root_active_marker(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "markerless-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.mark_recovery_committing(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {manifest['operation_id']}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "root active recovery marker" in result.stderr
    with sqlite3.connect(paths.ledger) as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions WHERE reward > 0"
        ).fetchone() == (10,)


def test_cli_resume_rejects_mismatched_root_operation_id(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "mismatched-marker-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)
    tool.mark_recovery_committing(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    root_marker_path = root / tool.ROOT_ACTIVE_MARKER
    root_marker = json.loads(root_marker_path.read_text(encoding="utf-8"))
    root_marker["operation_id"] = "different-operation"
    tool.write_manifest_atomic(root_marker_path, root_marker)

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {manifest['operation_id']}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "operation ID" in result.stderr
    with sqlite3.connect(paths.ledger) as connection:
        assert connection.execute(
            "SELECT MAX(block_height) FROM transactions WHERE reward > 0"
        ).fetchone() == (10,)


def test_cli_resume_rejects_changed_retained_history(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "interrupted-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)
    tool.mark_recovery_committing(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    with sqlite3.connect(paths.ledger) as connection:
        connection.execute(
            "UPDATE transactions SET block_hash = ? WHERE block_height = 4",
            (valid_hash("tampered-4"),),
        )
        connection.commit()

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {manifest['operation_id']}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "retained database content changed" in result.stderr
    with sqlite3.connect(paths.ledger) as connection:
        assert connection.execute("SELECT MAX(block_height) FROM transactions").fetchone() == (10,)


def test_cli_refuses_new_plan_when_recovery_is_pending(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "pending-custom-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)

    result = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--bismuth-dir", str(root)],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 1
    assert "pending recovery operation" in result.stderr
    assert "--resume" in result.stderr


def test_cli_resume_cleans_stale_marker_after_complete_manifest(tmp_path):
    tool = load_tool()
    root = create_fake_bismuth_root(tmp_path)
    paths = tool.DbPaths(
        root / "static/ledger.db",
        root / "static/hyper.db",
        root / "static/index.db",
    )
    plan = tool.RecoveryPlan(
        10,
        valid_hash("fork-10"),
        5,
        valid_hash("shared-5"),
        6,
        5,
    )
    bundle = tmp_path / "completed-bundle"
    tool.write_recovery_bundle(paths, plan, bundle)
    tool.write_root_active_marker(root, bundle)
    tool.mark_recovery_committing(bundle)
    with tool.hold_database_locks(paths) as locked:
        tool.apply_atomic_rollbacks(locked.connection, plan.first_delete_height)
        tool.assert_post_recovery_locked(locked.connection, plan)
    tool.mark_recovery_restoring(bundle)
    tool.complete_recovery_bundle(bundle)
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))

    result = subprocess.run(
        [
            sys.executable,
            str(TOOL_PATH),
            "--bismuth-dir",
            str(root),
            "--apply",
            "--resume",
            str(bundle),
        ],
        input=f"RESUME {manifest['operation_id']}\n",
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert not (root / tool.ROOT_ACTIVE_MARKER).exists()
