import os
import shutil
import pytest
import time
import gc
from uuid import uuid4
from shardlite.shardliteCore.manager import ShardManager
from shardlite.shardliteCore.router.router import Router
from shardlite.shardliteCore.strategy.hash_strategy import HashShardingStrategy
from shardlite.shardliteCore.config import ShardliteConfig

@pytest.fixture(scope="function")
def temp_router():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../testing_data"))
    os.makedirs(base_dir, exist_ok=True)
    test_dir = os.path.join(base_dir, f"router_test_{uuid4().hex}")
    os.makedirs(test_dir, exist_ok=True)
    config = ShardliteConfig(num_shards=3, db_dir=test_dir, auto_create_dirs=True)
    strategy = HashShardingStrategy(num_shards=3)
    manager = ShardManager(config, strategy)
    router = Router(manager, strategy)
    try:
        yield router, manager
    finally:
        manager.shutdown()
        gc.collect()
        time.sleep(0.1)
        def on_rm_error(func, path, exc_info):
            if isinstance(exc_info[1], PermissionError):
                time.sleep(0.1)
                try:
                    func(path)
                except Exception:
                    pass
        shutil.rmtree(test_dir, onerror=on_rm_error)


def test_route_insert_and_select(temp_router):
    router, manager = temp_router
    # Create table on all shards
    manager.apply_schema("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    # Insert into different shards
    for i in range(6):
        router.route_insert("test", {"id": i, "value": f"val{i}"}, key=i)
    # Select from all shards
    all_rows = router.route_select("test")
    assert len(all_rows) == 6
    # Select by key (should route to correct shard)
    row = router.route_select("test", where={"id": 3}, key=3)
    assert row[0]["id"] == 3
    assert row[0]["value"] == "val3"


def test_route_update_and_delete(temp_router):
    router, manager = temp_router
    manager.apply_schema("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
    for i in range(3):
        router.route_insert("test", {"id": i, "value": "old"}, key=i)
    # Update one row
    affected = router.route_update("test", {"value": "new"}, where={"id": 1}, key=1)
    assert affected == 1
    updated = router.route_select("test", key=1)
    assert updated[0]["value"] == "new"
    # Delete one row
    deleted = router.route_delete("test", where={"id": 1}, key=1)
    assert deleted == 1
    remaining = router.route_select("test")
    assert len(remaining) == 2


def test_route_aggregate(temp_router):
    router, manager = temp_router
    manager.apply_schema("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
    for i in range(6):
        router.route_insert("test", {"id": i, "value": i * 10}, key=i)
    # COUNT
    count = router.route_aggregate("test", "COUNT(*)")
    assert count["COUNT(*)"] == 6
    # SUM
    total = router.route_aggregate("test", "SUM(value)")
    assert total["SUM(value)"] == sum(i * 10 for i in range(6))
    # AVG
    avg = router.route_aggregate("test", "AVG(value)")
    assert avg["AVG(value)"] == pytest.approx(sum(i * 10 for i in range(6)) / 6)
    # MAX
    maxv = router.route_aggregate("test", "MAX(value)")
    assert maxv["MAX(value)"] == 50
    # MIN
    minv = router.route_aggregate("test", "MIN(value)")
    assert minv["MIN(value)"] == 0 