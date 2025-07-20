import pytest
from shardlite.shardliteCore.strategy.hash_strategy import HashShardingStrategy


def test_hash_sharding_routing_consistency():
    strategy = HashShardingStrategy(num_shards=4)
    # Same key always maps to same shard
    for key in [0, 1, 42, 123456, 9999]:
        assert strategy.get_shard_id(key) == key % 4


def test_hash_sharding_distribution():
    strategy = HashShardingStrategy(num_shards=5)
    # Check that keys are distributed across all shards
    counts = [0] * 5
    for key in range(1000):
        shard = strategy.get_shard_id(key)
        counts[shard] += 1
    # All shards should have at least some keys
    assert all(c > 0 for c in counts)


def test_hash_sharding_range_query():
    strategy = HashShardingStrategy(num_shards=3)
    # Range should cover all relevant shards
    shards = strategy.get_shard_range(0, 10)
    assert set(shards).issubset({0, 1, 2})
    assert len(set(shards)) >= 1


def test_hash_sharding_all_shard_ids():
    strategy = HashShardingStrategy(num_shards=7)
    all_ids = strategy.get_all_shard_ids()
    assert set(all_ids) == set(range(7))


def test_hash_sharding_edge_cases():
    strategy = HashShardingStrategy(num_shards=2)
    # Negative keys
    assert strategy.get_shard_id(-1) == (-1) % 2
    # Large keys
    assert strategy.get_shard_id(10**12) == (10**12) % 2
    # Zero shards should raise error
    with pytest.raises(Exception):
        HashShardingStrategy(num_shards=0) 