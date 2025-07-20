import os
import tempfile
import yaml
import pytest
from shardlite.shardliteCore.config import ShardliteConfig


def test_config_from_dict_and_validation():
    config_dict = {
        "num_shards": 3,
        "db_dir": "/tmp/shardlite_test_config",
        "auto_create_dirs": True
    }
    config = ShardliteConfig.load_from_dict(config_dict)
    assert config.num_shards == 3
    assert config.db_dir == "/tmp/shardlite_test_config"
    assert config.auto_create_dirs is True
    # Validation should pass
    config.validate()


def test_config_from_yaml(tmp_path):
    yaml_content = {
        "num_shards": 2,
        "db_dir": str(tmp_path / "db"),
        "auto_create_dirs": True
    }
    yaml_file = tmp_path / "config.yaml"
    with open(yaml_file, "w") as f:
        yaml.dump(yaml_content, f)
    config = ShardliteConfig.load_from_file(str(yaml_file))
    assert config.num_shards == 2
    assert config.db_dir == str(tmp_path / "db")
    assert config.auto_create_dirs is True
    config.validate()


def test_shard_file_path_helpers(tmp_path):
    config = ShardliteConfig(num_shards=2, db_dir=str(tmp_path), auto_create_dirs=True)
    # Should generate correct file paths
    path0 = config.get_shard_file_path(0)
    path1 = config.get_shard_file_path(1)
    assert str(tmp_path) in path0
    assert str(tmp_path) in path1
    assert path0 != path1
    # get_all_shard_paths returns correct number
    all_paths = config.get_all_shard_paths()
    assert len(all_paths) == 2
    assert path0 in all_paths.values() and path1 in all_paths.values() 