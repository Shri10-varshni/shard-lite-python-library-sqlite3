#!/usr/bin/env python3
"""
Configuration Example

This example shows different ways to configure Shardlite:
1. Basic configuration with parameters
2. Configuration from dictionary
3. Configuration from YAML file
4. Configuration validation
"""

import sys
import yaml
import tempfile
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shardlite.shardliteCore.api import initialize, shutdown
from shardlite.shardliteCore.config import ShardliteConfig


def create_sample_yaml_config():
    """Create a sample YAML configuration file."""
    config_data = {
        'num_shards': 4,
        'db_dir': './config_example_data',
        'connection_timeout': 30,
        'auto_create_dirs': True,
        'max_connections_per_shard': 5
    }
    
    # Create temporary YAML file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False)
    yaml.dump(config_data, temp_file)
    temp_file.close()
    
    return temp_file.name


def main():
    print("=== Configuration Examples ===\n")
    
    try:
        # Example 1: Basic configuration with parameters
        print("1. Basic configuration with parameters:")
        manager1 = initialize(
            num_shards=2,
            db_dir="./basic_config_data",
            connection_timeout=20,
            auto_create_dirs=True
        )
        print("✅ Basic configuration initialized")
        shutdown()
        print()
        
        # Example 2: Configuration from dictionary
        print("2. Configuration from dictionary:")
        config_dict = {
            'num_shards': 3,
            'db_dir': './dict_config_data',
            'connection_timeout': 25,
            'max_connections_per_shard': 8
        }
        
        manager2 = initialize(config=config_dict)
        print("✅ Dictionary configuration initialized")
        shutdown()
        print()
        
        # Example 3: Configuration from YAML file
        print("3. Configuration from YAML file:")
        yaml_file = create_sample_yaml_config()
        
        try:
            manager3 = initialize(config=yaml_file)
            print("✅ YAML configuration initialized")
            shutdown()
        finally:
            # Clean up temporary file
            os.unlink(yaml_file)
        print()
        
        # Example 4: Configuration validation
        print("4. Configuration validation:")
        
        # Valid configuration
        try:
            valid_config = ShardliteConfig(num_shards=4, db_dir="./valid_data")
            print("✅ Valid configuration created")
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
        
        # Invalid configuration
        try:
            invalid_config = ShardliteConfig(num_shards=0, db_dir="./invalid_data")
            print("❌ Should have failed with invalid num_shards")
        except ValueError as e:
            print(f"✅ Correctly caught invalid configuration: {e}")
        
        print()
        
        # Example 5: Configuration inspection
        print("5. Configuration inspection:")
        config = ShardliteConfig(
            num_shards=4,
            db_dir="./inspect_data",
            connection_timeout=30,
            auto_create_dirs=True,
            max_connections_per_shard=10
        )
        
        print(f"   Number of shards: {config.num_shards}")
        print(f"   Database directory: {config.db_dir}")
        print(f"   Connection timeout: {config.connection_timeout} seconds")
        print(f"   Auto create directories: {config.auto_create_dirs}")
        print(f"   Max connections per shard: {config.max_connections_per_shard}")
        
        # Get shard file paths
        print("\n   Shard file paths:")
        for shard_id in range(config.num_shards):
            path = config.get_shard_file_path(shard_id)
            print(f"     Shard {shard_id}: {path}")
        
        # Convert to dictionary
        config_dict = config.to_dict()
        print(f"\n   Configuration as dictionary: {config_dict}")
        
        print()
        print("🎉 Configuration examples completed successfully!")
        print("\n💡 Key points:")
        print("   - Multiple ways to configure Shardlite")
        print("   - Configuration validation prevents errors")
        print("   - YAML files are great for production deployments")
        print("   - Configuration objects provide helper methods")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    main() 