"""
Configuration management for Shardlite.

This module provides the ShardliteConfig class for managing shard configuration,
including loading from files, validation, and providing default values.
"""

from typing import Dict, Any, Optional, Union
from pathlib import Path
import yaml
import json
import os


class ShardliteConfig:
    """
    Configuration management for Shardlite sharding system.
    
    This class handles all configuration aspects including shard count,
    database directory, connection settings, and validation.
    
    Attributes:
        num_shards (int): Number of shards to distribute data across
        db_dir (str): Directory path for storing shard database files
        connection_timeout (int): SQLite connection timeout in seconds
        auto_create_dirs (bool): Whether to automatically create directories
        max_connections_per_shard (int): Maximum connections per shard pool
    """
    
    def __init__(
        self, 
        num_shards: int = 4, 
        db_dir: str = "./data", 
        connection_timeout: int = 30,
        auto_create_dirs: bool = True,
        max_connections_per_shard: int = 10,
        **kwargs: Any
    ) -> None:
        """
        Initialize ShardliteConfig with default or custom values.
        
        Args:
            num_shards: Number of shards to distribute data across
            db_dir: Directory path for storing shard database files
            connection_timeout: SQLite connection timeout in seconds
            auto_create_dirs: Whether to automatically create directories
            max_connections_per_shard: Maximum connections per shard pool
            **kwargs: Additional configuration options
        """
        self.num_shards = num_shards
        self.db_dir = db_dir
        self.connection_timeout = connection_timeout
        self.auto_create_dirs = auto_create_dirs
        self.max_connections_per_shard = max_connections_per_shard
        self._additional_config = kwargs
        
        # Validate configuration
        if not self.validate():
            raise ValueError("Invalid configuration parameters")
    
    @classmethod
    def load_from_file(cls, config_path: str) -> 'ShardliteConfig':
        """
        Load configuration from a file (YAML or JSON).
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            ShardliteConfig: Loaded configuration object
            
        Raises:
            FileNotFoundError: If configuration file doesn't exist
            ValueError: If configuration file is invalid
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                if path.suffix.lower() in ['.yaml', '.yml']:
                    config_dict = yaml.safe_load(f)
                elif path.suffix.lower() == '.json':
                    config_dict = json.load(f)
                else:
                    raise ValueError(f"Unsupported configuration file format: {path.suffix}")
            
            return cls.load_from_dict(config_dict)
        except (yaml.YAMLError, json.JSONDecodeError) as e:
            raise ValueError(f"Invalid configuration file format: {e}")
    
    @classmethod
    def load_from_dict(cls, config_dict: Dict[str, Any]) -> 'ShardliteConfig':
        """
        Load configuration from a dictionary.
        
        Args:
            config_dict: Dictionary containing configuration parameters
            
        Returns:
            ShardliteConfig: Configuration object created from dictionary
        """
        return cls(**config_dict)
    
    def validate(self) -> bool:
        """
        Validate configuration parameters.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        # Validate num_shards
        if not isinstance(self.num_shards, int) or self.num_shards <= 0:
            return False
        
        # Validate db_dir
        if not isinstance(self.db_dir, str) or not self.db_dir:
            return False
        
        # Validate connection_timeout
        if not isinstance(self.connection_timeout, int) or self.connection_timeout <= 0:
            return False
        
        # Validate auto_create_dirs
        if not isinstance(self.auto_create_dirs, bool):
            return False
        
        # Validate max_connections_per_shard
        if not isinstance(self.max_connections_per_shard, int) or self.max_connections_per_shard <= 0:
            return False
        
        return True
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary.
        
        Returns:
            Dict[str, Any]: Dictionary representation of configuration
        """
        config_dict = {
            'num_shards': self.num_shards,
            'db_dir': self.db_dir,
            'connection_timeout': self.connection_timeout,
            'auto_create_dirs': self.auto_create_dirs,
            'max_connections_per_shard': self.max_connections_per_shard,
        }
        config_dict.update(self._additional_config)
        return config_dict
    
    def get_shard_file_path(self, shard_id: int) -> str:
        """
        Get the file path for a specific shard.
        
        Args:
            shard_id: ID of the shard
            
        Returns:
            str: Full path to the shard database file
        """
        if shard_id < 0 or shard_id >= self.num_shards:
            raise ValueError(f"Invalid shard ID: {shard_id}")
        
        filename = f"shard_{shard_id}.db"
        return os.path.join(self.db_dir, filename)
    
    def get_all_shard_paths(self) -> Dict[int, str]:
        """
        Get file paths for all shards.
        
        Returns:
            Dict[int, str]: Mapping of shard ID to file path
        """
        return {shard_id: self.get_shard_file_path(shard_id) 
                for shard_id in range(self.num_shards)}
    
    def __repr__(self) -> str:
        """Return string representation of configuration."""
        return (f"ShardliteConfig(num_shards={self.num_shards}, "
                f"db_dir='{self.db_dir}', connection_timeout={self.connection_timeout})")
    
    def __eq__(self, other: Any) -> bool:
        """Check if this configuration equals another."""
        if not isinstance(other, ShardliteConfig):
            return False
        return self.to_dict() == other.to_dict() 