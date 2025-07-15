"""
ShardManager - Main orchestrator for Shardlite.

This module provides the ShardManager class that serves as the central
orchestrator for all sharding operations, coordinating between the
API layer and the underlying shard infrastructure.
"""

import os
from typing import Dict, List, Any, Optional, Union
from .config import ShardliteConfig
from .strategy.hash_strategy import HashShardingStrategy
from .strategy.base import ShardingStrategy
from .router import Router
from .transaction.coordinator import ParallelTransactionCoordinator
from .transaction.logger import TransactionLogger, NullTransactionLogger


class ShardManager:
    """
    Main orchestrator for Shardlite sharding system.
    
    This class serves as the central coordinator for all sharding operations,
    managing shard initialization, routing, and transaction coordination.
    
    The ShardManager provides:
    - Unified interface for all database operations
    - Shard initialization and management
    - Configuration management
    - Transaction coordination
    - Connection pool management
    
    Attributes:
        config: Configuration object for shard settings
        strategy: Sharding strategy for routing decisions
        router: Router for directing operations to shards
        coordinator: Transaction coordinator for cross-shard operations
        shard_registry: Mapping of shard IDs to shard information
    """
    
    def __init__(
        self, 
        config: Optional[Union[ShardliteConfig, str, dict]] = None,
        strategy: Optional[ShardingStrategy] = None,
        logger: Optional[TransactionLogger] = None,
        **kwargs: Any
    ) -> None:
        """
        Initialize ShardManager with configuration and components.
        
        Args:
            config: Configuration object, file path, or dictionary
            strategy: Sharding strategy (defaults to HashShardingStrategy)
            logger: Transaction logger for monitoring
            **kwargs: Additional configuration parameters
            
        Raises:
            ValueError: If configuration is invalid
            FileNotFoundError: If configuration file doesn't exist
        """
        # Load configuration
        if config is None:
            self.config = ShardliteConfig(**kwargs)
        elif isinstance(config, ShardliteConfig):
            self.config = config
        elif isinstance(config, str):
            self.config = ShardliteConfig.load_from_file(config)
        elif isinstance(config, dict):
            self.config = ShardliteConfig.load_from_dict(config)
        else:
            raise ValueError(f"Invalid config type: {type(config)}")
        
        # Initialize sharding strategy
        if strategy is None:
            self.strategy = HashShardingStrategy(self.config.num_shards)
        else:
            self.strategy = strategy
        
        # Initialize transaction logger
        self.logger = logger or NullTransactionLogger()
        
        # Initialize components
        self.router = Router(self, self.strategy)
        self.coordinator = ParallelTransactionCoordinator(self, self.logger)
        
        # Shard registry
        self.shard_registry: Dict[int, Dict[str, Any]] = {}
        
        # Initialize shards
        self._initialize_shards()
    
    def _initialize_shards(self) -> None:
        """
        Initialize all shards and build shard registry.
        
        This method creates the shard database files and builds
        the internal registry mapping shard IDs to file paths.
        """
        # Create database directory if it doesn't exist
        if self.config.auto_create_dirs:
            os.makedirs(self.config.db_dir, exist_ok=True)
        
        # Build shard registry
        self._build_shard_registry()
        
        # Create shard files
        self._create_shard_files()
    
    def _build_shard_registry(self) -> None:
        """
        Build internal registry of shard information.
        
        This method creates the mapping of shard IDs to their
        corresponding database file paths and metadata.
        """
        self.shard_registry = {}
        
        for shard_id in range(self.config.num_shards):
            db_path = self.config.get_shard_file_path(shard_id)
            
            self.shard_registry[shard_id] = {
                'db_path': db_path,
                'shard_id': shard_id,
                'exists': os.path.exists(db_path),
                'size': os.path.getsize(db_path) if os.path.exists(db_path) else 0
            }
    
    def _create_shard_files(self) -> None:
        """
        Create SQLite database files for all shards.
        
        This method creates the physical SQLite database files
        for each shard if they don't already exist.
        """
        for shard_id, shard_info in self.shard_registry.items():
            db_path = shard_info['db_path']
            
            if not os.path.exists(db_path):
                # Create empty SQLite database
                import sqlite3
                conn = sqlite3.connect(db_path)
                conn.close()
                
                # Update registry
                self.shard_registry[shard_id]['exists'] = True
                self.shard_registry[shard_id]['size'] = 0
    
    def apply_schema(self, sql: str) -> None:
        """
        Apply DDL schema changes across all shards.
        
        Args:
            sql: SQL DDL statement to execute on all shards
            
        Raises:
            ValueError: If SQL is invalid
            sqlite3.Error: If database operation fails
        """
        if not sql or not isinstance(sql, str):
            raise ValueError("SQL must be a non-empty string")
        
        # Execute schema change on all shards
        for shard_id in self.strategy.get_all_shard_ids():
            connection_pool = self.router.get_connection_for_key(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                conn.execute(sql)
                conn.commit()
    
    def insert(self, table: str, row: Dict[str, Any], key: int) -> None:
        """
        Insert data into the appropriate shard.
        
        Args:
            table: Target table name
            row: Data to insert (column_name: value)
            key: Sharding key for routing
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        self.router.route_insert(table, row, key)
    
    def select(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Query data from appropriate shard(s).
        
        Args:
            table: Source table name
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, query all shards)
            
        Returns:
            List[Dict[str, Any]]: Query results
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        return self.router.route_select(table, where, key)
    
    def update(
        self, 
        table: str, 
        set_values: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> int:
        """
        Update data in appropriate shard(s).
        
        Args:
            table: Target table name
            set_values: Values to set (column_name: value)
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, update all shards)
            
        Returns:
            int: Number of rows affected
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        return self.router.route_update(table, set_values, where, key)
    
    def delete(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> int:
        """
        Delete data from appropriate shard(s).
        
        Args:
            table: Target table name
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, delete from all shards)
            
        Returns:
            int: Number of rows affected
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        return self.router.route_delete(table, where, key)
    
    def aggregate(self, table: str, agg_expr: str) -> Dict[str, Any]:
        """
        Execute aggregation across all shards.
        
        Args:
            table: Source table name
            agg_expr: Aggregation expression (e.g., "COUNT(*)", "SUM(amount)")
            
        Returns:
            Dict[str, Any]: Aggregation results
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        return self.router.route_aggregate(table, agg_expr)
    
    def transaction(self, shard_keys: List[int], logger: Optional[TransactionLogger] = None) -> 'TransactionContext':
        """
        Create a cross-shard transaction context.
        
        Args:
            shard_keys: List of shard keys involved in the transaction
            logger: Optional transaction logger (overrides default)
            
        Returns:
            TransactionContext: Transaction context for managing the transaction
            
        Raises:
            ValueError: If shard_keys is empty or invalid
        """
        if not shard_keys:
            raise ValueError("shard_keys cannot be empty")
        
        # Use provided logger or default
        tx_logger = logger or self.logger
        
        # Create transaction coordinator with specific logger
        coordinator = ParallelTransactionCoordinator(self, tx_logger)
        
        return coordinator.begin(shard_keys)
    
    def get_shard_info(self) -> Dict[int, Dict[str, Any]]:
        """
        Get information about all shards.
        
        Returns:
            Dict[int, Dict[str, Any]]: Mapping of shard ID to shard information
        """
        # Update file sizes
        for shard_id, shard_info in self.shard_registry.items():
            db_path = shard_info['db_path']
            if os.path.exists(db_path):
                shard_info['size'] = os.path.getsize(db_path)
                shard_info['exists'] = True
            else:
                shard_info['size'] = 0
                shard_info['exists'] = False
        
        return self.shard_registry.copy()
    
    def get_shard_stats(self) -> Dict[str, Any]:
        """
        Get statistics about shard distribution and usage.
        
        Returns:
            Dict[str, Any]: Shard statistics including distribution, sizes, etc.
        """
        shard_info = self.get_shard_info()
        
        total_size = sum(info['size'] for info in shard_info.values())
        existing_shards = sum(1 for info in shard_info.values() if info['exists'])
        
        # Get shard distribution from strategy
        distribution = self.strategy.get_shard_distribution()
        
        return {
            'total_shards': self.config.num_shards,
            'existing_shards': existing_shards,
            'total_size_bytes': total_size,
            'avg_size_bytes': total_size / self.config.num_shards if self.config.num_shards > 0 else 0,
            'shard_distribution': distribution,
            'strategy_type': type(self.strategy).__name__,
            'db_directory': self.config.db_dir
        }
    
    def validate_shard_files(self) -> bool:
        """
        Validate that all shard files exist and are accessible.
        
        Returns:
            bool: True if all shard files are valid, False otherwise
        """
        for shard_id, shard_info in self.shard_registry.items():
            db_path = shard_info['db_path']
            
            if not os.path.exists(db_path):
                return False
            
            # Try to open the database
            try:
                import sqlite3
                conn = sqlite3.connect(db_path)
                conn.execute("SELECT 1")
                conn.close()
            except Exception:
                return False
        
        return True
    
    def reinitialize_shards(self) -> None:
        """
        Reinitialize all shards.
        
        This method recreates the shard registry and validates
        all shard files. Useful after configuration changes.
        """
        self._build_shard_registry()
        self._create_shard_files()
    
    def shutdown(self) -> None:
        """
        Shutdown the ShardManager and clean up resources.
        
        This method should be called when the ShardManager is no longer
        needed to properly clean up connections and resources.
        """
        # Shutdown transaction coordinator
        self.coordinator.shutdown()
        
        # Close all connection pools
        for connection_pool in self.router.connection_pools.values():
            connection_pool.close_all()
    
    def __repr__(self) -> str:
        """Return string representation of the ShardManager."""
        stats = self.get_shard_stats()
        return (f"ShardManager(shards={stats['total_shards']}, "
                f"strategy={stats['strategy_type']}, "
                f"total_size={stats['total_size_bytes']} bytes)")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - shutdown manager."""
        self.shutdown() 