"""
Public API façade for Shardlite.

This module provides the main public interface for the Shardlite library,
exposing simplified wrapper functions around the ShardManager functionality.
"""

from typing import Dict, List, Any, Optional, Union
from .manager import ShardManager
from .config import ShardliteConfig
from .strategy.base import ShardingStrategy
from .transaction.logger import TransactionLogger


# Global ShardManager instance for simplified API
_default_manager: Optional[ShardManager] = None


def initialize(
    config: Optional[Union[ShardliteConfig, str, dict]] = None,
    strategy: Optional[ShardingStrategy] = None,
    logger: Optional[TransactionLogger] = None,
    **kwargs: Any
) -> ShardManager:
    """
    Initialize the Shardlite system with configuration.
    
    This function creates and configures the global ShardManager instance.
    It should be called before using any other API functions.
    
    Args:
        config: Configuration object, file path, or dictionary
        strategy: Sharding strategy (defaults to HashShardingStrategy)
        logger: Transaction logger for monitoring
        **kwargs: Additional configuration parameters
        
    Returns:
        ShardManager: Configured ShardManager instance
        
    Raises:
        ValueError: If configuration is invalid
        FileNotFoundError: If configuration file doesn't exist
    """
    global _default_manager
    
    _default_manager = ShardManager(config, strategy, logger, **kwargs)
    return _default_manager


def get_manager() -> ShardManager:
    """
    Get the current ShardManager instance.
    
    Returns:
        ShardManager: Current ShardManager instance
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
    """
    if _default_manager is None:
        raise RuntimeError("Shardlite has not been initialized. Call shardlite.initialize() first.")
    
    return _default_manager


def apply_schema(sql: str) -> None:
    """
    Apply DDL schema changes across all shards.
    
    Args:
        sql: SQL DDL statement to execute on all shards
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If SQL is invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    manager.apply_schema(sql)


def insert(table: str, row: Dict[str, Any], key: int) -> None:
    """
    Insert data into the appropriate shard.
    
    Args:
        table: Target table name
        row: Data to insert (column_name: value)
        key: Sharding key for routing
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    manager.insert(table, row, key)


def select(
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
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    return manager.select(table, where, key)


def update(
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
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    return manager.update(table, set_values, where, key)


def delete(
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
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    return manager.delete(table, where, key)


def aggregate(table: str, agg_expr: str) -> Dict[str, Any]:
    """
    Execute aggregation across all shards.
    
    Args:
        table: Source table name
        agg_expr: Aggregation expression (e.g., "COUNT(*)", "SUM(amount)")
        
    Returns:
        Dict[str, Any]: Aggregation results
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
        sqlite3.Error: If database operation fails
    """
    manager = get_manager()
    return manager.aggregate(table, agg_expr)


def transaction(shard_keys: List[int], logger: Optional[TransactionLogger] = None):
    """
    Create a cross-shard transaction context.
    
    Args:
        shard_keys: List of shard keys involved in the transaction
        logger: Optional transaction logger (overrides default)
        
    Returns:
        TransactionContext: Transaction context for managing the transaction
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If shard_keys is empty or invalid
    """
    manager = get_manager()
    return manager.transaction(shard_keys, logger)


def get_shard_info() -> Dict[int, Dict[str, Any]]:
    """
    Get information about all shards.
    
    Returns:
        Dict[int, Dict[str, Any]]: Mapping of shard ID to shard information
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
    """
    manager = get_manager()
    return manager.get_shard_info()


def get_shard_stats() -> Dict[str, Any]:
    """
    Get statistics about shard distribution and usage.
    
    Returns:
        Dict[str, Any]: Shard statistics including distribution, sizes, etc.
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
    """
    manager = get_manager()
    return manager.get_shard_stats()


def validate_shard_files() -> bool:
    """
    Validate that all shard files exist and are accessible.
    
    Returns:
        bool: True if all shard files are valid, False otherwise
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
    """
    manager = get_manager()
    return manager.validate_shard_files()


def shutdown() -> None:
    """
    Shutdown the Shardlite system and clean up resources.
    
    This function should be called when the Shardlite system is no longer
    needed to properly clean up connections and resources.
    """
    global _default_manager
    
    if _default_manager is not None:
        _default_manager.shutdown()
        _default_manager = None


# Context manager for automatic initialization and cleanup
class ShardliteContext:
    """
    Context manager for automatic Shardlite initialization and cleanup.
    
    This class provides a convenient way to use Shardlite with automatic
    resource management.
    
    Usage:
        with ShardliteContext(config) as shardlite:
            shardlite.insert("users", {"name": "John"}, 123)
    """
    
    def __init__(
        self,
        config: Optional[Union[ShardliteConfig, str, dict]] = None,
        strategy: Optional[ShardingStrategy] = None,
        logger: Optional[TransactionLogger] = None,
        **kwargs: Any
    ):
        """
        Initialize Shardlite context.
        
        Args:
            config: Configuration object, file path, or dictionary
            strategy: Sharding strategy (defaults to HashShardingStrategy)
            logger: Transaction logger for monitoring
            **kwargs: Additional configuration parameters
        """
        self.config = config
        self.strategy = strategy
        self.logger = logger
        self.kwargs = kwargs
        self.manager = None
    
    def __enter__(self):
        """Context manager entry - initialize Shardlite."""
        self.manager = initialize(self.config, self.strategy, self.logger, **self.kwargs)
        return self.manager
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - shutdown Shardlite."""
        if self.manager is not None:
            self.manager.shutdown()


# Convenience functions for common operations
def create_table(table_name: str, columns: Dict[str, str]) -> None:
    """
    Create a table across all shards.
    
    Args:
        table_name: Name of the table to create
        columns: Dictionary mapping column names to SQL types
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError("table_name must be a non-empty string")
    
    if not columns or not isinstance(columns, dict):
        raise ValueError("columns must be a non-empty dictionary")
    
    # Build CREATE TABLE SQL
    column_definitions = []
    for column_name, column_type in columns.items():
        column_definitions.append(f"{column_name} {column_type}")
    
    sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
    apply_schema(sql)


def drop_table(table_name: str) -> None:
    """
    Drop a table from all shards.
    
    Args:
        table_name: Name of the table to drop
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If table_name is invalid
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError("table_name must be a non-empty string")
    
    sql = f"DROP TABLE IF EXISTS {table_name}"
    apply_schema(sql)


def count(table: str, where: Optional[Dict[str, Any]] = None) -> int:
    """
    Count rows in a table across all shards.
    
    Args:
        table: Table name to count rows from
        where: WHERE conditions (column_name: value)
        
    Returns:
        int: Total number of rows across all shards
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
    """
    result = aggregate(table, "COUNT(*)")
    return result.get("COUNT(*)", 0)


def sum_column(table: str, column: str, where: Optional[Dict[str, Any]] = None) -> float:
    """
    Sum a column across all shards.
    
    Args:
        table: Table name
        column: Column name to sum
        where: WHERE conditions (column_name: value)
        
    Returns:
        float: Sum of the column across all shards
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
    """
    result = aggregate(table, f"SUM({column})")
    return result.get(f"SUM({column})", 0.0)


def avg_column(table: str, column: str, where: Optional[Dict[str, Any]] = None) -> float:
    """
    Calculate average of a column across all shards.
    
    Args:
        table: Table name
        column: Column name to average
        where: WHERE conditions (column_name: value)
        
    Returns:
        float: Average of the column across all shards
        
    Raises:
        RuntimeError: If Shardlite has not been initialized
        ValueError: If parameters are invalid
    """
    result = aggregate(table, f"AVG({column})")
    return result.get(f"AVG({column})", 0.0) 