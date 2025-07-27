"""
Shardlite - A lightweight Python library for SQLite database sharding.

Shardlite provides a unified, extensible API for sharding SQLite databases,
addressing gaps that currently require custom, error-prone code in real-world projects.
The library's value lies in simplifying routing, aggregation, and schema management.

Core Features:
- Hash-based sharding with deterministic routing
- Unified CRUD API for all database operations
- Cross-shard transactions with 2PC protocol
- Schema management across all shards
- Connection pooling for performance
- Comprehensive error handling and logging

Example Usage:
    import shardlite
    
    # Initialize with configuration
    shardlite.initialize(num_shards=4, db_dir="./data")
    
    # Create tables
    shardlite.create_table("users", {
        "id": "INTEGER PRIMARY KEY",
        "name": "TEXT",
        "email": "TEXT"
    })
    
    # Insert data
    shardlite.insert("users", {"name": "John", "email": "john@example.com"}, 123)
    
    # Query data
    users = shardlite.select("users", key=123)
    
    # Cross-shard transactions
    with shardlite.transaction([123, 456]) as tx:
        # Perform operations
        pass
"""

# Version information
__version__ = "0.1.0"
__author__ = "Shardlite Team"
__email__ = "team@shardlite.org"

# Core classes and functions
from .api import (
    # Main API functions
    initialize,
    get_manager,
    apply_schema,
    insert,
    select,
    update,
    delete,
    aggregate,
    transaction,
    get_shard_info,
    get_shard_stats,
    validate_shard_files,
    shutdown,
    
    # Context manager
    ShardliteContext,
    
    # Convenience functions
    create_table,
    drop_table,
    count,
    sum_column,
    avg_column,
)

from .manager import ShardManager
from .config import ShardliteConfig

# Strategy classes
from .strategy.base import ShardingStrategy
from .strategy.hash_strategy import HashShardingStrategy

# Transaction classes
from .transaction.logger import (
    TransactionLogger,
    TransactionState,
    TransactionEvent,
    ConsoleTransactionLogger,
    NullTransactionLogger,
    TransactionMetrics,
)
from .transaction.coordinator import (
    ParallelTransactionCoordinator,
    Transaction,
)

# Connection classes
from .connection.pool import ConnectionPool

# Router class
from .router.router import Router

# Utility functions
from .utils.helpers import (
    ensure_directory,
    get_shard_filename,
    validate_shard_files,
    get_file_size,
    format_bytes,
    validate_sql_identifier,
    sanitize_table_name,
    build_where_clause,
    build_set_clause,
    validate_row_data,
    merge_results,
    aggregate_results,
    calculate_shard_distribution,
    check_disk_space,
    get_database_info,
)

# Public API exports
__all__ = [
    # Version and metadata
    "__version__",
    "__author__",
    "__email__",
    
    # Main API functions
    "initialize",
    "get_manager",
    "apply_schema",
    "insert",
    "select",
    "update",
    "delete",
    "aggregate",
    "transaction",
    "get_shard_info",
    "get_shard_stats",
    "validate_shard_files",
    "shutdown",
    
    # Context manager
    "ShardliteContext",
    
    # Convenience functions
    "create_table",
    "drop_table",
    "count",
    "sum_column",
    "avg_column",
    
    # Core classes
    "ShardManager",
    "ShardliteConfig",
    
    # Strategy classes
    "ShardingStrategy",
    "HashShardingStrategy",
    
    # Transaction classes
    "TransactionLogger",
    "TransactionState",
    "TransactionEvent",
    "ConsoleTransactionLogger",
    "NullTransactionLogger",
    "TransactionMetrics",
    "ParallelTransactionCoordinator",
    "Transaction",
    
    # Connection classes
    "ConnectionPool",
    
    # Router class
    "Router",
    
    # Utility functions
    "ensure_directory",
    "get_shard_filename",
    "validate_shard_files",
    "get_file_size",
    "format_bytes",
    "validate_sql_identifier",
    "sanitize_table_name",
    "build_where_clause",
    "build_set_clause",
    "validate_row_data",
    "merge_results",
    "aggregate_results",
    "calculate_shard_distribution",
    "check_disk_space",
    "get_database_info",
]
