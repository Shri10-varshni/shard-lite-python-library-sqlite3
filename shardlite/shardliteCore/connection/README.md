# Connection Management Module

> **Note:** Always call `shutdown()` on the manager (or use the API’s `shutdown()` function) before deleting or moving database files, especially on Windows, to ensure all connections are closed and files can be safely removed.

This module provides SQLite connection pooling and management for Shardlite, ensuring efficient database connections across all shards.

## Overview

The connection module implements a thread-safe connection pool that manages SQLite database connections for each shard. It provides connection reuse, lifecycle management, and performance optimization.

## Components

### ConnectionPool

The main connection pool class that manages SQLite connections for a single shard.

**Key Features:**
- Thread-safe connection management
- Connection reuse to avoid overhead
- Automatic connection cleanup
- Connection health monitoring
- Configurable pool size and timeouts

**Constructor:**
```python
ConnectionPool(
    db_path: str,
    max_connections: int = 10,
    timeout: int = 30,
    check_same_thread: bool = False
)
```

**Key Methods:**
- `get_connection() -> sqlite3.Connection`: Get a connection from pool
- `return_connection(conn: sqlite3.Connection) -> None`: Return connection to pool
- `close_connection(conn: sqlite3.Connection) -> None`: Close a specific connection
- `close_all() -> None`: Close all connections in pool
- `get_pool_stats() -> Dict[str, Any]`: Get pool statistics

**Context Manager:**
```python
with pool.get_connection_context() as conn:
    conn.execute("SELECT * FROM table")
```

## Usage Examples

### Basic Connection Pool Usage

```python
from shardlite.connection import ConnectionPool

# Create connection pool
pool = ConnectionPool(
    db_path="./data/shard_0.db",
    max_connections=10,
    timeout=30
)

# Get connection
conn = pool.get_connection()
try:
    conn.execute("SELECT * FROM users")
    results = conn.fetchall()
finally:
    pool.return_connection(conn)
```

### Using Context Manager

```python
# Automatic connection management
with pool.get_connection_context() as conn:
    conn.execute("INSERT INTO users (name, email) VALUES (?, ?)", ["John", "john@example.com"])
    conn.commit()
```

### Pool Statistics

```python
# Get pool statistics
stats = pool.get_pool_stats()
print(f"Active connections: {stats['active_connections']}")
print(f"Pool size: {stats['pool_size']}")
print(f"Total created: {stats['total_connections_created']}")
```

### Multiple Pools

```python
# Create pools for multiple shards
pools = {}
for shard_id in range(4):
    db_path = f"./data/shard_{shard_id}.db"
    pools[shard_id] = ConnectionPool(db_path, max_connections=5)

# Use pools
for shard_id, pool in pools.items():
    with pool.get_connection_context() as conn:
        conn.execute("SELECT COUNT(*) FROM users")
        count = conn.fetchone()[0]
        print(f"Shard {shard_id}: {count} users")
```

## Configuration Options

### Connection Pool Settings

- **max_connections**: Hard limit on active connections (not pool size) (default: 10)
- **timeout**: Connection timeout in seconds when pool is exhausted (default: 30)
- **check_same_thread**: Whether to check if connections are used in same thread (default: False)

### SQLite Configuration

The connection pool automatically configures SQLite connections with:
- Foreign key constraints enabled
- WAL journal mode for better concurrency
- Appropriate timeout settings

## Performance Considerations

### Connection Reuse

The pool reuses connections to avoid the overhead of creating new SQLite connections:
- Reduces connection establishment time
- Minimizes memory usage
- Improves overall performance

### Thread Safety

The connection pool is thread-safe:
- Multiple threads can safely use the same pool
- Connections are properly managed across threads
- No race conditions in connection allocation

### Pool Sizing

Optimal pool size depends on:
- **Concurrent users**: More users need more connections
- **Query complexity**: Complex queries hold connections longer
- **Available memory**: Each connection uses memory
- **Database size**: Larger databases may need more connections

**Guidelines:**
- Start with 5-10 connections per shard
- Monitor pool statistics to adjust
- Consider application load patterns

## Error Handling

### Connection Failures

The pool handles various connection failures:
- **Invalid connections**: Automatically detected and replaced
- **Database errors**: Properly propagated to application
- **Timeout errors**: Clear error messages with timeout information

### Recovery Mechanisms

- **Automatic cleanup**: Invalid connections are removed from pool
- **Health checks**: Connections are validated before reuse
- **Graceful degradation**: Pool continues working even with some failed connections

## Monitoring and Debugging

### Pool Statistics

Monitor pool health with statistics:
```python
stats = pool.get_pool_stats()
print(f"Active: {stats['active_connections']}")
print(f"Available: {stats['pool_size']}")
print(f"Max: {stats['max_connections']}")
print(f"Total created: {stats['total_connections_created']}")
```

### Connection Health

Check connection health:
```python
with pool.get_connection_context() as conn:
    try:
        conn.execute("SELECT 1")
        print("Connection is healthy")
    except Exception as e:
        print(f"Connection error: {e}")
```

## Best Practices

### Connection Management

1. **Always use context managers**: Ensures proper cleanup
2. **Don't hold connections long**: Return them quickly
3. **Handle exceptions properly**: Use try-finally or context managers
4. **Monitor pool usage**: Watch for connection leaks

### Pool Configuration

1. **Size appropriately**: Not too small, not too large
2. **Set reasonable timeouts**: Balance responsiveness with resource usage
3. **Monitor performance**: Adjust based on actual usage patterns
4. **Consider application patterns**: Peak vs. average usage

### Error Handling

1. **Catch specific exceptions**: Handle different error types appropriately
2. **Log connection issues**: Monitor for recurring problems
3. **Implement retry logic**: For transient connection failures
4. **Graceful degradation**: Continue working with reduced capacity

## Integration with Shardlite

The connection pool integrates seamlessly with other Shardlite components:

### ShardManager Integration

```python
from shardlite import ShardManager

# ShardManager automatically creates connection pools
manager = ShardManager(num_shards=4, db_dir="./data")

# Pools are managed automatically
manager.insert("users", {"name": "John"}, 123)
```

### Router Integration

```python
from shardlite.router import Router

# Router uses connection pools for operations
router = Router(manager, strategy)
router.route_insert("users", {"name": "John"}, 123)
```

## Module Dependencies

- `sqlite3`: For SQLite database connections
- `threading`: For thread-safe operations
- `queue`: For connection pool implementation
- `contextlib`: For context manager support
- `typing`: For type hints

## Future Enhancements

Potential improvements:
- **Connection encryption**: Support for encrypted SQLite databases
- **Connection pooling strategies**: Different pooling algorithms
- **Performance monitoring**: Detailed performance metrics
- **Automatic scaling**: Dynamic pool size adjustment
- **Connection multiplexing**: Multiple queries per connection

## Troubleshooting

### Common Issues

1. **Connection timeout**: Increase timeout or reduce pool size
2. **Memory usage**: Reduce max_connections
3. **Connection leaks**: Ensure proper cleanup with context managers
4. **Performance issues**: Monitor pool statistics and adjust configuration

### Debugging Tips

1. **Enable SQLite logging**: Set appropriate log levels
2. **Monitor pool stats**: Watch for unusual patterns
3. **Check file permissions**: Ensure database files are accessible
4. **Verify thread safety**: Check for concurrent access issues 