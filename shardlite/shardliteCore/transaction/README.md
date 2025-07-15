# Transaction Management Module

This module provides cross-shard transaction management for Shardlite, implementing the Two-Phase Commit (2PC) protocol to ensure ACID compliance across multiple shards.

## Overview

The transaction module implements distributed transaction coordination using the 2PC protocol. It provides transaction logging, monitoring, and ensures data consistency across all shards involved in a transaction.

## Components

### ParallelTransactionCoordinator

The main coordinator class that implements the 2PC protocol for cross-shard transactions.

**Key Features:**
- Two-Phase Commit protocol implementation
- Parallel execution for performance
- Comprehensive error handling
- Transaction state management
- Resource cleanup

**Constructor:**
```python
ParallelTransactionCoordinator(
    shard_manager: ShardManager,
    logger: Optional[TransactionLogger] = None,
    max_workers: int = 4
)
```

**Key Methods:**
- `begin(shard_keys: List[int]) -> TransactionContext`: Start a new transaction
- `prepare(context: TransactionContext) -> bool`: Execute prepare phase
- `commit(context: TransactionContext) -> bool`: Execute commit phase
- `rollback(context: TransactionContext) -> None`: Execute rollback phase
- `run(shard_keys: List[int], operations: List[Callable]) -> bool`: Run transaction with operations

### TransactionContext

Context manager for transaction lifecycle management.

**Key Features:**
- Context manager interface
- Transaction state tracking
- Operation collection
- Automatic cleanup

**Usage:**
```python
with coordinator.begin([123, 456]) as context:
    # Add operations to transaction
    context.add_operation(lambda: insert_data())
    context.add_operation(lambda: update_data())
    # Transaction commits automatically on exit
```

### TransactionLogger (Protocol)

Interface for transaction logging and monitoring.

**Key Methods:**
- `on_prepare(transaction_id: str, shard_keys: List[int], **kwargs) -> None`
- `on_vote(transaction_id: str, shard_id: int, vote: bool, **kwargs) -> None`
- `on_commit(transaction_id: str, shard_keys: List[int], **kwargs) -> None`
- `on_rollback(transaction_id: str, shard_keys: List[int], reason: str, **kwargs) -> None`
- `on_complete(transaction_id: str, state: TransactionState, duration_ms: float, **kwargs) -> None`
- `on_error(transaction_id: str, error: Exception, **kwargs) -> None`

### TransactionState (Enum)

Enumeration of transaction states:
- `INITIAL`: Transaction created
- `PREPARING`: Prepare phase in progress
- `PREPARED`: All shards prepared successfully
- `COMMITTING`: Commit phase in progress
- `COMMITTED`: Transaction committed successfully
- `ROLLING_BACK`: Rollback phase in progress
- `ROLLED_BACK`: Transaction rolled back
- `FAILED`: Transaction failed

## Usage Examples

### Basic Transaction Usage

```python
from shardlite.transaction import ParallelTransactionCoordinator
from shardlite.transaction.logger import ConsoleTransactionLogger

# Create coordinator with logging
logger = ConsoleTransactionLogger(verbose=True)
coordinator = ParallelTransactionCoordinator(shard_manager, logger)

# Begin transaction
with coordinator.begin([123, 456]) as context:
    # Transaction automatically commits on successful exit
    # or rolls back on exception
    pass
```

### Manual Transaction Control

```python
# Begin transaction
context = coordinator.begin([123, 456])

try:
    # Prepare phase
    if coordinator.prepare(context):
        # Commit phase
        success = coordinator.commit(context)
        if success:
            print("Transaction committed successfully")
        else:
            print("Transaction commit failed")
    else:
        print("Transaction prepare failed")
        coordinator.rollback(context)
except Exception as e:
    print(f"Transaction error: {e}")
    coordinator.rollback(context)
```

### Transaction with Operations

```python
def insert_user(user_id: int, name: str):
    shard_manager.insert("users", {"id": user_id, "name": name}, user_id)

def update_user(user_id: int, email: str):
    shard_manager.update("users", {"email": email}, {"id": user_id}, user_id)

# Run transaction with operations
operations = [
    lambda: insert_user(123, "John"),
    lambda: update_user(456, "jane@example.com")
]

success = coordinator.run([123, 456], operations)
print(f"Transaction {'succeeded' if success else 'failed'}")
```

### Custom Transaction Logger

```python
from shardlite.transaction.logger import TransactionLogger

class MyTransactionLogger(TransactionLogger):
    def on_prepare(self, transaction_id: str, shard_keys: List[int], **kwargs):
        print(f"Preparing transaction {transaction_id} for shards {shard_keys}")
    
    def on_vote(self, transaction_id: str, shard_id: int, vote: bool, **kwargs):
        print(f"Shard {shard_id} voted {'YES' if vote else 'NO'}")
    
    def on_commit(self, transaction_id: str, shard_keys: List[int], **kwargs):
        print(f"Committing transaction {transaction_id}")
    
    def on_rollback(self, transaction_id: str, shard_keys: List[int], reason: str, **kwargs):
        print(f"Rolling back transaction {transaction_id}: {reason}")
    
    def on_complete(self, transaction_id: str, state: TransactionState, duration_ms: float, **kwargs):
        print(f"Transaction {transaction_id} completed with state {state.value} in {duration_ms:.2f}ms")
    
    def on_error(self, transaction_id: str, error: Exception, **kwargs):
        print(f"Transaction {transaction_id} error: {error}")

# Use custom logger
logger = MyTransactionLogger()
coordinator = ParallelTransactionCoordinator(shard_manager, logger)
```

## Two-Phase Commit Protocol

### Phase 1: Prepare Phase

1. **BEGIN IMMEDIATE**: Start transaction on all shards
2. **Write Rollback Journal**: SQLite automatically creates rollback journal
3. **Vote Collection**: Each shard votes YES/NO
4. **Decision**: All YES → proceed to commit, Any NO → rollback

### Phase 2: Decision Phase

1. **All YES**: Execute COMMIT on all shards
2. **Any NO**: Execute ROLLBACK on all shards
3. **Cleanup**: Remove rollback journals and notify logger

### Error Handling

- **Shard Unavailable**: Mark transaction as failed
- **Network Timeout**: Retry with exponential backoff
- **Partial Failure**: Rollback all prepared shards
- **Logger Failure**: Continue transaction, log locally

## Performance Considerations

### Parallel Execution

The coordinator executes operations in parallel:
- **Prepare Phase**: All shards prepare simultaneously
- **Commit Phase**: All shards commit simultaneously
- **Rollback Phase**: All shards rollback simultaneously

### Worker Threads

- **Configurable**: Set max_workers for parallel operations
- **Thread Pool**: Reuses threads for efficiency
- **Resource Management**: Proper cleanup of thread resources

### Transaction Size

- **Small Transactions**: Fast execution, low overhead
- **Large Transactions**: More shards, higher coordination overhead
- **Optimal Range**: 2-10 shards per transaction

## Monitoring and Debugging

### Transaction Metrics

```python
from shardlite.transaction.logger import TransactionMetrics

# Create metrics collector
metrics = TransactionMetrics()

# Record transaction
metrics.record_transaction(TransactionState.COMMITTED, 150.5)

# Get summary
summary = metrics.get_summary()
print(f"Success rate: {summary['success_rate']:.1f}%")
print(f"Average duration: {summary['avg_duration_ms']:.2f}ms")
```

### Console Logging

```python
# Enable verbose logging
logger = ConsoleTransactionLogger(verbose=True)
coordinator = ParallelTransactionCoordinator(shard_manager, logger)

# All transaction events will be logged to console
```

### Null Logging

```python
# Disable logging for performance
logger = NullTransactionLogger()
coordinator = ParallelTransactionCoordinator(shard_manager, logger)
```

## Best Practices

### Transaction Design

1. **Keep transactions small**: Limit number of shards involved
2. **Minimize duration**: Complete transactions quickly
3. **Handle failures gracefully**: Implement proper error handling
4. **Use appropriate timeouts**: Balance responsiveness with reliability

### Error Handling

1. **Catch specific exceptions**: Handle different error types
2. **Implement retry logic**: For transient failures
3. **Log transaction events**: For debugging and monitoring
4. **Graceful degradation**: Continue working with reduced functionality

### Performance Optimization

1. **Batch operations**: Group related operations
2. **Optimize shard selection**: Minimize cross-shard transactions
3. **Monitor performance**: Track transaction metrics
4. **Adjust worker count**: Based on system resources

## Integration with Shardlite

### ShardManager Integration

```python
from shardlite import ShardManager

# ShardManager provides transaction interface
manager = ShardManager(num_shards=4)

# Use transaction context
with manager.transaction([123, 456]) as context:
    manager.insert("users", {"name": "John"}, 123)
    manager.update("users", {"email": "john@example.com"}, {"id": 456}, 456)
```

### API Integration

```python
import shardlite

# Initialize system
shardlite.initialize(num_shards=4)

# Use transaction API
with shardlite.transaction([123, 456]) as context:
    shardlite.insert("users", {"name": "John"}, 123)
    shardlite.update("users", {"email": "john@example.com"}, {"id": 456}, 456)
```

## Module Dependencies

- `threading`: For thread-safe operations
- `concurrent.futures`: For parallel execution
- `time`: For timing and duration tracking
- `uuid`: For transaction ID generation
- `typing`: For type hints

## Future Enhancements

Potential improvements:
- **Three-Phase Commit**: For better fault tolerance
- **Distributed logging**: Centralized transaction logging
- **Performance optimization**: Advanced parallelization strategies
- **Recovery mechanisms**: Automatic transaction recovery
- **Monitoring dashboard**: Real-time transaction monitoring

## Troubleshooting

### Common Issues

1. **Transaction timeouts**: Increase timeout or reduce transaction size
2. **Partial failures**: Implement proper error handling and retry logic
3. **Performance issues**: Monitor metrics and optimize transaction design
4. **Resource leaks**: Ensure proper cleanup with context managers

### Debugging Tips

1. **Enable verbose logging**: Use ConsoleTransactionLogger with verbose=True
2. **Monitor metrics**: Track transaction performance and success rates
3. **Check shard health**: Ensure all shards are accessible
4. **Review transaction design**: Optimize shard selection and operation grouping 