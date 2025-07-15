# Sharding Strategy Module

This module provides the sharding strategy framework for Shardlite, defining how data is distributed across multiple shards.

## Overview

The strategy module implements the Strategy pattern to allow different sharding algorithms to be used interchangeably. This provides flexibility in choosing the most appropriate sharding method for your specific use case.

## Components

### ShardingStrategy (Abstract Base Class)

The abstract base class that all sharding strategies must inherit from.

**Key Methods:**
- `get_shard_id(key: int) -> int`: Determine which shard a key belongs to
- `get_shard_range(start_key: int, end_key: int) -> List[int]`: Get shards for a key range
- `get_all_shard_ids() -> List[int]`: Get all shard IDs
- `get_num_shards() -> int`: Get total number of shards

**Utility Methods:**
- `validate_key(key: int) -> bool`: Validate if a key is acceptable
- `validate_key_range(start_key: int, end_key: int) -> bool`: Validate key range
- `get_shard_distribution(num_keys: int = 1000) -> List[int]`: Get distribution analysis

### HashShardingStrategy

Implements hash-based sharding using the modulo operation: `shard_id = key % num_shards`

**Features:**
- Deterministic routing (same key always goes to same shard)
- Simple and fast implementation
- Good distribution for sequential keys
- Easy to understand and debug

**Constructor:**
```python
HashShardingStrategy(num_shards: int)
```

**Additional Methods:**
- `get_optimal_shard_range(start_key: int, end_key: int) -> List[int]`: Optimized range query
- `get_shard_for_key_range(key_range_size: int) -> List[int]`: Estimate shards needed

## Usage Examples

### Basic Hash Sharding

```python
from shardlite.strategy import HashShardingStrategy

# Create strategy with 4 shards
strategy = HashShardingStrategy(4)

# Route a key to a shard
shard_id = strategy.get_shard_id(123)  # Returns 3 (123 % 4)

# Get all shard IDs
all_shards = strategy.get_all_shard_ids()  # Returns [0, 1, 2, 3]
```

### Custom Strategy Implementation

```python
from shardlite.strategy.base import ShardingStrategy

class RangeShardingStrategy(ShardingStrategy):
    def __init__(self, ranges: List[Tuple[int, int]]):
        self.ranges = ranges
        self.num_shards = len(ranges)
    
    def get_shard_id(self, key: int) -> int:
        for i, (start, end) in enumerate(self.ranges):
            if start <= key <= end:
                return i
        raise ValueError(f"Key {key} not in any range")
    
    def get_shard_range(self, start_key: int, end_key: int) -> List[int]:
        # Implementation for range-based strategy
        pass
    
    def get_all_shard_ids(self) -> List[int]:
        return list(range(self.num_shards))
    
    def get_num_shards(self) -> int:
        return self.num_shards
```

### Distribution Analysis

```python
# Analyze key distribution
strategy = HashShardingStrategy(4)
distribution = strategy.get_shard_distribution(1000)
print(distribution)  # Shows how 1000 keys would be distributed
```

## Strategy Selection Guidelines

### Use HashShardingStrategy when:
- You need deterministic routing
- Keys are relatively random or sequential
- You want simple, fast routing
- You don't need range-based queries

### Consider Custom Strategies when:
- You need range-based sharding
- You have specific distribution requirements
- You need geographic or time-based sharding
- You want to optimize for specific query patterns

## Performance Characteristics

### HashShardingStrategy
- **Routing Complexity**: O(1)
- **Memory Usage**: O(1)
- **Range Query Performance**: O(n) where n is number of shards
- **Key Distribution**: Even for random keys, may be uneven for sequential keys

### General Strategy Interface
- **Routing Complexity**: Depends on implementation
- **Memory Usage**: Depends on implementation
- **Range Query Performance**: Depends on implementation
- **Key Distribution**: Depends on implementation

## Extending the Framework

To create a new sharding strategy:

1. Inherit from `ShardingStrategy`
2. Implement all abstract methods
3. Add any strategy-specific methods
4. Consider implementing utility methods for better integration

Example:
```python
class MyCustomStrategy(ShardingStrategy):
    def __init__(self, config):
        # Initialize your strategy
        pass
    
    def get_shard_id(self, key: int) -> int:
        # Implement your routing logic
        pass
    
    # Implement other abstract methods...
    
    def my_custom_method(self):
        # Add strategy-specific functionality
        pass
```

## Testing Strategies

When testing sharding strategies:

1. **Test routing consistency**: Same key should always route to same shard
2. **Test distribution**: Keys should be distributed reasonably evenly
3. **Test edge cases**: Handle boundary conditions and invalid inputs
4. **Test range queries**: Verify range query behavior
5. **Test performance**: Measure routing speed for your use case

## Future Enhancements

Potential future strategies:
- **Consistent Hashing**: For dynamic shard addition/removal
- **Geographic Sharding**: Based on location data
- **Time-based Sharding**: Based on timestamps
- **Composite Sharding**: Combining multiple strategies
- **Load-aware Sharding**: Based on shard load

## Module Dependencies

- `abc`: For abstract base class
- `typing`: For type hints
- No external dependencies

## Error Handling

The strategy module provides comprehensive error handling:

- **Invalid keys**: Strategies validate input keys
- **Invalid ranges**: Range queries are validated
- **Configuration errors**: Invalid strategy parameters are caught
- **Boundary conditions**: Edge cases are handled gracefully

All errors include descriptive messages to aid debugging and development. 