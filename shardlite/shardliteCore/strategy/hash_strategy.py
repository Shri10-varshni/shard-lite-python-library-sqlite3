"""
Hash-based sharding strategy implementation.

This module provides the HashShardingStrategy class that implements
deterministic sharding using the modulo operation on keys.
"""

from typing import List, Set
from .base import ShardingStrategy


class HashShardingStrategy(ShardingStrategy):
    """
    Hash-based sharding strategy using modulo operation.
    
    This strategy distributes data across shards using the formula:
    shard_id = key % num_shards
    
    This provides:
    - Deterministic routing: Same key always goes to same shard
    - Simple implementation: Easy to understand and debug
    - Good distribution: Keys are evenly distributed when sequential
    
    Attributes:
        num_shards (int): Number of shards to distribute data across
    """
    
    def __init__(self, num_shards: int) -> None:
        """
        Initialize hash-based sharding strategy.
        
        Args:
            num_shards: Number of shards to distribute data across
            
        Raises:
            ValueError: If num_shards is not positive
        """
        if not isinstance(num_shards, int) or num_shards <= 0:
            raise ValueError(f"num_shards must be a positive integer, got {num_shards}")
        
        self._num_shards = num_shards
    
    def get_shard_id(self, key: int) -> int:
        """
        Determine shard ID for given key using modulo operation.
        
        Args:
            key: Integer key for routing
            
        Returns:
            int: Shard ID (0 to num_shards-1)
            
        Raises:
            ValueError: If key is not an integer
        """
        if not self.validate_key(key):
            raise ValueError(f"Key must be an integer, got {type(key)}")
        
        return abs(key) % self._num_shards
    
    def get_shard_range(self, start_key: int, end_key: int) -> List[int]:
        """
        Get list of shards that may contain keys in the given range.
        
        For hash-based sharding, this returns all shards since keys
        in a range may be distributed across all shards.
        
        Args:
            start_key: Starting key of the range (inclusive)
            end_key: Ending key of the range (inclusive)
            
        Returns:
            List[int]: List of all shard IDs that may contain keys in range
            
        Raises:
            ValueError: If start_key > end_key or keys are invalid
        """
        if not self.validate_key_range(start_key, end_key):
            raise ValueError(f"Invalid key range: start_key={start_key}, end_key={end_key}")
        
        # For hash-based sharding, keys in a range may be in any shard
        # So we return all shards
        return self.get_all_shard_ids()
    
    def get_all_shard_ids(self) -> List[int]:
        """
        Get list of all shard IDs.
        
        Returns:
            List[int]: List of all shard IDs (0 to num_shards-1)
        """
        return list(range(self._num_shards))
    
    def get_num_shards(self) -> int:
        """
        Get the total number of shards.
        
        Returns:
            int: Total number of shards
        """
        return self._num_shards
    
    def get_optimal_shard_range(self, start_key: int, end_key: int) -> List[int]:
        """
        Get a more optimized list of shards for a key range.
        
        This method attempts to reduce the number of shards to query
        by analyzing the key distribution pattern.
        
        Args:
            start_key: Starting key of the range (inclusive)
            end_key: Ending key of the range (inclusive)
            
        Returns:
            List[int]: Optimized list of shard IDs to query
        """
        if not self.validate_key_range(start_key, end_key):
            raise ValueError(f"Invalid key range: start_key={start_key}, end_key={end_key}")
        
        # For small ranges, we can be more specific
        if end_key - start_key < self._num_shards:
            shards: Set[int] = set()
            for key in range(start_key, end_key + 1):
                shards.add(self.get_shard_id(key))
            return sorted(list(shards))
        
        # For large ranges, return all shards
        return self.get_all_shard_ids()
    
    def get_shard_for_key_range(self, key_range_size: int) -> List[int]:
        """
        Get shards that would be needed for a key range of given size.
        
        This is useful for estimating query performance and resource usage.
        
        Args:
            key_range_size: Size of the key range
            
        Returns:
            List[int]: List of shard IDs that would be involved
        """
        if key_range_size <= 0:
            return []
        
        if key_range_size < self._num_shards:
            # For small ranges, we can estimate which shards are needed
            shards: Set[int] = set()
            for i in range(min(key_range_size, self._num_shards)):
                shards.add(i % self._num_shards)
            return sorted(list(shards))
        
        # For large ranges, all shards are likely needed
        return self.get_all_shard_ids()
    
    def __repr__(self) -> str:
        """Return string representation of the hash sharding strategy."""
        return f"HashShardingStrategy(num_shards={self._num_shards})"
    
    def __eq__(self, other: object) -> bool:
        """Check if this strategy equals another."""
        if not isinstance(other, HashShardingStrategy):
            return False
        return self._num_shards == other._num_shards
    
    def __hash__(self) -> int:
        """Return hash value for this strategy."""
        return hash(self._num_shards) 