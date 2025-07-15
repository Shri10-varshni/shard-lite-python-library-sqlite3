"""
Abstract base classes for sharding strategies.

This module defines the ShardingStrategy abstract base class that all
sharding strategy implementations must inherit from.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple, Optional


class ShardingStrategy(ABC):
    """
    Abstract base class for sharding strategies.
    
    This class defines the interface that all sharding strategies must implement.
    A sharding strategy determines how data is distributed across multiple shards
    based on a key value.
    
    Subclasses must implement:
    - get_shard_id: Determine which shard a key belongs to
    - get_shard_range: Get the range of shards for a key range
    """
    
    @abstractmethod
    def get_shard_id(self, key: int) -> int:
        """
        Determine which shard a given key belongs to.
        
        Args:
            key: The key to route to a shard
            
        Returns:
            int: The shard ID (0 to num_shards-1)
            
        Raises:
            ValueError: If key is invalid or out of range
        """
        pass
    
    @abstractmethod
    def get_shard_range(self, start_key: int, end_key: int) -> List[int]:
        """
        Get the list of shards that may contain keys in the given range.
        
        Args:
            start_key: The starting key of the range (inclusive)
            end_key: The ending key of the range (inclusive)
            
        Returns:
            List[int]: List of shard IDs that may contain keys in the range
            
        Raises:
            ValueError: If start_key > end_key or keys are invalid
        """
        pass
    
    @abstractmethod
    def get_all_shard_ids(self) -> List[int]:
        """
        Get a list of all shard IDs managed by this strategy.
        
        Returns:
            List[int]: List of all shard IDs (0 to num_shards-1)
        """
        pass
    
    @abstractmethod
    def get_num_shards(self) -> int:
        """
        Get the total number of shards managed by this strategy.
        
        Returns:
            int: Total number of shards
        """
        pass
    
    def validate_key(self, key: int) -> bool:
        """
        Validate if a key is acceptable for this sharding strategy.
        
        Args:
            key: The key to validate
            
        Returns:
            bool: True if key is valid, False otherwise
        """
        return isinstance(key, int)
    
    def validate_key_range(self, start_key: int, end_key: int) -> bool:
        """
        Validate if a key range is acceptable for this sharding strategy.
        
        Args:
            start_key: The starting key of the range
            end_key: The ending key of the range
            
        Returns:
            bool: True if key range is valid, False otherwise
        """
        return (self.validate_key(start_key) and 
                self.validate_key(end_key) and 
                start_key <= end_key)
    
    def get_shard_distribution(self, num_keys: int = 1000) -> List[int]:
        """
        Get the distribution of keys across shards for analysis.
        
        This method generates a sample of keys and shows how they
        would be distributed across shards.
        
        Args:
            num_keys: Number of sample keys to generate
            
        Returns:
            List[int]: Count of keys assigned to each shard
        """
        distribution = [0] * self.get_num_shards()
        
        for i in range(num_keys):
            shard_id = self.get_shard_id(i)
            distribution[shard_id] += 1
        
        return distribution
    
    def __repr__(self) -> str:
        """Return string representation of the sharding strategy."""
        return f"{self.__class__.__name__}(num_shards={self.get_num_shards()})" 