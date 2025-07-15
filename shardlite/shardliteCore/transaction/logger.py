"""
Transaction logging interface for Shardlite.

This module defines the TransactionLogger protocol that provides an interface
for logging transaction lifecycle events and monitoring transaction status.
"""

from typing import Protocol, Dict, Any, Optional, List
from enum import Enum
from datetime import datetime


class TransactionState(Enum):
    """Enumeration of transaction states."""
    INITIAL = "initial"
    PREPARING = "preparing"
    PREPARED = "prepared"
    COMMITTING = "committing"
    COMMITTED = "committed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


class TransactionEvent(Enum):
    """Enumeration of transaction events."""
    BEGIN = "begin"
    PREPARE = "prepare"
    VOTE = "vote"
    COMMIT = "commit"
    ROLLBACK = "rollback"
    COMPLETE = "complete"
    ERROR = "error"


class TransactionLogger(Protocol):
    """
    Protocol for transaction logging interface.
    
    This protocol defines the interface that transaction loggers must implement
    to provide monitoring and debugging capabilities for cross-shard transactions.
    
    Implementations can provide:
    - Console logging for development
    - File logging for debugging
    - Database logging for audit trails
    - Network logging for distributed monitoring
    """
    
    def on_prepare(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """
        Log transaction prepare phase start.
        
        Args:
            transaction_id: Unique identifier for the transaction
            shard_keys: List of shard keys involved in the transaction
            **kwargs: Additional context information
        """
        ...
    
    def on_vote(self, transaction_id: str, shard_id: int, vote: bool, **kwargs: Any) -> None:
        """
        Log shard vote during prepare phase.
        
        Args:
            transaction_id: Unique identifier for the transaction
            shard_id: ID of the shard that voted
            vote: True for YES vote, False for NO vote
            **kwargs: Additional context information
        """
        ...
    
    def on_commit(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """
        Log transaction commit phase start.
        
        Args:
            transaction_id: Unique identifier for the transaction
            shard_keys: List of shard keys involved in the transaction
            **kwargs: Additional context information
        """
        ...
    
    def on_rollback(self, transaction_id: str, shard_keys: List[int], reason: str, **kwargs: Any) -> None:
        """
        Log transaction rollback.
        
        Args:
            transaction_id: Unique identifier for the transaction
            shard_keys: List of shard keys involved in the transaction
            reason: Reason for rollback
            **kwargs: Additional context information
        """
        ...
    
    def on_complete(self, transaction_id: str, state: TransactionState, duration_ms: float, **kwargs: Any) -> None:
        """
        Log transaction completion.
        
        Args:
            transaction_id: Unique identifier for the transaction
            state: Final state of the transaction
            duration_ms: Transaction duration in milliseconds
            **kwargs: Additional context information
        """
        ...
    
    def on_error(self, transaction_id: str, error: Exception, **kwargs: Any) -> None:
        """
        Log transaction error.
        
        Args:
            transaction_id: Unique identifier for the transaction
            error: Exception that occurred
            **kwargs: Additional context information
        """
        ...


class ConsoleTransactionLogger:
    """
    Console-based transaction logger implementation.
    
    This logger outputs transaction events to the console for development
    and debugging purposes.
    """
    
    def __init__(self, verbose: bool = True) -> None:
        """
        Initialize console transaction logger.
        
        Args:
            verbose: Whether to output detailed information
        """
        self.verbose = verbose
    
    def on_prepare(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """Log transaction prepare phase start."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] TX {transaction_id}: PREPARE phase started for shards {shard_keys}")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")
    
    def on_vote(self, transaction_id: str, shard_id: int, vote: bool, **kwargs: Any) -> None:
        """Log shard vote during prepare phase."""
        timestamp = datetime.now().isoformat()
        vote_str = "YES" if vote else "NO"
        print(f"[{timestamp}] TX {transaction_id}: Shard {shard_id} voted {vote_str}")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")
    
    def on_commit(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """Log transaction commit phase start."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] TX {transaction_id}: COMMIT phase started for shards {shard_keys}")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")
    
    def on_rollback(self, transaction_id: str, shard_keys: List[int], reason: str, **kwargs: Any) -> None:
        """Log transaction rollback."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] TX {transaction_id}: ROLLBACK for shards {shard_keys} - {reason}")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")
    
    def on_complete(self, transaction_id: str, state: TransactionState, duration_ms: float, **kwargs: Any) -> None:
        """Log transaction completion."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] TX {transaction_id}: COMPLETED with state {state.value} in {duration_ms:.2f}ms")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")
    
    def on_error(self, transaction_id: str, error: Exception, **kwargs: Any) -> None:
        """Log transaction error."""
        timestamp = datetime.now().isoformat()
        print(f"[{timestamp}] TX {transaction_id}: ERROR - {type(error).__name__}: {error}")
        if self.verbose and kwargs:
            print(f"  Context: {kwargs}")


class NullTransactionLogger:
    """
    Null transaction logger that does nothing.
    
    This logger can be used when no logging is desired, providing
    a no-op implementation of the TransactionLogger protocol.
    """
    
    def on_prepare(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """No-op implementation."""
        pass
    
    def on_vote(self, transaction_id: str, shard_id: int, vote: bool, **kwargs: Any) -> None:
        """No-op implementation."""
        pass
    
    def on_commit(self, transaction_id: str, shard_keys: List[int], **kwargs: Any) -> None:
        """No-op implementation."""
        pass
    
    def on_rollback(self, transaction_id: str, shard_keys: List[int], reason: str, **kwargs: Any) -> None:
        """No-op implementation."""
        pass
    
    def on_complete(self, transaction_id: str, state: TransactionState, duration_ms: float, **kwargs: Any) -> None:
        """No-op implementation."""
        pass
    
    def on_error(self, transaction_id: str, error: Exception, **kwargs: Any) -> None:
        """No-op implementation."""
        pass


class TransactionMetrics:
    """
    Transaction metrics collector.
    
    This class collects metrics about transaction performance and outcomes
    for monitoring and analysis purposes.
    """
    
    def __init__(self) -> None:
        """Initialize transaction metrics collector."""
        self.total_transactions = 0
        self.successful_transactions = 0
        self.failed_transactions = 0
        self.total_duration_ms = 0.0
        self.avg_duration_ms = 0.0
        self.min_duration_ms = float('inf')
        self.max_duration_ms = 0.0
        
        # State tracking
        self.state_counts: Dict[TransactionState, int] = {
            state: 0 for state in TransactionState
        }
        
        # Error tracking
        self.error_counts: Dict[str, int] = {}
    
    def record_transaction(self, state: TransactionState, duration_ms: float, error: Optional[Exception] = None) -> None:
        """
        Record a completed transaction.
        
        Args:
            state: Final state of the transaction
            duration_ms: Transaction duration in milliseconds
            error: Exception if transaction failed
        """
        self.total_transactions += 1
        self.state_counts[state] += 1
        
        # Update duration statistics
        self.total_duration_ms += duration_ms
        self.avg_duration_ms = self.total_duration_ms / self.total_transactions
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)
        
        # Update success/failure counts
        if state == TransactionState.COMMITTED:
            self.successful_transactions += 1
        elif state in [TransactionState.FAILED, TransactionState.ROLLED_BACK]:
            self.failed_transactions += 1
        
        # Track errors
        if error:
            error_type = type(error).__name__
            self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of transaction metrics.
        
        Returns:
            Dict[str, Any]: Summary of collected metrics
        """
        return {
            'total_transactions': self.total_transactions,
            'successful_transactions': self.successful_transactions,
            'failed_transactions': self.failed_transactions,
            'success_rate': (self.successful_transactions / self.total_transactions * 100) if self.total_transactions > 0 else 0,
            'avg_duration_ms': self.avg_duration_ms,
            'min_duration_ms': self.min_duration_ms if self.min_duration_ms != float('inf') else 0,
            'max_duration_ms': self.max_duration_ms,
            'state_distribution': {state.value: count for state, count in self.state_counts.items()},
            'error_distribution': self.error_counts
        }
    
    def reset(self) -> None:
        """Reset all metrics to initial state."""
        self.total_transactions = 0
        self.successful_transactions = 0
        self.failed_transactions = 0
        self.total_duration_ms = 0.0
        self.avg_duration_ms = 0.0
        self.min_duration_ms = float('inf')
        self.max_duration_ms = 0.0
        self.state_counts = {state: 0 for state in TransactionState}
        self.error_counts = {} 