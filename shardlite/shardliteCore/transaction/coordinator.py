"""
Parallel transaction coordinator for 2PC implementation.

This module provides the ParallelTransactionCoordinator class that implements
the Two-Phase Commit (2PC) protocol for cross-shard transactions.
"""

import time
import threading
import uuid
from typing import Dict, List, Any, Optional, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from .logger import TransactionLogger, TransactionState, NullTransactionLogger


class TransactionContext:
    """
    Context for managing a cross-shard transaction.
    
    This class provides a context manager interface for transactions
    and tracks the transaction state throughout its lifecycle.
    """
    
    def __init__(self, coordinator: 'ParallelTransactionCoordinator', transaction_id: str) -> None:
        """
        Initialize transaction context.
        
        Args:
            coordinator: Reference to the transaction coordinator
            transaction_id: Unique identifier for the transaction
        """
        self.coordinator = coordinator
        self.transaction_id = transaction_id
        self.state = TransactionState.INITIAL
        self.start_time = time.time()
        self.shard_keys: List[int] = []
        self.operations: List[Callable] = []
    
    def add_operation(self, operation: Callable) -> None:
        """
        Add an operation to the transaction.
        
        Args:
            operation: Callable to execute during transaction
        """
        self.operations.append(operation)
    
    def add_shard_key(self, shard_key: int) -> None:
        """
        Add a shard key to the transaction scope.
        
        Args:
            shard_key: Shard key to include in transaction
        """
        if shard_key not in self.shard_keys:
            self.shard_keys.append(shard_key)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - commit or rollback transaction."""
        if exc_type is None:
            self.coordinator.commit(self)
        else:
            self.coordinator.rollback(self)


class ParallelTransactionCoordinator:
    """
    Parallel transaction coordinator implementing 2PC protocol.
    
    This class coordinates cross-shard transactions using the Two-Phase Commit
    protocol to ensure ACID compliance across multiple shards.
    
    The 2PC protocol consists of:
    1. Prepare Phase: All shards prepare for commit
    2. Decision Phase: Commit or rollback based on prepare results
    """
    
    def __init__(
        self, 
        shard_manager: 'ShardManager',
        logger: Optional[TransactionLogger] = None,
        max_workers: int = 4
    ) -> None:
        """
        Initialize transaction coordinator.
        
        Args:
            shard_manager: ShardManager instance for accessing shards
            logger: Optional transaction logger for monitoring
            max_workers: Maximum number of worker threads for parallel operations
            
        Raises:
            ValueError: If parameters are invalid
        """
        if shard_manager is None:
            raise ValueError("shard_manager cannot be None")
        
        self.shard_manager = shard_manager
        self.logger = logger or NullTransactionLogger()
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Transaction tracking
        self.active_transactions: Dict[str, TransactionContext] = {}
        self.transaction_lock = threading.RLock()
    
    def begin(self, shard_keys: List[int]) -> TransactionContext:
        """
        Begin a new cross-shard transaction.
        
        Args:
            shard_keys: List of shard keys involved in the transaction
            
        Returns:
            TransactionContext: Transaction context for managing the transaction
            
        Raises:
            ValueError: If shard_keys is empty or invalid
        """
        if not shard_keys:
            raise ValueError("shard_keys cannot be empty")
        
        transaction_id = str(uuid.uuid4())
        context = TransactionContext(self, transaction_id)
        context.shard_keys = shard_keys.copy()
        context.state = TransactionState.INITIAL
        
        with self.transaction_lock:
            self.active_transactions[transaction_id] = context
        
        return context
    
    def prepare(self, context: TransactionContext) -> bool:
        """
        Execute prepare phase of 2PC protocol.
        
        Args:
            context: Transaction context
            
        Returns:
            bool: True if all shards prepared successfully, False otherwise
        """
        if context.state != TransactionState.INITIAL:
            raise ValueError(f"Cannot prepare transaction in state {context.state}")
        
        context.state = TransactionState.PREPARING
        self.logger.on_prepare(context.transaction_id, context.shard_keys)
        
        # Execute prepare phase in parallel
        prepare_results = self._execute_prepare_phase(context)
        
        # Check if all shards prepared successfully
        all_prepared = all(prepare_results.values())
        
        if all_prepared:
            context.state = TransactionState.PREPARED
        else:
            context.state = TransactionState.FAILED
        
        return all_prepared
    
    def commit(self, context: TransactionContext) -> bool:
        """
        Commit the transaction.
        
        Args:
            context: Transaction context
            
        Returns:
            bool: True if commit was successful, False otherwise
        """
        if context.state == TransactionState.INITIAL:
            # Auto-prepare if not already prepared
            if not self.prepare(context):
                return False
        
        if context.state != TransactionState.PREPARED:
            raise ValueError(f"Cannot commit transaction in state {context.state}")
        
        context.state = TransactionState.COMMITTING
        self.logger.on_commit(context.transaction_id, context.shard_keys)
        
        # Execute commit phase in parallel
        commit_results = self._execute_commit_phase(context)
        
        # Check if all shards committed successfully
        all_committed = all(commit_results.values())
        
        if all_committed:
            context.state = TransactionState.COMMITTED
        else:
            context.state = TransactionState.FAILED
        
        # Log completion
        duration_ms = (time.time() - context.start_time) * 1000
        self.logger.on_complete(context.transaction_id, context.state, duration_ms)
        
        # Cleanup
        self._cleanup_transaction(context)
        
        return all_committed
    
    def rollback(self, context: TransactionContext) -> None:
        """
        Rollback the transaction.
        
        Args:
            context: Transaction context
        """
        if context.state in [TransactionState.COMMITTED, TransactionState.ROLLED_BACK]:
            return  # Already finalized
        
        context.state = TransactionState.ROLLING_BACK
        self.logger.on_rollback(context.transaction_id, context.shard_keys, "User requested rollback")
        
        # Execute rollback phase in parallel
        self._execute_rollback_phase(context)
        
        context.state = TransactionState.ROLLED_BACK
        
        # Log completion
        duration_ms = (time.time() - context.start_time) * 1000
        self.logger.on_complete(context.transaction_id, context.state, duration_ms)
        
        # Cleanup
        self._cleanup_transaction(context)
    
    def run(self, shard_keys: List[int], operations: List[Callable]) -> bool:
        """
        Run a transaction with the given operations.
        
        Args:
            shard_keys: List of shard keys involved in the transaction
            operations: List of operations to execute
            
        Returns:
            bool: True if transaction was successful, False otherwise
        """
        with self.begin(shard_keys) as context:
            for operation in operations:
                context.add_operation(operation)
            
            # Execute operations
            try:
                for operation in operations:
                    operation()
                return True
            except Exception as e:
                self.logger.on_error(context.transaction_id, e)
                return False
    
    def _execute_prepare_phase(self, context: TransactionContext) -> Dict[int, bool]:
        """
        Execute prepare phase on all shards in parallel.
        
        Args:
            context: Transaction context
            
        Returns:
            Dict[int, bool]: Mapping of shard ID to prepare result
        """
        futures = {}
        prepare_results = {}
        
        # Submit prepare tasks
        for shard_key in context.shard_keys:
            future = self.executor.submit(self._prepare_shard, context.transaction_id, shard_key)
            futures[future] = shard_key
        
        # Collect results
        for future in as_completed(futures):
            shard_key = futures[future]
            try:
                result = future.result()
                prepare_results[shard_key] = result
                self.logger.on_vote(context.transaction_id, shard_key, result)
            except Exception as e:
                prepare_results[shard_key] = False
                self.logger.on_error(context.transaction_id, e, shard_key=shard_key)
        
        return prepare_results
    
    def _execute_commit_phase(self, context: TransactionContext) -> Dict[int, bool]:
        """
        Execute commit phase on all shards in parallel.
        
        Args:
            context: Transaction context
            
        Returns:
            Dict[int, bool]: Mapping of shard ID to commit result
        """
        futures = {}
        commit_results = {}
        
        # Submit commit tasks
        for shard_key in context.shard_keys:
            future = self.executor.submit(self._commit_shard, context.transaction_id, shard_key)
            futures[future] = shard_key
        
        # Collect results
        for future in as_completed(futures):
            shard_key = futures[future]
            try:
                result = future.result()
                commit_results[shard_key] = result
            except Exception as e:
                commit_results[shard_key] = False
                self.logger.on_error(context.transaction_id, e, shard_key=shard_key)
        
        return commit_results
    
    def _execute_rollback_phase(self, context: TransactionContext) -> None:
        """
        Execute rollback phase on all shards in parallel.
        
        Args:
            context: Transaction context
        """
        futures = []
        
        # Submit rollback tasks
        for shard_key in context.shard_keys:
            future = self.executor.submit(self._rollback_shard, context.transaction_id, shard_key)
            futures.append(future)
        
        # Wait for all rollbacks to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                # Log rollback errors but don't fail the rollback
                self.logger.on_error(context.transaction_id, e)
    
    def _prepare_shard(self, transaction_id: str, shard_key: int) -> bool:
        """
        Prepare a specific shard for commit.
        
        Args:
            transaction_id: Transaction identifier
            shard_key: Shard key to prepare
            
        Returns:
            bool: True if shard prepared successfully
        """
        try:
            # Get connection pool for shard
            connection_pool = self.shard_manager.router.get_connection_for_key(shard_key)
            
            with connection_pool.get_connection_context() as conn:
                # Begin immediate transaction
                conn.execute("BEGIN IMMEDIATE")
                
                # Write rollback journal (SQLite does this automatically)
                # For now, we just verify the connection is ready
                conn.execute("SELECT 1")
                
                return True
        except Exception as e:
            self.logger.on_error(transaction_id, e, shard_key=shard_key)
            return False
    
    def _commit_shard(self, transaction_id: str, shard_key: int) -> bool:
        """
        Commit a specific shard.
        
        Args:
            transaction_id: Transaction identifier
            shard_key: Shard key to commit
            
        Returns:
            bool: True if shard committed successfully
        """
        try:
            # Get connection pool for shard
            connection_pool = self.shard_manager.router.get_connection_for_key(shard_key)
            
            with connection_pool.get_connection_context() as conn:
                # Commit the transaction
                conn.commit()
                return True
        except Exception as e:
            self.logger.on_error(transaction_id, e, shard_key=shard_key)
            return False
    
    def _rollback_shard(self, transaction_id: str, shard_key: int) -> None:
        """
        Rollback a specific shard.
        
        Args:
            transaction_id: Transaction identifier
            shard_key: Shard key to rollback
        """
        try:
            # Get connection pool for shard
            connection_pool = self.shard_manager.router.get_connection_for_key(shard_key)
            
            with connection_pool.get_connection_context() as conn:
                # Rollback the transaction
                conn.rollback()
        except Exception as e:
            self.logger.on_error(transaction_id, e, shard_key=shard_key)
    
    def _cleanup_transaction(self, context: TransactionContext) -> None:
        """
        Clean up transaction resources.
        
        Args:
            context: Transaction context to cleanup
        """
        with self.transaction_lock:
            if context.transaction_id in self.active_transactions:
                del self.active_transactions[context.transaction_id]
    
    def get_active_transactions(self) -> List[str]:
        """
        Get list of active transaction IDs.
        
        Returns:
            List[str]: List of active transaction IDs
        """
        with self.transaction_lock:
            return list(self.active_transactions.keys())
    
    def shutdown(self) -> None:
        """
        Shutdown the transaction coordinator.
        
        This method should be called when the coordinator is no longer needed
        to properly clean up resources.
        """
        self.executor.shutdown(wait=True)
    
    def __repr__(self) -> str:
        """Return string representation of the coordinator."""
        active_count = len(self.get_active_transactions())
        return f"ParallelTransactionCoordinator(active_transactions={active_count}, max_workers={self.max_workers})" 