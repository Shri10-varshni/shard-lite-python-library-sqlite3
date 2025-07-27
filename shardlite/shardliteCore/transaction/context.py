"""
Transaction classes for Shardlite transactional operations.

Defines the Transaction base class (state/lifecycle) and OperationStager child class (operation staging, CRUD).
"""
from typing import Set, List, Dict, Any, Callable, Optional
from .transaction_states import TransactionState

class Transaction:
    """
    Base Transaction class: manages transaction state, involved shards, and held connections.
    Does not implement operation staging or CRUD.
    """
    def __init__(self, coordinator, logger=None):
        self.coordinator = coordinator
        self.logger = logger
        self.involved_shards: Set[int] = set()
        self.connections: Dict[int, Any] = {}  # int: shard_id -> Connection
        self.state: TransactionState = TransactionState.INITIAL

    def commit(self):
        """Commit the transaction using the coordinator (2PC)."""
        pass

    def rollback(self):
        """Rollback the transaction using the coordinator (2PC)."""
        pass

    def __enter__(self):
        """Enter the transaction context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the transaction context, committing or rolling back as needed."""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()

class OperationStager(Transaction):
    """
    OperationStager extends Transaction to support operation staging and CRUD methods.
    All transactional CRUD operations should be performed via this class.
    Includes a 'prepared' flag to indicate if all operations were staged successfully for 2PC.
    """
    def __init__(self, coordinator, logger=None):
        super().__init__(coordinator, logger)
        self.staged_operations: List[Dict[str, Any]] = []  # Each op: {'type': str, 'args': tuple, 'kwargs': dict}
        self.current_state: Dict[str, Any] = {}
        self.prepared: bool = True  # True if all operations staged successfully

    def mark_unprepared(self, reason: str = ""):
        """Mark the transaction as unprepared (staging failed). Optionally log the reason."""
        self.prepared = False
        if self.logger:
            self.logger.on_error(getattr(self, 'transaction_id', None), reason)

    def insert(self, table: str, row: Dict[str, Any], key: int):
        """Stage an insert operation for the given table and key."""
        # Shard discovery and operation staging logic will go here
        pass

    def update(self, table: str, set_values: Dict[str, Any], where: Dict[str, Any], key: Optional[int] = None):
        """Stage an update operation for the given table and key (or all shards if key is None)."""
        pass

    def delete(self, table: str, where: Dict[str, Any], key: Optional[int] = None):
        """Stage a delete operation for the given table and key (or all shards if key is None)."""
        pass

    def select(self, table: str, where: Dict[str, Any], key: Optional[int] = None):
        """Stage a select operation for the given table and key (or all shards if key is None)."""
        pass 