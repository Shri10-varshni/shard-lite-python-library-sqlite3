"""
Transaction state and event enums for Shardlite transaction system.

This module centralizes transaction state, event, and related enums.
"""

from enum import Enum, auto

class TransactionState(Enum):
    INITIAL = auto()
    PREPARING = auto()
    PREPARED = auto()
    COMMITTING = auto()
    COMMITTED = auto()
    ROLLING_BACK = auto()
    ROLLED_BACK = auto()
    FAILED = auto()

class TransactionEvent(Enum):
    BEGIN = auto()
    PREPARE = auto()
    COMMIT = auto()
    ROLLBACK = auto()
    COMPLETE = auto()
    ERROR = auto() 