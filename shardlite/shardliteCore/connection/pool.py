"""
SQLite connection pool management.

This module provides the ConnectionPool class for managing SQLite database
connections with connection reuse and lifecycle management.
"""

import sqlite3
import threading
import time
from typing import Optional, List, Dict, Any
from contextlib import contextmanager
from queue import Queue, Empty


class ConnectionPool:
    """
    SQLite connection pool for managing database connections.
    
    This class provides connection pooling functionality to efficiently
    manage SQLite database connections with reuse and lifecycle management.
    
    Features:
    - Connection reuse to avoid overhead
    - Thread-safe operations
    - Connection timeout handling
    - Automatic connection cleanup
    - Connection health monitoring
    
    Attributes:
        db_path (str): Path to the SQLite database file
        max_connections (int): Maximum number of connections in the pool
        timeout (int): Connection timeout in seconds
    """
    
    def __init__(
        self, 
        db_path: str, 
        max_connections: int = 10,
        timeout: int = 30,
        check_same_thread: bool = False
    ) -> None:
        """
        Initialize connection pool.
        
        Args:
            db_path: Path to the SQLite database file
            max_connections: Maximum number of connections in the pool
            timeout: Connection timeout in seconds
            check_same_thread: Whether to check if connections are used in same thread
            
        Raises:
            ValueError: If parameters are invalid
        """
        if not db_path or not isinstance(db_path, str):
            raise ValueError("db_path must be a non-empty string")
        
        if not isinstance(max_connections, int) or max_connections <= 0:
            raise ValueError("max_connections must be a positive integer")
        
        if not isinstance(timeout, int) or timeout <= 0:
            raise ValueError("timeout must be a positive integer")
        
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.check_same_thread = check_same_thread
        
        # Thread-safe connection pool
        self._pool: Queue[sqlite3.Connection] = Queue(maxsize=max_connections)
        self._lock = threading.RLock()
        self._active_connections = 0
        self._total_connections_created = 0
        
        # Connection tracking
        self._connection_info: Dict[int, Dict[str, Any]] = {}
    
    def get_connection(self) -> sqlite3.Connection:
        """
        Get a connection from the pool.
        
        Returns:
            sqlite3.Connection: Available connection from pool
            
        Raises:
            TimeoutError: If no connection is available within timeout
            sqlite3.Error: If connection creation fails
        """
        with self._lock:
            # Try to get existing connection from pool
            try:
                conn = self._pool.get_nowait()
                self._active_connections += 1
                return conn
            except Empty:
                pass
            
            # Create new connection if pool is not full
            if self._active_connections < self.max_connections:
                conn = self._create_connection()
                self._active_connections += 1
                self._total_connections_created += 1
                return conn
            
            # Wait for available connection
            try:
                conn = self._pool.get(timeout=self.timeout)
                self._active_connections += 1
                return conn
            except Empty:
                raise TimeoutError(f"No connection available after {self.timeout} seconds")
    
    def return_connection(self, conn: sqlite3.Connection) -> None:
        """
        Return a connection to the pool.
        
        Args:
            conn: SQLite connection to return to pool
            
        Raises:
            ValueError: If connection is None or invalid
        """
        if conn is None:
            raise ValueError("Cannot return None connection")
        
        with self._lock:
            # Check if connection is still valid
            try:
                conn.execute("SELECT 1")
            except (sqlite3.Error, AttributeError):
                # Connection is invalid, don't return it to pool
                self._active_connections -= 1
                return
            
            # Return connection to pool
            try:
                self._pool.put_nowait(conn)
                self._active_connections -= 1
            except:
                # Pool is full, close the connection
                self._close_connection(conn)
                self._active_connections -= 1
    
    def close_connection(self, conn: sqlite3.Connection) -> None:
        """
        Close a connection and remove it from tracking.
        
        Args:
            conn: SQLite connection to close
        """
        if conn is None:
            return
        
        with self._lock:
            self._close_connection(conn)
            self._active_connections -= 1
    
    def close_all(self) -> None:
        """
        Close all connections in the pool.
        
        This method closes all active and pooled connections.
        """
        with self._lock:
            # Close all connections in pool
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    self._close_connection(conn)
                except Empty:
                    break
            
            self._active_connections = 0
    
    @contextmanager
    def get_connection_context(self):
        """
        Context manager for getting and returning connections.
        
        Usage:
            with pool.get_connection_context() as conn:
                conn.execute("SELECT * FROM table")
        
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        finally:
            if conn is not None:
                self.return_connection(conn)
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.
        
        Returns:
            Dict[str, Any]: Pool statistics including active connections,
                           pool size, total created, etc.
        """
        with self._lock:
            return {
                'active_connections': self._active_connections,
                'pool_size': self._pool.qsize(),
                'max_connections': self.max_connections,
                'total_connections_created': self._total_connections_created,
                'db_path': self.db_path,
                'timeout': self.timeout
            }
    
    def _create_connection(self) -> sqlite3.Connection:
        """
        Create a new SQLite connection.
        
        Returns:
            sqlite3.Connection: New database connection
            
        Raises:
            sqlite3.Error: If connection creation fails
        """
        conn = sqlite3.connect(
            self.db_path,
            timeout=self.timeout,
            check_same_thread=self.check_same_thread
        )
        
        # Configure connection
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        
        return conn
    
    def _close_connection(self, conn: sqlite3.Connection) -> None:
        """
        Close a SQLite connection.
        
        Args:
            conn: SQLite connection to close
        """
        try:
            if conn is not None:
                conn.close()
        except sqlite3.Error:
            # Ignore errors when closing
            pass
    
    def __repr__(self) -> str:
        """Return string representation of the connection pool."""
        stats = self.get_pool_stats()
        return (f"ConnectionPool(db_path='{self.db_path}', "
                f"active={stats['active_connections']}, "
                f"pool_size={stats['pool_size']}, "
                f"max={self.max_connections})")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close all connections."""
        self.close_all() 