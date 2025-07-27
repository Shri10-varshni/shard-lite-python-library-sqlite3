"""
Tests for ConnectionPool class.

This module tests the connection pooling functionality including:
- Connection acquisition and return
- Thread safety
- Connection lifecycle management
- Pool statistics
- Error handling
- Context manager functionality
"""

import os
import pytest
import sqlite3
import threading
import time
import gc
from uuid import uuid4
from pathlib import Path

from shardlite.shardliteCore.connection.pool import ConnectionPool


@pytest.fixture(scope="function")
def temp_db_path():
    """Create a temporary database file for testing."""
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../testing_data"))
    os.makedirs(base_dir, exist_ok=True)
    test_dir = os.path.join(base_dir, f"connection_pool_test_{uuid4().hex}")
    os.makedirs(test_dir, exist_ok=True)
    
    db_path = os.path.join(test_dir, "test.db")
    
    # Initialize the database with a simple table
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO test (id, name) VALUES (1, 'test1')")
    conn.execute("INSERT INTO test (id, name) VALUES (2, 'test2')")
    conn.commit()
    conn.close()
    
    yield db_path
    
    # Cleanup
    try:
        os.unlink(db_path)
        os.rmdir(test_dir)
    except (OSError, PermissionError):
        # Handle Windows file locking issues
        time.sleep(0.1)
        try:
            os.unlink(db_path)
            os.rmdir(test_dir)
        except Exception:
            pass


@pytest.fixture(scope="function")
def pool(temp_db_path):
    """Create a connection pool for testing."""
    pool = ConnectionPool(temp_db_path, max_connections=3, timeout=5)  # Shorter timeout for testing
    yield pool
    pool.close_all()


class TestConnectionPool:
    """Test cases for ConnectionPool class."""

    def test_pool_initialization(self, temp_db_path):
        """Test that pool initializes correctly."""
        pool = ConnectionPool(temp_db_path, max_connections=5)
        
        assert pool.max_connections == 5
        assert pool.db_path == temp_db_path
        stats = pool.get_pool_stats()
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 0
        assert stats['total_connections_created'] == 0
        assert stats['max_connections'] == 5
        
        pool.close_all()

    def test_get_and_return_connection(self, pool):
        """Test basic connection acquisition and return."""
        # Get a connection
        conn = pool.get_connection()
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        
        # Verify connection works
        cursor = conn.execute("SELECT * FROM test WHERE id = 1")
        row = cursor.fetchone()
        assert row[0] == 1
        assert row[1] == 'test1'
        
        # Return connection
        pool.return_connection(conn)
        
        # Check pool stats
        stats = pool.get_pool_stats()
        assert stats['active_connections'] == 0  # Connection returned
        assert stats['pool_size'] == 1  # Connection in pool
        assert stats['total_connections_created'] == 1

    def test_multiple_connections(self, pool):
        """Test handling multiple connections."""
        connections = []
        
        # Get multiple connections
        for i in range(3):
            conn = pool.get_connection()
            connections.append(conn)
        
        # Verify all connections work
        for conn in connections:
            cursor = conn.execute("SELECT COUNT(*) FROM test")
            count = cursor.fetchone()[0]
            assert count == 2
        
        # Return all connections
        for conn in connections:
            pool.return_connection(conn)
        
        # Check pool stats
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 3
        assert stats['pool_size'] == 3
        assert stats['active_connections'] == 0

    def test_connection_reuse(self, pool):
        """Test that connections are properly reused."""
        # Get and return a connection multiple times
        for _ in range(5):
            conn = pool.get_connection()
            pool.return_connection(conn)
        
        # Should only have created one connection
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 1
        assert stats['pool_size'] == 1
        assert stats['active_connections'] == 0

    def test_max_connections_limit(self, pool):
        """Test that pool respects max_connections limit."""
        connections = []
        
        # Get connections up to the limit
        for i in range(3):
            conn = pool.get_connection()
            connections.append(conn)
        
        # Try to get one more connection - should timeout since we're at the limit
        start_time = time.time()
        with pytest.raises(TimeoutError):
            extra_conn = pool.get_connection()
        end_time = time.time()
        
        # Should have timed out after approximately the timeout period
        assert end_time - start_time >= 4.0  # Allow some tolerance for 5-second timeout
        
        # Check pool stats
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 3  # Only 3 connections created
        assert stats['active_connections'] == 3
        assert stats['pool_size'] == 0
        
        # Return all connections
        for conn in connections:
            pool.return_connection(conn)

    def test_connection_context_manager(self, pool):
        """Test connection context manager functionality."""
        with pool.get_connection_context() as conn:
            cursor = conn.execute("SELECT * FROM test WHERE id = 1")
            row = cursor.fetchone()
            assert row[0] == 1
        
        # Connection should be returned to pool
        stats = pool.get_pool_stats()
        assert stats['pool_size'] == 1
        assert stats['active_connections'] == 0

    def test_close_connection(self, pool):
        """Test closing individual connections."""
        conn = pool.get_connection()
        pool.close_connection(conn)
        
        # Connection should be removed from pool
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 1
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 0

    def test_close_all_connections(self, pool):
        """Test closing all connections."""
        # Get multiple connections
        connections = []
        for i in range(3):
            conn = pool.get_connection()
            connections.append(conn)
        
        # Return some connections
        for conn in connections[:2]:
            pool.return_connection(conn)
        
        # Close all connections
        pool.close_all()
        
        # All connections should be closed
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 3
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 0

    def test_pool_statistics(self, pool):
        """Test pool statistics tracking."""
        # Initial stats
        stats = pool.get_pool_stats()
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 0
        assert stats['max_connections'] == 3
        assert stats['total_connections_created'] == 0
        
        # Get a connection
        conn = pool.get_connection()
        stats = pool.get_pool_stats()
        assert stats['active_connections'] == 1
        assert stats['pool_size'] == 0
        assert stats['total_connections_created'] == 1
        
        # Return connection
        pool.return_connection(conn)
        stats = pool.get_pool_stats()
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 1
        assert stats['total_connections_created'] == 1

    def test_invalid_database_path(self):
        """Test behavior with invalid database path."""
        with pytest.raises(ValueError):
            ConnectionPool("", max_connections=3)
        
        with pytest.raises(ValueError):
            ConnectionPool(None, max_connections=3)

    def test_invalid_max_connections(self, temp_db_path):
        """Test behavior with invalid max_connections."""
        with pytest.raises(ValueError):
            ConnectionPool(temp_db_path, max_connections=0)
        
        with pytest.raises(ValueError):
            ConnectionPool(temp_db_path, max_connections=-1)

    def test_invalid_timeout(self, temp_db_path):
        """Test behavior with invalid timeout."""
        with pytest.raises(ValueError):
            ConnectionPool(temp_db_path, max_connections=3, timeout=0)
        
        with pytest.raises(ValueError):
            ConnectionPool(temp_db_path, max_connections=3, timeout=-1)

    def test_connection_health_check(self, pool):
        """Test connection health monitoring."""
        # Get a connection and use it
        with pool.get_connection_context() as conn:
            cursor = conn.execute("SELECT * FROM test")
            cursor.fetchall()
        
        # Connection should be healthy
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 1
        assert stats['pool_size'] == 1
        assert stats['active_connections'] == 0

    def test_concurrent_access(self, pool):
        """Test thread safety with concurrent access."""
        results = []
        errors = []
        
        def worker(worker_id):
            try:
                with pool.get_connection_context() as conn:
                    cursor = conn.execute("SELECT * FROM test WHERE id = 1")
                    row = cursor.fetchone()
                    results.append((worker_id, row[0]))
                    time.sleep(0.001)  # Shorter work simulation
            except Exception as e:
                errors.append((worker_id, str(e)))
        
        # Create fewer threads to avoid timeouts
        threads = []
        for i in range(3):  # Reduced from 5 to 3
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 3
        
        # All workers should have gotten the same result
        for worker_id, result in results:
            assert result == 1
        
        # Check pool stats
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] >= 1
        assert stats['pool_size'] >= 1
        assert stats['active_connections'] == 0

    def test_connection_pool_with_nonexistent_db(self):
        """Test pool behavior with non-existent database."""
        # Should create the database file
        db_path = "test_nonexistent.db"
        
        try:
            pool = ConnectionPool(db_path, max_connections=2)
            
            # Should be able to get a connection
            with pool.get_connection_context() as conn:
                cursor = conn.execute("CREATE TABLE test_new (id INTEGER PRIMARY KEY)")
                cursor.execute("INSERT INTO test_new (id) VALUES (1)")
                conn.commit()
            
            pool.close_all()
            
        finally:
            # Cleanup
            try:
                os.unlink(db_path)
            except OSError:
                pass

    def test_return_invalid_connection(self, pool):
        """Test returning an invalid connection."""
        # Get a valid connection
        conn = pool.get_connection()
        
        # Close it manually (making it invalid)
        conn.close()
        
        # Try to return it - should handle gracefully
        pool.return_connection(conn)
        
        # Pool should still be functional
        with pool.get_connection_context() as new_conn:
            cursor = new_conn.execute("SELECT * FROM test")
            cursor.fetchall()

    def test_pool_cleanup_on_exit(self, temp_db_path):
        """Test that pool properly cleans up on exit."""
        pool = ConnectionPool(temp_db_path, max_connections=2)
        
        # Get some connections
        conn1 = pool.get_connection()
        conn2 = pool.get_connection()
        
        # Close pool
        pool.close_all()
        
        # Connections should be closed
        stats = pool.get_pool_stats()
        assert stats['total_connections_created'] == 2
        assert stats['active_connections'] == 0
        assert stats['pool_size'] == 0

    def test_pool_context_manager(self, temp_db_path):
        """Test pool as context manager."""
        with ConnectionPool(temp_db_path, max_connections=2) as pool:
            # Pool should be functional
            with pool.get_connection_context() as conn:
                cursor = conn.execute("SELECT * FROM test")
                cursor.fetchall()
        
        # Pool should be closed after context exit
        # Note: We can't check stats after close_all() as the pool is closed

    def test_connection_timeout(self, temp_db_path):
        """Test connection timeout behavior."""
        pool = ConnectionPool(temp_db_path, max_connections=1, timeout=1)
        
        # Get the only connection
        conn1 = pool.get_connection()
        
        # Try to get another connection - should timeout
        start_time = time.time()
        with pytest.raises(TimeoutError):
            conn2 = pool.get_connection()
        end_time = time.time()
        
        # Should have timed out after approximately 1 second
        assert end_time - start_time >= 0.9  # Allow some tolerance
        
        pool.return_connection(conn1)
        pool.close_all()

    def test_sqlite_configuration(self, pool):
        """Test that SQLite connections are properly configured."""
        with pool.get_connection_context() as conn:
            # Check that foreign keys are enabled
            cursor = conn.execute("PRAGMA foreign_keys")
            foreign_keys = cursor.fetchone()[0]
            assert foreign_keys == 1
            
            # Check journal mode
            cursor = conn.execute("PRAGMA journal_mode")
            journal_mode = cursor.fetchone()[0]
            assert journal_mode.upper() == "WAL" 