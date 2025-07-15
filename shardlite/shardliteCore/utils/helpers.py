"""
Utility helper functions for Shardlite.

This module provides utility functions for common operations such as
directory management, file validation, and data processing.
"""

import os
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path


def ensure_directory(path: str) -> None:
    """
    Ensure a directory exists, creating it if necessary.
    
    Args:
        path: Directory path to ensure exists
        
    Raises:
        OSError: If directory cannot be created
    """
    if not path:
        raise ValueError("Path cannot be empty")
    
    os.makedirs(path, exist_ok=True)


def get_shard_filename(shard_id: int, db_dir: str) -> str:
    """
    Generate filename for a shard database.
    
    Args:
        shard_id: Shard ID
        db_dir: Database directory path
        
    Returns:
        str: Full path to the shard database file
        
    Raises:
        ValueError: If parameters are invalid
    """
    if not isinstance(shard_id, int) or shard_id < 0:
        raise ValueError(f"shard_id must be a non-negative integer, got {shard_id}")
    
    if not db_dir or not isinstance(db_dir, str):
        raise ValueError("db_dir must be a non-empty string")
    
    filename = f"shard_{shard_id}.db"
    return os.path.join(db_dir, filename)


def validate_shard_files(shard_registry: Dict[int, str]) -> bool:
    """
    Validate that all shard files exist and are accessible.
    
    Args:
        shard_registry: Mapping of shard ID to file path
        
    Returns:
        bool: True if all shard files are valid, False otherwise
    """
    if not shard_registry:
        return False
    
    for shard_id, db_path in shard_registry.items():
        if not os.path.exists(db_path):
            return False
        
        # Try to open the database
        try:
            conn = sqlite3.connect(db_path)
            conn.execute("SELECT 1")
            conn.close()
        except Exception:
            return False
    
    return True


def get_file_size(file_path: str) -> int:
    """
    Get the size of a file in bytes.
    
    Args:
        file_path: Path to the file
        
    Returns:
        int: File size in bytes, 0 if file doesn't exist
        
    Raises:
        ValueError: If file_path is invalid
    """
    if not file_path or not isinstance(file_path, str):
        raise ValueError("file_path must be a non-empty string")
    
    try:
        return os.path.getsize(file_path)
    except OSError:
        return 0


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes into human-readable string.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        str: Human-readable string (e.g., "1.5 MB")
    """
    if bytes_value < 1024:
        return f"{bytes_value} B"
    elif bytes_value < 1024 * 1024:
        return f"{bytes_value / 1024:.1f} KB"
    elif bytes_value < 1024 * 1024 * 1024:
        return f"{bytes_value / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_value / (1024 * 1024 * 1024):.1f} GB"


def validate_sql_identifier(identifier: str) -> bool:
    """
    Validate if a string is a valid SQL identifier.
    
    Args:
        identifier: String to validate
        
    Returns:
        bool: True if valid SQL identifier, False otherwise
    """
    if not identifier or not isinstance(identifier, str):
        return False
    
    # Check for SQL keywords and invalid characters
    sql_keywords = {
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE',
        'DROP', 'TABLE', 'INDEX', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES',
        'UNIQUE', 'NOT', 'NULL', 'DEFAULT', 'CHECK', 'CONSTRAINT', 'ORDER',
        'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'JOIN', 'LEFT', 'RIGHT',
        'INNER', 'OUTER', 'ON', 'AS', 'AND', 'OR', 'IN', 'EXISTS', 'BETWEEN',
        'LIKE', 'IS', 'DISTINCT', 'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'
    }
    
    # Check if it's a SQL keyword
    if identifier.upper() in sql_keywords:
        return False
    
    # Check for valid characters (alphanumeric and underscore)
    if not identifier.replace('_', '').isalnum():
        return False
    
    # Check if it starts with a letter or underscore
    if not (identifier[0].isalpha() or identifier[0] == '_'):
        return False
    
    return True


def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize a table name for safe SQL usage.
    
    Args:
        table_name: Table name to sanitize
        
    Returns:
        str: Sanitized table name
        
    Raises:
        ValueError: If table_name is invalid
    """
    if not table_name or not isinstance(table_name, str):
        raise ValueError("table_name must be a non-empty string")
    
    # Remove any potentially dangerous characters
    sanitized = ''.join(c for c in table_name if c.isalnum() or c == '_')
    
    if not sanitized:
        raise ValueError("Table name contains no valid characters")
    
    # Ensure it doesn't start with a number
    if sanitized[0].isdigit():
        sanitized = f"t_{sanitized}"
    
    return sanitized


def build_where_clause(where_dict: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Build WHERE clause from dictionary of conditions.
    
    Args:
        where_dict: Dictionary of column: value pairs
        
    Returns:
        Tuple[str, List[Any]]: WHERE clause string and parameter values
        
    Raises:
        ValueError: If where_dict is invalid
    """
    if not where_dict:
        return "", []
    
    if not isinstance(where_dict, dict):
        raise ValueError("where_dict must be a dictionary")
    
    conditions = []
    params = []
    
    for column, value in where_dict.items():
        if not validate_sql_identifier(column):
            raise ValueError(f"Invalid column name: {column}")
        
        conditions.append(f"{column} = ?")
        params.append(value)
    
    where_clause = " AND ".join(conditions)
    return where_clause, params


def build_set_clause(set_dict: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Build SET clause from dictionary of values.
    
    Args:
        set_dict: Dictionary of column: value pairs
        
    Returns:
        Tuple[str, List[Any]]: SET clause string and parameter values
        
    Raises:
        ValueError: If set_dict is invalid
    """
    if not set_dict:
        raise ValueError("set_dict cannot be empty")
    
    if not isinstance(set_dict, dict):
        raise ValueError("set_dict must be a dictionary")
    
    set_clauses = []
    params = []
    
    for column, value in set_dict.items():
        if not validate_sql_identifier(column):
            raise ValueError(f"Invalid column name: {column}")
        
        set_clauses.append(f"{column} = ?")
        params.append(value)
    
    set_clause = ", ".join(set_clauses)
    return set_clause, params


def validate_row_data(row: Dict[str, Any]) -> bool:
    """
    Validate row data for insertion/update.
    
    Args:
        row: Dictionary of column: value pairs
        
    Returns:
        bool: True if row data is valid, False otherwise
    """
    if not isinstance(row, dict):
        return False
    
    if not row:
        return False
    
    for column, value in row.items():
        if not validate_sql_identifier(column):
            return False
        
        # Check for None values (use NULL in SQL)
        if value is None:
            continue
        
        # Check for basic data types
        if not isinstance(value, (str, int, float, bool)):
            return False
    
    return True


def merge_results(results_list: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """
    Merge results from multiple shards.
    
    Args:
        results_list: List of result lists from different shards
        
    Returns:
        List[Dict[str, Any]]: Merged results
    """
    if not results_list:
        return []
    
    merged = []
    for results in results_list:
        if isinstance(results, list):
            merged.extend(results)
    
    return merged


def aggregate_results(results_list: List[Dict[str, Any]], agg_type: str) -> Dict[str, Any]:
    """
    Aggregate results from multiple shards.
    
    Args:
        results_list: List of result dictionaries from different shards
        agg_type: Type of aggregation ('sum', 'count', 'avg', 'max', 'min')
        
    Returns:
        Dict[str, Any]: Aggregated results
    """
    if not results_list:
        return {}
    
    aggregated = {}
    
    for result in results_list:
        if not isinstance(result, dict):
            continue
        
        for key, value in result.items():
            if key not in aggregated:
                aggregated[key] = value
            else:
                # Aggregate based on type
                if agg_type == 'sum' and isinstance(value, (int, float)):
                    aggregated[key] += value
                elif agg_type == 'count' and isinstance(value, (int, float)):
                    aggregated[key] += value
                elif agg_type == 'max' and isinstance(value, (int, float)):
                    aggregated[key] = max(aggregated[key], value)
                elif agg_type == 'min' and isinstance(value, (int, float)):
                    aggregated[key] = min(aggregated[key], value)
                # For avg, we need to track count separately
                # This is a simplified implementation
    
    return aggregated


def calculate_shard_distribution(num_keys: int, num_shards: int) -> List[int]:
    """
    Calculate expected distribution of keys across shards.
    
    Args:
        num_keys: Number of keys to distribute
        num_shards: Number of shards
        
    Returns:
        List[int]: Expected count of keys per shard
    """
    if num_keys <= 0 or num_shards <= 0:
        return []
    
    base_count = num_keys // num_shards
    remainder = num_keys % num_shards
    
    distribution = [base_count] * num_shards
    
    # Distribute remainder
    for i in range(remainder):
        distribution[i] += 1
    
    return distribution


def check_disk_space(path: str, required_bytes: int) -> bool:
    """
    Check if there's enough disk space available.
    
    Args:
        path: Path to check disk space for
        required_bytes: Required space in bytes
        
    Returns:
        bool: True if enough space is available, False otherwise
    """
    try:
        statvfs = os.statvfs(path)
        free_bytes = statvfs.f_frsize * statvfs.f_bavail
        return free_bytes >= required_bytes
    except OSError:
        return False


def get_database_info(db_path: str) -> Dict[str, Any]:
    """
    Get information about a SQLite database.
    
    Args:
        db_path: Path to the SQLite database
        
    Returns:
        Dict[str, Any]: Database information
        
    Raises:
        ValueError: If db_path is invalid
        sqlite3.Error: If database cannot be accessed
    """
    if not db_path or not isinstance(db_path, str):
        raise ValueError("db_path must be a non-empty string")
    
    if not os.path.exists(db_path):
        return {
            'exists': False,
            'size': 0,
            'tables': [],
            'version': None
        }
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get database version
        cursor.execute("SELECT sqlite_version()")
        version = cursor.fetchone()[0]
        
        # Get list of tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return {
            'exists': True,
            'size': os.path.getsize(db_path),
            'tables': tables,
            'version': version
        }
    except sqlite3.Error as e:
        return {
            'exists': True,
            'size': os.path.getsize(db_path),
            'tables': [],
            'version': None,
            'error': str(e)
        } 