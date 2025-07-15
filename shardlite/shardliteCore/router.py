"""
Query routing component for Shardlite.

This module provides the Router class that routes database operations
to appropriate shards using the configured sharding strategy.
"""

from typing import Dict, List, Any, Optional, Union
from .strategy.base import ShardingStrategy
from .connection.pool import ConnectionPool


class Router:
    """
    Query routing component for directing operations to appropriate shards.
    
    This class is responsible for routing database operations to the correct
    shards based on the configured sharding strategy. It provides a unified
    interface for all routing operations.
    
    Attributes:
        shard_manager: Reference to the ShardManager for accessing shards
        strategy: Sharding strategy for determining shard routing
        connection_pools: Mapping of shard IDs to connection pools
    """
    
    def __init__(
        self, 
        shard_manager: 'ShardManager', 
        strategy: ShardingStrategy
    ) -> None:
        """
        Initialize router with shard manager and strategy.
        
        Args:
            shard_manager: ShardManager instance for accessing shards
            strategy: ShardingStrategy implementation for routing logic
            
        Raises:
            ValueError: If parameters are invalid
        """
        if shard_manager is None:
            raise ValueError("shard_manager cannot be None")
        
        if strategy is None:
            raise ValueError("strategy cannot be None")
        
        self.shard_manager = shard_manager
        self.strategy = strategy
        self.connection_pools: Dict[int, ConnectionPool] = {}
    
    def route(self, operation: str, **kwargs: Any) -> Any:
        """
        Route a database operation to appropriate shard(s).
        
        This is the main routing method that determines which shard(s)
        should handle the operation based on the operation type and parameters.
        
        Args:
            operation: Type of operation ('insert', 'select', 'update', 'delete', 'aggregate')
            **kwargs: Operation-specific parameters
            
        Returns:
            Any: Result of the operation
            
        Raises:
            ValueError: If operation type is not supported
            KeyError: If required parameters are missing
        """
        if operation == 'insert':
            return self.route_insert(
                table=kwargs['table'],
                row=kwargs['row'],
                key=kwargs['key']
            )
        elif operation == 'select':
            return self.route_select(
                table=kwargs['table'],
                where=kwargs.get('where', {}),
                key=kwargs.get('key')
            )
        elif operation == 'update':
            return self.route_update(
                table=kwargs['table'],
                set_values=kwargs['set_values'],
                where=kwargs.get('where', {}),
                key=kwargs.get('key')
            )
        elif operation == 'delete':
            return self.route_delete(
                table=kwargs['table'],
                where=kwargs.get('where', {}),
                key=kwargs.get('key')
            )
        elif operation == 'aggregate':
            return self.route_aggregate(
                table=kwargs['table'],
                agg_expr=kwargs['agg_expr']
            )
        else:
            raise ValueError(f"Unsupported operation: {operation}")
    
    def route_insert(self, table: str, row: Dict[str, Any], key: int) -> None:
        """
        Route insert operation to appropriate shard.
        
        Args:
            table: Target table name
            row: Data to insert (column_name: value)
            key: Sharding key for routing
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")
        
        if not isinstance(row, dict):
            raise ValueError("row must be a dictionary")
        
        if not self.strategy.validate_key(key):
            raise ValueError(f"Invalid key: {key}")
        
        shard_id = self.strategy.get_shard_id(key)
        connection_pool = self._get_connection_pool(shard_id)
        
        with connection_pool.get_connection_context() as conn:
            self._execute_insert(conn, table, row)
    
    def route_select(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Route select operation to appropriate shard(s).
        
        Args:
            table: Source table name
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, query all shards)
            
        Returns:
            List[Dict[str, Any]]: Query results
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")
        
        if where is None:
            where = {}
        
        if key is not None:
            # Route to specific shard
            if not self.strategy.validate_key(key):
                raise ValueError(f"Invalid key: {key}")
            
            shard_id = self.strategy.get_shard_id(key)
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                return self._execute_select(conn, table, where)
        else:
            # Query all shards
            results = []
            for shard_id in self.strategy.get_all_shard_ids():
                connection_pool = self._get_connection_pool(shard_id)
                
                with connection_pool.get_connection_context() as conn:
                    shard_results = self._execute_select(conn, table, where)
                    results.extend(shard_results)
            
            return results
    
    def route_update(
        self, 
        table: str, 
        set_values: Dict[str, Any],
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> int:
        """
        Route update operation to appropriate shard(s).
        
        Args:
            table: Target table name
            set_values: Values to set (column_name: value)
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, update all shards)
            
        Returns:
            int: Number of rows affected
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")
        
        if not isinstance(set_values, dict):
            raise ValueError("set_values must be a dictionary")
        
        if where is None:
            where = {}
        
        if key is not None:
            # Route to specific shard
            if not self.strategy.validate_key(key):
                raise ValueError(f"Invalid key: {key}")
            
            shard_id = self.strategy.get_shard_id(key)
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                return self._execute_update(conn, table, set_values, where)
        else:
            # Update all shards
            total_affected = 0
            for shard_id in self.strategy.get_all_shard_ids():
                connection_pool = self._get_connection_pool(shard_id)
                
                with connection_pool.get_connection_context() as conn:
                    affected = self._execute_update(conn, table, set_values, where)
                    total_affected += affected
            
            return total_affected
    
    def route_delete(
        self, 
        table: str, 
        where: Optional[Dict[str, Any]] = None,
        key: Optional[int] = None
    ) -> int:
        """
        Route delete operation to appropriate shard(s).
        
        Args:
            table: Target table name
            where: WHERE conditions (column_name: value)
            key: Sharding key for routing (if None, delete from all shards)
            
        Returns:
            int: Number of rows affected
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")
        
        if where is None:
            where = {}
        
        if key is not None:
            # Route to specific shard
            if not self.strategy.validate_key(key):
                raise ValueError(f"Invalid key: {key}")
            
            shard_id = self.strategy.get_shard_id(key)
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                return self._execute_delete(conn, table, where)
        else:
            # Delete from all shards
            total_affected = 0
            for shard_id in self.strategy.get_all_shard_ids():
                connection_pool = self._get_connection_pool(shard_id)
                
                with connection_pool.get_connection_context() as conn:
                    affected = self._execute_delete(conn, table, where)
                    total_affected += affected
            
            return total_affected
    
    def route_aggregate(self, table: str, agg_expr: str) -> Dict[str, Any]:
        """
        Route aggregate operation across all shards.
        
        Args:
            table: Source table name
            agg_expr: Aggregation expression (e.g., "COUNT(*)", "SUM(amount)")
            
        Returns:
            Dict[str, Any]: Aggregation results
            
        Raises:
            ValueError: If parameters are invalid
            sqlite3.Error: If database operation fails
        """
        if not table or not isinstance(table, str):
            raise ValueError("table must be a non-empty string")
        
        if not agg_expr or not isinstance(agg_expr, str):
            raise ValueError("agg_expr must be a non-empty string")
        
        # Handle different aggregation types
        agg_type = agg_expr.upper().split('(')[0]
        
        if agg_type in ['AVG', 'AVERAGE']:
            # For average, we need to get sum and count separately
            return self._handle_average_aggregation(table, agg_expr)
        elif agg_type in ['MAX', 'MIN']:
            # For MAX/MIN, we need to find the actual max/min across all shards
            return self._handle_maxmin_aggregation(table, agg_expr)
        elif agg_type in ['COUNT', 'SUM']:
            # For COUNT and SUM, combine results from all shards
            return self._handle_standard_aggregation(table, agg_expr)
        else:
            # Unknown aggregation type, try standard approach
            return self._handle_standard_aggregation(table, agg_expr)
    
    def _handle_average_aggregation(self, table: str, agg_expr: str) -> Dict[str, Any]:
        """Handle average aggregation by calculating sum and count separately."""
        # Extract column name from AVG(column)
        column = agg_expr[agg_expr.find('(')+1:agg_expr.find(')')]
        
        total_sum = 0
        total_count = 0
        
        # Get sum and count from all shards
        for shard_id in self.strategy.get_all_shard_ids():
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                # Get sum
                sum_result = self._execute_aggregate(conn, table, f"SUM({column})")
                sum_value = sum_result.get(f"SUM({column})", 0) or 0
                
                # Get count
                count_result = self._execute_aggregate(conn, table, f"COUNT({column})")
                count_value = count_result.get(f"COUNT({column})", 0) or 0
                
                total_sum += sum_value
                total_count += count_value
        
        # Calculate average
        avg_value = total_sum / total_count if total_count > 0 else 0
        return {agg_expr: avg_value}
    
    def _handle_maxmin_aggregation(self, table: str, agg_expr: str) -> Dict[str, Any]:
        """Handle MAX/MIN aggregations by finding the actual max/min across all shards."""
        agg_type = agg_expr.upper().split('(')[0]
        column = agg_expr[agg_expr.find('(')+1:agg_expr.find(')')]
        
        all_values = []
        
        # Get all values from all shards
        for shard_id in self.strategy.get_all_shard_ids():
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                # Get all values for this column
                sql = f"SELECT {column} FROM {table} WHERE {column} IS NOT NULL"
                cursor = conn.execute(sql)
                values = [row[0] for row in cursor.fetchall()]
                all_values.extend(values)
        
        if not all_values:
            return {agg_expr: None}
        
        # Find max or min
        if agg_type == 'MAX':
            result_value = max(all_values)
        else:  # MIN
            result_value = min(all_values)
        
        return {agg_expr: result_value}
    
    def _handle_standard_aggregation(self, table: str, agg_expr: str) -> Dict[str, Any]:
        """Handle standard aggregations (COUNT, SUM, MAX, MIN)."""
        results = {}
        
        # Execute aggregation on all shards
        for shard_id in self.strategy.get_all_shard_ids():
            connection_pool = self._get_connection_pool(shard_id)
            
            with connection_pool.get_connection_context() as conn:
                shard_result = self._execute_aggregate(conn, table, agg_expr)
                
                # Combine results
                for key, value in shard_result.items():
                    if key not in results:
                        results[key] = value
                    else:
                        # For numeric aggregations, sum the values
                        if isinstance(value, (int, float)) and isinstance(results[key], (int, float)):
                            results[key] += value
                        else:
                            # For non-numeric, use the last value
                            results[key] = value
        
        return results
    
    def get_connection_for_key(self, key: int) -> ConnectionPool:
        """
        Get connection pool for a specific key.
        
        Args:
            key: Sharding key
            
        Returns:
            ConnectionPool: Connection pool for the shard containing the key
            
        Raises:
            ValueError: If key is invalid
        """
        if not self.strategy.validate_key(key):
            raise ValueError(f"Invalid key: {key}")
        
        shard_id = self.strategy.get_shard_id(key)
        return self._get_connection_pool(shard_id)
    
    def _get_connection_pool(self, shard_id: int) -> ConnectionPool:
        """
        Get or create connection pool for a shard.
        
        Args:
            shard_id: Shard ID
            
        Returns:
            ConnectionPool: Connection pool for the shard
        """
        if shard_id not in self.connection_pools:
            # Get shard info from shard manager
            shard_info = self.shard_manager.get_shard_info().get(shard_id)
            if not shard_info:
                raise ValueError(f"Shard {shard_id} not found")
            
            db_path = shard_info['db_path']
            config = self.shard_manager.config
            
            self.connection_pools[shard_id] = ConnectionPool(
                db_path=db_path,
                max_connections=config.max_connections_per_shard,
                timeout=config.connection_timeout
            )
        
        return self.connection_pools[shard_id]
    
    def _execute_insert(self, conn, table: str, row: Dict[str, Any]) -> None:
        """Execute INSERT operation on a connection."""
        columns = list(row.keys())
        placeholders = ', '.join(['?' for _ in columns])
        values = list(row.values())
        
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        conn.execute(sql, values)
        conn.commit()
    
    def _execute_select(self, conn, table: str, where: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Execute SELECT operation on a connection."""
        sql = f"SELECT * FROM {table}"
        params = []
        
        if where:
            conditions = []
            for column, value in where.items():
                conditions.append(f"{column} = ?")
                params.append(value)
            sql += f" WHERE {' AND '.join(conditions)}"
        
        cursor = conn.execute(sql, params)
        columns = [description[0] for description in cursor.description]
        
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def _execute_update(self, conn, table: str, set_values: Dict[str, Any], where: Dict[str, Any]) -> int:
        """Execute UPDATE operation on a connection."""
        set_clause = ', '.join([f"{column} = ?" for column in set_values.keys()])
        sql = f"UPDATE {table} SET {set_clause}"
        params = list(set_values.values())
        
        if where:
            conditions = []
            for column, value in where.items():
                conditions.append(f"{column} = ?")
                params.append(value)
            sql += f" WHERE {' AND '.join(conditions)}"
        
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor.rowcount
    
    def _execute_delete(self, conn, table: str, where: Dict[str, Any]) -> int:
        """Execute DELETE operation on a connection."""
        sql = f"DELETE FROM {table}"
        params = []
        
        if where:
            conditions = []
            for column, value in where.items():
                conditions.append(f"{column} = ?")
                params.append(value)
            sql += f" WHERE {' AND '.join(conditions)}"
        
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor.rowcount
    
    def _execute_aggregate(self, conn, table: str, agg_expr: str) -> Dict[str, Any]:
        """Execute aggregate operation on a connection."""
        sql = f"SELECT {agg_expr} FROM {table}"
        cursor = conn.execute(sql)
        row = cursor.fetchone()
        
        if row:
            return {agg_expr: row[0]}
        return {agg_expr: None}
    
    def __repr__(self) -> str:
        """Return string representation of the router."""
        return f"Router(strategy={self.strategy}, shards={len(self.connection_pools)})" 