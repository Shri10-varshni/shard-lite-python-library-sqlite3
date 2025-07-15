# Utility Functions Module

This module provides utility functions for common operations in Shardlite, including file management, data validation, SQL generation, and performance monitoring.

## Overview

The utils module contains helper functions that support the core Shardlite functionality. These utilities handle common tasks such as directory management, file validation, SQL query building, and data processing.

## Components

### File and Directory Management

#### ensure_directory(path: str) -> None
Ensures a directory exists, creating it if necessary.

```python
from shardlite.utils import ensure_directory

# Create directory if it doesn't exist
ensure_directory("./data/shard_0")
```

#### get_shard_filename(shard_id: int, db_dir: str) -> str
Generates filename for a shard database.

```python
from shardlite.utils import get_shard_filename

# Generate shard filename
filename = get_shard_filename(0, "./data")
# Returns: "./data/shard_0.db"
```

#### validate_shard_files(shard_registry: Dict[int, str]) -> bool
Validates that all shard files exist and are accessible.

```python
from shardlite.utils import validate_shard_files

# Validate shard files
registry = {0: "./data/shard_0.db", 1: "./data/shard_1.db"}
is_valid = validate_shard_files(registry)
```

#### get_file_size(file_path: str) -> int
Gets the size of a file in bytes.

```python
from shardlite.utils import get_file_size

# Get file size
size = get_file_size("./data/shard_0.db")
print(f"File size: {size} bytes")
```

#### format_bytes(bytes_value: int) -> str
Formats bytes into human-readable string.

```python
from shardlite.utils import format_bytes

# Format bytes
formatted = format_bytes(1024 * 1024)  # Returns "1.0 MB"
```

### SQL and Data Validation

#### validate_sql_identifier(identifier: str) -> bool
Validates if a string is a valid SQL identifier.

```python
from shardlite.utils import validate_sql_identifier

# Validate table name
is_valid = validate_sql_identifier("users")  # True
is_valid = validate_sql_identifier("SELECT")  # False (SQL keyword)
is_valid = validate_sql_identifier("123table")  # False (starts with number)
```

#### sanitize_table_name(table_name: str) -> str
Sanitizes a table name for safe SQL usage.

```python
from shardlite.utils import sanitize_table_name

# Sanitize table name
safe_name = sanitize_table_name("123users")  # Returns "t_123users"
safe_name = sanitize_table_name("user-table")  # Returns "usertable"
```

#### validate_row_data(row: Dict[str, Any]) -> bool
Validates row data for insertion/update.

```python
from shardlite.utils import validate_row_data

# Validate row data
row = {"name": "John", "age": 30, "email": "john@example.com"}
is_valid = validate_row_data(row)  # True

invalid_row = {"name": "John", "age": "thirty"}  # Invalid type
is_valid = validate_row_data(invalid_row)  # False
```

### SQL Query Building

#### build_where_clause(where_dict: Dict[str, Any]) -> Tuple[str, List[Any]]
Builds WHERE clause from dictionary of conditions.

```python
from shardlite.utils import build_where_clause

# Build WHERE clause
where_dict = {"name": "John", "age": 30}
where_clause, params = build_where_clause(where_dict)
# Returns: ("name = ? AND age = ?", ["John", 30])
```

#### build_set_clause(set_dict: Dict[str, Any]) -> Tuple[str, List[Any]]
Builds SET clause from dictionary of values.

```python
from shardlite.utils import build_set_clause

# Build SET clause
set_dict = {"name": "Jane", "email": "jane@example.com"}
set_clause, params = build_set_clause(set_dict)
# Returns: ("name = ?, email = ?", ["Jane", "jane@example.com"])
```

### Data Processing

#### merge_results(results_list: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]
Merges results from multiple shards.

```python
from shardlite.utils import merge_results

# Merge results from multiple shards
shard1_results = [{"id": 1, "name": "John"}]
shard2_results = [{"id": 2, "name": "Jane"}]
all_results = merge_results([shard1_results, shard2_results])
# Returns: [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
```

#### aggregate_results(results_list: List[Dict[str, Any]], agg_type: str) -> Dict[str, Any]
Aggregates results from multiple shards.

```python
from shardlite.utils import aggregate_results

# Aggregate results
shard1_result = {"COUNT(*)": 100}
shard2_result = {"COUNT(*)": 150}
total = aggregate_results([shard1_result, shard2_result], "sum")
# Returns: {"COUNT(*)": 250}
```

#### calculate_shard_distribution(num_keys: int, num_shards: int) -> List[int]
Calculates expected distribution of keys across shards.

```python
from shardlite.utils import calculate_shard_distribution

# Calculate distribution
distribution = calculate_shard_distribution(100, 4)
# Returns: [25, 25, 25, 25]

distribution = calculate_shard_distribution(101, 4)
# Returns: [26, 25, 25, 25] (remainder distributed)
```

### System Utilities

#### check_disk_space(path: str, required_bytes: int) -> bool
Checks if there's enough disk space available.

```python
from shardlite.utils import check_disk_space

# Check disk space
has_space = check_disk_space("./data", 1024 * 1024 * 100)  # 100MB
```

#### get_database_info(db_path: str) -> Dict[str, Any]
Gets information about a SQLite database.

```python
from shardlite.utils import get_database_info

# Get database info
info = get_database_info("./data/shard_0.db")
print(f"Size: {info['size']} bytes")
print(f"Tables: {info['tables']}")
print(f"SQLite version: {info['version']}")
```

## Usage Examples

### Complete Workflow Example

```python
from shardlite.utils import (
    ensure_directory, get_shard_filename, validate_shard_files,
    sanitize_table_name, build_where_clause, format_bytes
)

# Setup shard directory
ensure_directory("./data")

# Generate and validate shard files
shard_files = {}
for i in range(4):
    filename = get_shard_filename(i, "./data")
    shard_files[i] = filename

# Validate all shard files
if validate_shard_files(shard_files):
    print("All shard files are valid")
else:
    print("Some shard files are invalid")

# Work with SQL
table_name = sanitize_table_name("user-data")
print(f"Sanitized table name: {table_name}")

# Build queries
where_conditions = {"status": "active", "age": 25}
where_clause, params = build_where_clause(where_conditions)
print(f"WHERE clause: {where_clause}")
print(f"Parameters: {params}")

# Format file sizes
for shard_id, filename in shard_files.items():
    size = get_file_size(filename)
    formatted_size = format_bytes(size)
    print(f"Shard {shard_id}: {formatted_size}")
```

### Data Validation Example

```python
from shardlite.utils import validate_sql_identifier, validate_row_data

# Validate table and column names
table_name = "users"
if validate_sql_identifier(table_name):
    print(f"'{table_name}' is a valid table name")
else:
    print(f"'{table_name}' is not a valid table name")

# Validate row data
user_data = {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "active": True
}

if validate_row_data(user_data):
    print("Row data is valid")
else:
    print("Row data is invalid")
```

### Query Building Example

```python
from shardlite.utils import build_where_clause, build_set_clause

# Build WHERE clause for SELECT
select_conditions = {
    "status": "active",
    "department": "engineering",
    "salary": 75000
}
where_clause, where_params = build_where_clause(select_conditions)

sql_select = f"SELECT * FROM employees WHERE {where_clause}"
print(f"SELECT SQL: {sql_select}")
print(f"Parameters: {where_params}")

# Build SET clause for UPDATE
update_values = {
    "salary": 80000,
    "last_updated": "2024-01-15"
}
set_clause, set_params = build_set_clause(update_values)

sql_update = f"UPDATE employees SET {set_clause} WHERE {where_clause}"
print(f"UPDATE SQL: {sql_update}")
print(f"Parameters: {set_params + where_params}")
```

## Performance Considerations

### File Operations

- **Directory creation**: Use `ensure_directory()` to avoid race conditions
- **File validation**: `validate_shard_files()` checks both existence and accessibility
- **Size calculations**: `get_file_size()` handles missing files gracefully

### SQL Generation

- **Identifier validation**: Prevents SQL injection and syntax errors
- **Parameterized queries**: All generated SQL uses parameterized queries
- **Sanitization**: Table names are automatically sanitized for safety

### Data Processing

- **Efficient merging**: `merge_results()` handles large result sets efficiently
- **Memory usage**: Functions are designed to minimize memory overhead
- **Error handling**: All functions include proper error handling

## Error Handling

### File Operations

- **Missing directories**: `ensure_directory()` creates directories as needed
- **Permission errors**: Functions handle permission issues gracefully
- **Disk space**: `check_disk_space()` validates available space

### Data Validation

- **Invalid identifiers**: Clear error messages for invalid SQL identifiers
- **Type validation**: Comprehensive type checking for row data
- **Boundary conditions**: Functions handle edge cases properly

### SQL Generation

- **SQL injection prevention**: All user input is properly sanitized
- **Syntax errors**: Generated SQL is validated for correctness
- **Parameter binding**: Proper parameter binding prevents injection attacks

## Best Practices

### File Management

1. **Always validate paths**: Check file existence and permissions
2. **Use absolute paths**: When possible, use absolute paths for reliability
3. **Handle errors gracefully**: Implement proper error handling for file operations
4. **Monitor disk space**: Check available space before large operations

### Data Validation

1. **Validate early**: Check data validity as early as possible
2. **Use type hints**: Leverage type hints for better code clarity
3. **Handle edge cases**: Consider boundary conditions and edge cases
4. **Provide clear errors**: Give meaningful error messages

### SQL Generation

1. **Use parameterized queries**: Always use parameterized queries for safety
2. **Validate identifiers**: Check table and column names before use
3. **Sanitize input**: Clean user input before SQL generation
4. **Test generated SQL**: Verify generated SQL in development

## Module Dependencies

- `os`: For file and directory operations
- `sqlite3`: For database information and validation
- `pathlib`: For path manipulation
- `typing`: For type hints

## Future Enhancements

Potential improvements:
- **Caching**: Add caching for frequently accessed file information
- **Async support**: Add async versions of file operations
- **Compression**: Support for compressed database files
- **Encryption**: Support for encrypted database files
- **Performance monitoring**: Add performance metrics for utility functions

## Troubleshooting

### Common Issues

1. **Permission errors**: Check file and directory permissions
2. **Path issues**: Use absolute paths when possible
3. **Memory usage**: Monitor memory usage with large datasets
4. **SQL errors**: Validate generated SQL in development

### Debugging Tips

1. **Enable logging**: Add logging to utility functions for debugging
2. **Test edge cases**: Test with boundary conditions and invalid input
3. **Monitor performance**: Track execution time for performance-critical functions
4. **Validate output**: Verify function output matches expectations 