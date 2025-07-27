# Shardlite

A lightweight Python library for sharding in SQLite.

Shardlite provides a unified, extensible API for sharding SQLite databases, addressing gaps that currently require custom, error-prone code in real-world projects. The library's value lies in simplifying routing, aggregation, schema management, and cross-shard transactions.

---

## ✨ Features
<aside>

- **Hash-based sharding** with deterministic routing
- **Unified CRUD API** for all database operations
- **Cross-shard transactions** with Two-Phase Commit (2PC) protocol (Under development)
- **Schema management** across all shards
- **Connection pooling** for performance and concurrency
- **Aggregation and analytics** across shards
- **Comprehensive error handling and logging**
- **Extensible sharding strategies** (Strategy pattern)
- **Easy configuration** (code, dict, or YAML)
- **ACID compliance at shard level**
</aside>

---

## 🏗️ Architecture Overview

Shardlite is organized into clear layers, each with specific responsibilities:

### Layer Responsibilities
<aside>

- **API Layer**
    - Provides a unified, user-friendly interface for all database operations
    - Hides sharding complexity from end users
    - Exposes CRUD, schema, aggregation, and transaction operations
    - Handles cross-shard transactions transparently

- **Orchestration Layer**
    - Coordinates operations across shards
    - Manages shard registry and configuration
    - Routes operations to appropriate shards
    - Implements sharding strategies (currently hash-based, extensible)
    - Coordinates cross-shard transactions

- **Connection Layer**
    - Manages database connections
    - Maintains thread-safe connection pools for each shard
    - Handles connection lifecycle and reuse
    - Provides connection abstraction

- **Data Layer**
    - Physical SQLite database files
    - Individual SQLite databases per shard
    - ACID compliance at shard level
    - Optimized for read/write operation
</aside>

---

## ⚙️ Workflow

<aside>

1. **User Code Calls API**
    - The user calls a function from the public API (e.g., shardlite.select(...), shardlite.update(...)).
2. **API Layer Receives the Request**
    - The API function (in api.py) receives the call.
    - It ensures the system is initialized and gets the global ShardManager instance.
3. **Manager Orchestrates the Operation**
    - The ShardManager method (e.g., select, update, etc.) is called.
    - The manager handles any high-level orchestration, such as:
        - Validating configuration
        - Managing transactions (if needed)
        - Gathering system-wide information
4. **Router Handles Routing Logic**
    - The ShardManager delegates the actual database operation to its Router instance.
    - The Router decides:
        - Which shard(s) should handle the request (using the sharding strategy)
        - How to execute the operation (e.g., run on one shard, or all shards for aggregates
    - The Router manages connections and executes the SQL on the correct shard(s).
5. **Query Execution**
    - The router uses a connection from the pool to execute the SQL operation on the correct shard(s).
6. **Result Flows Back Up**
    - The result of the operation (data, row count, etc.) is returned from the Router to the Manager, then to the API, and finally to the user.
</aside>

---


## 📚 Example Gallery

See the [`examples/`](shardlite/examples/) directory for runnable scripts:

- **01_basic_crud.py**: Basic CRUD operations, sharding key usage, cross-shard queries
- **02_aggregations.py**: Cross-shard aggregations (COUNT, SUM, AVG, etc.)
- **03_transactions.py**: Cross-shard transactions with rollback and logging
- **04_configuration.py**: Multiple configuration methods, YAML config, inspection

Each example is self-contained and demonstrates a core feature.

---

> **Note:** Always call `shutdown()` on the manager (or use the API’s `shutdown()` function) before deleting or moving database files, especially on Windows, to ensure all connections are closed and files can be safely removed.

> **Note:** When using `key` in `select`, it only determines which shard to query. If you want to select a specific row, always provide a `where` clause (e.g., `where={'id': key}`) to filter the results. Otherwise, all rows in the shard will be returned.

---

## 🧩 Extensibility

- **Sharding Strategies:** Plug in your own by subclassing `ShardingStrategy`
- **Custom Transaction Logging:** Implement the `TransactionLogger` protocol
- **Connection Pooling:** Configurable per-shard pools
- **Schema Management:** Apply DDL across all shards

---


## 🛡️ Error Handling

- All operations include comprehensive error handling
- Clear error messages for invalid input, missing files, or SQL errors
- Connection pool handles timeouts, invalid connections, and recovery

---

## 📈 Performance

- Connection pooling for concurrency and speed
- Efficient routing and aggregation
- Designed for minimal memory overhead

---

## 🔮 Future TODOs

- **Consistent Hashing:** For dynamic shard addition/removal
- **Distributed Deadlock Detection/Avoidance:** For cross-shard transactions and connection management
- **Compression/Encryption:** Support for compressed and encrypted database files
- **Performance Monitoring:** Real-time metrics and dashboards
- **Advanced Sharding Strategies:** Geographic, time-based, composite, and load-aware sharding
- **Automatic Scaling:** Dynamic pool size and shard management

---

## 📄 License

MIT License

---