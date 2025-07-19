# Shardlite
 A light-weight python library for sharding in SQLite

> **Note:** Always call `shutdown()` on the manager (or use the API’s `shutdown()` function) before deleting or moving database files, especially on Windows, to ensure all connections are closed and files can be safely removed.

> **Note:** When using `key` in `route_select`, it only determines which shard to query. If you want to select a specific row, always provide a `where` clause (e.g., `where={'id': key}`) to filter the results. Otherwise, all rows in the shard will be returned.
