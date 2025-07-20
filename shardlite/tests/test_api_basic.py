import os
import shutil
import tempfile
import pytest
from shardlite.shardliteCore.api import (
    initialize, create_table, insert, select, update, delete, shutdown
)

@pytest.fixture(scope="function")
def temp_db_dir():
    dirpath = tempfile.mkdtemp(prefix="shardlite_test_")
    yield dirpath
    shutil.rmtree(dirpath)


def test_basic_crud_flow(temp_db_dir):
    # Initialize Shardlite
    manager = initialize(num_shards=2, db_dir=temp_db_dir, auto_create_dirs=True)
    
    # Create table
    create_table("items", {
        "id": "INTEGER PRIMARY KEY",
        "name": "TEXT NOT NULL",
        "value": "INTEGER"
    })

    # Insert data
    item = {"name": "Widget", "value": 42}
    insert("items", item, 1)

    # Select data
    results = select("items")
    assert len(results) == 1
    assert results[0]["name"] == "Widget"
    assert results[0]["value"] == 42

    # Update data
    update("items", {"value": 100}, where={"id": 1}, key=1)
    updated = select("items", key=1)
    assert updated[0]["value"] == 100

    # Delete data
    delete("items", where={"id": 1}, key=1)
    remaining = select("items")
    assert len(remaining) == 0

    # Shutdown
    shutdown() 