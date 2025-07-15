#!/usr/bin/env python3
"""
Basic CRUD Operations Example

This example shows how to:
1. Initialize Shardlite
2. Create a table
3. Insert data
4. Query data
5. Update data
6. Delete data
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shardlite.shardliteCore.api import (
    initialize, insert, select, update, delete, create_table, shutdown
)


def main():
    print("=== Basic CRUD Operations Example ===\n")
    
    # Configuration
    db_dir = "./example_data"
    num_shards = 2  # Simple 2-shard setup
    
    try:
        # Step 1: Initialize Shardlite
        print("1. Initializing Shardlite...")
        manager = initialize(
            num_shards=num_shards,
            db_dir=db_dir,
            auto_create_dirs=True
        )
        print("✅ Initialized successfully\n")
        
        # Step 2: Create a table
        print("2. Creating 'products' table...")
        create_table("products", {
            "id": "INTEGER PRIMARY KEY",
            "name": "TEXT NOT NULL",
            "price": "REAL",
            "category": "TEXT"
        })
        print("✅ Table created across all shards\n")
        
        # Step 3: Insert data
        print("3. Inserting products...")
        products = [
            {"name": "Laptop", "price": 999.99, "category": "electronics"},
            {"name": "Mouse", "price": 29.99, "category": "electronics"},
            {"name": "Book", "price": 19.99, "category": "books"},
            {"name": "Coffee Mug", "price": 12.99, "category": "kitchen"}
        ]
        
        for i, product in enumerate(products):
            # Use product ID as sharding key
            insert("products", product, i + 1)
            print(f"✅ Inserted: {product['name']} (ID: {i + 1})")
        print()
        
        # Step 4: Query data
        print("4. Querying data...")
        
        # Query all products
        all_products = select("products")
        print(f"Total products: {len(all_products)}")
        for product in all_products:
            print(f"  - {product['name']} (${product['price']})")
        print()
        
        # Query specific product by ID
        laptop = select("products", key=1)  # Query by sharding key
        print(f"Product with ID 1: {laptop}")
        print()
        
        # Query by category
        electronics = select("products", where={"category": "electronics"})
        print(f"Electronics products: {len(electronics)} found")
        for product in electronics:
            print(f"  - {product['name']}")
        print()
        
        # Step 5: Update data
        print("5. Updating data...")
        affected = update("products", {"price": 899.99}, where={"id": 1}, key=1)
        print(f"✅ Updated {affected} product(s)")
        
        # Verify update
        updated_laptop = select("products", key=1)
        print(f"Updated laptop price: ${updated_laptop[0]['price']}")
        print()
        
        # Step 6: Delete data
        print("6. Deleting data...")
        deleted = delete("products", where={"id": 4}, key=4)
        print(f"✅ Deleted {deleted} product(s)")
        
        # Verify deletion
        remaining = select("products")
        print(f"Remaining products: {len(remaining)}")
        print()
        
        print("🎉 Basic CRUD operations completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    finally:
        # Cleanup
        print("\nCleaning up...")
        shutdown()
        print("✅ Shutdown completed")


if __name__ == "__main__":
    main() 