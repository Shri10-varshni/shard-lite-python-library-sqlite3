#!/usr/bin/env python3
"""
Aggregations Example

This example shows how to perform aggregations across multiple shards:
1. COUNT - Count total records
2. SUM - Sum numeric values
3. AVG - Calculate averages
4. MAX/MIN - Find extremes
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shardlite.shardliteCore.api import (
    initialize, insert, aggregate, create_table, shutdown
)


def main():
    print("=== Aggregations Example ===\n")
    
    # Configuration
    db_dir = "./example_data"
    num_shards = 3  # 3 shards for better distribution
    
    try:
        # Step 1: Initialize
        print("1. Initializing Shardlite...")
        manager = initialize(
            num_shards=num_shards,
            db_dir=db_dir,
            auto_create_dirs=True
        )
        print("✅ Initialized successfully\n")
        
        # Step 2: Create sales table
        print("2. Creating 'sales' table...")
        create_table("sales", {
            "id": "INTEGER PRIMARY KEY",
            "product": "TEXT NOT NULL",
            "amount": "REAL NOT NULL",
            "region": "TEXT",
            "date": "TEXT"
        })
        print("✅ Table created\n")
        
        # Step 3: Insert sample sales data
        print("3. Inserting sales data...")
        sales_data = [
            {"product": "Laptop", "amount": 1200.00, "region": "North", "date": "2024-01-15"},
            {"product": "Mouse", "amount": 25.50, "region": "South", "date": "2024-01-16"},
            {"product": "Keyboard", "amount": 89.99, "region": "North", "date": "2024-01-17"},
            {"product": "Monitor", "amount": 299.99, "region": "East", "date": "2024-01-18"},
            {"product": "Headphones", "amount": 149.99, "region": "West", "date": "2024-01-19"},
            {"product": "Webcam", "amount": 79.99, "region": "North", "date": "2024-01-20"},
            {"product": "Microphone", "amount": 199.99, "region": "South", "date": "2024-01-21"},
            {"product": "Tablet", "amount": 399.99, "region": "East", "date": "2024-01-22"},
            {"product": "Speaker", "amount": 129.99, "region": "West", "date": "2024-01-23"},
            {"product": "Printer", "amount": 249.99, "region": "North", "date": "2024-01-24"}
        ]
        
        for i, sale in enumerate(sales_data):
            insert("sales", sale, i + 1)
            print(f"✅ Inserted: {sale['product']} (${sale['amount']})")
        print()
        
        # Step 4: Perform aggregations
        print("4. Performing aggregations...\n")
        
        # Count total sales
        total_count = aggregate("sales", "COUNT(*)")
        print(f"📊 Total sales: {total_count['COUNT(*)']}")
        
        # Sum total amount
        total_amount = aggregate("sales", "SUM(amount)")
        print(f"💰 Total revenue: ${total_amount['SUM(amount)']:.2f}")
        
        # Average sale amount
        avg_amount = aggregate("sales", "AVG(amount)")
        print(f"📈 Average sale: ${avg_amount['AVG(amount)']:.2f}")
        
        # Maximum sale
        max_amount = aggregate("sales", "MAX(amount)")
        print(f"🔥 Highest sale: ${max_amount['MAX(amount)']:.2f}")
        
        # Minimum sale
        min_amount = aggregate("sales", "MIN(amount)")
        print(f"❄️  Lowest sale: ${min_amount['MIN(amount)']:.2f}")
        
        print()
        
        # Step 5: Show shard distribution
        print("5. Shard distribution...")
        shard_info = manager.get_shard_info()
        for shard_id, info in shard_info.items():
            print(f"  Shard {shard_id}: {info['db_path']} ({info['size']} bytes)")
        
        print()
        print("🎉 Aggregations completed successfully!")
        print("\n💡 Note: These aggregations work across all shards automatically!")
        
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