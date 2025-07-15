#!/usr/bin/env python3
"""
Cross-Shard Transactions Example

This example shows how to use transactions across multiple shards:
1. Transfer money between accounts on different shards
2. Ensure data consistency with 2PC protocol
3. Handle transaction rollback on errors
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from shardlite.shardliteCore.api import (
    initialize, insert, select, update, transaction, create_table, shutdown
)
from shardlite.shardliteCore.transaction.logger import ConsoleTransactionLogger


def main():
    print("=== Cross-Shard Transactions Example ===\n")
    
    # Configuration
    db_dir = "./example_data"
    num_shards = 4  # 4 shards for better distribution
    
    try:
        # Step 1: Initialize with transaction logging
        print("1. Initializing Shardlite with transaction logging...")
        logger = ConsoleTransactionLogger(verbose=True)
        manager = initialize(
            num_shards=num_shards,
            db_dir=db_dir,
            logger=logger,
            auto_create_dirs=True
        )
        print("✅ Initialized successfully\n")
        
        # Step 2: Create accounts table
        print("2. Creating 'accounts' table...")
        create_table("accounts", {
            "id": "INTEGER PRIMARY KEY",
            "name": "TEXT NOT NULL",
            "balance": "REAL NOT NULL DEFAULT 0.0",
            "email": "TEXT"
        })
        print("✅ Table created\n")
        
        # Step 3: Create sample accounts
        print("3. Creating sample accounts...")
        accounts = [
            {"name": "Alice", "balance": 1000.00, "email": "alice@example.com"},
            {"name": "Bob", "balance": 500.00, "email": "bob@example.com"},
            {"name": "Charlie", "balance": 750.00, "email": "charlie@example.com"},
            {"name": "Diana", "balance": 1200.00, "email": "diana@example.com"}
        ]
        
        for i, account in enumerate(accounts):
            insert("accounts", account, i + 1)
            print(f"✅ Created account: {account['name']} (${account['balance']})")
        print()
        
        # Step 4: Show initial balances
        print("4. Initial account balances:")
        all_accounts = select("accounts")
        for account in all_accounts:
            print(f"  {account['name']}: ${account['balance']:.2f}")
        print()
        
        # Step 5: Perform a cross-shard transfer
        print("5. Performing cross-shard transfer...")
        print("   Transferring $200 from Alice (ID: 1) to Bob (ID: 2)")
        
        # Use transaction context manager
        with transaction([1, 2]) as tx:
            # Get current balances
            alice = select("accounts", key=1)[0]
            bob = select("accounts", key=2)[0]
            
            print(f"   Before: Alice=${alice['balance']:.2f}, Bob=${bob['balance']:.2f}")
            
            # Update balances
            update("accounts", {"balance": alice['balance'] - 200}, where={"id": 1}, key=1)
            update("accounts", {"balance": bob['balance'] + 200}, where={"id": 2}, key=2)
            
            print("   ✅ Transaction committed successfully!")
        
        # Step 6: Verify transfer
        print("\n6. Verifying transfer results:")
        all_accounts = select("accounts")
        for account in all_accounts:
            print(f"  {account['name']}: ${account['balance']:.2f}")
        print()
        
        # Step 7: Demonstrate transaction rollback
        print("7. Demonstrating transaction rollback...")
        print("   Attempting invalid transfer (insufficient funds)")
        
        try:
            with transaction([2, 3]) as tx:
                # Try to transfer more than Bob has
                bob = select("accounts", key=2)[0]
                charlie = select("accounts", key=3)[0]
                
                print(f"   Before: Bob=${bob['balance']:.2f}, Charlie=${charlie['balance']:.2f}")
                print(f"   Attempting to transfer ${bob['balance'] + 1000} from Bob (will fail)")
                
                # This will cause an error (negative balance)
                update("accounts", {"balance": bob['balance'] - (bob['balance'] + 1000)}, where={"id": 2}, key=2)
                update("accounts", {"balance": charlie['balance'] + (bob['balance'] + 1000)}, where={"id": 3}, key=3)
                
        except Exception as e:
            print(f"   ❌ Transaction failed: {e}")
            print("   ✅ Transaction was rolled back automatically!")
        
        # Step 8: Verify no changes occurred
        print("\n8. Verifying no changes occurred:")
        all_accounts = select("accounts")
        for account in all_accounts:
            print(f"  {account['name']}: ${account['balance']:.2f}")
        
        print()
        print("🎉 Transaction example completed successfully!")
        print("\n💡 Key points:")
        print("   - Transactions ensure data consistency across shards")
        print("   - Failed transactions are automatically rolled back")
        print("   - 2PC protocol ensures all-or-nothing execution")
        
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