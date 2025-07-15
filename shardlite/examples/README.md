# Shardlite Examples

This directory contains simple, understandable examples that demonstrate the key features of the Shardlite library.

## 📁 Example Files

### 1. `01_basic_crud.py` - Basic CRUD Operations
**What it shows:**
- Initialize Shardlite with 2 shards
- Create a products table
- Insert, query, update, and delete data
- Query by specific keys and conditions

**Run it:**
```bash
python shardlite/examples/01_basic_crud.py
```

**Key concepts:**
- Basic database operations
- Sharding key usage
- Cross-shard queries

---

### 2. `02_aggregations.py` - Aggregation Operations
**What it shows:**
- Initialize with 3 shards
- Create a sales table with sample data
- Perform COUNT, SUM, AVG, MAX, MIN operations
- Show how aggregations work across all shards

**Run it:**
```bash
python shardlite/examples/02_aggregations.py
```

**Key concepts:**
- Cross-shard aggregations
- Automatic result combination
- Data distribution visualization

---

### 3. `03_transactions.py` - Cross-Shard Transactions
**What it shows:**
- Initialize with transaction logging
- Create accounts table
- Perform money transfers between accounts on different shards
- Demonstrate transaction rollback on errors

**Run it:**
```bash
python shardlite/examples/03_transactions.py
```

**Key concepts:**
- Two-Phase Commit (2PC) protocol
- Transaction consistency
- Automatic rollback
- Transaction logging

---

### 4. `04_configuration.py` - Configuration Options
**What it shows:**
- Different ways to configure Shardlite
- Configuration validation
- YAML file configuration
- Configuration inspection

**Run it:**
```bash
python shardlite/examples/04_configuration.py
```

**Key concepts:**
- Multiple configuration methods
- Configuration validation
- Production-ready configuration

---

## 🚀 How to Run Examples

### Prerequisites
1. Install dependencies:
   ```bash
   pip install -r shardlite/requirements.txt
   ```

2. Make sure you're in the project root directory:
   ```bash
   cd /path/to/shard-lite
   ```

### Running Examples
Each example can be run independently:

```bash
# Run basic CRUD operations
python shardlite/examples/01_basic_crud.py

# Run aggregation examples
python shardlite/examples/02_aggregations.py

# Run transaction examples
python shardlite/examples/03_transactions.py

# Run configuration examples
python shardlite/examples/04_configuration.py
```

## 📊 Expected Output

Each example will:
1. ✅ Initialize Shardlite successfully
2. 📝 Create tables and insert sample data
3. 🔍 Demonstrate the specific feature
4. 📈 Show results and statistics
5. 🧹 Clean up resources

## 💡 Learning Path

**For beginners:**
1. Start with `01_basic_crud.py` to understand basic operations
2. Try `02_aggregations.py` to see cross-shard calculations
3. Explore `04_configuration.py` to understand setup options
4. Finally, try `03_transactions.py` for advanced features

**For experienced users:**
- All examples show real-world patterns
- Use them as templates for your own applications
- Modify configurations and data to match your needs

## 🔧 Customization

Each example is designed to be easily modified:

- **Change shard count**: Modify `num_shards` parameter
- **Use different data**: Replace sample data with your own
- **Add more operations**: Extend examples with additional features
- **Test error handling**: Modify examples to trigger errors

## 📝 Notes

- Examples create temporary data directories that are cleaned up automatically
- Each example is self-contained and can be run multiple times
- Error handling is demonstrated in each example
- Console output shows exactly what's happening at each step

## 🎯 Next Steps

After running these examples:
1. Try combining features from different examples
2. Experiment with different shard configurations
3. Build your own applications using these patterns
4. Check the main documentation for advanced features 