# insert_overwrite Strategy - Usage Examples

This document demonstrates how to use the `insert_overwrite` strategy to selectively overwrite specific date partitions.

## Date Range Strategy

**Source Data**: Spans from **first day of previous month** to **today**
- Example: If today is Nov 27, 2025, data spans Oct 1, 2025 to Nov 27, 2025 (58 days)
- **REALISTIC distribution** mimicking real-world patterns:
  - **60% uniform**: All dates fairly represented (~345 orders/day for 20K total)
  - **25% recency bias**: Last 2 weeks have slightly more orders (~400-500 orders/day)
  - **15% mid-period**: Middle of date range has slight bump (~360-380 orders/day)
  - **Natural daily variability**: Each partition has different size (200-600 orders/day range)
  - **Business hours patterns**: Lunch peak (25%), evening peak (20%)
  - **Expected average**: ~345 orders/day (20,000 orders ÷ 58 days)

## How Dynamic Partition Overwrites Work

This project demonstrates **Dynamic Partition Overwrites** using dbt's `insert_overwrite` incremental strategy.

### Implementation Details

**dbt Strategy**: We use [dbt's insert_overwrite](https://docs.getdbt.com/docs/build/incremental-strategy#insert_overwrite) which:
- Translates to `INSERT OVERWRITE` or `REPLACE WHERE` SQL statements
- Works with Databricks SQL (runs on SQL Warehouse)
- **Does NOT** use `spark.sql.sources.partitionOverwriteMode=dynamic` (Spark config not supported in Databricks SQL)
- Implements [Databricks Dynamic Partition Overwrites](https://docs.databricks.com/aws/en/delta/selective-overwrite#dynamic-partition-overwrites-with-replace-using) at SQL level

### CRITICAL: --full-refresh with insert_overwrite

**`--full-refresh` does NOT mean "replace all data" with insert_overwrite!**

- ✅ `--full-refresh` just tells dbt to skip incremental checks (bypasses slow information_schema queries)
- ✅ `insert_overwrite` strategy remains **data-driven** and **selective**
- ✅ Only partitions with data in the result get overwritten
- ✅ **This is the recommended approach** - faster and more reliable!

### Key Concept: Automatic Detection (The True Power!)
- ✅ Process **ALL source data** and aggregate by partition key (e.g., `order_date`)
- ✅ `insert_overwrite` **automatically detects** which `order_date` partitions/values exist in the result
- ✅ Overwrites **ONLY those partitions** - no filters, no dates, completely data-driven!
- ✅ This is the true USP of dynamic partition overwrites - let the data determine what gets overwritten
- ✅ Using `--full-refresh` makes this faster by skipping unnecessary metadata checks

### Partitioned Tables (`orders_mart_partitioned`)
- **Partition-level overwrites**: Replaces entire `order_date` partitions
- Aggregates ALL source data by `order_date`
- `insert_overwrite` automatically detects which partitions are in the result
- Example: Source has data for 3 dates → only those 3 partitions overwritten, rest untouched

### Liquid Clustered Tables (`orders_mart_liquid`)
- **Predicate-based overwrites**: Replaces rows matching `order_date` values
- Aggregates ALL source data by `customer_id, order_date`
- `insert_overwrite` automatically overwrites rows with affected `order_date` values
- No explicit partitions, but same efficient result!

## Usage Examples

### Example 1: Run Complete Workflow (RECOMMENDED)

```bash
# Run the complete workflow with default settings
databricks bundle run dbt_workflow --target dev
```

**What happens:**
- Runs all 5 steps in sequence (setup → dbt → simulate → dbt → verify)
- Processes **ALL source data** and aggregates by `order_date`
- `insert_overwrite` automatically detects which `order_date` partitions exist in the result
- Overwrites ONLY those partitions (completely data-driven, no filters!)
- ✅ **Complete end-to-end demo in one command!**

### Example 2: Custom Data Volume

```bash
# Generate 50,000 orders instead of default 20,000
databricks bundle run dbt_workflow --target dev --params num_orders=50000
```

**Use case:** Test with larger data volumes. Late-arriving order counts are automatically randomized (20-100 per date) to simulate realistic scenarios.

### Example 3: Run Only Partitioned Model

```bash
# Run only the partitioned model
dbt run --select orders_mart_partitioned
```

### Example 4: Run Only Liquid Clustered Model

```bash
# Run only the liquid clustered model
dbt run --select orders_mart_liquid
```

## Local dbt Development (Optional)

If testing locally (requires dbt-databricks installed):

```bash
cd dbt_project

# Set environment variables
export DBT_CATALOG="main"
export DBT_SCHEMA="your_schema"
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/abc123def4567890"
export DATABRICKS_TOKEN="<your-token>"

# Run partitioned model with insert_overwrite (processes ALL source data)
dbt run --select orders_mart_partitioned

# Run liquid clustered model
dbt run --select orders_mart_liquid

# Run both models
dbt run --select marts.*

# Full refresh (bypass incremental logic)
dbt run --select marts.* --full-refresh
```

## Verification Queries

After running insert_overwrite, verify the results:

```sql
-- Check partition counts (partitioned table)
SELECT
    order_date,
    total_orders,
    unique_customers,
    total_revenue,
    last_updated_at
FROM <catalog>.<schema>.orders_mart_partitioned
ORDER BY order_date DESC
LIMIT 30;

-- Check which dates were recently updated
SELECT
    order_date,
    last_updated_at,
    DATEDIFF(CURRENT_TIMESTAMP(), last_updated_at) as hours_since_update
FROM <catalog>.<schema>.orders_mart_partitioned
WHERE last_updated_at >= CURRENT_DATE()
ORDER BY order_date;

-- Check liquid clustered table (customer-level aggregations by date)
SELECT
    customer_id,
    order_date,
    total_orders,
    total_spent,
    customer_segment,
    engagement_level,
    last_updated_at
FROM <catalog>.<schema>.orders_mart_liquid
WHERE last_updated_at >= CURRENT_DATE()
ORDER BY order_date DESC, customer_id
LIMIT 50;
```

## Understanding the Results

### What Gets Overwritten

**Partitioned Table** (orders_mart_partitioned):
- ✅ **Entire partitions** for dates in your filter range
- ✅ All rows in those partitions are replaced
- ❌ Partitions outside the range are untouched

**Liquid Clustered Table** (orders_mart_liquid):
- ✅ Rows matching the date range predicate are replaced
- ✅ Customer data aggregated by order_date
- ❌ Data outside the date range is untouched

### Example Scenario

Source data: Oct 1, 2025 - Nov 27, 2025 (58 days, ~20,000 orders)

**Initial Workflow Run**:
```bash
databricks bundle run dbt_workflow --target dev
# What happens:
# 1. Setup: Creates source tables with 20K orders (spanning ~32-61 days)
# 2. Initial dbt build with --full-refresh: Creates mart tables with full load
# 3. Simulate: Adds 50 late-arriving orders to 3 auto-calculated dates
# 4. dbt build --full-refresh: Processes ALL source data (fast, no information_schema queries!)
#    - --full-refresh flag bypasses incremental checks
#    - insert_overwrite strategy STILL only overwrites partitions with data in result
#    - ONLY overwrites the 3 affected partitions (not all 32-61 days!)
# 5. Verify: Confirms only target dates were refreshed
#
# Result:
#   - Partitioned: ONLY 3 partitions overwritten (auto-detected dates)
#   - Liquid: Customer aggregations recalculated for those 3 dates only
#   - All other dates (29-58 partitions): UNCHANGED
#   - Perfect demonstration of data-driven partition detection!
#   - Fast execution (no slow information_schema queries!)
```

## Complete Demo Workflow: Late-Arriving Data

This workflow demonstrates the real-world value of `insert_overwrite` by simulating late-arriving orders.

### Option 1: Run Complete Workflow (Recommended - One Command!)

```bash
# Run the complete dbt insert_overwrite demo workflow
# This single job orchestrates: setup → dbt → simulate → dbt → verify
databricks bundle run dbt_workflow --target dev
```

This consolidated workflow automatically runs all 5 steps:

1. **Setup (02a)**: Creates source tables and generates 20,000 sample orders
2. **Initial dbt build with --full-refresh**: Creates mart tables with full load (all partitions)
3. **Simulate late arrivals (02b)**: Appends random new orders (20-100 per date) to randomly selected dates (demonstrates out-of-sync scenario)
4. **dbt build with --full-refresh**: Bypasses information_schema queries, but insert_overwrite STILL only updates affected partitions!
5. **Verify sync (02c)**: Confirms synchronization and validates the demo

**Benefits:**
- ✅ End-to-end demonstration in one command
- ✅ All tasks orchestrated with proper dependencies
- ✅ Atomic workflow - succeeds or fails together
- ✅ Perfect for demos and testing

**Customization:**
```bash
# Customize parameters (optional)
databricks bundle run dbt_workflow --target dev \
  --params num_orders=50000
```
Note: Late-arriving order counts are automatically randomized (20-100 per date) for realistic scenarios.

### Option 2: Run Individual Notebooks (For Development/Testing)

If you need to run steps individually (e.g., for debugging or development), you can still execute notebooks directly in the workspace:

**Step 1: Initial Setup**
- Run notebook: `02a_setup_and_generate_data.ipynb`
- Creates source tables and generates data

**Step 2: Initial dbt Build**
- Run dbt locally or via workspace with `--full-refresh`
- Creates mart tables with full load

**Step 3: Simulate Late-Arriving Data**
- Run notebook: `02b_simulate_late_arrivals.ipynb`
- Adds new orders to randomly selected dates

**Step 4: Refresh with insert_overwrite**
- Run dbt again with `--full-refresh` (bypasses slow information_schema queries)
- insert_overwrite strategy automatically detects and overwrites ONLY affected partitions

**Step 5: Verify Synchronization**
- Run notebook: `02c_verify_sync.ipynb`
- Confirms mart tables are back in sync

### Step 5: Manual SQL Verification (Optional)

```sql
-- Check which dates were updated (look at last_updated_at)
SELECT
    order_date,
    total_orders,
    last_updated_at,
    DATEDIFF(CURRENT_TIMESTAMP(), last_updated_at) as minutes_since_update
FROM <catalog>.<schema>.orders_mart_partitioned
WHERE order_date IN ('2025-11-05', '2025-11-15', '2025-12-01')
ORDER BY order_date;

-- Compare source vs mart counts
SELECT
    'Source' as table_type,
    order_date,
    COUNT(*) as order_count
FROM <catalog>.<schema>.orders_partitioned
WHERE order_date IN ('2025-11-05', '2025-11-15', '2025-12-01')
GROUP BY order_date
UNION ALL
SELECT
    'Mart' as table_type,
    order_date,
    total_orders
FROM <catalog>.<schema>.orders_mart_partitioned
WHERE order_date IN ('2025-11-05', '2025-11-15', '2025-12-01')
ORDER BY order_date, table_type;
```

Expected result: Counts should match, and `last_updated_at` should be recent for only these 3 dates!

### Why This Matters

**Without insert_overwrite:**
- ❌ Would need to reprocess ALL 32-61 days of data
- ❌ Expensive and time-consuming
- ❌ Overwrites data that didn't change

**With insert_overwrite:**
- ✅ Process only 3 dates (97% less work!)
- ✅ Fast and efficient
- ✅ Surgical precision - only touch what changed

## Key Benefits

1. **Idempotent**: Running the same date range multiple times produces the same result
2. **Efficient**: Only rewrites data for specified dates, not entire table
3. **Flexible**: Can reprocess any date range (yesterday, last week, specific dates)
4. **Safe**: Other partitions/data remain untouched
5. **Real-world Ready**: Works with variable partition sizes (different row counts per date)
6. **Demonstrable**: Check `last_updated_at` to see which partitions were overwritten
7. **Cost-Effective**: Process only changed data, not entire datasets

## Common Use Cases

1. **Daily Incremental**: Overwrite yesterday's data every day
2. **Backfill**: Reprocess specific historical dates after bug fixes
3. **Weekly Refresh**: Overwrite last 7 days to catch late-arriving data
4. **Month-End Close**: Reprocess entire previous month after month-end adjustments
5. **Ad-Hoc Reprocessing**: Selectively reprocess any date range as needed
