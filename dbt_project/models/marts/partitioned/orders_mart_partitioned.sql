{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by='order_date',
        file_format='delta',
        tags=['marts', 'partitioned', 'daily']
    )
}}

/*
  Daily orders aggregation with PARTITION BY insert_overwrite strategy

  Key features:
  - Partitioned by order_date for efficient querying
  - Uses insert_overwrite to replace entire partitions
  - Ideal for daily batch processing where you want to reprocess specific dates

  How insert_overwrite works with partitions:
  - Only partitions matching the incremental filter are overwritten
  - Other partitions remain untouched
  - Perfect for idempotent daily runs
*/

with daily_orders as (
    select
        order_date,
        count(distinct order_id) as total_orders,
        count(distinct customer_id) as unique_customers,
        count(distinct product_id) as unique_products,
        sum(quantity) as total_quantity,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        min(total_amount) as min_order_value,
        max(total_amount) as max_order_value,

        -- Status breakdown
        count(case when status = 'confirmed' then 1 end) as confirmed_orders,
        count(case when status = 'shipped' then 1 end) as shipped_orders,
        count(case when status = 'delivered' then 1 end) as delivered_orders,
        count(case when status = 'cancelled' then 1 end) as cancelled_orders,

        -- Data quality metrics
        count(case when has_data_quality_issue then 1 end) as orders_with_issues,

        -- Metadata
        current_timestamp() as last_updated_at

    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- Dynamic Partition Overwrite Strategy:
        -- Process ALL source data, let insert_overwrite detect which partitions have changed
        -- This is the true power of insert_overwrite - it's completely data-driven!
        --
        -- How it works:
        -- 1. Read all source data and aggregate by order_date
        -- 2. insert_overwrite detects which order_date partitions are in the result
        -- 3. Only those partitions are overwritten in the target table
        -- 4. All other partitions remain unchanged
        --
        -- This approach:
        -- ✅ Handles late-arriving data regardless of when it arrived
        -- ✅ No assumptions about data freshness or job frequency
        -- ✅ Idempotent - running multiple times produces same result
        -- ✅ True dynamic partition detection
        --
        -- Note: We don't filter here - we let the partition-based table layout
        -- handle efficiency. For very large tables (100M+ rows), consider adding
        -- a filter based on your specific use case (updated_at, CDC flags, etc.)
    {% endif %}

    group by order_date
)

select * from daily_orders
