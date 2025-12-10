{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        liquid_clustered_by='order_date',
        file_format='delta',
        table_properties={
            'delta.enableChangeDataFeed': 'true',
            'delta.autoOptimize.optimizeWrite': 'true',
            'delta.autoOptimize.autoCompact': 'true'
        },
        tags=['marts', 'liquid_clustered', 'daily']
    )
}}

/*
  Daily orders aggregation with LIQUID CLUSTERING insert_overwrite strategy

  Key features:
  - Liquid clustered by order_date (no traditional partitions)
  - Uses insert_overwrite with date-based predicates
  - Automatically optimizes data layout for date queries
  - Demonstrates liquid clustering as alternative to partitioning

  Comparison with orders_mart_partitioned:
  - Same data, same organization (by order_date)
  - Different storage mechanism (liquid clustering vs explicit partitions)
  - Both support efficient date-based insert_overwrite

  How insert_overwrite works with liquid clustering:
  - Uses a WHERE predicate to determine which rows to overwrite (by order_date)
  - Databricks automatically handles data organization
  - More flexible than traditional partitioning
  - No partition metadata overhead
  - Still efficient for date-based filtering and overwrites
*/

-- Source data from liquid clustered table
with source_orders as (
    select
        order_id,
        customer_id,
        order_date,
        product_id,
        quantity,
        unit_price,
        -- Calculate total_amount if not present
        coalesce(total_amount, quantity * unit_price) as total_amount,
        status,
        created_at
    from {{ source('raw_sales', 'orders_liquid') }}
),

customer_orders as (
    select
        customer_id,
        order_date,
        count(distinct order_id) as total_orders,
        count(distinct product_id) as unique_products_purchased,
        sum(quantity) as total_quantity,
        sum(total_amount) as total_spent,
        avg(total_amount) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,

        -- Calculate customer lifetime metrics
        datediff(max(order_date), min(order_date)) as customer_tenure_days,

        -- Order status breakdown
        count(case when status = 'delivered' then 1 end) as delivered_orders,
        count(case when status = 'cancelled' then 1 end) as cancelled_orders,

        -- Calculate fulfillment rate
        count(case when status = 'delivered' then 1 end) * 100.0 /
            nullif(count(order_id), 0) as fulfillment_rate_pct,

        -- Data quality tracking
        count(case when quantity <= 0 or unit_price <= 0 then 1 end) as orders_with_issues,

        -- Metadata
        current_timestamp() as last_updated_at

    from source_orders

    {% if is_incremental() %}
        -- Dynamic Overwrite Strategy for Liquid Clustering:
        -- Process ALL source data, let insert_overwrite detect which rows to update
        -- This is the true power of insert_overwrite with liquid clustering!
        --
        -- How it works:
        -- 1. Read all source data and aggregate by customer_id, order_date
        -- 2. insert_overwrite detects which (customer_id, order_date) combinations exist
        -- 3. Only rows matching those predicates are overwritten in the target table
        -- 4. All other rows remain unchanged
        --
        -- This approach:
        -- ✅ Handles late-arriving data regardless of when it arrived
        -- ✅ Leverages liquid clustering for efficient data layout
        -- ✅ Idempotent - running multiple times produces same result
        -- ✅ True predicate-based overwrite (not full table replacement)
    {% endif %}

    group by customer_id, order_date
),

customer_summary as (
    select
        customer_id,
        order_date,
        total_orders,
        unique_products_purchased,
        total_quantity,
        total_spent,
        avg_order_value,
        first_order_date,
        last_order_date,
        customer_tenure_days,
        delivered_orders,
        cancelled_orders,
        fulfillment_rate_pct,
        orders_with_issues,
        last_updated_at,

        -- Add customer segmentation
        case
            when total_spent >= 1000 then 'high_value'
            when total_spent >= 500 then 'medium_value'
            when total_spent >= 100 then 'low_value'
            else 'minimal'
        end as customer_segment,

        -- Calculate recency
        datediff(current_date(), last_order_date) as days_since_last_order,

        -- Engagement level
        case
            when total_orders >= 10 then 'highly_engaged'
            when total_orders >= 5 then 'moderately_engaged'
            when total_orders >= 2 then 'occasionally_engaged'
            else 'new_customer'
        end as engagement_level

    from customer_orders
)

select * from customer_summary
