{{
    config(
        materialized='view',
        tags=['staging', 'daily']
    )
}}

/*
  Staging model for orders
  - Applies basic transformations and cleaning
  - Calculates derived fields
  - Used as source for incremental models
*/

with source_data as (
    select *
    from {{ source('raw_sales', 'orders_partitioned') }}
),

transformed as (
    select
        order_id,
        customer_id,
        date(order_date) as order_date,
        product_id,
        quantity,
        unit_price,
        -- Calculate total if not provided
        coalesce(total_amount, quantity * unit_price) as total_amount,
        status,
        created_at,

        -- Add derived fields
        year(order_date) as order_year,
        month(order_date) as order_month,
        dayofweek(order_date) as order_day_of_week,

        -- Add data quality flags
        case
            when quantity <= 0 then true
            when unit_price <= 0 then true
            else false
        end as has_data_quality_issue,

        -- Current timestamp for processing tracking
        current_timestamp() as processed_at

    from source_data
    where order_date is not null  -- Filter out invalid dates
)

select * from transformed
