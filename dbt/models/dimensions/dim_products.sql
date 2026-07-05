{{
  config(
    materialized='table',
    properties={
      "format": "'PARQUET'"
    }
  )
}}

/*
  Product dimension (SCD Type 2 stub).

  Since the Silver layer currently only contains purchase events (no dedicated
  product master feed), this model derives the product catalog from distinct
  product_ids observed in purchases. Each product gets a single row with:
    - valid_from = first date the product appeared in a purchase
    - valid_to   = NULL (currently active; future SCD loads will close old rows)
    - is_current = true

  When a product master source becomes available, this model should be replaced
  with a proper SCD Type 2 snapshot strategy.
*/

with product_stats as (
    select
        product_id,
        min(event_date)                as first_seen_date,
        max(event_date)                as last_seen_date,
        count(distinct event_id)       as total_orders,
        cast(sum(quantity) as bigint)   as total_units_sold,
        round(sum(gross_amount), 2)     as total_revenue,
        round(avg(unit_price), 2)       as avg_unit_price
    from {{ ref('stg_purchases') }}
    group by product_id
)

select
    product_id,
    first_seen_date,
    last_seen_date,
    total_orders,
    total_units_sold,
    total_revenue,
    avg_unit_price,
    -- SCD Type 2 columns
    cast(first_seen_date as timestamp) as valid_from,
    cast(null as timestamp)            as valid_to,
    true                               as is_current
from product_stats
