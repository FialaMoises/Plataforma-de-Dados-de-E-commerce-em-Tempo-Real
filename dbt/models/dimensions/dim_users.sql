{{
  config(
    materialized='table',
    properties={
      "format": "'PARQUET'"
    }
  )
}}

/*
  User dimension (SCD Type 2 stub).

  Since the Silver layer currently only contains purchase events (no dedicated
  user/customer master feed), this model derives user profiles from distinct
  user_ids observed in purchases. Each user gets a single row with:
    - valid_from = first date the user made a purchase
    - valid_to   = NULL (currently active; future SCD loads will close old rows)
    - is_current = true

  When a CRM or user-profile source becomes available, this model should be
  replaced with a proper SCD Type 2 snapshot strategy.
*/

with user_stats as (
    select
        user_id,
        min(event_date)                                as first_purchase_date,
        max(event_date)                                as last_purchase_date,
        count(distinct event_id)                       as total_orders,
        cast(sum(quantity) as bigint)                   as total_units_bought,
        round(sum(gross_amount), 2)                     as total_spent,
        round(avg(gross_amount), 2)                     as avg_order_value,
        count(distinct product_id)                     as distinct_products,
        count(distinct currency)                       as currencies_used
    from {{ ref('stg_purchases') }}
    group by user_id
)

select
    user_id,
    first_purchase_date,
    last_purchase_date,
    total_orders,
    total_units_bought,
    total_spent,
    avg_order_value,
    distinct_products,
    currencies_used,
    -- SCD Type 2 columns
    cast(first_purchase_date as timestamp)  as valid_from,
    cast(null as timestamp)                 as valid_to,
    true                                    as is_current
from user_stats
