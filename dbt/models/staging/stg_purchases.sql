{{
  config(
    materialized='view'
  )
}}

/*
  Staging model for Silver purchases.

  This is a thin view on top of the Silver layer, serving as the single
  entry point for all downstream dbt models. Any column renaming, casting,
  or lightweight transformations should be applied here so that mart/fact
  models never reference the source directly.
*/

select
    event_id,
    user_id,
    product_id,
    quantity,
    unit_price,
    gross_amount,
    currency,
    event_ts,
    channel,
    event_date
from {{ source('silver', 'purchases') }}
