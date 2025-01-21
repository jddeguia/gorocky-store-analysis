{{
    config(
      materialized='table'
    )
}}

WITH base AS (
    SELECT
      order_date,
      ship_date,
      order_id,
      ship_mode,
      customer_id,
      product_id,
      sales,
      quantity,
      discount,
      profit,
      (sales - profit) / (1 - discount) AS cost
    FROM {{ref('stg_orders')}}
)

SELECT * FROM base