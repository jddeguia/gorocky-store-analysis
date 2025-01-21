{{
    config(
      materialized='table'
    )
}}

WITH base AS (
    SELECT 
        customer_id,
        customer_name,
        city,
        state,
        postal_code,
        region,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS row_number
    FROM {{ref('stg_orders')}}
)

SELECT * FROM base
WHERE row_number = 1