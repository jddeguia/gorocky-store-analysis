{{
    config(
      materialized='table'
    )
}}

WITH base AS (
    SELECT 
        product_id,
        segment,
        category,
        sub_category,
        product_name,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY order_date DESC) AS row_number
    FROM {{ref('stg_orders')}}
)

SELECT *
FROM base
WHERE row_number = 1
