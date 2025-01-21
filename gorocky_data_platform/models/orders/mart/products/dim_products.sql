{{
    config(
      materialized='table'
    )
}}

WITH base AS (
    SELECT DISTINCT
        product_id,
        segment,
        category,
        sub_category,
        product_name
    FROM {{ref('stg_orders')}}
)

SELECT * FROM base