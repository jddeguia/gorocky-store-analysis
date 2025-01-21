{{
    config(
      materialized='table'
    )
}}

WITH base AS (
    SELECT DISTINCT
        customer_id,
        customer_name,
        city,
        state,
        postal_code,
        region
    FROM {{ref('stg_orders')}}
)

SELECT * FROM base