{{
    config(
      materialized='table'
    )
}}

{%- set columns = adapter.get_columns_in_relation(ref('superstore')) -%}

WITH change_columns AS (
    SELECT
        {%- for column in columns -%}
            {%- if not loop.first %} {% endif %}
            cast("{{ column.name }}" AS VARCHAR) AS "{{ column.name | lower | replace(' ', '_') | replace('-', '_') }}"
            {%- if not loop.last %}, {% endif %}
        {%- endfor %}
    FROM {{ ref('superstore') }}
)

SELECT *
FROM change_columns
