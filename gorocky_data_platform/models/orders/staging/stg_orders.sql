{% set column_types = {
    'order_id': 'string',
    'ship_mode': 'string',
    'customer_id': 'string',
    'customer_name': 'string',
    'segment': 'string',
    'country': 'string',
    'city': 'string',
    'state': 'string',
    'postal_code': 'string',
    'region': 'string',
    'product_id': 'string',
    'category': 'string',
    'sub_category': 'string',
    'product_name': 'string',
    'quantity': 'integer',
    'sales': 'float',
    'discount': 'float',
    'profit': 'float',
    'order_date': 'date',
    'ship_date': 'date'
} %}

{% set columns = ['order_id', 'ship_mode', 'customer_id', 'customer_name', 
                  'segment', 'country', 'city', 'state', 'postal_code', 'region', 
                  'product_id', 'category', 'sub_category', 'product_name', 
                  'quantity', 'sales', 'discount', 'profit', 'order_date', 'ship_date'] %}

WITH order_base AS (
    SELECT * 
    FROM {{ ref('base_orders') }}
),

cast_data_type AS (
    SELECT
        {% for column in columns %}
            {% set data_type = column_types[column] %}
            {% if loop.index > 1 %}, {% endif %}
            {% if data_type == 'string' %}
                CAST({{ column }} AS VARCHAR) AS {{ column }}
            {% elif data_type == 'integer' %}
                CAST({{ column }} AS INT) AS {{ column }}
            {% elif data_type == 'float' %}
                CAST({{ column }} AS FLOAT) AS {{ column }}
            {% elif data_type == 'date' %}
                TRY_CAST(STRPTIME({{ column }}, '%m/%d/%Y') AS DATE) AS {{ column }}
            {% else %}
                {{ column }} AS {{ column }}
            {% endif %}
        {% endfor %}
    FROM order_base
)

SELECT * FROM cast_data_type
