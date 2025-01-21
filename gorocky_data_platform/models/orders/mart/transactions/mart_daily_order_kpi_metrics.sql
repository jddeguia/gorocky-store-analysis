WITH order_data AS (
    SELECT
        order_date,
        SUM(sales) AS total_sales,
        SUM(quantity) AS total_quantity,
        SUM(profit) AS total_profit,
        SUM(cost) AS total_cost,
        SUM(sales * discount) AS total_discount,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM {{ ref('mart_fct_order_transactions') }}
    GROUP BY order_date
),

fulfillment_rate AS (
    SELECT
        order_date,
        COUNT(CASE WHEN DATEDIFF('DAY', CAST(order_date AS DATE), CAST(ship_date AS DATE)) <= 3 THEN 1 END) * 1.0 / COUNT(*) AS fulfillment_rate
    FROM {{ ref('mart_fct_order_transactions') }}
    GROUP BY order_date
),

monthly_sales_and_profit AS (
    SELECT
        DATE_TRUNC('MONTH', order_date) AS order_month,
        SUM(sales) AS total_sales,
        SUM(profit) AS total_profit
    FROM {{ ref('mart_fct_order_transactions') }}
    GROUP BY DATE_TRUNC('MONTH', order_date)
),

sales_and_profit_growth AS (
    SELECT
        order_month,
        LAG(total_sales) OVER (ORDER BY order_month) AS previous_month_sales,
        LAG(total_profit) OVER (ORDER BY order_month) AS previous_month_profit,
        -- Sales Growth with Capping
        CASE 
            WHEN LAG(total_sales) OVER (ORDER BY order_month) = 0 THEN NULL
            WHEN ((total_sales - LAG(total_sales) OVER (ORDER BY order_month)) 
                  / LAG(total_sales) OVER (ORDER BY order_month) * 100) > 100 THEN 100
            ELSE (total_sales - LAG(total_sales) OVER (ORDER BY order_month)) 
                 / LAG(total_sales) OVER (ORDER BY order_month) * 100
        END AS sales_growth_percentage,
        -- Profit Growth with Capping
        CASE 
            WHEN LAG(total_profit) OVER (ORDER BY order_month) = 0 THEN NULL
            WHEN ((total_profit - LAG(total_profit) OVER (ORDER BY order_month)) 
                  / LAG(total_profit) OVER (ORDER BY order_month) * 100) > 100 THEN 100
            ELSE (total_profit - LAG(total_profit) OVER (ORDER BY order_month)) 
                 / LAG(total_profit) OVER (ORDER BY order_month) * 100
        END AS profit_growth_percentage
    FROM monthly_sales_and_profit
)

SELECT
    o.order_date,
    o.total_sales,
    o.total_quantity,
    o.total_profit,
    o.total_cost,
    o.total_discount,
    o.total_orders,
    o.total_customers,
    f.fulfillment_rate,
    g.sales_growth_percentage,
    g.profit_growth_percentage
FROM order_data o
LEFT JOIN fulfillment_rate f ON o.order_date = f.order_date
LEFT JOIN sales_and_profit_growth g 
    ON DATE_TRUNC('MONTH', o.order_date) = g.order_month
ORDER BY o.order_date
