{{
    config(
        materialized='table',
        tags=['gold', 'report', 'sales', 'monthly']
    )
}}

/* 
 * Análisis de tendencias mensuales para evaluar el rendimiento a largo plazo
 */

WITH orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
)

SELECT
    'monthly_trends' AS report_type,
    DATE_TRUNC('month', orders.created_at) AS month,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(order_total) AS total_sales,
    SUM(order_total) / COUNT(DISTINCT user_id) AS revenue_per_customer,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM orders
GROUP BY DATE_TRUNC('month', orders.created_at)