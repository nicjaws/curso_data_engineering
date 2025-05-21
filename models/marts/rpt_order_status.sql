{{
    config(
        materialized='table',
        tags=['gold', 'report', 'sales', 'status']
    )
}}

/* 
 * Rendimiento por estado de orden para analizar la eficiencia operativa
 */

WITH orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
)

SELECT 
    'order_status' AS report_type,
    status,
    COUNT(*) AS order_count,
    SUM(order_total) AS total_sales,
    AVG(DATEDIFF('day', created_at, delivered_at)) AS avg_delivery_days,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM orders
WHERE delivered_at IS NOT NULL
GROUP BY status