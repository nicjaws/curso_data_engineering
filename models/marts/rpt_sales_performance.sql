{{
    config(
        materialized='table',
        tags=['gold', 'report', 'sales']
    )
}}

/* 
 * Este modelo consolida el análisis de ventas para múltiples dimensiones:
 * - Ventas diarias (tendencias a corto plazo)
 * - Ventas por producto (rendimiento de catálogo)
 * - Rendimiento por estado de orden (eficiencia operativa)
 * - Tendencias mensuales (rendimiento a largo plazo)
 *
 * Cada dimensión se crea como una vista materializada independiente
 * para evitar problemas con valores NULL y optimizar consultas.
 */

-- Crear cuatro modelos separados en lugar de un único modelo con UNION ALL

-- 1. Análisis de ventas diarias para tendencias a corto plazo
WITH orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
)

SELECT
    'daily_sales' AS report_type,
    DATE_TRUNC('day', orders.created_at) AS date_period,
    COUNT(DISTINCT order_id) AS order_count,
    COUNT(DISTINCT user_id) AS unique_customers,
    SUM(order_total) AS total_sales,
    SUM(shipping_cost) AS total_shipping,
    AVG(order_total) AS avg_order_value,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM orders
GROUP BY DATE_TRUNC('day', orders.created_at)