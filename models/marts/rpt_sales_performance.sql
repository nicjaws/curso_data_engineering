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
 */

WITH orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('gold_fact_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('gold_dim_products') }}
),

-- Análisis de ventas diarias para tendencias a corto plazo
daily_sales AS (
    SELECT
        DATE_TRUNC('day', orders.order_date) AS date,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS unique_customers,
        SUM(order_total) AS total_sales,
        SUM(shipping_cost) AS total_shipping,
        AVG(order_total) AS avg_order_value
    FROM orders
    GROUP BY DATE_TRUNC('day', orders.order_date)
),

-- Análisis de ventas por producto para evaluar el rendimiento del catálogo
product_sales AS (
    SELECT
        order_items.product_id,
        products.name AS product_name,
        COUNT(DISTINCT order_items.order_id) AS times_ordered,
        SUM(order_items.quantity) AS units_sold,
        SUM(order_items.item_total) AS total_product_revenue,
        AVG(order_items.quantity) AS avg_qty_per_order
    FROM order_items
    JOIN products 
        ON order_items.product_id = products.product_id
    GROUP BY order_items.product_id, products.name
),

-- Rendimiento por estado de orden para analizar la eficiencia operativa
order_status_performance AS (
    SELECT 
        status,
        COUNT(*) AS order_count,
        SUM(order_total) AS total_value,
        AVG(delivery_duration_days) AS avg_delivery_days
    FROM orders
    GROUP BY status
),

-- Análisis de tendencias mensuales para evaluar el rendimiento a largo plazo
monthly_trends AS (
    SELECT
        DATE_TRUNC('month', orders.order_date) AS month,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(DISTINCT user_id) AS unique_customers,
        SUM(order_total) AS total_sales,
        SUM(order_total) / COUNT(DISTINCT user_id) AS revenue_per_customer
    FROM orders
    GROUP BY DATE_TRUNC('month', orders.order_date)
)

-- Unificamos todos los análisis en un solo modelo para facilitar el consumo
SELECT
    'daily_sales' AS report_type,
    date AS date_period,
    order_count,
    unique_customers,
    total_sales,
    total_shipping,
    avg_order_value,
    NULL AS product_id,
    NULL AS product_name,
    NULL AS units_sold,
    NULL AS status,
    NULL AS avg_delivery_days,
    NULL AS revenue_per_customer,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM daily_sales

UNION ALL

SELECT
    'product_sales' AS report_type,
    NULL AS date_period,
    times_ordered AS order_count,
    NULL AS unique_customers,
    total_product_revenue AS total_sales,
    NULL AS total_shipping,
    NULL AS avg_order_value,
    product_id,
    product_name,
    units_sold,
    NULL AS status,
    NULL AS avg_delivery_days,
    NULL AS revenue_per_customer,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM product_sales

UNION ALL

SELECT
    'order_status' AS report_type,
    NULL AS date_period,
    order_count,
    NULL AS unique_customers,
    total_value AS total_sales,
    NULL AS total_shipping,
    NULL AS avg_order_value,
    NULL AS product_id,
    NULL AS product_name,
    NULL AS units_sold,
    status,
    avg_delivery_days,
    NULL AS revenue_per_customer,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM order_status_performance

UNION ALL

SELECT
    'monthly_trends' AS report_type,
    month AS date_period,
    order_count,
    unique_customers,
    total_sales,
    NULL AS total_shipping,
    NULL AS avg_order_value,
    NULL AS product_id,
    NULL AS product_name,
    NULL AS units_sold,
    NULL AS status,
    NULL AS avg_delivery_days,
    revenue_per_customer,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM monthly_trends