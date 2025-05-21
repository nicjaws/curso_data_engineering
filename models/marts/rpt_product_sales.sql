{{
    config(
        materialized='table',
        tags=['gold', 'report', 'sales', 'product']
    )
}}

/* 
 * Análisis de ventas por producto para evaluar el rendimiento del catálogo
 */

WITH order_items AS (
    SELECT * FROM {{ ref('gold_fact_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('gold_dim_products') }}
)

SELECT
    'product_sales' AS report_type,
    order_items.product_id,
    products.name AS product_name,
    COUNT(DISTINCT order_items.order_id) AS order_count,
    SUM(order_items.quantity) AS units_sold,
    SUM(products.price * order_items.quantity) AS total_sales,
    AVG(order_items.quantity) AS avg_qty_per_order,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM order_items
JOIN products 
    ON order_items.product_id = products.product_id
GROUP BY order_items.product_id, products.name