{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

-- Cálculo de métricas adicionales
order_items AS (
    SELECT 
        order_id,
        COUNT(DISTINCT product_id) AS unique_products,
        SUM(quantity) AS total_items
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

final AS (
    SELECT
        -- Claves
        orders.order_id,
        orders.user_id,
        orders.address_id,
        orders.promo_id,
        
        -- Fechas
        orders.created_at,
        orders.estimated_delivery_at,
        orders.delivered_at,
        
        -- Extracción de componentes de fecha
        EXTRACT(YEAR FROM orders.created_at) AS order_year,
        EXTRACT(MONTH FROM orders.created_at) AS order_month,
        EXTRACT(DAY FROM orders.created_at) AS order_day,
        EXTRACT(DAYOFWEEK FROM orders.created_at) AS order_day_of_week,
        
        -- Métricas
        orders.order_cost,
        orders.shipping_cost,
        orders.order_total,
        
        -- Duración de entrega (en días)
        DATEDIFF('day', orders.created_at, COALESCE(orders.delivered_at, CURRENT_TIMESTAMP())) AS delivery_duration_days,
        
        -- Estado de la orden
        orders.status,
        CASE 
            WHEN orders.status = 'delivered' THEN TRUE
            ELSE FALSE
        END AS is_delivered,
        
        -- Información de envío
        orders.shipping_service,
        orders.tracking_id,
        
        -- Información agregada de items
        COALESCE(order_items.unique_products, 0) AS unique_products,
        COALESCE(order_items.total_items, 0) AS total_items,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM orders
    LEFT JOIN order_items 
        ON orders.order_id = order_items.order_id
)

SELECT * FROM final