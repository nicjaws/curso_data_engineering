{{
    config(
        materialized='table',
        tags=['marts', 'fact']
    )
}}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

final AS (
    SELECT
        -- Crear una clave sustituta única para cada línea de pedido
        {{ dbt_utils.generate_surrogate_key(['order_items.order_id', 'order_items.product_id']) }} AS order_item_id,
        
        -- Claves foráneas
        order_items.order_id,
        order_items.product_id,
        orders.user_id,
        
        -- Información del pedido
        orders.created_at AS order_date,
        orders.status AS order_status,
        
        -- Métricas
        order_items.quantity,
        products.price AS unit_price,
        (order_items.quantity * products.price) AS item_total,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM order_items
    LEFT JOIN orders 
        ON order_items.order_id = orders.order_id
    LEFT JOIN products 
        ON order_items.product_id = products.product_id
)

SELECT * FROM final