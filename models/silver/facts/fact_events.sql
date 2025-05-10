{{
    config(
        materialized='table',
        tags=['silver', 'fact']
    )
}}

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

-- Obtener la conversión (si el evento generó una orden)
order_info AS (
    SELECT
        user_id,
        product_id,
        order_id,
        created_at AS order_created_at
    FROM {{ ref('stg_orders') }}
    JOIN {{ ref('stg_order_items') }} USING (order_id)
),

final AS (
    SELECT
        -- Claves
        events.event_id,
        events.user_id,
        events.product_id,
        events.session_id,
        
        -- Información del evento
        events.event_type,
        events.page_url,
        events.created_at AS event_timestamp,
        
        -- Componentes de fecha/hora
        EXTRACT(YEAR FROM events.created_at) AS event_year,
        EXTRACT(MONTH FROM events.created_at) AS event_month,
        EXTRACT(DAY FROM events.created_at) AS event_day,
        EXTRACT(HOUR FROM events.created_at) AS event_hour,
        
        -- Clasificación de eventos para análisis
        CASE 
            WHEN events.event_type = 'page_view' THEN 'Navigation'
            WHEN events.event_type = 'add_to_cart' THEN 'Cart'
            WHEN events.event_type = 'checkout' THEN 'Checkout'
            WHEN events.event_type = 'purchase' THEN 'Purchase'
            ELSE 'Other'
        END AS event_category,
        
        -- Categorías de páginas basadas en URL
        CASE
            WHEN events.page_url LIKE '%/product/%' THEN 'Product Page'
            WHEN events.page_url LIKE '%/category/%' THEN 'Category Page'
            WHEN events.page_url LIKE '%/cart%' THEN 'Cart Page'
            WHEN events.page_url LIKE '%/checkout%' THEN 'Checkout Page'
            ELSE 'Other Page'
        END AS page_category,
        
        -- Conversión a orden (si existe)
        CASE WHEN order_info.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS led_to_purchase,
        order_info.order_id,
        order_info.order_created_at,
        
        -- Tiempo hasta conversión (en horas, si existe)
        CASE 
            WHEN order_info.order_created_at IS NOT NULL 
            THEN DATEDIFF('hour', events.created_at, order_info.order_created_at)
            ELSE NULL
        END AS hours_to_purchase,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM events
    LEFT JOIN order_info
        ON events.user_id = order_info.user_id
        AND events.product_id = order_info.product_id
        AND events.created_at <= order_info.order_created_at
)

SELECT * FROM final