{{
    config(
        materialized='table',
        tags=['gold', 'report']
    )
}}

-- Este modelo crea un reporte sobre el comportamiento de los usuarios utilizando las tablas de la capa GOLD
-- Une datos de las dimensiones y hechos de la capa GOLD usando ref()
WITH users AS (
    -- Referencia al modelo gold_dim_users
    SELECT * FROM {{ ref('gold_dim_users') }}
),

orders AS (
    -- Referencia al modelo gold_fact_orders
    SELECT * FROM {{ ref('gold_fact_orders') }}
),

events AS (
    -- Referencia al modelo gold_fact_events
    SELECT * FROM {{ ref('gold_fact_events') }}
),

-- Análisis del comportamiento de compra de los usuarios
user_purchase_behavior AS (
    SELECT
        users.user_id,
        users.email,
        users.first_name,
        users.last_name,
        -- Usamos la tabla gold_fact_orders (órdenes) y sus columnas
        MIN(orders.created_at) AS first_purchase_date,
        MAX(orders.created_at) AS last_purchase_date,
        COUNT(DISTINCT orders.order_id) AS total_orders,
        SUM(orders.order_total) AS lifetime_value,
        AVG(orders.order_total) AS average_order_value,
        -- Evitamos división por cero para avg_basket_size
        SUM(orders.order_total) / NULLIF(COUNT(DISTINCT orders.order_id), 0) AS avg_basket_size,
        -- Calculamos la antigüedad del cliente en días
        DATEDIFF('day', MIN(orders.created_at), MAX(orders.created_at)) AS customer_tenure_days
    FROM users
    LEFT JOIN orders
        ON users.user_id = orders.user_id
    GROUP BY users.user_id, users.email, users.first_name, users.last_name
),

-- Recencia, Frecuencia, Valor Monetario (RFM)
user_rfm AS (
    SELECT
        user_id,
        -- Calculamos la recencia en días relativa a la fecha actual
        DATEDIFF('day', MAX(created_at), CURRENT_DATE()) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(order_total) AS monetary_value
    FROM orders
    GROUP BY user_id
),

-- Análisis de actividad en el sitio
user_site_activity AS (
    -- Esta CTE usa la CTE 'events', que referencia gold_fact_events
    SELECT
        user_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN event_id END) AS add_to_carts,
        COUNT(DISTINCT CASE WHEN event_type = 'checkout' THEN event_id END) AS checkouts,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) AS purchases
    FROM events
    WHERE user_id IS NOT NULL -- Filtramos eventos sin usuario
    GROUP BY user_id
),

-- Cálculo de tasas de conversión
user_conversion_rates AS (
    -- Esta CTE usa user_site_activity, derivada de la CTE 'events'
    SELECT
        user_id,
        total_events,
        page_views,
        add_to_carts,
        purchases,
        -- Calculamos la tasa de vista a compra, manejamos división por cero
        CASE
            WHEN page_views > 0 THEN ROUND((purchases::FLOAT / page_views) * 100, 2)
            ELSE 0
        END AS view_to_purchase_rate,
        CASE
            WHEN add_to_carts > 0 THEN ROUND((purchases::FLOAT / add_to_carts) * 100, 2)
            ELSE 0
        END AS cart_to_purchase_rate
    FROM user_site_activity
)

-- Unión final para combinar todas las métricas de comportamiento de usuario
SELECT
    upb.user_id,
    upb.email,
    upb.first_name,
    upb.last_name,
    upb.first_purchase_date,
    upb.last_purchase_date,
    upb.total_orders,
    upb.lifetime_value,
    upb.average_order_value,
    upb.avg_basket_size,
    upb.customer_tenure_days,

    -- Métricas RFM
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary_value,

    -- Segmentación RFM simplificada
    CASE
        WHEN rfm.recency_days <= 30 AND rfm.frequency >= 3 AND rfm.monetary_value >= 300 THEN 'Champions'
        WHEN rfm.recency_days <= 90 AND rfm.frequency >= 2 THEN 'Loyal Customers'
        WHEN rfm.recency_days > 90 AND rfm.frequency >= 1 THEN 'At Risk'
        WHEN rfm.recency_days <= 30 AND rfm.frequency = 1 THEN 'New Customers'
        ELSE 'Inactive'
    END AS customer_segment,

    -- Métricas de actividad en el sitio
    usa.total_events,
    usa.total_sessions,
    usa.page_views,
    usa.add_to_carts,
    usa.checkouts,
    usa.purchases,

    -- Tasas de conversión
    ucr.view_to_purchase_rate,
    ucr.cart_to_purchase_rate
FROM user_purchase_behavior upb
LEFT JOIN user_rfm rfm ON upb.user_id = rfm.user_id
LEFT JOIN user_site_activity usa ON upb.user_id = usa.user_id
LEFT JOIN user_conversion_rates ucr ON upb.user_id = ucr.user_id