{{
    config(
        materialized='table',
        tags=['gold', 'report']
    )
}}

-- This model creates a report on user behavior in the GOLD layer (marts)
-- It joins data from the SILVER layer dimensions and facts using ref()
WITH users AS (
    -- Reference the SILVER dim_users model
    SELECT * FROM {{ ref('silver_dim_users') }}
),

orders AS (
    -- Reference the SILVER fact_orders model
    -- The column containing the order timestamp in silver_fact_orders is 'order_date'
    SELECT * FROM {{ ref('silver_fact_orders') }}
),

events AS (
    -- Reference the SILVER fact_events model
    SELECT * FROM {{ ref('silver_fact_events') }}
),

-- User purchase behavior analysis
user_purchase_behavior AS (
    SELECT
        users.user_id,
        users.email,
        users.first_name,
        users.last_name,
        -- Use orders.order_date instead of orders.created_at
        MIN(orders.order_date) AS first_purchase_date,
        MAX(orders.order_date) AS last_purchase_date,
        COUNT(DISTINCT orders.order_id) AS total_orders,
        SUM(orders.order_total) AS lifetime_value,
        AVG(orders.order_total) AS average_order_value,
        -- Ensure no division by zero for avg_basket_size
        SUM(orders.order_total) / NULLIF(COUNT(DISTINCT orders.order_id), 0) AS avg_basket_size,
        -- Calculate customer tenure in days using orders.order_date
        DATEDIFF('day', MIN(orders.order_date), MAX(orders.order_date)) AS customer_tenure_days
    FROM users
    LEFT JOIN orders
        ON users.user_id = orders.user_id
    GROUP BY users.user_id, users.email, users.first_name, users.last_name
),

-- Recency, Frequency, Monetary Value (RFM)
user_rfm AS (
    SELECT
        user_id,
        -- Calculate recency in days relative to the current date using order_date
        DATEDIFF('day', MAX(order_date), CURRENT_DATE()) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(order_total) AS monetary_value
    FROM orders
    GROUP BY user_id
),

-- User site activity analysis
user_site_activity AS (
    -- This CTE uses the 'events' CTE, which references silver_fact_events.
    -- Ensure silver_fact_events has the necessary columns (event_type, session_id, event_id, led_to_purchase).
    SELECT
        user_id,
        COUNT(*) AS total_events,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT CASE WHEN event_type = 'page_view' THEN event_id END) AS page_views,
        COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN event_id END) AS add_to_carts,
        COUNT(DISTINCT CASE WHEN event_type = 'checkout' THEN event_id END) AS checkouts,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN event_id END) AS purchases,
        COUNT(DISTINCT CASE WHEN led_to_purchase = TRUE THEN event_id END) AS conversion_events
    FROM events
    WHERE user_id IS NOT NULL -- Filter out events without a user
    GROUP BY user_id
),

-- User conversion rates calculation
user_conversion_rates AS (
    -- This CTE uses user_site_activity, which is derived from the 'events' CTE.
    SELECT
        user_id,
        total_events,
        page_views,
        add_to_carts,
        purchases,
        -- Calculate view-to-purchase rate, handle division by zero
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

-- Final join to combine all user behavior metrics
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

    -- RFM metrics
    rfm.recency_days,
    rfm.frequency,
    rfm.monetary_value,

    -- Simplified RFM segmentation
    CASE
        WHEN rfm.recency_days <= 30 AND rfm.frequency >= 3 AND rfm.monetary_value >= 300 THEN 'Champions'
        WHEN rfm.recency_days <= 90 AND rfm.frequency >= 2 THEN 'Loyal Customers'
        WHEN rfm.recency_days > 90 AND rfm.frequency >= 1 THEN 'At Risk'
        WHEN rfm.recency_days <= 30 AND rfm.frequency = 1 THEN 'New Customers'
        ELSE 'Inactive'
    END AS customer_segment,

    -- Site activity metrics
    usa.total_events,
    usa.total_sessions,
    usa.page_views,
    usa.add_to_carts,
    usa.checkouts,
    usa.purchases,

    -- Conversion rates
    ucr.view_to_purchase_rate,
    ucr.cart_to_purchase_rate
FROM user_purchase_behavior upb
LEFT JOIN user_rfm rfm ON upb.user_id = rfm.user_id
LEFT JOIN user_site_activity usa ON upb.user_id = usa.user_id
LEFT JOIN user_conversion_rates ucr ON upb.user_id = ucr.user_id