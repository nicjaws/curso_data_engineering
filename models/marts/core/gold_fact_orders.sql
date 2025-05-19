{{
    config(
        materialized='table', 
        tags=['gold', 'fact'] 
    )
}}

SELECT
    -- Keys
    order_id,
    user_id,
    address_id,
    promo_id,
    
    -- Order Information
    created_at,
    status,
    shipping_service,
    shipping_cost,
    order_cost,
    order_total,
    
    -- Fulfillment Information
    estimated_delivery_at,
    delivered_at,
    tracking_id

FROM {{ ref('stg_orders') }}