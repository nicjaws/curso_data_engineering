{{
    config(
        materialized='table',
        tags=['gold', 'fact']
    )
}}

-- Select data from the SILVER stg_orders model.

SELECT
    -- Keys from the SILVER fact table
    order_id,
    user_id,
    address_id,
    promo_id,

    -- Dates from SILVER
    estimated_delivery_at,
    delivered_at,


    -- Measures from SILVER
    order_cost,
    shipping_cost,
    


    -- Status and Shipping Info from SILVER
    status,
    shipping_service,
    tracking_id,



FROM {{ ref('stg_orders') }} -- Reference to your SILVER model
