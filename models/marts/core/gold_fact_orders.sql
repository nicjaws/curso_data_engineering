{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        tags=['gold', 'fact'] 
    )
}}

WITH orders_source AS (
    SELECT * FROM {{ ref('stg_orders') }}
    
    {% if is_incremental() %}
    -- Only process records that are newer than what we already have
    WHERE created_at > (SELECT MAX(created_at) FROM {{ this }})
    OR status != (SELECT status FROM {{ this }} WHERE order_id = stg_orders.order_id)
    OR delivered_at IS NOT NULL AND delivered_at > (SELECT COALESCE(MAX(delivered_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

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
    tracking_id,
    
    -- Metadata for incremental loads
    _fivetran_synced,
    CURRENT_TIMESTAMP() as dbt_updated_at

FROM {{ ref('stg_orders') }}