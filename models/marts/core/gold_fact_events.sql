{{
    config(
        materialized='table', 
        tags=['gold', 'fact']
    )
}}

SELECT
    -- Keys
    event_id,
    user_id,
    product_id,
    session_id,

    -- Event Information
    event_type,
    page_url,

    -- Conversion Information (from SILVER)
    
    order_id,
  


FROM {{ ref('stg_events') }}