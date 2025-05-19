{{
    config(
        materialized='table', 
        tags=['gold', 'dimension'] 
    )
}}

SELECT
    
    user_id,
    email,
    first_name,
    last_name,
    phone_number,
    created_at,
    updated_at,
    total_orders,
    address_id,                         

FROM {{ ref('stg_users') }} 