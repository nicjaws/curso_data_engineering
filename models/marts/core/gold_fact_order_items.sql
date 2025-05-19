{{
    config(
        materialized='table', 
        tags=['gold', 'fact'] 
    )
}}

SELECT
    -- Keys from the SILVER fact table
    order_id,
    product_id,
   

    -- Measures from SILVER
    quantity,


FROM {{ ref('stg_order_items') }} 