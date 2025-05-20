{{
    config(
        materialized='table', 
        tags=['gold', 'fact'] 
    )
}}

SELECT
    -- Keys from the SILVER fact table
    order_items.order_id,
    order_items.product_id,
   
    -- Measures from SILVER
    order_items.quantity,
    
    -- Calculate item total 
    products.price * order_items.quantity AS item_revenue_total
    

FROM {{ ref('stg_order_items') }} as order_items
JOIN {{ ref('stg_products') }} as products
    ON order_items.product_id = products.product_id