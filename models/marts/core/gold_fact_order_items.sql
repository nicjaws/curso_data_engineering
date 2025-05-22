{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'product_id'],
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

{% if is_incremental() %}
  -- Filtrar solo los registros que han cambiado en staging
  WHERE (order_items.order_id, order_items.product_id) NOT IN (
    SELECT order_id, product_id 
    FROM {{ this }}
  )
{% endif %}