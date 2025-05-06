SELECT
    order_id,
    product_id,
    quantity
FROM {{ ref('base_order_items') }};