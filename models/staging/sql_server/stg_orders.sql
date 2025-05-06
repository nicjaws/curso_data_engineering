SELECT
    order_id,
    user_id,
    promo_id,
    address_id,
    shipping_service,
    shipping_cost,
    order_cost,
    order_total,
    created_at,
    estimated_delivery_at,
    delivery_at,
    tracking_id,
    status,
    COALESCE(order_total - shipping_cost, order_total) AS final_total
FROM {{ ref('base_orders') }};