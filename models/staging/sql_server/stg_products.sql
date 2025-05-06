SELECT
    product_id,
    name,
    price,
    inventory
FROM {{ ref('base_products') }};