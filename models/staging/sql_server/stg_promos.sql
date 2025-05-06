SELECT
    promo_id,
    discount,
    status
FROM {{ ref('base_promos') }};