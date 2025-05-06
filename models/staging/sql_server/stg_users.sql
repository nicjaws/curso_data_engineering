SELECT
    user_id,
    CONCAT(first_name, '', last_name) AS full_name,
    email,
    phone_number,
    address_id,
    created_at,
    updated_at,
    total_orders
FROM {{ ref('base_users') }};