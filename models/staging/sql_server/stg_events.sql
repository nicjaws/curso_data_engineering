SELECT
    event_id,
    session_id,
    user_id,
    product_id,
    order_id,
    event_type,
    page_url,
    created_at
FROM {{ ref('base_events') }};