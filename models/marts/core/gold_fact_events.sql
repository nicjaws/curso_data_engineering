{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        tags=['gold', 'fact']
    )
}}

SELECT
    -- Keys
    event_id,
    user_id,
    product_id,
    session_id,
    event_type,
    page_url,
    order_id,
    created_at,
    _fivetran_synced,
    CURRENT_TIMESTAMP() as dbt_updated_at

FROM {{ ref('stg_events') }}

{% if is_incremental() %}
    -- Simple approach: use a reasonable lookback period
    WHERE created_at >= CURRENT_DATE - 7  -- Process last 7 days on incremental runs
{% endif %}