{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        tags=['gold', 'fact']
    )
}}

-- Primero intentemos un modelo sin lógica incremental para confirmar que funciona
SELECT
    event_id,
    user_id,
    session_id,
    product_id,
    order_id,
    page_url,
    event_type,
    created_at,
    fivetran_synced,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM 
    {{ ref('stg_events') }}