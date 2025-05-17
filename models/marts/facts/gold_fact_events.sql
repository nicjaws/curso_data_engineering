{{
    config(
        materialized='table', 
        tags=['gold', 'fact']
    )
}}

SELECT
    -- Keys
    event_id,
    user_id,
    product_id,
    session_id,

    -- Event Information
    event_type,
    page_url,
    event_timestamp,

    -- Date/Time Components (from SILVER)
    event_year,
    event_month,
    event_day,
    event_hour,

    -- Event Classification (from SILVER)
    event_category,
    page_category,

    -- Conversion Information (from SILVER)
    led_to_purchase,
    order_id,
    order_created_at,
    hours_to_purchase,

    -- Audit Fields (from SILVER)
    dbt_updated_at,
    dbt_job_id,
    dbt_model_name

FROM {{ ref('silver_fact_events') }}