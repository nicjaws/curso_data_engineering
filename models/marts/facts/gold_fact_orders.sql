{{
    config(
        materialized='table', 
        tags=['gold', 'fact'] 
    )
}}

SELECT
    -- Keys from the SILVER fact table
    order_id,
    user_id,
    address_id,
    promo_id,

    -- Dates from SILVER
    created_at, -- Order creation timestamp
    estimated_delivery_at,
    delivered_at,

    -- Date components from SILVER
    order_year,
    order_month,
    order_day,
    order_day_of_week,

    -- Measures from SILVER
    order_cost,
    shipping_cost,
    order_total, -- Total order amount
    delivery_duration_days, -- Calculated in SILVER

    -- Status and Shipping Info from SILVER
    status,
    is_delivered, -- Boolean flag from SILVER
    shipping_service,
    tracking_id,

    -- Aggregated Item Info from SILVER
    unique_products, -- Aggregated in SILVER
    total_items,     -- Aggregated in SILVER

    -- Audit Fields from SILVER
    dbt_updated_at,
    dbt_job_id,
    dbt_model_name

FROM {{ ref('silver_fact_orders') }} 