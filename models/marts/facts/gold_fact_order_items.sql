{{
    config(
        materialized='table', 
        tags=['gold', 'fact'] 
    )
}}

SELECT
    -- Keys from the SILVER fact table
    order_item_id, -- Surrogate key generated in SILVER
    order_id,
    product_id,
    user_id,

    -- Order Information from SILVER
    order_date,
    order_status,

    -- Measures from SILVER
    quantity,
    unit_price,
    item_total, -- Calculated in SILVER

    -- Audit Fields from SILVER
    dbt_updated_at,
    dbt_job_id,
    dbt_model_name
FROM {{ ref('silver_fact_order_items') }} 