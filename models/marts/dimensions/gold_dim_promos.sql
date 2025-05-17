{{
    config(
        materialized='table', 
        tags=['gold', 'dimension']
    )
}}

SELECT
    promo_id,
    discount,
    status,
    discount_tier,    -- Field calculated in the SILVER model
    dbt_updated_at,   -- Audit field from SILVER
    dbt_job_id,       -- Audit field from SILVER
    dbt_model_name    -- Audit field from SILVER
    -- Add any additional fields or transformations specific to the GOLD layer here
FROM {{ ref('silver_dim_promos') }} -- <-- References your SILVER dim_promos model