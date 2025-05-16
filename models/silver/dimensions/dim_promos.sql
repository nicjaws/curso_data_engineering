{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

WITH promos AS (
    SELECT * FROM {{ ref('stg_promos') }}
),

final AS (
    SELECT
        promo_id,
        discount,
        status,
        
        -- Clasificación de promociones
        CASE 
            WHEN discount < 10 THEN 'Low discount'
            WHEN discount BETWEEN 10 AND 25 THEN 'Medium discount'
            ELSE 'High discount'
        END AS discount_tier,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM promos
)

SELECT * FROM final