{{
    config(
        materialized='table',
        tags=['silver', 'dimension']
    )
}}

WITH addresses AS (
    SELECT * FROM {{ ref('stg_addresses') }}
),

final AS (
    SELECT
        address_id,
        address,
        zipcode,
        state,
        country,
        
        -- Enriquecimiento de datos geográficos
        CASE 
            WHEN country = 'USA' THEN 'United States'
            WHEN country = 'UK' THEN 'United Kingdom'
            ELSE country
        END AS country_full_name,
        
        -- Región (ejemplo simple)
        CASE 
            WHEN country IN ('USA', 'Canada', 'Mexico') THEN 'North America'
            WHEN country IN ('UK', 'France', 'Germany', 'Spain', 'Italy') THEN 'Europe'
            ELSE 'Other'
        END AS region,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM addresses
)

SELECT * FROM final