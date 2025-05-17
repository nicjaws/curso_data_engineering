{{
    config(
        materialized='table', 
        tags=['gold', 'dimension']
    )
}}

SELECT
    address_id,
    address,
    zipcode,
    state,
    country,
    country_full_name, 
    region,            
    dbt_updated_at,    
    dbt_job_id,        
    dbt_model_name     
FROM {{ ref('silver_dim_addresses') }} 