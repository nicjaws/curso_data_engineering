{{
    config(
        materialized='table', 
        tags=['gold', 'dimension'] 
    )
}}

SELECT
    -- Keys from the SILVER dim_users model
    user_id,
    email,
    first_name,
    last_name,
    phone_number,
    created_at,
    updated_at,
    total_orders,
    address_id,    
    address,       
    zipcode,       
    state,         
    country,       

    -- Audit Fields from SILVER
    dbt_updated_at,
    dbt_job_id,
    dbt_model_name

FROM {{ ref('silver_dim_users') }} 