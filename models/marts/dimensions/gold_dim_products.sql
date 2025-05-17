{{
    config(
        materialized='table', 
        tags=['gold', 'dimension']
    )
}}

SELECT
    product_id,
    name,
    price,
    inventory,
    price_category,       
    inventory_status,     
    budgeted_quantity,    
    dbt_updated_at,       
    dbt_job_id,           
    dbt_model_name        
FROM {{ ref('silver_dim_products') }}