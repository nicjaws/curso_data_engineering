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
    
FROM {{ ref('stg_promos') }}