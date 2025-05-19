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
    inventory
FROM {{ ref('stg_products') }}