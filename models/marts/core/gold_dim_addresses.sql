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
    country
FROM {{ ref('stg_addresses') }}