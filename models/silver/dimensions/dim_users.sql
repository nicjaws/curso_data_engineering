{{
    config(
        materialized='table',
        tags=['silver', 'dimension']
    )
}}

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_addresses') }}
),

final AS (
    SELECT
        users.user_id,
        users.email,
        users.first_name,
        users.last_name,
        users.phone_number,
        users.created_at,
        users.updated_at,
        users.total_orders,
        
        -- Información de dirección
        addresses.address_id,
        addresses.address,
        addresses.zipcode,
        addresses.state,
        addresses.country,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM users
    LEFT JOIN addresses 
        ON users.address_id = addresses.address_id
)

SELECT * FROM final