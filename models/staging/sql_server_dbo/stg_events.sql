WITH source AS (
    SELECT * FROM {{ source('sql_server_dbo', 'events') }}
)

SELECT
    -- Identificadores
    "EVENT_ID" as event_id,
    "USER_ID" as user_id,
    "SESSION_ID" as session_id,
    "PRODUCT_ID" as product_id,
    "ORDER_ID" as order_id,
    
    -- Datos del evento
    "PAGE_URL" as page_url,
    "EVENT_TYPE" as event_type,
    "CREATED_AT" as created_at,  -- Explícitamente usamos mayúsculas, y lo convertimos a minúsculas
    
    -- Metadatos
    "_FIVETRAN_SYNCED" as fivetran_synced

FROM source
WHERE "_FIVETRAN_DELETED" = FALSE