SELECT
  user_id,
  first_name,
  last_name,
  email,
  phone_number,
  created_at,
  updated_at,
  address_id,
  total_orders,
  _fivetran_synced
FROM {{ source('sql_server_dbo', 'users') }}
WHERE _fivetran_deleted IS NULL;