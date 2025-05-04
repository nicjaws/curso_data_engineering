SELECT
  order_id,
  user_id,
  promo_id,
  address_id,
  shipping_service,
  shipping_cost,
  order_cost,
  order_total,
  created_at,
  estimated_delivery_at,
  delivery_at,
  tracking_id,
  status,
  _fivetran_synced
FROM {{ source('sql_server_dbo', 'orders') }}
WHERE _fivetran_deleted IS NULL;
