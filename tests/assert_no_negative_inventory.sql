SELECT *
FROM {{ ref('rpt_inventory_analysis') }}
WHERE current_stock < 0