{{ config(materialized='view') }}

SELECT 
    product_name,
    current_stock,
    days_of_inventory_remaining,
    inventory_status,
    recommended_reorder_qty
FROM {{ ref('rpt_inventory_analysis') }}
WHERE inventory_status = 'CRÍTICO - Reordenar inmediatamente'