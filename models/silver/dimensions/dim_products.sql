{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- Opcional: Agregar información de presupuesto si está disponible
budgets AS (
    SELECT 
        product_id,
        SUM(quantity) AS budgeted_quantity
    FROM {{ ref('stg_google_sheets__budgets') }}
    GROUP BY product_id
),

final AS (
    SELECT
        products.product_id,
        products.name,
        products.price,
        products.inventory,
        
        -- Categorización de productos (ejemplo)
        CASE 
            WHEN products.price < 50 THEN 'Bajo costo'
            WHEN products.price BETWEEN 50 AND 200 THEN 'Costo medio'
            ELSE 'Premium'
        END AS price_category,
        
        -- Estado de inventario
        CASE 
            WHEN products.inventory = 0 THEN 'Sin stock'
            WHEN products.inventory < 10 THEN 'Stock bajo'
            WHEN products.inventory < 50 THEN 'Stock medio'
            ELSE 'Stock alto'
        END AS inventory_status,
        
        -- Información de presupuesto
        COALESCE(budgets.budgeted_quantity, 0) AS budgeted_quantity,
        
        -- Campos de auditoría
        CURRENT_TIMESTAMP() AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_job_id,
        '{{ this }}' AS dbt_model_name
    FROM products
    LEFT JOIN budgets 
        ON products.product_id = budgets.product_id
)

SELECT * FROM final