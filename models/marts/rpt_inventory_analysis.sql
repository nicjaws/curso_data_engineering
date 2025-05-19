{{
    config(
        materialized='table',
        tags=['gold', 'report', 'inventory']
    )
}}

/*
 * Este modelo crea un análisis completo de inventario y previsión que combina:
 * - Inventario actual y métricas de rotación
 * - Datos de ventas históricos para análisis de tendencias
 * - Presupuestos y objetivos de ventas desde Google Sheets
 * - Alertas y recomendaciones para reabastecimiento
 */

WITH products AS (
    SELECT * FROM {{ ref('gold_dim_products') }}
),

order_items AS (
    SELECT * FROM {{ ref('gold_fact_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
),

-- Combinar pedidos con sus items para análisis
order_item_detail AS (
    SELECT
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.item_total,
        o.created_at AS order_date,
        o.status AS order_status,
        DATE_TRUNC('day', o.created_at) AS day,
        DATE_TRUNC('week', o.created_at) AS week,
        DATE_TRUNC('month', o.created_at) AS month
    FROM order_items oi
    INNER JOIN orders o ON oi.order_id = o.order_id
    -- Solo incluimos pedidos completados o en proceso para el análisis de inventario
    WHERE o.status NOT IN ('cancelled', 'returned')
),

-- Obtener datos de presupuesto/objetivos de ventas de Google Sheets
budget_data AS (
    SELECT 
        product_id,
        quantity AS budget_quantity,
        monthly_date
    FROM {{ ref('stg_google_sheets__budgets') }}
),

-- Calcular ventas diarias por producto para los últimos 90 días
daily_sales AS (
    SELECT
        product_id,
        day,
        SUM(quantity) AS units_sold
    FROM order_item_detail
    WHERE day >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY product_id, day
),

-- Calcular ventas semanales por producto para tendencias
weekly_sales AS (
    SELECT
        product_id,
        week,
        SUM(quantity) AS units_sold,
        COUNT(DISTINCT order_id) AS order_count
    FROM order_item_detail
    WHERE week >= DATEADD('week', -12, DATE_TRUNC('week', CURRENT_DATE()))
    GROUP BY product_id, week
),

-- Calcular ventas mensuales por producto para comparación con presupuesto
monthly_sales AS (
    SELECT
        product_id,
        month,
        SUM(quantity) AS units_sold,
        COUNT(DISTINCT order_id) AS order_count
    FROM order_item_detail
    WHERE month >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE()))
    GROUP BY product_id, month
),

-- Calcular métricas de rotación de inventario
inventory_turnover AS (
    SELECT
        p.product_id,
        p.name AS product_name,
        p.inventory AS current_inventory,
        p.price AS unit_price,
        COALESCE(SUM(oid.quantity), 0) AS units_sold_90days,
        -- Valor del inventario actual
        (p.inventory * p.price) AS inventory_value,
        -- Cálculo del inventario promedio en los últimos 90 días
        -- (asumimos que tenemos el inventario actual como aproximación)
        p.inventory AS avg_inventory_90days,
        -- Días de inventario restantes basados en la tasa de venta de 90 días
        CASE 
            WHEN COALESCE(SUM(oid.quantity), 0) > 0 THEN 
                (p.inventory / (SUM(oid.quantity) / 90.0)) 
            ELSE NULL 
        END AS days_of_inventory_remaining,
        -- Rotación de inventario (proporción de unidades vendidas vs. inventario)
        CASE 
            WHEN p.inventory > 0 THEN 
                COALESCE(SUM(oid.quantity), 0) / NULLIF(p.inventory, 0)
            ELSE NULL 
        END AS inventory_turnover_ratio
    FROM products p
    LEFT JOIN order_item_detail oid 
        ON p.product_id = oid.product_id
        AND oid.order_date >= DATEADD('day', -90, CURRENT_DATE())
    GROUP BY p.product_id, p.name, p.inventory, p.price
),

-- Comparar ventas reales con objetivos de presupuesto
budget_vs_actual AS (
    SELECT
        ms.product_id,
        ms.month AS sales_month,
        bd.monthly_date AS budget_month,
        ms.units_sold AS actual_units_sold,
        bd.budget_quantity AS target_units,
        -- Calcular la diferencia y el porcentaje de cumplimiento
        (ms.units_sold - bd.budget_quantity) AS units_variance,
        CASE 
            WHEN bd.budget_quantity > 0 THEN 
                (ms.units_sold / bd.budget_quantity) * 100 
            ELSE NULL 
        END AS budget_achievement_pct
    FROM monthly_sales ms
    LEFT JOIN budget_data bd 
        ON ms.product_id = bd.product_id 
        AND ms.month = bd.monthly_date
    WHERE bd.budget_quantity IS NOT NULL
),

-- Generar alertas y recomendaciones de reabastecimiento
inventory_alerts AS (
    SELECT
        it.product_id,
        it.product_name,
        it.current_inventory,
        it.days_of_inventory_remaining,
        -- Clasificación de productos por velocidad de venta
        CASE
            WHEN it.inventory_turnover_ratio > 0.5 THEN 'Alto movimiento'
            WHEN it.inventory_turnover_ratio BETWEEN 0.1 AND 0.5 THEN 'Movimiento medio'
            WHEN it.inventory_turnover_ratio < 0.1 THEN 'Bajo movimiento'
            ELSE 'Sin ventas recientes'
        END AS product_velocity,
        -- Alertas basadas en días de inventario restantes
        CASE
            WHEN it.days_of_inventory_remaining < 15 THEN 'CRÍTICO - Reordenar inmediatamente'
            WHEN it.days_of_inventory_remaining < 30 THEN 'ALERTA - Nivel bajo de inventario'
            WHEN it.days_of_inventory_remaining > 120 THEN 'EXCESO - Considerar promoción'
            ELSE 'Normal'
        END AS inventory_status,
        -- Calcular cantidad recomendada para reordenar
        CASE
            WHEN it.days_of_inventory_remaining < 30 THEN 
                GREATEST(30 - it.current_inventory, 0)
            ELSE 0
        END AS recommended_reorder_qty
    FROM inventory_turnover it
)

-- Combinar todas las métricas en un solo reporte completo
SELECT
    -- Información del producto
    p.product_id,
    p.name AS product_name,
    p.price AS unit_price,
    
    -- Estado actual del inventario
    p.inventory AS current_stock,
    (p.inventory * p.price) AS inventory_value,
    
    -- Métricas de rotación de inventario
    it.units_sold_90days,
    it.inventory_turnover_ratio,
    it.days_of_inventory_remaining,
    
    -- Alertas y recomendaciones
    ia.product_velocity,
    ia.inventory_status,
    ia.recommended_reorder_qty,
    
    -- Tendencia de ventas (últimas 4 semanas)
    (SELECT SUM(units_sold) FROM weekly_sales ws 
     WHERE ws.product_id = p.product_id 
     AND ws.week >= DATEADD('week', -4, DATE_TRUNC('week', CURRENT_DATE()))) AS units_sold_last_4weeks,
    
    -- Comparación con presupuesto del mes actual
    bva.target_units AS current_month_target,
    bva.actual_units_sold AS current_month_actual,
    bva.budget_achievement_pct AS current_month_target_pct,
    
    -- Proyección para el próximo mes basada en tendencia actual
    CASE 
        WHEN it.units_sold_90days > 0 THEN 
            ROUND((it.units_sold_90days / 3), 0)  -- Proyección simple basada en el promedio mensual
        ELSE 0 
    END AS next_month_sales_forecast,
    
    -- Métricas financieras
    (it.units_sold_90days * p.price) AS revenue_last_90days,
    ((it.units_sold_90days * p.price) / 90) * 30 AS projected_monthly_revenue,
    
    -- Metadata y timestamps
    CURRENT_TIMESTAMP() AS report_generated_at
    
FROM products p
LEFT JOIN inventory_turnover it ON p.product_id = it.product_id
LEFT JOIN inventory_alerts ia ON p.product_id = ia.product_id
LEFT JOIN budget_vs_actual bva 
    ON p.product_id = bva.product_id 
    AND bva.sales_month = DATE_TRUNC('month', CURRENT_DATE())