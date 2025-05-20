{{
    config(
        materialized='table',
        tags=['gold', 'report', 'marketing']
    )
}}

/*
 * Este modelo crea un análisis completo de promociones que evalúa:
 * - Rendimiento de diferentes promociones/descuentos
 * - Impacto en ingresos y margen
 * - Comportamiento de conversión de clientes
 * - Efectividad y ROI de cada promoción
 */

WITH orders AS (
    SELECT * FROM {{ ref('gold_fact_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('gold_fact_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('gold_dim_products') }}
),

promos AS (
    SELECT * FROM {{ ref('gold_dim_promos') }}
),

users AS (
    SELECT * FROM {{ ref('gold_dim_users') }}
),

-- Ordenes con promociones aplicadas
promo_orders AS (
    SELECT
        o.order_id,
        o.user_id,
        o.promo_id,
        o.created_at AS order_date,
        o.order_total,
        o.order_cost,
        p.discount AS promo_discount_percent,
        p.status AS promo_status,
        -- Calculamos el descuento aplicado a la orden
        (o.order_cost * (p.discount / 100.0)) AS estimated_discount_value,
        -- Margen estimado (asumiendo que order_cost es el valor antes del descuento)
        (o.order_total - o.order_cost) AS order_margin,
        -- Clasificación de la orden por tamaño
        CASE
            WHEN o.order_total < 50 THEN 'pequeño'
            WHEN o.order_total BETWEEN 50 AND 150 THEN 'mediano'
            ELSE 'grande'
        END AS order_size_category
    FROM orders o
    LEFT JOIN promos p ON o.promo_id = p.promo_id
    WHERE o.promo_id IS NOT NULL -- Solo órdenes con promociones
      AND o.status != 'cancelled' -- Excluir órdenes canceladas
),

-- Agrupación por promoción para ver métricas agregadas
promo_performance AS (
    SELECT
        promo_id,
        promo_discount_percent,
        promo_status,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT user_id) AS unique_customers,
        SUM(order_total) AS total_revenue,
        SUM(estimated_discount_value) AS total_discount_value,
        AVG(order_total) AS avg_order_value,
        -- Contar segmentos de tamaño de orden
        COUNT(CASE WHEN order_size_category = 'pequeño' THEN 1 END) AS small_orders,
        COUNT(CASE WHEN order_size_category = 'mediano' THEN 1 END) AS medium_orders,
        COUNT(CASE WHEN order_size_category = 'grande' THEN 1 END) AS large_orders,
        -- Valor promedio por cada categoría de tamaño
        AVG(CASE WHEN order_size_category = 'pequeño' THEN order_total END) AS avg_small_order_value,
        AVG(CASE WHEN order_size_category = 'mediano' THEN order_total END) AS avg_medium_order_value,
        AVG(CASE WHEN order_size_category = 'grande' THEN order_total END) AS avg_large_order_value
    FROM promo_orders
    GROUP BY promo_id, promo_discount_percent, promo_status
),

-- Análisis temporal de uso de promociones
promo_time_analysis AS (
    SELECT
        promo_id,
        DATE_TRUNC('day', order_date) AS day,
        DATE_TRUNC('week', order_date) AS week,
        DATE_TRUNC('month', order_date) AS month,
        COUNT(DISTINCT order_id) AS orders,
        SUM(order_total) AS revenue,
        SUM(estimated_discount_value) AS discount_value
    FROM promo_orders
    GROUP BY promo_id, 
             DATE_TRUNC('day', order_date),
             DATE_TRUNC('week', order_date),
             DATE_TRUNC('month', order_date)
),

-- Análisis de clientes nuevos vs recurrentes
promo_customer_type AS (
    SELECT
        po.promo_id,
        po.user_id,
        MIN(u.created_at) AS user_created_at,
        MIN(po.order_date) AS first_promo_order_date,
        -- Definir si el cliente es nuevo (primera compra) o recurrente
        CASE
            WHEN DATEDIFF('day', u.created_at, MIN(po.order_date)) <= 1 THEN 'nuevo'
            ELSE 'recurrente'
        END AS customer_type
    FROM promo_orders po
    JOIN users u ON po.user_id = u.user_id
    GROUP BY po.promo_id, po.user_id, u.created_at
),

-- Agregamos métricas por tipo de cliente
promo_customer_metrics AS (
    SELECT
        promo_id,
        COUNT(DISTINCT CASE WHEN customer_type = 'nuevo' THEN user_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN customer_type = 'recurrente' THEN user_id END) AS returning_customers,
        COUNT(DISTINCT user_id) AS total_customers,
        CASE
            WHEN COUNT(DISTINCT user_id) > 0 
            THEN (COUNT(DISTINCT CASE WHEN customer_type = 'nuevo' THEN user_id END) * 100.0) / 
                 COUNT(DISTINCT user_id)
            ELSE 0
        END AS new_customer_percentage
    FROM promo_customer_type
    GROUP BY promo_id
),

-- Análisis de categorías de productos con promociones
promo_product_analysis AS (
    SELECT
        o.promo_id,
        oi.product_id,
        p.name AS product_name,
        p.price AS product_price,
        COUNT(DISTINCT o.order_id) AS orders,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.item_revenue_total) AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN products p ON oi.product_id = p.product_id
    WHERE o.promo_id IS NOT NULL
      AND o.status != 'cancelled'
    GROUP BY o.promo_id, oi.product_id, p.name, p.price
),

-- Agregamos los productos más vendidos por promoción
promo_top_products AS (
    SELECT
        promo_id,
        LISTAGG(product_name, ', ') WITHIN GROUP (ORDER BY units_sold DESC) AS top_products
    FROM (
        SELECT 
            promo_id,
            product_name,
            units_sold,
            ROW_NUMBER() OVER (PARTITION BY promo_id ORDER BY units_sold DESC) as rn
        FROM promo_product_analysis
    )
    WHERE rn <= 3  -- Top 3 productos
    GROUP BY promo_id
)

-- Unificamos todas las métricas en un reporte completo
SELECT
    -- Identificación y detalles de la promoción
    p.promo_id,
    p.promo_discount_percent AS discount_percentage,
    p.promo_status,
    
    -- Métricas generales de rendimiento
    p.total_orders,
    p.unique_customers,
    p.total_revenue,
    p.total_discount_value,
    p.avg_order_value,
    
    -- Índice de eficiencia (ingresos generados por cada $ de descuento)
    CASE 
        WHEN p.total_discount_value > 0 
        THEN p.total_revenue / p.total_discount_value 
        ELSE NULL 
    END AS revenue_per_discount_dollar,
    
    -- Métricas de adquisición de clientes
    pcm.new_customers,
    pcm.returning_customers,
    pcm.new_customer_percentage,
    
    -- Costo de adquisición de cliente (CAC) a través de promociones
    CASE 
        WHEN pcm.new_customers > 0 
        THEN p.total_discount_value / pcm.new_customers 
        ELSE NULL 
    END AS promo_cac,
    
    -- Distribución de tamaños de orden
    p.small_orders,
    p.medium_orders,
    p.large_orders,
    p.avg_small_order_value,
    p.avg_medium_order_value,
    p.avg_large_order_value,
    
    -- Tendencias temporales (últimos 30 días)
    (SELECT COUNT(orders) FROM promo_time_analysis pta 
     WHERE pta.promo_id = p.promo_id 
     AND pta.day >= DATEADD('day', -30, CURRENT_DATE())) AS orders_last_30_days,
     
    (SELECT SUM(revenue) FROM promo_time_analysis pta 
     WHERE pta.promo_id = p.promo_id 
     AND pta.day >= DATEADD('day', -30, CURRENT_DATE())) AS revenue_last_30_days,
    
    -- Productos más populares en esta promoción
    tp.top_products,
    
    -- Efectividad y ROI estimado
    CASE
        WHEN p.total_discount_value > 0 
        THEN ((p.total_revenue - p.total_discount_value) / p.total_discount_value) * 100
        ELSE NULL
    END AS estimated_roi_percentage,
    
    -- Clasificación de la promoción por efectividad
    CASE
        WHEN p.total_discount_value > 0 AND ((p.total_revenue - p.total_discount_value) / p.total_discount_value) > 3 THEN 'Muy efectiva'
        WHEN p.total_discount_value > 0 AND ((p.total_revenue - p.total_discount_value) / p.total_discount_value) BETWEEN 1 AND 3 THEN 'Efectiva'
        WHEN p.total_discount_value > 0 AND ((p.total_revenue - p.total_discount_value) / p.total_discount_value) BETWEEN 0 AND 1 THEN 'Moderada'
        WHEN p.total_discount_value > 0 AND ((p.total_revenue - p.total_discount_value) / p.total_discount_value) < 0 THEN 'Ineficiente'
        ELSE 'Sin datos suficientes'
    END AS promotion_effectiveness,
    
    -- Metadata y timestamp
    CURRENT_TIMESTAMP() AS report_generated_at
    
FROM promo_performance p
LEFT JOIN promo_customer_metrics pcm ON p.promo_id = pcm.promo_id
LEFT JOIN promo_top_products tp ON p.promo_id = tp.promo_id