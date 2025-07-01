{{ config(
    materialized='table', 
    description='A fact table summarizing sales and cost metrics per product.'
) }}

-- Aggregate delivered Order Items data 
WITH order_items_agg AS (
    SELECT
        oi.product_id AS product_key,
        SUM(oi.total_price) AS sales_amount,
        SUM(oi.quantity) AS total_units_sold,
        COUNT(DISTINCT oi.order_id) AS num_orders,
        AVG(oi.unit_price) AS avg_selling_price 
    FROM {{ ref('stg__order_items') }} oi
    LEFT JOIN {{ ref('stg__orders') }} o ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY oi.product_id
),

-- Cost of Goods Sold for delivered items
cost_analysis_agg AS (
    SELECT
        oi.product_id AS product_key,
        SUM(oi.quantity * p.cost) AS cost_of_goods_sold 
    FROM {{ ref('stg__order_items') }} oi
    LEFT JOIN {{ ref('stg__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg__orders') }} o ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY oi.product_id
)

SELECT
    dp.product_key, 
    COALESCE(oia.sales_amount, 0) AS sales_amount,
    COALESCE(oia.total_units_sold, 0) AS total_units_sold,
    COALESCE(oia.num_orders, 0) AS num_orders,
    oia.avg_selling_price, 
    COALESCE(caa.cost_of_goods_sold, 0) AS cost_of_goods_sold,
    (COALESCE(oia.sales_amount, 0) - COALESCE(caa.cost_of_goods_sold, 0)) AS profit_amount
FROM
    {{ ref('dim_products') }} dp 
LEFT JOIN
    order_items_agg oia ON dp.product_key = oia.product_key
LEFT JOIN
    cost_analysis_agg caa ON dp.product_key = caa.product_key