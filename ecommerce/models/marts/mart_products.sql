WITH product_base AS (
    SELECT
        product_id,
        name AS product_name,
        category AS product_category,
        subcategory AS product_subcategory,
        brand AS product_brand,
        price AS product_price,
        cost AS product_cost
    FROM {{ ref('stg__products') }}
),

order_items AS (
    SELECT
        oi.product_id,
        SUM(total_price) AS sales_amount,
        SUM(quantity) AS total_units_sold,
        COUNT(DISTINCT oi.order_id) AS num_orders,
        AVG(unit_price) AS avg_selling_price
    FROM {{ ref('stg__order_items') }} oi
    LEFT JOIN {{ ref('stg__orders') }} o ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY product_id
),

cost_analysis AS (
    SELECT
        oi.product_id,
        SUM(quantity * product_cost) AS cost_of_goods_sold
    FROM {{ ref('stg__order_items') }} oi
    LEFT JOIN {{ ref('stg__products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg__orders') }} o ON oi.order_id = o.order_id
    WHERE o.status = 'delivered'
    GROUP BY oi.product_id
)

SELECT
    pb.product_id,
    -- dimensions
    pb.product_name,
    pb.product_category,
    pb.product_subcategory,
    pb.product_brand,
    pb.product_price,
    pb.product_cost,
    -- facts (e.g. profit analysis)
    oi.sales_amount,
    oi.total_units_sold,
    oi.num_orders,
    oi.avg_selling_price,
    ca.cost_of_goods_sold,
    oi.sales_amount - ca.cost_of_goods_sold AS profit
FROM product_base pb
LEFT JOIN order_items oi ON pb.product_id = oi.product_id
LEFT JOIN cost_analysis ca ON pb.product_id = ca.product_id