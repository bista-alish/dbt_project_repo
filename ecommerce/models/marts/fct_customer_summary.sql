{{ config(
    materialized='table',
    description='A fact table summarizing order and payment activities per customer.'
) }}

-- Aggregate delivered Order Items data
WITH order_items_agg AS (
    SELECT
        o.customer_id AS customer_key,
        SUM(oi.total_price) AS total_amount_spent,
        COUNT(DISTINCT oi.order_item_id) AS total_items_purchased,
        MIN(o.order_date) AS first_order_completed_at,
        MAX(o.order_date) AS last_order_completed_at
    FROM {{ ref('stg__orders') }} o
    LEFT JOIN {{ ref('stg__order_items') }} oi ON o.order_id = oi.order_id
    WHERE o.status = 'delivered'
    GROUP BY o.customer_id
),

-- Aggregate all Orders data by status
orders_agg AS (
    SELECT
        customer_id AS customer_key,
        COUNT(DISTINCT order_id) AS num_orders,
        COUNT(DISTINCT CASE WHEN status = 'shipped' THEN order_id END) AS num_orders_shipped,
        COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) AS num_orders_delivered,
        COUNT(DISTINCT CASE WHEN status = 'pending' THEN order_id END) AS num_orders_pending,
        COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS num_orders_cancelled,
        COUNT(DISTINCT CASE WHEN status = 'returned' THEN order_id END) AS num_orders_returned
    FROM {{ ref('stg__orders') }}
    GROUP BY customer_id
),

-- Aggregate Payments data
payments_agg AS (
    SELECT
        o.customer_id AS customer_key,
        COUNT(DISTINCT p.payment_id) AS num_payments,
        COUNT(DISTINCT CASE WHEN p.payment_method = 'eSewa' THEN p.payment_id END) AS num_esewa_payments,
        COUNT(DISTINCT CASE WHEN p.payment_method = 'Khalti' THEN p.payment_id END) AS num_khalti_payments,
        COUNT(DISTINCT CASE WHEN p.payment_method = 'Cash on Delivery' THEN p.payment_id END) AS num_cod_payments,
        COUNT(DISTINCT CASE WHEN p.status = 'failed' THEN p.payment_id END) AS num_failed_payments
    FROM {{ ref('stg__orders') }} o
    LEFT JOIN {{ ref('stg__payments') }} p ON o.order_id = p.order_id
    GROUP BY o.customer_id
)

SELECT
    dc.customer_key, 
    COALESCE(oia.total_amount_spent, 0) AS total_amount_spent,
    COALESCE(oia.total_items_purchased, 0) AS total_items_purchased,
    oia.first_order_completed_at,
    oia.last_order_completed_at,
    COALESCE(oa.num_orders, 0) AS num_orders,
    COALESCE(oa.num_orders_shipped, 0) AS num_orders_shipped,
    COALESCE(oa.num_orders_delivered, 0) AS num_orders_delivered,
    COALESCE(oa.num_orders_pending, 0) AS num_orders_pending,
    COALESCE(oa.num_orders_cancelled, 0) AS num_orders_cancelled,
    COALESCE(oa.num_orders_returned, 0) AS num_orders_returned,
    COALESCE(pa.num_payments, 0) AS num_payments,
    COALESCE(pa.num_esewa_payments, 0) AS num_esewa_payments,
    COALESCE(pa.num_khalti_payments, 0) AS num_khalti_payments,
    COALESCE(pa.num_cod_payments, 0) AS num_cod_payments,
    COALESCE(pa.num_failed_payments, 0) AS num_failed_payments
FROM
    {{ ref('dim_customers') }} dc 
LEFT JOIN
    order_items_agg oia ON dc.customer_key = oia.customer_key
LEFT JOIN
    orders_agg oa ON dc.customer_key = oa.customer_key
LEFT JOIN
    payments_agg pa ON dc.customer_key = pa.customer_key