WITH customer_base AS (
    SELECT
        customer_id AS user_id,
        first_name AS customer_first_name,
        last_name AS customer_last_name,
        gender AS customer_gender,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth)) AS customer_age,
        created_at AS customer_since
    FROM {{ ref('stg__customers') }}
),

order_items AS (
    SELECT
        o.customer_id AS user_id,
        SUM(oi.total_price) AS total_amount_spent,
        COUNT(DISTINCT oi.order_item_id) AS total_items_purchased,
        MIN(o.order_date) AS first_order_completed_at,
        MAX(o.order_date) AS last_order_completed_at
    FROM {{ ref('stg__orders') }} o
    LEFT JOIN {{ ref('stg__order_items') }} oi ON o.order_id = oi.order_id
    WHERE o.status = 'delivered'
    GROUP BY o.customer_id
),

orders AS (
    SELECT
        customer_id AS user_id,
        COUNT(DISTINCT order_id) AS num_orders,
        COUNT(DISTINCT CASE WHEN status = 'shipped' THEN order_id END) AS num_orders_shipped,
        COUNT(DISTINCT CASE WHEN status = 'delivered' THEN order_id END) AS num_orders_delivered,
        COUNT(DISTINCT CASE WHEN status = 'pending' THEN order_id END) AS num_orders_pending,
        COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS num_orders_cancelled,
        COUNT(DISTINCT CASE WHEN status = 'returned' THEN order_id END) AS num_orders_returned
    FROM {{ ref('stg__orders') }}
    GROUP BY customer_id
),

payments AS (
    SELECT
        o.customer_id AS user_id,
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
    cb.user_id,
    -- dimensions
    cb.customer_first_name,
    cb.customer_last_name,
    cb.customer_gender,
    cb.customer_age,
    cb.customer_since,
    -- facts
    oi.total_amount_spent,
    oi.total_items_purchased,
    oi.first_order_completed_at,
    oi.last_order_completed_at,
    o.num_orders,
    o.num_orders_shipped,
    o.num_orders_delivered,
    o.num_orders_pending,
    o.num_orders_cancelled,
    o.num_orders_returned,
    p.num_payments,
    p.num_esewa_payments,
    p.num_khalti_payments,
    p.num_cod_payments,
    p.num_failed_payments
FROM customer_base cb
LEFT JOIN order_items oi ON cb.user_id = oi.user_id
LEFT JOIN orders o ON cb.user_id = o.user_id
LEFT JOIN payments p ON cb.user_id = p.user_id