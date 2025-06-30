SELECT 
    order_id,
    customer_id,
    order_date,
    status,
    total_amount,
    shipping_cost,
    discount_amount,
    shipping_address_id,
    created_at,
    updated_at
FROM {{source('ecommerce', 'orders')}}