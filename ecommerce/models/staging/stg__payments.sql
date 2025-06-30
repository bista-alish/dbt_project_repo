SELECT 
    payment_id,
    order_id,
    payment_method,
    amount,
    payment_date,
    status,
    transaction_id
FROM {{source('ecommerce', 'payments')}}