SELECT 
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    total_price,
    discount_amount
FROM {{source('ecommerce', 'order_items')}}