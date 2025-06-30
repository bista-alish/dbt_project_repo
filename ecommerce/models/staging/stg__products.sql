SELECT 
    product_id,
    name,
    category,
    subcategory,
    brand,
    price,
    cost,
    weight_kg,
    created_at
FROM {{ source('ecommerce', 'products') }}