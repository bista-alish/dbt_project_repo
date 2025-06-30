SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    created_at,
    updated_at 
FROM {{source('ecommerce', 'customers')}}