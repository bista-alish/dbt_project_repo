SELECT * 
FROM {{ source('ecommerce', 'products') }}