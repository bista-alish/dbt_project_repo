{{ config(
    materialized='table',
    description='A dimension table containing unique product attributes.'
) }}

SELECT
    product_id AS product_key,
    name AS product_name,
    category AS product_category,
    subcategory AS product_subcategory,
    brand AS product_brand,
    price AS product_list_price, 
    cost AS product_cost_at_source 
FROM
    {{ ref('stg__products') }}