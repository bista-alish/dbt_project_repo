{% snapshot customers_snapshot %}

{{ 
    config(
    target_schema='main',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
    ) 
}}

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

{% endsnapshot %}


