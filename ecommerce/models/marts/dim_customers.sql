{{ config(
    materialized='table',
    description='A dimension table containing unique customer attributes.'
) }}

SELECT
    customer_id AS customer_key,
    first_name AS customer_first_name,
    last_name AS customer_last_name,
    gender AS customer_gender,
    EXTRACT(YEAR FROM AGE(CURRENT_DATE, date_of_birth)) AS customer_age,
    created_at AS customer_since_timestamp
FROM
    {{ ref('stg__customers') }}