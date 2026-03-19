-- Bronze: raw customers as-is
-- Materialized as view — no transformation, just aliasing source

SELECT
    customer_id,
    full_name,
    email,
    phone,
    date_of_birth,
    address,
    risk_level,
    customer_segment,
    created_at,
    is_active,
    ingested_at,
    partition_date
FROM {{ source('bronze', 'raw_customers') }}