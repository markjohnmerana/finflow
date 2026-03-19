-- Silver: cleaned and validated customers
-- Rules: normalize text, cast types, remove nulls

WITH source AS (
    SELECT * FROM {{ ref('bronze_customers') }}
),

cleaned AS (
    SELECT
        customer_id,

        -- Normalize name and email
        INITCAP(TRIM(full_name))    AS full_name,
        LOWER(TRIM(email))          AS email,
        TRIM(phone)                 AS phone,

        -- Cast date properly
        CAST(date_of_birth AS DATE) AS date_of_birth,
        TRIM(address)               AS address,

        -- Standardize categorical fields
        LOWER(TRIM(risk_level))         AS risk_level,
        LOWER(TRIM(customer_segment))   AS customer_segment,

        -- Cast timestamps
        CAST(created_at AS TIMESTAMP)   AS created_at,
        is_active,
        ingested_at,
        partition_date
    FROM source
    WHERE
        -- Data quality filters
        customer_id IS NOT NULL
        AND full_name IS NOT NULL
        AND email IS NOT NULL
        AND email LIKE '%@%'           -- basic email validation
)

SELECT * FROM cleaned