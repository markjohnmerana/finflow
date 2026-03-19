-- Silver: cleaned and validated accounts

WITH source AS (
    SELECT * FROM {{ ref('bronze_accounts') }}
),

cleaned AS (
    SELECT
        account_id,
        customer_id,

        LOWER(TRIM(account_type))   AS account_type,
        LOWER(TRIM(account_status)) AS account_status,

        -- Ensure balance is properly typed
        CAST(balance AS NUMERIC(15,2))      AS balance,
        UPPER(TRIM(currency))               AS currency,
        CAST(credit_limit AS NUMERIC(15,2)) AS credit_limit,

        CAST(opened_date AS DATE)           AS opened_date,
        CAST(last_activity_date AS DATE)    AS last_activity_date,

        is_negative_balance,
        ingested_at,
        partition_date
    FROM source
    WHERE
        account_id  IS NOT NULL
        AND customer_id IS NOT NULL
        AND balance IS NOT NULL
)

SELECT * FROM cleaned