-- Silver: cleaned and validated transactions

WITH source AS (
    SELECT * FROM {{ ref('bronze_transactions') }}
),

cleaned AS (
    SELECT
        transaction_id,
        account_id,

        LOWER(TRIM(transaction_type))   AS transaction_type,
        CAST(amount AS NUMERIC(15,2))   AS amount,
        UPPER(TRIM(currency))           AS currency,

        TRIM(merchant_name)             AS merchant_name,
        LOWER(TRIM(merchant_category))  AS merchant_category,
        LOWER(TRIM(transaction_status)) AS transaction_status,

        -- Parse timestamp properly
        CAST(timestamp AS TIMESTAMP)    AS transaction_timestamp,
        UPPER(TRIM(location_country))   AS location_country,

        -- Fraud signals (carry forward from bronze)
        is_foreign_transaction,
        signal_high_value,
        signal_rapid_succession,
        signal_foreign_transaction,

        CAST(avg_account_amount AS NUMERIC(15,2)) AS avg_account_amount,
        ingested_at,
        partition_date
    FROM source
    WHERE
        transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount IS NOT NULL
        AND amount > 0              -- reject zero/negative amounts
)

SELECT * FROM cleaned