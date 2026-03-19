-- Gold: transactions with fraud scoring applied
-- This is the core fraud detection model

WITH transactions AS (
    SELECT * FROM {{ ref('silver_transactions') }}
),

accounts AS (
    SELECT * FROM {{ ref('silver_accounts') }}
),

-- Apply fraud rules
fraud_scored AS (
    SELECT
        t.transaction_id,
        t.account_id,
        t.transaction_type,
        t.amount,
        t.currency,
        t.merchant_name,
        t.merchant_category,
        t.transaction_status,
        t.transaction_timestamp,
        t.location_country,

        -- Individual fraud flags
        t.signal_high_value,
        t.signal_rapid_succession,
        t.signal_foreign_transaction,

        -- Fraud score: count how many signals triggered
        (
            CASE WHEN t.signal_high_value          THEN 1 ELSE 0 END +
            CASE WHEN t.signal_rapid_succession     THEN 1 ELSE 0 END +
            CASE WHEN t.signal_foreign_transaction  THEN 1 ELSE 0 END
        ) AS fraud_signal_count,

        -- Final fraud flag: 2 or more signals = flagged
        CASE
            WHEN (
                CASE WHEN t.signal_high_value         THEN 1 ELSE 0 END +
                CASE WHEN t.signal_rapid_succession   THEN 1 ELSE 0 END +
                CASE WHEN t.signal_foreign_transaction THEN 1 ELSE 0 END
            ) >= 2 THEN TRUE
            ELSE FALSE
        END AS is_fraud_flagged,

        -- Risk classification
        CASE
            WHEN (
                CASE WHEN t.signal_high_value         THEN 1 ELSE 0 END +
                CASE WHEN t.signal_rapid_succession   THEN 1 ELSE 0 END +
                CASE WHEN t.signal_foreign_transaction THEN 1 ELSE 0 END
            ) = 0 THEN 'clean'
            WHEN (
                CASE WHEN t.signal_high_value         THEN 1 ELSE 0 END +
                CASE WHEN t.signal_rapid_succession   THEN 1 ELSE 0 END +
                CASE WHEN t.signal_foreign_transaction THEN 1 ELSE 0 END
            ) = 1 THEN 'suspicious'
            ELSE 'high_risk'
        END AS risk_classification,

        a.account_type,
        a.account_status,
        t.partition_date

    FROM transactions t
    LEFT JOIN accounts a ON t.account_id = a.account_id
)

SELECT * FROM fraud_scored