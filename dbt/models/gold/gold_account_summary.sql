-- Gold: account-level KPIs for dashboard

WITH accounts AS (
    SELECT * FROM {{ ref('silver_accounts') }}
),

transactions AS (
    SELECT * FROM {{ ref('gold_transactions_flagged') }}
),

account_stats AS (
    SELECT
        account_id,
        COUNT(*)                                    AS total_transactions,
        SUM(amount)                                 AS total_volume,
        ROUND(AVG(amount), 2)                       AS avg_transaction_amount,
        MAX(amount)                                 AS max_transaction_amount,
        SUM(CASE WHEN is_fraud_flagged THEN 1
                 ELSE 0 END)                        AS fraud_flagged_count,
        SUM(CASE WHEN transaction_status = 'completed'
                 THEN amount ELSE 0 END)            AS completed_volume
    FROM transactions
    GROUP BY account_id
)

SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    a.account_status,
    a.balance,
    a.currency,
    a.credit_limit,
    a.is_negative_balance,

    -- Transaction KPIs
    COALESCE(s.total_transactions, 0)       AS total_transactions,
    COALESCE(s.total_volume, 0)             AS total_volume,
    COALESCE(s.avg_transaction_amount, 0)   AS avg_transaction_amount,
    COALESCE(s.max_transaction_amount, 0)   AS max_transaction_amount,
    COALESCE(s.fraud_flagged_count, 0)      AS fraud_flagged_count,
    COALESCE(s.completed_volume, 0)         AS completed_volume,

    -- Fraud rate per account
    CASE
        WHEN COALESCE(s.total_transactions, 0) = 0 THEN 0
        ELSE ROUND(
            s.fraud_flagged_count * 100.0 / s.total_transactions, 2
        )
    END AS fraud_rate_pct,

    a.partition_date

FROM accounts a
LEFT JOIN account_stats s ON a.account_id = s.account_id