-- Gold: customer-level risk profile for dashboard

WITH customers AS (
    SELECT * FROM {{ ref('silver_customers') }}
),

accounts AS (
    SELECT * FROM {{ ref('gold_account_summary') }}
),

customer_stats AS (
    SELECT
        customer_id,
        COUNT(*)                            AS total_accounts,
        SUM(total_transactions)             AS total_transactions,
        SUM(total_volume)                   AS total_volume,
        SUM(fraud_flagged_count)            AS total_fraud_flags,
        SUM(CASE WHEN is_negative_balance
                 THEN 1 ELSE 0 END)         AS negative_balance_accounts,
        MAX(fraud_rate_pct)                 AS max_account_fraud_rate
    FROM accounts
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.full_name,
    c.email,
    c.risk_level,
    c.customer_segment,
    c.is_active,

    -- Account metrics
    COALESCE(s.total_accounts, 0)           AS total_accounts,
    COALESCE(s.total_transactions, 0)       AS total_transactions,
    COALESCE(s.total_volume, 0)             AS total_volume,
    COALESCE(s.total_fraud_flags, 0)        AS total_fraud_flags,
    COALESCE(s.negative_balance_accounts,0) AS negative_balance_accounts,
    COALESCE(s.max_account_fraud_rate, 0)   AS max_account_fraud_rate,

    -- Overall customer risk score
    CASE
        WHEN c.risk_level = 'high'
            OR COALESCE(s.total_fraud_flags, 0) > 3
            OR COALESCE(s.negative_balance_accounts, 0) > 0
        THEN 'high_risk'
        WHEN c.risk_level = 'medium'
            OR COALESCE(s.total_fraud_flags, 0) BETWEEN 1 AND 3
        THEN 'medium_risk'
        ELSE 'low_risk'
    END AS overall_risk_profile,

    c.partition_date

FROM customers c
LEFT JOIN customer_stats s ON c.customer_id = s.customer_id