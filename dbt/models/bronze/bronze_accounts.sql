SELECT
    account_id,
    customer_id,
    account_type,
    account_status,
    balance,
    currency,
    credit_limit,
    opened_date,
    last_activity_date,
    is_negative_balance,
    ingested_at,
    partition_date
FROM {{ source('bronze', 'raw_accounts') }}