WITH device_transactions AS (
    SELECT
        device_id,
        SUM(
            CASE
                WHEN status = 'accepted' THEN amount_in_eur
                ELSE 0
            END
        ) AS total_amount_in_eur,
        COUNT(*) AS total_transaction_cnt,
        SUM(
            CASE
                WHEN status = 'accepted' THEN 1
                ELSE 0
            END
        ) AS accepted_transaction_cnt
    FROM
        {{ ref('fct_transaction') }}
    GROUP BY
        device_id
)
SELECT
    d.id,
    d.type,
    COALESCE(
        dt.total_amount_in_eur,
        0
    ) AS total_amount_in_eur,
    COALESCE(
        dt.total_transaction_cnt,
        0
    ) AS total_transaction_cnt,
    COALESCE(
        dt.accepted_transaction_cnt,
        0
    ) AS accepted_transaction_cnt
FROM
    {{ ref('dim_device') }}
    d
    LEFT JOIN device_transactions dt
    ON d.id = dt.device_id
