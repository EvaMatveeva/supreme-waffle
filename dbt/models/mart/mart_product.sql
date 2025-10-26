WITH product_transactions AS (
    SELECT
        product_id,
        SUM(
            CASE
                WHEN status = 'accepted' THEN amount_in_eur
                ELSE 0
            END
        ) AS total_amount_in_eur,
        SUM(
            CASE
                WHEN happened_at >= CURRENT_DATE - INTERVAL '30 days'
                AND status = 'accepted' THEN amount_in_eur
                ELSE 0
            END
        ) AS last_30d_amount_in_eur,
        COUNT(*) AS total_transaction_cnt,
        SUM(
            CASE
                WHEN status = 'accepted' THEN 1
                ELSE 0
            END
        ) AS accepted_transaction_cnt,
        MIN(
            CASE
                WHEN status = 'accepted' THEN happened_at
            END
        ) AS first_transaction_happened,
        MAX(
            CASE
                WHEN status = 'accepted' THEN happened_at
            END
        ) AS last_transaction_happened
    FROM
        {{ ref('fct_transaction') }}
    GROUP BY
        product_id
)
SELECT
    p.id,
    p.product_name,
    COALESCE(
        pt.total_amount_in_eur,
        0
    ) AS total_amount_in_eur,
    COALESCE(
        pt.last_30d_amount_in_eur,
        0
    ) AS last_30d_amount_in_eur,
    COALESCE(
        pt.total_transaction_cnt,
        0
    ) AS total_transaction_cnt,
    COALESCE(
        pt.accepted_transaction_cnt,
        0
    ) AS accepted_transaction_cnt,
    pt.first_transaction_happened,
    pt.last_transaction_happened
FROM
    {{ ref('dim_product') }}
    p
    LEFT JOIN product_transactions pt
    ON p.id = pt.product_id
