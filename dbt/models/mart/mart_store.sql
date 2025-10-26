WITH store_amounts AS (
    SELECT
        store_id,
        SUM(amount_in_eur) AS total_amount_in_eur,
        SUM(
            CASE
                WHEN happened_at >= CURRENT_DATE - INTERVAL '30 days' THEN amount_in_eur
                ELSE 0
            END
        ) AS last_30d_amount_in_eur
    FROM
        {{ ref('fct_transaction') }}
    WHERE
        status = 'accepted' -- We only care about successful transactions
    GROUP BY
        store_id
),
accepted_transactions AS (
    SELECT
        store_id,
        happened_at,
        created_at,
        ROW_NUMBER() over (
            PARTITION BY store_id
            ORDER BY
                happened_at
        ) AS rn_happened,
        ROW_NUMBER() over (
            PARTITION BY store_id
            ORDER BY
                created_at
        ) AS rn_created
    FROM
        {{ ref('fct_transaction') }}
    WHERE
        status = 'accepted'
),
all_transactions AS (
    SELECT
        store_id,
        happened_at,
        created_at,
        ROW_NUMBER() over (
            PARTITION BY store_id
            ORDER BY
                happened_at
        ) AS rn_happened,
        ROW_NUMBER() over (
            PARTITION BY store_id
            ORDER BY
                created_at
        ) AS rn_created
    FROM
        {{ ref('fct_transaction') }}
)
SELECT
    s.id,
    s.name,
    s.created_at,
    s.typology,
    s.country,
    COALESCE(
        sa.total_amount_in_eur,
        0
    ) AS total_amount_in_eur,
    COALESCE(
        sa.last_30d_amount_in_eur,
        0
    ) AS last_30d_amount_in_eur,
    DATE(
        s_t_acc_1.happened_at
    ) - DATE(
        s.created_at
    ) AS days_to_1st_transaction_happened,
    DATE(
        s_t_acc_5.happened_at
    ) - DATE(
        s.created_at
    ) AS days_to_5th_transaction_happened,
    DATE(
        s_t_acc_cr_1.created_at
    ) - DATE(
        s.created_at
    ) AS days_to_1st_transaction_created,
    DATE(
        s_t_acc_cr_5.created_at
    ) - DATE(
        s.created_at
    ) AS days_to_5th_transaction_created,
    DATE(
        s_t_all_5.happened_at
    ) - DATE(
        s.created_at
    ) AS days_to_any_5th_transaction_happened
FROM
    {{ ref('dim_store') }}
    s
    LEFT JOIN store_amounts sa
    ON s.id = sa.store_id
    LEFT JOIN accepted_transactions s_t_acc_1
    ON s.id = s_t_acc_1.store_id
    AND s_t_acc_1.rn_happened = 1
    LEFT JOIN accepted_transactions s_t_acc_5
    ON s.id = s_t_acc_5.store_id
    AND s_t_acc_5.rn_happened = 5
    LEFT JOIN accepted_transactions s_t_acc_cr_1
    ON s.id = s_t_acc_cr_1.store_id
    AND s_t_acc_cr_1.rn_created = 1
    LEFT JOIN accepted_transactions s_t_acc_cr_5
    ON s.id = s_t_acc_cr_5.store_id
    AND s_t_acc_cr_5.rn_created = 5
    LEFT JOIN all_transactions s_t_all_5
    ON s.id = s_t_all_5.store_id
    AND s_t_all_5.rn_happened = 5
