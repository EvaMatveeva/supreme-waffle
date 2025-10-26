------------------------------------------------------------------
-- Top 10 stores by transacted amount
/*
* Use DENSE_RANK() so that all stores with the same amount are shown. This is fairer.
* Use total_amount_in_eur, which includes only accepted transactions.
* */


WITH pre AS (
    SELECT
        id,
        name,
        typology,
        total_amount_in_eur,
        DENSE_RANK() over(
            PARTITION BY 1
            ORDER BY
                total_amount_in_eur DESC
        ) AS rk
    FROM
        dbt.mart_store
)
SELECT
    id,
    name,
    typology,
    total_amount_in_eur
FROM
    pre
WHERE
    rk <= 10;


------------------------------------------------------------------
-- Top 10 products sold
/*
* Use DENSE_RANK() so that all products with the same amount are shown. This is fairer.
* Use total_amount_in_eur, which includes only accepted transactions.
* The top ranking is based on the amount spent on products. To show the top 10 products by number of transactions, use the accepted_transaction_cnt field.
* */


WITH pre AS (
    SELECT
        id,
        product_name,
        total_amount_in_eur,
        accepted_transaction_cnt,
        total_transaction_cnt,
        DENSE_RANK() over(
            PARTITION BY 1
            ORDER BY
                total_amount_in_eur DESC
        ) AS rk
    FROM
        dbt.mart_product
)
SELECT
    id,
    product_name,
    total_amount_in_eur,
    accepted_transaction_cnt,
    total_transaction_cnt
FROM
    pre
WHERE
    rk <= 10;


------------------------------------------------------------------
-- Average transacted amount per store typology and country
/*
* Use CUBE to calculate all averages in a single result. A NULL value in a field indicates that the field is not part of that grouping.
* Added queries to show aggregations for typology and country separately.
* */


SELECT
    typology,
    country,
    AVG(
        total_amount_in_eur
    ) AS avg_total_amount_in_eur
FROM
    dbt.mart_store
GROUP BY
    cube (
        typology,
        country
    )
ORDER BY
    typology,
    country;


SELECT
    typology,
    AVG(
        total_amount_in_eur
    ) AS avg_total_amount_in_eur
FROM
    dbt.mart_store
GROUP BY
    typology
ORDER BY
    typology;


SELECT
    country,
    AVG(
        total_amount_in_eur
    ) AS avg_total_amount_in_eur
FROM
    dbt.mart_store
GROUP BY
    country
ORDER BY
    country;


------------------------------------------------------------------
-- Percentage of transactions per device type


WITH transactions_per_device_type AS (
    SELECT
        type,
        SUM(accepted_transaction_cnt) AS accepted_transaction_cnt
    FROM
        dbt.mart_device
    GROUP BY
        type
),
total_transactions AS (
    SELECT
        SUM(accepted_transaction_cnt) AS total_cnt
    FROM
        dbt.mart_device
)
SELECT
    dt.type,
    dt.accepted_transaction_cnt,
    t.total_cnt,
    ROUND(
        dt.accepted_transaction_cnt * 100.0 / t.total_cnt,
        4
    ) AS pct_of_total
FROM
    transactions_per_device_type dt
    CROSS JOIN total_transactions t
ORDER BY
    type;


-- Another appproach
    WITH pre AS (
        SELECT
            type,
            SUM(accepted_transaction_cnt) AS accepted_transaction_cnt
        FROM
            dbt.mart_device
        GROUP BY
            type
    )
SELECT
    type,
    accepted_transaction_cnt,
    SUM(accepted_transaction_cnt) over(
        PARTITION BY 1
    ) AS total_cnt,
    ROUND(
        accepted_transaction_cnt * 100.0 / SUM(accepted_transaction_cnt) over(
            PARTITION BY 1
        ),
        4
    ) AS pct_of_total
FROM
    pre
ORDER BY
    type;


------------------------------------------------------------------
-- Average time for a store to perform its first 5 transactions
/*
* Assumptions:
* * Only successful (accepted) transactions are considered.
* * The transaction date reflects when it happened (happened_at), not when it was created.
* * "Average" refers to the average, not the median (p50).
* Notes:
* * Additional fields have been added to mart_store in case we want to track metrics differently—for example, based on the creation date or including all transactions, not just accepted ones.
* * In the current dataset, a store's transaction date can be earlier than its creation date. This is due to generated test data; in real data, this would not occur.
* * Consequently, the average time for a store to perform its first five transactions can appear negative.
* * Stores with fewer than five transactions have been excluded from the analysis.
*/


SELECT
    id,
    name,
    AVG(days_to_5th_transaction_happened) AS avg_days_to_5th_transaction_happened
FROM
    dbt.mart_store
WHERE
    accepted_transaction_cnt >= 5
GROUP BY
    id,
    name
ORDER BY
    AVG(days_to_5th_transaction_happened) DESC;


------------------------------------------------------------------
