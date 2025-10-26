WITH t_device_x_store_x_customer AS (
    SELECT
        MD5(CAST(d.id AS VARCHAR)) AS device_id,
        MD5(CAST(s.id AS VARCHAR)) AS store_id,
        MD5(CAST(s.customer_id AS VARCHAR)) AS customer_id
    FROM
        {{ source(
            'dbt_raw',
            'device'
        ) }} d
         join     {{ source(
        'dbt_raw',
        'store'
    ) }} s on d.store_id = s.id
)
SELECT
    MD5(CAST(t.id AS VARCHAR)) AS id,
    t.id AS transaction_src_id,
    MD5(CAST(t.device_id AS VARCHAR)) AS device_id,
    MD5(CONCAT(t.product_sku, '~', t.product_name)) AS product_id,
    t.category_name,
    t.amount AS amount_in_eur,
    t.status,
    '**** **** **** ' || RIGHT(
        t.card_number,
        4
    ) AS card_masked,
    -- cvv,
    CAST(
        t.created_at AS TIMESTAMP
    ) AS created_at,
    CAST(
        t.happened_at AS TIMESTAMP
    ) AS happened_at,
    dsc.store_id,
    dsc.customer_id
FROM
    {{ source(
        'dbt_raw',
        'transaction'
    ) }}
    t
    LEFT JOIN t_device_x_store_x_customer dsc
    ON MD5(CAST(t.device_id AS VARCHAR)) = dsc.device_id
