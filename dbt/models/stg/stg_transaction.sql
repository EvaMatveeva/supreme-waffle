SELECT
    MD5(CAST(id AS VARCHAR)) AS id,
    id AS transaction_src_id,
    MD5(CAST(device_id AS VARCHAR)) AS device_id,
    MD5(CONCAT(product_sku, product_name)) AS product_id,
    amount AS amount_in_eur,
    status,
    '**** **** **** ' || RIGHT(card_number, 4) AS card_masked,
    -- cvv,
    CAST(created_at AS TIMESTAMP) AS created_at,
    CAST(happened_at AS TIMESTAMP) AS happened_at
FROM
    {{ source(
        'dbt_raw',
        'transaction'
    ) }}
