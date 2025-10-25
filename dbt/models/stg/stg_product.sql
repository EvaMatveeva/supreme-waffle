SELECT
    DISTINCT MD5(CONCAT(product_sku, '~', product_name)) AS id,
    product_sku,
    product_name
FROM
    {{ source(
        'dbt_raw',
        'transaction'
    ) }}
