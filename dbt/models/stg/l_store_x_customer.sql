SELECT
    MD5(CAST(id AS VARCHAR)) AS store_id,
    MD5(CAST(customer_id AS VARCHAR)) AS customer_id
FROM
    {{ source(
        'dbt_raw',
        'store'
    ) }}
