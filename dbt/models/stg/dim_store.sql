SELECT
    MD5(CAST(id AS VARCHAR)) AS id,
    id AS store_src_id,
    name,
    address,
    city,
    country,
    CAST(created_at AS TIMESTAMP) AS created_at,
    typology,
    MD5(CAST(customer_id AS VARCHAR)) AS customer_id
FROM
    {{ source(
        'dbt_raw',
        'store'
    ) }}
