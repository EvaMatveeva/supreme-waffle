SELECT 
    DISTINCT MD5(CAST(customer_id AS VARCHAR)) AS id,
    customer_id AS customer_src_id
FROM
    {{ source(
        'dbt_raw',
        'store'
    ) }}
