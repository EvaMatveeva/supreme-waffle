SELECT
    MD5(CAST(id AS VARCHAR)) AS device_id,
    MD5(CAST(store_id AS VARCHAR)) AS store_id
FROM
    {{ source(
        'dbt_raw',
        'device'
    ) }}
