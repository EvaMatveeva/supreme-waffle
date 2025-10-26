SELECT
    MD5(CAST(id AS VARCHAR)) AS id,
    id AS device_src_id,
    type,
    MD5(CAST(store_id AS VARCHAR)) AS store_id
FROM
    {{ source(
        'dbt_raw',
        'device'
    ) }}
