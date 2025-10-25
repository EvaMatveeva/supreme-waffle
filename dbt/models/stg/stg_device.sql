SELECT
    MD5(CAST(id AS VARCHAR)) AS id,
    id AS device_src_id,
    type
FROM
    {{ source(
        'dbt_raw',
        'device'
    ) }}
