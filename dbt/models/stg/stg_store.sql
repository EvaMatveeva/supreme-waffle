SELECT
    MD5(CAST(id AS VARCHAR)) AS id,
    id AS store_src_id,
    name,
    MD5(CONCAT(address, '~', city, '~', country)) AS location_id,
    CAST(created_at AS TIMESTAMP) AS created_at,
    typology
FROM
    {{ source(
        'dbt_raw',
        'store'
    ) }}
