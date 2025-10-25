SELECT
    MD5(CONCAT(address, '~', city, '~', country)) AS location_id,
    address,
    city,
    country
FROM
    {{ source(
        'dbt_raw',
        'store'
    ) }}
