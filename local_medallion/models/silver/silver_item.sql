{{ config(materialized='table') }}

SELECT
    CAST(id AS BIGINT) AS item_id,
    name AS item_name,
    category,
    COALESCE(adjective, 'unknown') AS adjective,
    COALESCE(modifier, 'unknown') AS modifier,
    CAST(price AS DECIMAL(10, 2)) AS price,
    STRFTIME('%Y-%m-%d %H:%M:%S', created_at) AS created_at_ts

FROM
    -- reads from models/bronze/bronze_sources.yml
    {{ source('main', 'bronze_item') }}