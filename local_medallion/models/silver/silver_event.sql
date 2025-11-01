{{ config(materialized='table') }}

SELECT
    event_id,
    STRFTIME('%Y-%m-%d %H:%M:%S', event_time) AS event_at_ts,
    CAST(user_id AS BIGINT) AS user_id,
    JSON_EXTRACT(event_payload, '$.event_name') AS event_name,
    JSON_EXTRACT(event_payload, '$.platform') AS platform,
    JSON_EXTRACT(event_payload, '$.parameter_name') AS parameter_name,
    JSON_EXTRACT(event_payload, '$.parameter_value') AS parameter_value,
    CAST(STRFTIME('%Y', event_time) AS INTEGER) AS event_year

FROM
    -- reads from models/bronze/bronze_sources.yml
    {{ source('main', 'bronze_event') }}
WHERE
    event_time IS NOT NULL