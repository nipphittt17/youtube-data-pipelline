SELECT
    recorded_at,
    channel_id,
    username,
    channel_title,
    published_at
FROM {{ source('snapshot', 'stg_channel_daily_snapshot') }}
-- One row per channel: the most recent snapshot.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY channel_id
    ORDER BY recorded_at DESC
) = 1