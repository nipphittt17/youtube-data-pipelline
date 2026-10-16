SELECT
    recorded_at,
    vdo_id,
    vdo_title,
    published_at,
    vdo_description,
    vdo_category_id,
    vdo_tags,
    vdo_default_language,
    channel_id,
    thumbnails_url,
    vdo_duration,
    vdo_definition,
    vdo_caption
FROM {{ source('snapshot', 'stg_video_daily_snapshot') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY vdo_id
    ORDER BY recorded_at DESC
) = 1