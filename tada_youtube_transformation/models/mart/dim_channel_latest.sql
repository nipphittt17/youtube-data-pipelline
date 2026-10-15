{{ config(materialized='table') }}

SELECT
    RECORDED_AT,
    USERNAME,
    TITLE,
    PUBLISHED_AT,
    TOTAL_VIEW,
    TOTAL_SUBSCRIBER,
    TOTAL_VIDEO
FROM {{ source('snapshot', 'stg_channel_daily_snapshot') }}
-- One row per channel: the most recent snapshot.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY USERNAME
    ORDER BY RECORDED_AT DESC
) = 1

