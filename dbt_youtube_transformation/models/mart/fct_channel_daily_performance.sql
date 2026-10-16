WITH
    channel_snapshot AS (
        SELECT
            recorded_at,
            channel_id,
            published_at,
            total_view,
            total_subscriber,
            total_video
        FROM {{ source('snapshot', 'stg_channel_daily_snapshot') }}
    ),

    yesterday_stats AS (
        SELECT
            recorded_at,
            channel_id,
            published_at,
            total_view AS td_view,
            total_subscriber AS td_subscriber,
            total_video AS td_video,

            LAG(total_view) OVER (
                PARTITION BY channel_id
                ORDER BY recorded_at
            ) AS ytd_view,

            LAG(total_subscriber) OVER (
                PARTITION BY channel_id
                ORDER BY recorded_at
            ) AS ytd_subscriber,

            LAG(total_video) OVER (
                PARTITION BY channel_id
                ORDER BY recorded_at
            ) AS ytd_video
        FROM channel_snapshot
    ),

    performance_metrics AS (

        SELECT *,
            td_view - ytd_view AS daily_view_growth,
            (
                (td_view::FLOAT / NULLIF(ytd_view, 0)) - 1
            ) * 100 AS daily_view_growth_pct,

            td_subscriber - ytd_subscriber AS daily_subscriber_growth,

            (
                (td_subscriber::FLOAT / NULLIF(ytd_subscriber, 0)) - 1
            ) * 100 AS daily_subscriber_growth_pct,

            td_video - ytd_video AS daily_video_growth,

            (
                (td_video::FLOAT / NULLIF(ytd_video, 0)) - 1
            ) * 100 AS daily_video_growth_pct

        FROM yesterday_stats
    )

SELECT *
FROM performance_metrics
ORDER BY
    channel_id