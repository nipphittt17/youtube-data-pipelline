WITH
    vdo_snapshot AS (
        SELECT
            recorded_at,
            vdo_id,
            published_at,

            vdo_view_cnt,
            vdo_like_cnt,
            vdo_comment_cnt
        FROM {{ source('snapshot', 'stg_video_daily_snapshot') }}
    ),

    yesterday_stats AS (
        SELECT
            recorded_at,
            vdo_id,
            published_at,

            vdo_view_cnt AS td_view,
            vdo_like_cnt AS td_like,
            vdo_comment_cnt AS td_comment,

            LAG(vdo_view_cnt) OVER (
                PARTITION BY vdo_id
                ORDER BY recorded_at
            ) AS ytd_view,

            LAG(vdo_like_cnt) OVER (
                PARTITION BY vdo_id
                ORDER BY recorded_at
            ) AS ytd_like,

            LAG(vdo_comment_cnt) OVER (
                PARTITION BY vdo_id
                ORDER BY recorded_at
            ) AS ytd_comment
        FROM vdo_snapshot
    ),


    performance_metrics AS (
        SELECT
            *,

        td_view - ytd_view AS daily_view_growth,

        (
            (td_view::FLOAT / NULLIF(ytd_view, 0)) - 1
        ) * 100 AS daily_view_growth_pct,

        td_like - ytd_like AS daily_like_growth,

        (
            (td_like::FLOAT / NULLIF(ytd_like, 0)) - 1
        ) * 100 AS daily_like_growth_pct,
        
        td_comment - ytd_comment AS daily_comment_growth,

        (
            (td_comment::FLOAT / NULLIF(ytd_comment, 0)) - 1
        ) * 100 AS daily_comment_growth_pct,

        td_like::FLOAT / NULLIF(td_view, 0) AS like_view_ratio,

        td_comment::FLOAT / NULLIF(td_view, 0) AS comment_view_ratio,

        (td_like::FLOAT + td_comment::FLOAT) / NULLIF(td_view, 0) AS interaction_view_ratio

        FROM yesterday_stats
    )

SELECT *,
    DATEDIFF( 'day',
        TO_DATE(published_at),
        TO_DATE(recorded_at)
    ) AS video_age_days
FROM performance_metrics

ORDER BY
    vdo_id