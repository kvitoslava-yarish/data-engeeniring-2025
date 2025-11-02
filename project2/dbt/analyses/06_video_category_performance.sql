{{ config(
    materialized = 'view'
) }}

WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        like_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_video_performance') }}
),
videos_with_duration_sec AS (
    SELECT
        video_sk,
        is_current,
        category_name,
        (
            toInt64(coalesce(regexpExtract(duration, 'PT(?:(\d+)H)?', 1), '0')) * 3600
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)M)?', 1), '0')) * 60
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)S)?', 1), '0'))
        ) AS duration_seconds
    FROM {{ ref('dim_video') }}
    WHERE duration IS NOT NULL AND is_current = true
)
SELECT
    vds.category_name,
    AVG(vds.duration_seconds) / 60 AS avg_duration_minutes,
    SUM(lvm.view_count) AS total_views,
    SUM(lvm.like_count) AS total_likes,
    COUNT(DISTINCT vds.video_sk) AS video_count
FROM latest_video_metrics AS lvm
JOIN videos_with_duration_sec AS vds
    ON lvm.video_sk = vds.video_sk
WHERE
    lvm.rn = 1
GROUP BY vds.category_name
HAVING video_count >= 50
ORDER BY total_views DESC
