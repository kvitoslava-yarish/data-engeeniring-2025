{{ config(
    materialized = 'view'
) }}

WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        (COALESCE(like_count, 0) + COALESCE(comment_count, 0)) AS total_interactions,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_video_performance') }}
    WHERE view_count > 0
),
videos_with_duration_sec AS (
    SELECT
        video_sk,
        is_current,
        (
            toInt64(coalesce(regexpExtract(duration, 'PT(?:(\d+)H)?', 1), '0')) * 3600
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)M)?', 1), '0')) * 60
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)S)?', 1), '0'))
        ) AS duration_seconds
    FROM {{ ref('dim_video') }}
    WHERE duration IS NOT NULL AND is_current = true
)
SELECT
    -- This buckets durations into 1-minute groups
    round(vds.duration_seconds / 60, 0) AS duration_minutes,
    AVG(lvm.total_interactions / lvm.view_count) AS avg_engagement_rate,
    COUNT(DISTINCT vds.video_sk) AS video_count
FROM latest_video_metrics AS lvm
JOIN videos_with_duration_sec AS vds
    ON lvm.video_sk = vds.video_sk
WHERE
    lvm.rn = 1
GROUP BY duration_minutes
HAVING video_count > 10 -- Filter for statistical significance
ORDER BY avg_engagement_rate DESC
LIMIT 10