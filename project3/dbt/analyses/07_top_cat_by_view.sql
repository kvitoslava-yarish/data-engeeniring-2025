{{ config(
    materialized = 'view'
) }}

WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_video_performance') }}
),
current_videos AS (
    SELECT
        video_sk,
        category_name
    FROM {{ ref('dim_video') }}
    WHERE is_current = true
)
SELECT
    cv.category_name,
    SUM(lvm.view_count) AS total_views,
    COUNT(DISTINCT cv.video_sk) AS video_count
FROM latest_video_metrics AS lvm
JOIN current_videos AS cv
    ON lvm.video_sk = cv.video_sk
WHERE
    lvm.rn = 1
GROUP BY cv.category_name
ORDER BY total_views DESC
LIMIT 10