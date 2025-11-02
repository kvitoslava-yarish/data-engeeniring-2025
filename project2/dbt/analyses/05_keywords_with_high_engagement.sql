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
video_tag_engagement AS (
    SELECT
        lvm.video_sk,
        lvm.total_interactions / lvm.view_count AS engagement_rate
    FROM latest_video_metrics AS lvm
    WHERE lvm.rn = 1
)
SELECT
    dt.tag_name,
    AVG(vte.engagement_rate) AS avg_engagement_rate,
    COUNT(DISTINCT vte.video_sk) AS video_count
FROM video_tag_engagement AS vte
JOIN {{ ref('bridge_video_tag') }} AS bvt
    ON vte.video_sk = bvt.video_sk
JOIN {{ ref('dim_tag') }} AS dt
    ON bvt.tag_id = dt.tag_id
WHERE
    bvt.is_current = true
GROUP BY dt.tag_name
HAVING video_count >= 10
ORDER BY avg_engagement_rate DESC
LIMIT 10