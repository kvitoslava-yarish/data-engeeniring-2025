{{ config(
    materialized = 'view'
) }}

WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_video_performance') }}
)
SELECT
    dv.video_title,
    dv.category_name,
    SUM(lvm.view_count) AS total_views
FROM latest_video_metrics AS lvm
JOIN {{ ref('dim_video') }} AS dv
    ON lvm.video_sk = dv.video_sk
WHERE
    lvm.rn = 1
    AND dv.is_current = true
GROUP BY dv.video_title, dv.category_name
ORDER BY total_views DESC
LIMIT 10;
