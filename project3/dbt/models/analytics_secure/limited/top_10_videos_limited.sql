{{ config(
    materialized='view',
    database='youtube_analytics'
) }}

WITH latest AS (
    SELECT
        video_sk,
        view_count,
        ROW_NUMBER() OVER (PARTITION BY video_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_video_performance') }}
)

SELECT
    toString(cityHash64(dv.title)) AS video_title_masked,
    '***' AS category_title_masked,
    l.view_count
FROM latest l
JOIN {{ ref('dim_video') }} dv
    ON l.video_sk = dv.video_sk
WHERE l.rn = 1
ORDER BY l.view_count DESC
LIMIT 10