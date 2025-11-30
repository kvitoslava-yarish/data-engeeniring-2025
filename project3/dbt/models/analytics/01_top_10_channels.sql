{{ config(
    materialized = 'view',
    database = 'youtube_queries'
) }}

WITH latest_channel_metrics AS (
    SELECT
        channel_sk,
        views,
        ROW_NUMBER() OVER(PARTITION BY channel_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_channel_metrics') }}
)
SELECT
    dc.title,
    SUM(lcm.views) AS total_views
FROM latest_channel_metrics AS lcm
JOIN {{ ref('dim_channel') }} AS dc
    ON lcm.channel_sk = dc.channel_sk
WHERE
    lcm.rn = 1
    AND dc.is_current = true
GROUP BY dc.title
ORDER BY total_views DESC
LIMIT 10
