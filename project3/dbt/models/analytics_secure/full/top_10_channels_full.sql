{{ config(
    materialized='view',
    database='youtube_analytics'
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
    lcm.views
FROM latest_channel_metrics AS lcm
JOIN {{ ref('dim_channel') }} AS dc
    ON lcm.channel_sk = dc.channel_sk
WHERE
    lcm.rn = 1
    AND dc.is_current = 1
ORDER BY lcm.views DESC
LIMIT 10
