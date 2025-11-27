{{ config(
    materialized = 'view'
) }}

WITH latest_channel_metrics AS (
    SELECT
        channel_sk,
        subscribers,
        ROW_NUMBER() OVER(PARTITION BY channel_sk ORDER BY date_key DESC) AS rn
    FROM {{ ref('fact_channel_metrics') }}
),
current_channel_keywords AS (
    SELECT
        bc.channel_sk,
        dk.keyword
    FROM {{ ref('bridge_channel_keyword') }} AS bc
    JOIN {{ ref('dim_keyword') }} AS dk
        ON bc.keyword_id = dk.keyword_id
    WHERE bc.is_current = true
)
SELECT
    cck.keyword,
    SUM(lcm.subscribers) AS total_subscribers,
    COUNT(DISTINCT cck.channel_sk) AS channel_count
FROM latest_channel_metrics AS lcm
JOIN current_channel_keywords AS cck
    ON lcm.channel_sk = cck.channel_sk
WHERE
    lcm.rn = 1
GROUP BY cck.keyword
ORDER BY total_subscribers DESC
LIMIT 10