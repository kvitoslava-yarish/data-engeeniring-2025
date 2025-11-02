WITH latest_channel_metrics AS (
    SELECT
        channel_sk,
        subscribers,
        ROW_NUMBER() OVER(PARTITION BY channel_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_channel_metrics
)
SELECT
    dk.keyword,
    SUM(lcm.subscribers) AS total_subscribers,
    COUNT(DISTINCT dc.channel_sk) AS channel_count
FROM latest_channel_metrics AS lcm
JOIN analytics_youtube.dim_channel AS dc
    ON lcm.channel_sk = dc.channel_sk
JOIN analytics_youtube.bridge_channel_keyword AS bck
    ON dc.channel_sk = bck.channel_sk
JOIN analytics_youtube.dim_keyword AS dk
    ON bck.keyword_id = dk.keyword_id
WHERE
    lcm.rn = 1
    AND dc.is_current = true
    AND bck.is_current = true
GROUP BY dk.keyword
ORDER BY total_subscribers DESC
LIMIT 10;
