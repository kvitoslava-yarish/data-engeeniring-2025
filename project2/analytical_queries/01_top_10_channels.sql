WITH latest_channel_metrics AS (
    SELECT
        channel_sk,
        subscribers,
        views,
        -- Assigns a row number to each channel's metric history, starting with 1 for the newest date
        ROW_NUMBER() OVER(PARTITION BY channel_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_channel_metrics
)
SELECT
    dc.title,
    lcm.subscribers,
    lcm.views
FROM latest_channel_metrics AS lcm
JOIN analytics_youtube.dim_channel AS dc
    ON lcm.channel_sk = dc.channel_sk
JOIN analytics_youtube.bridge_channel_keyword AS bck
    ON dc.channel_sk = bck.channel_sk
JOIN analytics_youtube.dim_keyword AS dk
    ON bck.keyword_id = dk.keyword_id
WHERE
    lcm.rn = 1  -- Filter for *only* the latest metrics
    AND dc.is_current = true
    AND bck.is_current = true
    AND lower(dk.keyword) LIKE '%music%' -- Filter on the clean keyword
GROUP BY dc.title, lcm.subscribers, lcm.views -- Group because a channel can have multiple keywords
ORDER BY lcm.subscribers DESC
LIMIT 10;
