WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        (COALESCE(like_count, 0) + COALESCE(comment_count, 0)) AS total_interactions
    FROM analytics_youtube.fact_video_performance
    WHERE 
        -- Get the most recent day's data, as engagement rate is a ratio.
        -- Or, use the ROW_NUMBER() CTE from other queries.
        date_key = (SELECT max(date_key) FROM analytics_youtube.fact_video_performance)
        AND view_count > 0
)
SELECT
    dt.tag_name,
    SUM(lvm.total_interactions) / SUM(lvm.view_count) AS engagement_rate,
    SUM(lvm.view_count) AS total_views
FROM latest_video_metrics AS lvm
JOIN analytics_youtube.bridge_video_tag AS bvt
    ON lvm.video_sk = bvt.video_sk
JOIN analytics_youtube.dim_tag AS dt
    ON bvt.tag_id = dt.tag_id
WHERE
    bvt.is_current = true
GROUP BY dt.tag_name
HAVING SUM(lvm.view_count) > 100000 -- Add a threshold to avoid noise from unpopular tags
ORDER BY engagement_rate DESC
LIMIT 20;
