WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_video_performance
)
SELECT
    dv.category_name,
    COUNT(DISTINCT dv.video_sk) AS video_count,
    SUM(lvm.view_count) AS total_views,
    AVG(lvm.view_count) AS avg_views
FROM latest_video_metrics AS lvm
JOIN analytics_youtube.dim_video AS dv
    ON lvm.video_sk = dv.video_sk
WHERE
    lvm.rn = 1
    AND dv.is_current = true
GROUP BY dv.category_name
ORDER BY total_views DESC;
