WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        like_count,
        comment_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_video_performance
)
SELECT
    dv.title,
    lvm.view_count,
    lvm.like_count,
    lvm.comment_count,
    dv.topic_categories
FROM latest_video_metrics AS lvm
JOIN analytics_youtube.dim_video AS dv
    ON lvm.video_sk = dv.video_sk
WHERE
    lvm.rn = 1  -- Get only the latest metrics for each video
    AND dv.is_current = true
    AND lower(dv.topic_categories) LIKE '%music%'
ORDER BY lvm.view_count DESC
LIMIT 10;
