WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        like_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_video_performance
),
videos_with_duration_sec AS (
    SELECT
        video_sk,
        is_current,
        category_name,
        (
            toInt64(coalesce(regexpExtract(duration, 'PT(?:(\d+)H)?', 1), '0')) * 3600
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)M)?', 1), '0')) * 60
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)S)?', 1), '0'))
        ) AS duration_seconds
    FROM analytics_youtube.dim_video
    WHERE duration IS NOT NULL
)
SELECT
    vds.category_name,
    AVG(vds.duration_seconds) / 60 AS avg_duration_minutes,
    AVG(lvm.view_count) AS avg_views,
    AVG(lvm.like_count) AS avg_likes,
    COUNT(DISTINCT vds.video_sk) AS video_count
FROM latest_video_metrics AS lvm
JOIN videos_with_duration_sec AS vds
    ON lvm.video_sk = vds.video_sk
WHERE
    lvm.rn = 1
    AND vds.is_current = true
GROUP BY vds.category_name
ORDER BY avg_views DESC;
