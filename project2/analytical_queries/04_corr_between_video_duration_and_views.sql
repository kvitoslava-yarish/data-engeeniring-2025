
WITH latest_video_metrics AS (
    SELECT
        video_sk,
        view_count,
        ROW_NUMBER() OVER(PARTITION BY video_sk ORDER BY date_key DESC) as rn
    FROM analytics_youtube.fact_video_performance
),
videos_with_duration_sec AS (
    SELECT
        video_sk,
        is_current,
        (
            toInt64(coalesce(regexpExtract(duration, 'PT(?:(\d+)H)?', 1), '0')) * 3600
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)M)?', 1), '0')) * 60
            + toInt64(coalesce(regexpExtract(duration, 'PT(?:.*?(\d+)S)?', 1), '0'))
        ) AS duration_seconds
    FROM analytics_youtube.dim_video
    WHERE duration IS NOT NULL
)
SELECT
    corr(vds.duration_seconds, lvm.view_count) AS duration_view_correlation
FROM latest_video_metrics AS lvm
JOIN videos_with_duration_sec AS vds
    ON lvm.video_sk = vds.video_sk
WHERE
    lvm.rn = 1
    AND vds.is_current = true
    AND lvm.view_count IS NOT NULL;
