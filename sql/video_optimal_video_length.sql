WITH durations AS (
    SELECT
        categoryId,
        videoId,
        EXTRACT(EPOCH FROM duration),
        viewCount,
        (COALESCE(likeCount, 0) + COALESCE(commentCount, 0))::float AS total_interactions
    FROM videos_metadata
    WHERE duration IS NOT NULL AND viewCount > 0
)
SELECT
    ROUND(duration_seconds / 60, 0) AS duration_minutes,
    AVG(total_interactions / viewCount) AS avg_engagement_rate
FROM durations
WHERE categoryId IN (/* insert category ID(s) for Y here, e.g. 25 for News */)
GROUP BY ROUND(duration_seconds / 60, 0)
ORDER BY avg_engagement_rate DESC
    LIMIT 10;
