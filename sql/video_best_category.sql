SELECT
    categoryId,
    COUNT(videoId) AS video_count,
    SUM(viewCount) AS total_views,
    AVG(viewCount) AS avg_views
FROM videos_metadata
WHERE viewCount IS NOT NULL
GROUP BY categoryId
ORDER BY total_views DESC;
