SELECT
    category_id,
    AVG(EXTRACT(EPOCH FROM duration)) / 60 AS avg_duration_minutes,
    AVG(view_count) AS avg_views,
    AVG(like_count) AS avg_likes,
    COUNT(*) AS video_count
FROM videos
WHERE duration IS NOT NULL
GROUP BY category_id
ORDER BY avg_views DESC;