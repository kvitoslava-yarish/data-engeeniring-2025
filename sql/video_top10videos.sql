SELECT
    video_id,
    title,
    view_count,
    like_count,
    comment_count,
    topic_categories
FROM videos
WHERE
    topic_categories ILIKE '%music%'
ORDER BY view_count DESC
    LIMIT 10