SELECT
    trim(tag) AS tag,
    SUM(COALESCE(like_count, 0) + COALESCE(comment_count, 0))::float /
    NULLIF(SUM(view_count), 0) AS engagement_rate
FROM (
         SELECT
             unnest(string_to_array(tags, ',')) AS tag,
             like_count,
             comment_count,
             view_count
         FROM videos_metadata
         WHERE tags IS NOT NULL
     ) t
GROUP BY tag
ORDER BY engagement_rate DESC
    LIMIT 20;