WITH engagement AS (
    SELECT
        unnest(string_to_array(tags, ',')) AS tag,
        (COALESCE(like_count,0) + COALESCE(comment_count,0)) AS engagement_rate
    FROM videos_metadata
    WHERE tags IS NOT NULL
)
SELECT
    trim(tag) AS tag,
    AVG(engagement_rate) AS avg_engagement,
FROM engagement
GROUP BY tag
ORDER BY avg_engagement DESC
    LIMIT 20;