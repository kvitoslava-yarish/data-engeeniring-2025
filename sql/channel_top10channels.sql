SELECT c.channel_id, c.title, c.subscribers, c.views
FROM youtube_channels c
         JOIN LATERAL unnest(string_to_array(c.topics, ',')) AS t(topic) ON TRUE
WHERE LOWER(trim(t.topic)) LIKE '%music%'
GROUP BY c.channel_id, c.title, c.subscribers, c.views
ORDER BY c.subscribers DESC
    LIMIT 10;