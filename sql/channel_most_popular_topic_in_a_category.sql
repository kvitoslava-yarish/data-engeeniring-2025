SELECT t.topic AS topic_url, SUM(c.subscribers) AS total_subscribers
FROM youtube_channels c
         JOIN LATERAL unnest(c.topics) AS t(topic) ON TRUE
WHERE LOWER(t.topic) LIKE '%sport%'
GROUP BY t.topic
ORDER BY total_subscribers DESC
    LIMIT 1;
