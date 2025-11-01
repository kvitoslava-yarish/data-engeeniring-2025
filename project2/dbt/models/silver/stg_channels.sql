{{ config(
    schema='staging_youtube',
    materialized='incremental',
    unique_key='channel_id || date_key',
    on_schema_change='sync_all_columns'
) }}

WITH daily_ranked AS (
    SELECT
        channelId AS channel_id,
        title,
        description,
        customUrl AS custom_url,
        subscribers,
        views AS total_views,
        videos AS total_videos,
        hiddenSubscribers AS hidden_subscribers,
        publishedAt AS published_at,
        country,
        defaultLanguage AS default_language,
        keywords,
        uploadsPlaylistId AS uploads_playlist_id,
        topics,
        ts,
        toDate(ts) AS date_key,
        row_number() OVER (
            PARTITION BY channelId, toDate(ts)
            ORDER BY ts DESC
        ) AS rn
    FROM {{ source('raw_youtube', 'channels') }}
    {% if is_incremental() %}
      WHERE ts > (SELECT coalesce(max(ts), toDateTime('1970-01-01')) FROM {{ this }})
    {% endif %}
)

SELECT
    channel_id,
    title,
    description,
    custom_url,
    subscribers,
    total_views,
    total_videos,
    hidden_subscribers,
    published_at,
    country,
    default_language,
    keywords,
    uploads_playlist_id,
    topics,
    ts AS loaded_at,
    date_key
FROM daily_ranked
WHERE rn = 1;
