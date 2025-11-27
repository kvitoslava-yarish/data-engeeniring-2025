{{ config(
    materialized='incremental',
    unique_key='channel_id || date_key',
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
    {% set max_ts_query %}
        SELECT max(loaded_at) AS max_ts FROM {{ this }}
    {% endset %}
    {% set results = run_query(max_ts_query) %}
    {% if results and results[0]['max_ts'] is not none %}
        {% set max_ts = results[0]['max_ts'] %}
    {% else %}
        {% set max_ts = '1970-01-01 00:00:00' %}
    {% endif %}
{% else %}
    {% set max_ts = '1970-01-01 00:00:00' %}
{% endif %}

WITH daily_ranked AS (

    SELECT
        channelId::String AS channel_id,
        nullif(trim(title), '') AS title,
        nullif(trim(description), '') AS description,
        nullif(trim(customUrl), '') AS custom_url,
        subscribers,
        views AS total_views,
        videos AS total_videos,
        hiddenSubscribers AS hidden_subscribers,

        publishedAt AS published_at,
        nullif(trim(country), '') AS country,
        nullif(trim(defaultLanguage), '') AS default_language,

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
    WHERE ts > parseDateTimeBestEffort('{{ max_ts }}')
),

cleaned AS (

    SELECT
        channel_id,
        title,
        description,
        custom_url,

        case when subscribers >= 0 then subscribers else NULL end AS subscribers,
        case when total_views >= 0 then total_views else NULL end AS total_views,
        case when total_videos >= 0 then total_videos else NULL end AS total_videos,
        case when hidden_subscribers >= 0 then hidden_subscribers else NULL end AS hidden_subscribers,

        case
            when published_at <= now()
            then published_at
            else NULL
        end AS published_at,

        country,
        default_language,
        keywords,
        uploads_playlist_id,
        topics,

        ts AS loaded_at,
        date_key,

        rn

    FROM daily_ranked

    WHERE NOT (
        subscribers IS NULL
        AND total_views IS NULL
        AND total_videos IS NULL
        AND hidden_subscribers IS NULL
        AND title IS NULL
    )
)

SELECT *
FROM cleaned
WHERE rn = 1
