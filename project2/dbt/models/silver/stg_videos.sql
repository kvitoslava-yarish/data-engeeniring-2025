{{ config(
    materialized='incremental',
    unique_key='video_id || date_key',
    on_schema_change='sync_all_columns'
) }}

{% if is_incremental() %}
    {% set relation_exists_query %}
        SELECT count() > 0 AS exists
        FROM system.tables
        WHERE database = currentDatabase()
          AND name = '{{ this.identifier }}'
    {% endset %}

    {% set exists = run_query(relation_exists_query) %}

    {% if exists and exists[0]['exists'] == 1 %}
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
{% else %}
    {% set max_ts = '1970-01-01 00:00:00' %}
{% endif %}


WITH ranked AS (

    SELECT
        videoId::String AS video_id,
        channelId::String AS channel_id,

        nullif(trim(title), '') AS title,

        publishedAt AS published_at,

        tags,
        categoryId AS category_id,

        duration,
        definition,
        caption,
        licensedContent AS licensed_content,
        regionRestriction AS region_restriction,

        -- raw metrics, cleaned later
        viewCount AS view_count,
        likeCount AS like_count,
        commentCount AS comment_count,

        privacyStatus AS privacy_status,
        license,
        topicCategories AS topic_categories,

        ts,
        toDate(ts) AS date_key,

        row_number() OVER (
            PARTITION BY videoId, toDate(ts)
            ORDER BY ts DESC
        ) AS rn

    FROM {{ source('raw_youtube', 'videos') }}
    WHERE ts > parseDateTimeBestEffort('{{ max_ts }}')
),

cleaned AS (

    SELECT
        video_id,
        channel_id,
        title,
        case
            when published_at <= now() then published_at
            else NULL
        end AS published_at,

        tags,
        category_id,

        duration,
        definition,
        caption,
        licensed_content,
        region_restriction,

        case when view_count >= 0 then view_count else NULL end AS view_count,
        case when like_count >= 0 then like_count else NULL end AS like_count,
        case when comment_count >= 0 then comment_count else NULL end AS comment_count,

        privacy_status,
        license,
        topic_categories,

        ts AS loaded_at,
        date_key,
        rn

    FROM ranked

    WHERE NOT (
        title IS NULL
        AND view_count IS NULL
        AND like_count IS NULL
        AND comment_count IS NULL
    )
)

SELECT *
FROM cleaned
WHERE rn = 1;
