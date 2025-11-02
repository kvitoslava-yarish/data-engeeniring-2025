{{ config(
    schema='staging_youtube',
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
        videoId AS video_id,
        channelId AS channel_id,
        title,
        publishedAt AS published_at,
        tags,
        categoryId AS category_id,
        duration,
        definition,
        caption,
        licensedContent AS licensed_content,
        regionRestriction AS region_restriction,
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

)

SELECT
    video_id,
    channel_id,
    title,
    published_at,
    tags,
    category_id,
    duration,
    definition,
    caption,
    licensed_content,
    region_restriction,
    view_count,
    like_count,
    comment_count,
    privacy_status,
    license,
    topic_categories,
    ts AS loaded_at,
    date_key
FROM ranked
WHERE rn = 1