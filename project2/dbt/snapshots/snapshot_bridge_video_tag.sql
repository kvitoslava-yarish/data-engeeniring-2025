{% snapshot snapshot_bridge_video_tag %}

{{
    config(
      target_schema='youtube_analytics',
      unique_key="video_sk || '-' || tag_id",
      strategy='timestamp',
      updated_at='loaded_at',
      invalidate_hard_deletes=True
    )
}}

WITH source_data AS (
    SELECT
        video_id,
        trimBoth(arrayJoin(splitByChar(',', tags))) AS tag_name,
        loaded_at
    FROM {{ ref('stg_videos') }}
    WHERE tags IS NOT NULL
      AND tags != ''
    ),

    joined_to_dims AS (
SELECT
    dim_v.video_sk,
    dim_t.tag_id,
    s.loaded_at AS loaded_at
FROM source_data AS s
    INNER JOIN {{ ref('snapshot_dim_video') }} AS dim_v
ON s.video_id = dim_v.video_id
    INNER JOIN {{ ref('dim_tag') }} AS dim_t
    ON s.tag_name = dim_t.tag_name
WHERE s.loaded_at >= dim_v.dbt_valid_from
  AND s.loaded_at < coalesce(dim_v.dbt_valid_to, toDateTime('2999-12-31 00:00:00'))
    )

SELECT
    video_sk,
    tag_id,
    max(loaded_at) AS loaded_at
FROM joined_to_dims
GROUP BY video_sk, tag_id

{% endsnapshot %}