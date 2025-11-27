{% snapshot snapshot_bridge_channel_keyword %}

{{
    config(
        target_schema='youtube_gold',
        strategy='check',
        unique_key='channel_sk || keyword_id',
        check_cols=['channel_sk', 'keyword_id', 'loaded_at'],
        invalidate_hard_deletes=True
    )
}}

WITH source_data AS (
    SELECT
        channel_id,
        trimBoth(arrayJoin(splitByChar(',', keywords))) AS keyword_name,
        loaded_at
    FROM {{ ref('stg_channels') }}
    WHERE keywords IS NOT NULL
      AND keywords != ''
    ),

    joined_to_dims AS (
SELECT
    dim_c.channel_sk,
    dim_k.keyword_id,
    s.loaded_at AS loaded_at
FROM source_data AS s
    INNER JOIN {{ ref('snapshot_dim_channel') }} AS dim_c
ON s.channel_id = dim_c.natural_channel_id
    INNER JOIN {{ ref('dim_keyword') }} AS dim_k
    ON s.keyword_name = dim_k.keyword
WHERE s.loaded_at >= dim_c.dbt_valid_from
  AND s.loaded_at < coalesce(dim_c.dbt_valid_to, toDateTime('2999-12-31 00:00:00'))
    )

SELECT
    channel_sk,
    keyword_id,
    max(loaded_at) AS loaded_at
FROM joined_to_dims
GROUP BY channel_sk, keyword_id

{% endsnapshot %}
