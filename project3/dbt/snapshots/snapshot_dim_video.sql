{% snapshot snapshot_dim_video %}

{{
    config(
      strategy='check',
      unique_key='video_id',
      check_cols=[
          'title', 'category_id', 'duration', 'definition', 'caption',
          'licensed_content', 'privacy_status', 'license', 'topic_categories',
          'region_restriction'
      ],
      invalidate_hard_deletes=True
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['v.video_id']) }} as video_sk,
    v.video_id,
    v.channel_id,
    v.title,
    v.published_at,
    v.category_id,
    v.duration,
    v.definition,
    v.caption,
    v.licensed_content,
    v.privacy_status,
    v.license,
    v.region_restriction,
    v.topic_categories,
    v.loaded_at
from {{ ref('stg_videos') }} as v

{% endsnapshot %}