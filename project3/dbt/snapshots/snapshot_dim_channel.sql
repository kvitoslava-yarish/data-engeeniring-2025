{% snapshot snapshot_dim_channel %}

{{
    config(
      strategy='check',
      unique_key='natural_channel_id',
      check_cols=[
          'title', 'description', 'custom_url', 'hidden_subscribers',
          'country', 'default_language', 'topics'
      ],
      invalidate_hard_deletes=True
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['channel_id']) }} as channel_sk,
    channel_id as natural_channel_id,
    title,
    description,
    custom_url,
    hidden_subscribers,
    published_at,
    country,
    default_language,
    uploads_playlist_id,
    topics,
    loaded_at
from {{ ref('stg_channels') }}

{% endsnapshot %}