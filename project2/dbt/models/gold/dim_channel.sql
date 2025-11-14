{{ config(
    materialized='view'
) }}

select
    channel_sk,
    natural_channel_id,
    title,
    description,
    custom_url,
    hidden_subscribers,
    published_at,
    country,
    default_language,
    uploads_playlist_id,
    topics,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('snapshot_dim_channel') }}