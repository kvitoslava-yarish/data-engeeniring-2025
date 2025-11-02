{{ config(
    database = 'youtube_analytics',
    materialized='view'
) }}

select
    channel_sk,
    keyword_id,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('snapshot_bridge_channel_keyword') }}