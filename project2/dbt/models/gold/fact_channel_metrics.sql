{{ config(
    database = 'youtube_analytics',
    materialized='incremental',
    unique_key='id'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['stg.channel_id', 'stg.date_key']) }} as id,
    dim.channel_sk,
    stg.date_key,
    stg.subscribers,
    stg.total_views as views,
    stg.total_videos as videos
from {{ ref('stg_channels') }} as stg
         join {{ ref('dim_channel') }} as dim
              on stg.channel_id = dim.natural_channel_id
                  and dim.is_current = true

    {% if is_incremental() %}
where stg.date_key > (select max(date_key) from {{ this }})
{% endif %}