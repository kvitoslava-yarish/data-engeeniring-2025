{{ config(
    schema='analytics_youtube',
    materialized='incremental',
    unique_key='id'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['stg.video_id', 'stg.date_key']) }} as id,
    dim_v.video_sk,
    dim_c.channel_sk,
    stg.date_key,
    stg.view_count,
    stg.like_count,
    stg.comment_count

from {{ ref('stg_videos') }} as stg

         join {{ ref('dim_video') }} as dim_v
              on stg.video_id = dim_v.video_id
                  and dim_v.is_current = true

         join {{ ref('dim_channel') }} as dim_c
              on stg.channel_id = dim_c.natural_channel_id
                  and dim_c.is_current = true

    {% if is_incremental() %}
where stg.date_key > (select max(date_key) from {{ this }})
{% endif %}