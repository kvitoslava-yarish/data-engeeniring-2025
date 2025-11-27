{{ config(
    materialized='view'
) }}

select
    snap.video_sk,
    snap.video_id,
    snap.title,
    snap.published_at,
    cat.category_name,
    snap.duration,
    snap.definition,
    snap.caption,
    snap.licensed_content,
    snap.privacy_status,
    snap.license,
    snap.region_restriction,
    snap.topic_categories,
    snap.dbt_valid_from as valid_from,
    snap.dbt_valid_to as valid_to,
    (snap.dbt_valid_to is null) as is_current
from {{ ref('snapshot_dim_video') }} as snap
         left join {{ ref('video_categories') }} as cat
                   on snap.category_id = cat.category_id