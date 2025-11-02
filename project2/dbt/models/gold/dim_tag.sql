{{ config(
    schema='analytics_youtube',
    materialized='table'
) }}

with all_tags as (
    select distinct
        trim(arrayJoin(splitByChar(',', tags))) as tag_name
    from {{ ref('stg_videos') }}
    where tags is not null and tags != ''
    )
select
    {{ dbt_utils.generate_surrogate_key(['tag_name']) }} as tag_id,
    tag_name
from all_tags
where tag_name != ''