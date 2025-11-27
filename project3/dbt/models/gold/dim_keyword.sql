{{ config(
    materialized='table'
) }}

with all_keywords as (
    select distinct
        trim(arrayJoin(splitByChar(',', keywords))) as keyword
    from {{ ref('stg_channels') }}
    where keywords is not null and keywords != ''
    )
select
    {{ dbt_utils.generate_surrogate_key(['keyword']) }} as keyword_id,
    keyword
from all_keywords
where keyword != ''