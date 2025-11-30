-- 1. Create roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- 2. Create users
CREATE USER IF NOT EXISTS analyst_full_user IDENTIFIED BY 'full123';
CREATE USER IF NOT EXISTS analyst_limited_user IDENTIFIED BY 'limited123';

-- 3. Assign roles to users
GRANT analyst_full   TO analyst_full_user;
GRANT analyst_limited TO analyst_limited_user;

-- 4. Permissions for FULL analyst
GRANT SELECT ON youtube_dbt.dim_channel            TO analyst_full;
GRANT SELECT ON youtube_dbt.dim_video              TO analyst_full;
GRANT SELECT ON youtube_dbt.fact_channel_metrics   TO analyst_full;
GRANT SELECT ON youtube_dbt.fact_video_performance TO analyst_full;

GRANT SELECT ON youtube_dbt.top_10_channels_full   TO analyst_full;
GRANT SELECT ON youtube_dbt.top_10_videos_full     TO analyst_full;

-- 5. Permissions for LIMITED analyst
GRANT SELECT ON youtube_dbt.dim_channel            TO analyst_limited;
GRANT SELECT ON youtube_dbt.dim_video              TO analyst_limited;
GRANT SELECT ON youtube_dbt.fact_channel_metrics   TO analyst_limited;
GRANT SELECT ON youtube_dbt.fact_video_performance TO analyst_limited;

GRANT SELECT ON youtube_dbt.top_10_channels_limited TO analyst_limited;
GRANT SELECT ON youtube_dbt.top_10_videos_limited   TO analyst_limited;
