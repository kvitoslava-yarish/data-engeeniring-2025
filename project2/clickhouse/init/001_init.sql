CREATE DATABASE IF NOT EXISTS raw_youtube;

CREATE TABLE IF NOT EXISTS raw_youtube.videos
(
    ts DateTime DEFAULT now(),
    videoId String,
    title String,
    publishedAt DateTime,
    tags String,
    categoryId UInt32,
    channelId String,
    duration String,
    definition String,
    caption UInt8,
    licensedContent UInt8,
    regionRestriction String,
    viewCount UInt64,
    likeCount UInt64,
    commentCount UInt64,
    privacyStatus String,
    license String,
    topicCategories String,
    loaded_at Date DEFAULT today()
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (videoId, loaded_at)
TTL loaded_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS raw_youtube.channels
(
    ts DateTime DEFAULT now(),
    channelId String,
    title String,
    description String,
    customUrl String,
    subscribers UInt64,
    views UInt64,
    videos UInt64,
    hiddenSubscribers UInt8,
    publishedAt DateTime,
    country String,
    defaultLanguage String,
    keywords String,
    uploadsPlaylistId String,
    topics String,
    loaded_at Date DEFAULT today()
)
ENGINE = ReplacingMergeTree(ts)
PARTITION BY toYYYYMM(loaded_at)
ORDER BY (channelId, loaded_at)
TTL loaded_at + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE DATABASE youtube_staging;
CREATE DATABASE youtube_analytics;
