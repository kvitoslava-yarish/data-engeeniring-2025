from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
import requests
import logging
import os
import time


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "youtube")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))
API_KEY = os.getenv("YOUTUBE_API_KEY")
BASE_URL = "https://www.googleapis.com/youtube/v3"


def get_video_ids_from_playlist(playlist_id, api_key):
    logger.info(f"Fetching video IDs from playlist {playlist_id}")
    video_ids = []
    url = f"{BASE_URL}/playlistItems"
    params = {
        "part": "contentDetails",
        "playlistId": playlist_id,
        "maxResults": 50,
        "key": api_key
    }
    while True:
        resp = requests.get(url, params=params).json()
        for item in resp.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])
        if "nextPageToken" in resp:
            params["pageToken"] = resp["nextPageToken"]
        else:
            break
    return video_ids


def get_video_metadata(video_ids, api_key):
    logger.info("Fetching video metadata...")
    all_data = []
    url = f"{BASE_URL}/videos"
    parts = "snippet,contentDetails,statistics,status,topicDetails,recordingDetails"

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        params = {"part": parts, "id": ",".join(batch), "key": api_key}
        resp = requests.get(url, params=params).json()

        for item in resp.get("items", []):
            snippet = item.get("snippet", {})
            details = item.get("contentDetails", {})
            stats = item.get("statistics", {})
            status = item.get("status", {})
            all_data.append({
                "videoId": item["id"],
                "title": snippet.get("title", ""),
                "publishedAt": snippet.get("publishedAt", ""),
                "tags": ", ".join(snippet.get("tags", [])) if snippet.get("tags") else "",
                "categoryId": snippet.get("categoryId", 0),
                "channelId": snippet.get("channelId", ""),
                "duration": details.get("duration", ""),
                "definition": details.get("definition", ""),
                "caption": 1 if details.get("caption") == "true" else 0,
                "licensedContent": 1 if details.get("licensedContent") else 0,
                "regionRestriction": str(details.get("regionRestriction", "")),
                "viewCount": int(stats.get("viewCount", 0)) if stats.get("viewCount") else 0,
                "likeCount": int(stats.get("likeCount", 0)) if stats.get("likeCount") else 0,
                "commentCount": int(stats.get("commentCount", 0)) if stats.get("commentCount") else 0,
                "privacyStatus": status.get("privacyStatus", ""),
                "license": status.get("license", ""),
                "topicCategories": ", ".join(item.get("topicDetails", {}).get("topicCategories", []))
            })
    logger.info(f"Fetched metadata for {len(all_data)} videos")
    return all_data


def insert_batch_to_clickhouse(client, videos_data, batch_number):
    logger.info(f"Batch {batch_number}: Preparing {len(videos_data)} videos for insertion")

    if not videos_data:
        logger.warning(f"Batch {batch_number}: No videos to insert")
        return

    df = pd.DataFrame(videos_data).fillna("")
    df["ts"] = datetime.now()

    rows = []
    for _, row in df.iterrows():
        try:
            published_at = row["publishedAt"]
            if published_at:
                published_dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            else:
                published_dt = datetime.now()

            rows.append((
                row["ts"],
                row["videoId"],
                row["title"],
                published_dt,
                row["tags"],
                int(row["categoryId"]),
                row["channelId"],
                row["duration"],
                row["definition"],
                int(row["caption"]),
                int(row["licensedContent"]),
                row["regionRestriction"],
                int(row["viewCount"]),
                int(row["likeCount"]),
                int(row["commentCount"]),
                row["privacyStatus"],
                row["license"],
                row["topicCategories"]
            ))
        except Exception as e:
            logger.warning(f"Error processing video row: {e}")
            continue

    if rows:
        client.execute("""
            INSERT INTO raw_youtube.videos (
                ts, videoId, title, publishedAt, tags, categoryId,
                channelId, duration, definition, caption, licensedContent,
                regionRestriction, viewCount, likeCount, commentCount,
                privacyStatus, license, topicCategories
            ) VALUES
        """, rows)
        logger.info(f"Batch {batch_number}: Inserted {len(rows)} videos into ClickHouse")


def insert_videos_to_clickhouse(**context):
    logger.info("Starting video ingestion process")

    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    client.execute("""
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
        SETTINGS index_granularity = 8192
    """)

    logger.info("Fetching upload playlists from raw_youtube.channels")

    query = """
        SELECT DISTINCT uploadsPlaylistId 
        FROM raw_youtube.channels FINAL
        WHERE uploadsPlaylistId != ''
    """
    playlists = [row[0] for row in client.execute(query)]
    logger.info(f"Found {len(playlists)} playlists to process")

    all_videos = []
    batch_num = 0

    for idx, playlist_id in enumerate(playlists):
        try:
            video_ids = get_video_ids_from_playlist(playlist_id, API_KEY)
            if not video_ids:
                continue

            videos = get_video_metadata(video_ids, API_KEY)
            all_videos.extend(videos)
            logger.info(f"Playlist {idx + 1}/{len(playlists)}: collected {len(videos)} videos")

            if (idx + 1) % 100 == 0:
                insert_batch_to_clickhouse(client, all_videos, batch_num)
                all_videos = []
                batch_num += 1

        except Exception as e:
            logger.error(f"Error processing playlist {playlist_id}: {e}")
            continue

    if all_videos:
        insert_batch_to_clickhouse(client, all_videos, batch_num)

    logger.info("Video ingestion completed successfully")


with DAG(
    dag_id="videos_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 3 * * *",  # run at 3AM daily
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "raw"]
) as dag:

    insert_task = PythonOperator(
        task_id="insert_videos",
        python_callable=insert_videos_to_clickhouse,
        provide_context=True,
    )
