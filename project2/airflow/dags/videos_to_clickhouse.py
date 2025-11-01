from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
import time

import requests

import logging

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "analytics"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_TCP_PORT=9000
BASE_URL = "https://www.googleapis.com/youtube/v3"

API_KEY = ""

def get_video_ids_from_playlist(playlist_id, api_key):
    """Fetch all video IDs from an uploads playlist."""
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
    """Fetch video metadata (excluding thumbnails, embed, livestream)."""
    logger.info("Fetching video metadata...")
    all_data = []
    url = f"{BASE_URL}/videos"
    parts = "snippet,contentDetails,statistics,status,topicDetails,recordingDetails"

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        params = {
            "part": parts,
            "id": ",".join(batch),
            "key": api_key
        }
        resp = requests.get(url, params=params).json()
        for item in resp.get("items", []):
            data = {
                "videoId": item["id"],
                "title": item["snippet"].get("title"),
                "description": item["snippet"].get("description"),
                "publishedAt": item["snippet"].get("publishedAt"),
                "tags": item["snippet"].get("tags"),
                "categoryId": item["snippet"].get("categoryId"),
                "channelId": item["snippet"].get("channelId"),
                "duration": item["contentDetails"].get("duration"),
                "dimension": item["contentDetails"].get("dimension"),
                "definition": item["contentDetails"].get("definition"),
                "caption": item["contentDetails"].get("caption"),
                "licensedContent": item["contentDetails"].get("licensedContent"),
                "regionRestriction": item["contentDetails"].get("regionRestriction"),
                "viewCount": item["statistics"].get("viewCount"),
                "likeCount": item["statistics"].get("likeCount"),
                "commentCount": item["statistics"].get("commentCount"),
                "privacyStatus": item["status"].get("privacyStatus"),
                "license": item["status"].get("license"),
                "embeddable": item["status"].get("embeddable"),
                "publicStatsViewable": item["status"].get("publicStatsViewable"),
                "topicCategories": item.get("topicDetails", {}).get("topicCategories"),
                "recordingDate": item.get("recordingDetails", {}).get("recordingDate"),
                "recordingLocation": item.get("recordingDetails", {}).get("location")
            }
            all_data.append(data)
    logger.info("Fetched all video metadata")
    return all_data





def insert_videos_to_clickhouse(**context):
    """Insert collected video data into ClickHouse."""
    logger.info("Starting insert_videos_to_clickhouse")

    try:
        logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_TCP_PORT}")
        client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_TCP_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB
        )
        logger.info("ClickHouse connection successful")

        # Create videos table if not exists
        logger.info("Creating videos table if not exists")
        client.execute("""
            CREATE TABLE IF NOT EXISTS videos (
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
                topicCategories String
            ) ENGINE = ReplacingMergeTree(ts)
            ORDER BY (videoId)
        """)
        logger.info("Videos table creation/verification complete")

        logger.info("Querying channels table for playlist IDs")
        query = """
            SELECT DISTINCT channelId, uploadsPlaylistId 
            FROM channels FINAL
            WHERE uploadsPlaylistId != ''
        """
        result = client.execute(query)
        logger.info(f"Query returned {len(result)} channels")

        # Extract to separate lists
        channel_ids = [row[0] for row in result]
        uploads_playlists = [row[1] for row in result]
        logger.info(f"Extracted {len(channel_ids)} channel IDs and {len(uploads_playlists)} playlist IDs")

        all_videos_data = []
        batch_number = 0

        for idx, playlist_id in enumerate(uploads_playlists):
            logger.info(f"Processing playlist {idx + 1}/{len(uploads_playlists)}: {playlist_id}")

            try:
                video_ids = get_video_ids_from_playlist(playlist_id, API_KEY)
                logger.info(f"Retrieved {len(video_ids)} video IDs from playlist {playlist_id}")

                videos = get_video_metadata(video_ids, API_KEY)
                logger.info(f"Retrieved metadata for {len(videos)} videos from playlist {playlist_id}")

                all_videos_data.extend(videos)
                logger.info(f"Total videos collected so far: {len(all_videos_data)}")
            except Exception as e:
                logger.error(f"Error processing playlist {playlist_id}: {e}", exc_info=True)
                continue

            # Insert to ClickHouse every 150 playlists
            if (idx + 1) % 150 == 0:
                logger.info(f"Reached batch threshold at playlist {idx + 1}, inserting batch {batch_number}")
                insert_batch_to_clickhouse(client, all_videos_data, batch_number)
                all_videos_data = []  # Clear the list
                batch_number += 1

        # Insert remaining data
        if all_videos_data:
            logger.info(f"Inserting final batch {batch_number} with {len(all_videos_data)} videos")
            insert_batch_to_clickhouse(client, all_videos_data, batch_number)

        logger.info("insert_videos_to_clickhouse completed successfully")

    except Exception as e:
        logger.error(f"Fatal error in insert_videos_to_clickhouse: {e}", exc_info=True)
        raise


def insert_batch_to_clickhouse(client, videos_data, batch_number):
    """Helper function to insert a batch of videos into ClickHouse."""
    logger.info(f"Starting insert_batch_to_clickhouse for batch {batch_number}")

    if not videos_data:
        logger.warning(f"Batch {batch_number}: No data to insert")
        return

    logger.info(f"Batch {batch_number}: Converting {len(videos_data)} videos to DataFrame")
    df = pd.DataFrame(videos_data)
    logger.info(f"Batch {batch_number}: DataFrame shape: {df.shape}")

    # Handle nullable/missing fields
    logger.info(f"Batch {batch_number}: Filling null values")
    df = df.fillna({
        'tags': '',
        'regionRestriction': '',
        'topicCategories': '',
        'likeCount': 0,
        'commentCount': 0
    })

    df['ts'] = datetime.now()
    logger.info(f"Batch {batch_number}: Added timestamp column")

    rows = []
    error_count = 0

    for idx, row in df.iterrows():
        try:
            # Parse publishedAt
            published_at = row.get('publishedAt', '')
            if published_at and published_at != '':
                published_dt = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            else:
                published_dt = datetime.now()

            row_data = (
                row['ts'],
                str(row.get('videoId', '')),
                str(row.get('title', '')),
                published_dt,
                str(row.get('tags', '')),
                int(row.get('categoryId', 0)) if row.get('categoryId') else 0,
                str(row.get('channelId', '')),
                str(row.get('duration', '')),
                str(row.get('definition', '')),
                1 if row.get('caption') else 0,
                1 if row.get('licensedContent') else 0,
                str(row.get('regionRestriction', '')),
                int(row.get('viewCount', 0)) if row.get('viewCount') else 0,
                int(row.get('likeCount', 0)) if row.get('likeCount') else 0,
                int(row.get('commentCount', 0)) if row.get('commentCount') else 0,
                str(row.get('privacyStatus', '')),
                str(row.get('license', '')),
                str(row.get('topicCategories', ''))
            )
            rows.append(row_data)
        except Exception as e:
            error_count += 1
            logger.error(f"Batch {batch_number}: Error processing row {idx}: {e}")
            logger.error(f"Batch {batch_number}: Row data: {row.to_dict()}")
            continue

    if error_count > 0:
        logger.warning(f"Batch {batch_number}: {error_count} rows failed to process")

    if rows:
        try:
            logger.info(f"Batch {batch_number}: Inserting {len(rows)} rows into ClickHouse")
            client.execute("""
                INSERT INTO videos (
                    ts, videoId, title, publishedAt, tags, categoryId,
                    channelId, duration, definition, caption, licensedContent,
                    regionRestriction, viewCount, likeCount, commentCount,
                    privacyStatus, license, topicCategories
                ) VALUES
            """, rows)

            logger.info(f"Batch {batch_number}: Successfully inserted {len(rows)} videos into ClickHouse")
        except Exception as e:
            logger.error(f"Batch {batch_number}: Failed to insert into ClickHouse: {e}", exc_info=True)
            raise
    else:
        logger.warning(f"Batch {batch_number}: No valid rows to insert")


with DAG(
    dag_id="videos_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",  # every day
    catchup=False,
    max_active_runs=1
) as dag:

    insert_task = PythonOperator(
        task_id="insert_videos",
        python_callable=insert_videos_to_clickhouse,
        provide_context=True,
    )

