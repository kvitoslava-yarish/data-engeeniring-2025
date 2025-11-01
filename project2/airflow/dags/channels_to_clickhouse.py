from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
import requests
import time
import os


CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "youtube")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))
API_KEY = os.getenv("YOUTUBE_API_KEY")

def search_channels(query, max_results=50):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "channel",
        "maxResults": max_results,
        "regionCode": "EE",
        "relevanceLanguage": "et",
        "key": API_KEY
    }
    r = requests.get(url, params=params)
    data = r.json()
    channels = []
    for item in data.get("items", []):
        channels.append({
            "channelId": item["snippet"]["channelId"],
            "title": item["snippet"]["title"],
            "query": query
        })
    return channels


def get_channel_stats(channel_ids):
    stats = []
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "statistics,snippet,topicDetails,brandingSettings,contentDetails",
            "id": ",".join(batch),
            "key": API_KEY
        }
        r = requests.get(url, params=params)
        data = r.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            stats_block = item.get("statistics", {})
            branding = item.get("brandingSettings", {}).get("channel", {})
            content = item.get("contentDetails", {}).get("relatedPlaylists", {})

            stats.append({
                "channelId": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", "")[:500],
                "customUrl": snippet.get("customUrl", ""),
                "subscribers": int(stats_block.get("subscriberCount", 0)),
                "views": int(stats_block.get("viewCount", 0)),
                "videos": int(stats_block.get("videoCount", 0)),
                "hiddenSubscribers": stats_block.get("hiddenSubscriberCount", False),
                "publishedAt": snippet.get("publishedAt", ""),
                "country": snippet.get("country", ""),
                "defaultLanguage": snippet.get("defaultLanguage", ""),
                "keywords": branding.get("keywords", ""),
                "uploadsPlaylistId": content.get("uploads", ""),
                "topics": ", ".join(item.get("topicDetails", {}).get("topicCategories", []))
            })
        time.sleep(0.1)
    return stats


def get_channels():
    keywords = [
        "eesti", "tallinn", "tartu", "pärnu", "eesti vlog",
        "eesti gaming", "eesti podcast", "eesti muusika", "eesti sport"
    ]

    all_channels = []
    for kw in keywords:
        found = search_channels(kw)
        all_channels.extend(found)
        time.sleep(0.1)

    channel_ids = list({c["channelId"] for c in all_channels})
    stats = get_channel_stats(channel_ids)
    df = pd.DataFrame(stats)

    df_keywords = pd.DataFrame(all_channels).drop_duplicates("channelId")
    df = df.merge(df_keywords[["channelId", "query"]], on="channelId", how="left")
    df = df.drop_duplicates("channelId")

    df = df[df["videos"] > 0]
    df = df[~df["title"].str.contains("topic", na=False)]
    df = df.query("videos <= 1000")
    df = df[
        (df["country"].str.upper() == "EE") |
        (df["country"].isna()) |
        (df["country"] == "")
    ]
    return df[:100]


def insert_channels_to_clickhouse(**context):
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    client.execute("""
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
        SETTINGS index_granularity = 8192
    """)

    df = get_channels().fillna("")
    df["ts"] = datetime.now()

    rows = [
        (
            row["ts"],
            row["channelId"],
            row["title"],
            row["description"],
            row["customUrl"],
            int(row["subscribers"]),
            int(row["views"]),
            int(row["videos"]),
            1 if row["hiddenSubscribers"] else 0,
            datetime.fromisoformat(row["publishedAt"].replace("Z", "")) if row["publishedAt"] else datetime.now(),
            row["country"],
            row["defaultLanguage"],
            row["keywords"],
            row["uploadsPlaylistId"],
            row["topics"]
        )
        for _, row in df.iterrows()
    ]

    client.execute("""
        INSERT INTO raw_youtube.channels (
            ts, channelId, title, description, customUrl,
            subscribers, views, videos, hiddenSubscribers,
            publishedAt, country, defaultLanguage,
            keywords, uploadsPlaylistId, topics
        ) VALUES
    """, rows)

    print(f"Inserted {len(rows)} rows into youtube.raw_youtube.channels")


with DAG(
    dag_id="channels_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["youtube", "raw"]
) as dag:

    insert_task = PythonOperator(
        task_id="insert_channels",
        python_callable=insert_channels_to_clickhouse,
        provide_context=True,
    )
