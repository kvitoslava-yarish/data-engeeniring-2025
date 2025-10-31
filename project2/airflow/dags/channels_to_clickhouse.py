from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
from datetime import datetime
import pandas as pd
import time

import requests


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "analytics"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_TCP_PORT=9000

API_KEY = ""


def search_channels(query, max_results=50):
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "channel",
        "maxResults": max_results,
        "regionCode": "EE",  # Estonia - affects result ranking
        "relevanceLanguage": "et",
        "key": API_KEY
    }
    response = requests.get(url, params=params)
    data = response.json()
    print(data)
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
        batch_ids = channel_ids[i:i + 50]
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "statistics,snippet,topicDetails,brandingSettings,contentDetails",
            "id": ",".join(batch_ids),
            "key": API_KEY
        }
        response = requests.get(url, params=params)
        data = response.json()

        for item in data.get("items", []):
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            branding = item.get("brandingSettings", {}).get("channel", {})
            content = item.get("contentDetails", {}).get("relatedPlaylists", {})

            stats.append({
                "channelId": item["id"],
                "title": snippet.get("title", ""),
                "description": snippet.get("description", "")[:500],
                "customUrl": snippet.get("customUrl", ""),

                # Statistics
                "subscribers": int(statistics.get("subscriberCount", 0)),
                "views": int(statistics.get("viewCount", 0)),
                "videos": int(statistics.get("videoCount", 0)),
                "hiddenSubscribers": statistics.get("hiddenSubscriberCount", False),

                # Dates
                "publishedAt": snippet.get("publishedAt", ""),

                # Location & Language
                "country": snippet.get("country", ""),  # Sometimes set!
                "defaultLanguage": snippet.get("defaultLanguage", ""),

                # Branding
                "keywords": branding.get("keywords", ""),  # Channel keywords

                # Content
                "uploadsPlaylistId": content.get("uploads", ""),  # To get recent videos later

                # Topics (what you already have)
                "topics": ", ".join(item.get("topicDetails", {}).get("topicCategories", []))
            })
        time.sleep(0.1)
    return stats


# --- MAIN ---
def get_channels():
    keywords = [
        "eesti", "eesti keeles", "eesti youtuber", "eesti vlogija",

        "tallinn", "tartu", "pärnu", "narva",

        "eesti vlog", "eesti gaming", "eesti taskuhääling", "eesti podcast",
        "eesti muusika", "eesti komöödia", "eesti sketš",

        "kuidas teha", "minu elu", "eesti keeles",

        "eesti tech", "eesti tehnoloogia", "eesti programmeerimine",
        "eesti toit", "eesti kokkamine", "eesti retseptid",
        "eesti reis", "eesti loodus", "eesti ajalugu",
        "eesti poliitika", "eesti uudised", "eesti sport",
        "eesti jalgpall", "eesti korvpall", "eesti meelelahutus",
        "eesti filmid", "eesti investeerimine", "eesti äri",
        "eesti teadus", "eesti haridus", "eesti õppimine",

        "eesti kultuur", "eesti kunst", "eesti teater",
        "eesti rahvamuusika", "eesti laulud",

        "mängime", "vaatame", "proovime",

        "võrumaa", "saaremaa", "hiiumaa"
    ]

    all_channels = []
    for kw in keywords:
        print(f"Searching for keyword: {kw}")
        found = search_channels(kw, max_results=50)  # max 50 per request
        all_channels.extend(found)
        time.sleep(0.1)

    # Collect all unique channel IDs
    channel_ids = list({c["channelId"] for c in all_channels})
    print(f"Total unique channels found: {len(channel_ids)}")

    # Get channel stats
    stats = get_channel_stats(channel_ids)
    df = pd.DataFrame(stats)

    # Merge keyword info back in (optional, shows which keyword caught the channel)
    df_keywords = pd.DataFrame(all_channels).drop_duplicates("channelId")
    df = df.merge(df_keywords[["channelId", "query"]], on="channelId", how="left")

    # Drop duplicates just in case
    df = df.drop_duplicates("channelId")

    print(f"Initial channels: {len(df)}")

    # 1. Remove channels with 0 videos
    df = df[df['videos'] > 0]
    print(f"After removing 0 videos: {len(df)}")

    df = df[~df['title'].str.contains("topic", na=False)]

    df = df.query('videos <= 1000')

    # 2. Filter by country - keep only EE or null/empty
    df = df[
        (df['country'].str.upper() == 'EE') |
        (df['country'].isna()) |
        (df['country'] == '')
        ]

    return df


def insert_channels_to_clickhouse(**context):
    """Insert collected channel data into ClickHouse."""
    client = Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB
    )

    # Ensure table exists
    client.execute("""
        CREATE TABLE IF NOT EXISTS channels (
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
            topics String
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (ts, channelId)
    """)


    df = get_channels()

    df = df.fillna("")
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

    # Insert data
    client.execute("""
        INSERT INTO channels (
            ts, channelId, title, description, customUrl,
            subscribers, views, videos, hiddenSubscribers,
            publishedAt, country, defaultLanguage,
            keywords, uploadsPlaylistId, topics
        ) VALUES
    """, rows)

    print(f"Inserted {len(rows)} rows into ClickHouse")


with DAG(
    dag_id="channels_to_clickhouse",
    start_date=days_ago(1),
    schedule_interval="0 0 * * *",  # every hour
    catchup=False,
    max_active_runs=1
) as dag:

    insert_task = PythonOperator(
        task_id="insert_channels",
        python_callable=insert_channels_to_clickhouse,
        provide_context=True,
    )


