"""load the provided seed CSV files into ClickHouse"""

from datetime import datetime
import logging
import os
from typing import Iterable, List, Tuple

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from clickhouse_driver import Client

logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "raw_youtube")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_TCP_PORT = int(os.getenv("CLICKHOUSE_TCP_PORT", "9000"))
INITIAL_CSV_DIR = os.getenv("INITIAL_CSV_DIR", "/opt/airflow/")
CHANNELS_CSV = os.path.join(INITIAL_CSV_DIR, "estonian_youtubers.csv")
VIDEOS_CSV = os.path.join(INITIAL_CSV_DIR, "videos_metadata.csv")
BATCH_SIZE = 1000


def _clickhouse_client() -> Client:
    return Client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_TCP_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def _ensure_tables(client: Client) -> None:
    client.execute("CREATE DATABASE IF NOT EXISTS raw_youtube")

    client.execute(
        """
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
        """
    )

    client.execute(
        """
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
        """
    )


def _require_file(path: str) -> None:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"CSV not found at {path}. Ensure data.zip is extracted into {INITIAL_CSV_DIR}."
        )


def _chunked(iterable: Iterable, size: int) -> Iterable[List]:
    chunk: List = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _load_channels_from_csv() -> None:
    _require_file(CHANNELS_CSV)
    df = pd.read_csv(CHANNELS_CSV).fillna("")

    df["subscribers"] = pd.to_numeric(df["subscribers"], errors="coerce").fillna(0).astype(int)
    df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(0).astype(int)
    df["videos"] = pd.to_numeric(df["videos"], errors="coerce").fillna(0).astype(int)
    df["hiddenSubscribers"] = (
        df["hiddenSubscribers"].astype(str).str.lower().isin(["true", "1", "yes"])
    )
    df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce")

    rows: Iterable[Tuple] = (
        (
            datetime.now(),
            row.channelId,
            row.title,
            row.description,
            row.customUrl,
            int(row.subscribers),
            int(row.views),
            int(row.videos),
            1 if row.hiddenSubscribers else 0,
            row.publishedAt.to_pydatetime() if not pd.isna(row.publishedAt) else datetime.now(),
            row.country,
            row.defaultLanguage,
            row.keywords,
            row.uploadsPlaylistId,
            row.topics,
        )
        for row in df.itertuples(index=False)
    )

    client = _clickhouse_client()
    _ensure_tables(client)

    insert_sql = """
        INSERT INTO raw_youtube.channels (
            ts, channelId, title, description, customUrl,
            subscribers, views, videos, hiddenSubscribers,
            publishedAt, country, defaultLanguage,
            keywords, uploadsPlaylistId, topics
        ) VALUES
    """

    total_rows = 0
    for chunk in _chunked(rows, BATCH_SIZE):
        client.execute(insert_sql, chunk)
        total_rows += len(chunk)
        logger.info("Inserted %s channel rows", total_rows)

    logger.info("Finished loading %s channel rows", total_rows)


def _load_videos_from_csv() -> None:
    _require_file(VIDEOS_CSV)
    df = pd.read_csv(VIDEOS_CSV).fillna("")

    for col in ["viewCount", "likeCount", "commentCount", "categoryId"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    for col in ["caption", "licensedContent"]:
        df[col] = df[col].astype(str).str.lower().isin(["true", "1", "yes"])

    df["publishedAt"] = pd.to_datetime(df["publishedAt"], errors="coerce")

    rows: Iterable[Tuple] = (
        (
            datetime.now(),
            row.videoId,
            row.title,
            row.publishedAt.to_pydatetime() if not pd.isna(row.publishedAt) else datetime.now(),
            row.tags,
            int(row.categoryId),
            row.channelId,
            row.duration,
            row.definition,
            1 if row.caption else 0,
            1 if row.licensedContent else 0,
            row.regionRestriction,
            int(row.viewCount),
            int(row.likeCount),
            int(row.commentCount),
            row.privacyStatus,
            row.license,
            row.topicCategories,
        )
        for row in df.itertuples(index=False)
    )

    client = _clickhouse_client()
    _ensure_tables(client)

    insert_sql = """
        INSERT INTO raw_youtube.videos (
            ts, videoId, title, publishedAt, tags, categoryId,
            channelId, duration, definition, caption, licensedContent,
            regionRestriction, viewCount, likeCount, commentCount,
            privacyStatus, license, topicCategories
        ) VALUES
    """

    total_rows = 0
    for chunk in _chunked(rows, BATCH_SIZE):
        client.execute(insert_sql, chunk)
        total_rows += len(chunk)
        logger.info("Inserted %s video rows", total_rows)

    logger.info("Finished loading %s video rows", total_rows)


with DAG(
    dag_id="load_initial_csvs",
    description="One-off load of seed CSVs into ClickHouse raw tables",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["bootstrap", "clickhouse", "csv"],
) as dag:
    load_channels = PythonOperator(
        task_id="load_channels_csv",
        python_callable=_load_channels_from_csv,
    )

    load_videos = PythonOperator(
        task_id="load_videos_csv",
        python_callable=_load_videos_from_csv,
    )

    load_channels >> load_videos
