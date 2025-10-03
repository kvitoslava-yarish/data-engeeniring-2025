
import requests
import pandas as pd

API_KEY = ""

channels_df = pd.read_csv("estonian_youtubers_cleaner.csv")

channel_ids = channels_df['channelId']

uploads_playlists = channels_df['uploadsPlaylistId']

BASE_URL = "https://www.googleapis.com/youtube/v3"


def get_video_ids_from_playlist(playlist_id, api_key):
    """Fetch all video IDs from an uploads playlist."""
    print("Fetching video IDs from playlist", playlist_id)
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
    print("Fetching video metadata...")
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
    print("Fetched all video metadata")
    return all_data


all_videos_data = []
batch_number = 0

for idx, playlist_id in enumerate(uploads_playlists[150:], start=150):
    print(f"Getting video metadata for playlist {playlist_id} ({idx + 1}/{len(uploads_playlists)})")
    video_ids = get_video_ids_from_playlist(playlist_id, API_KEY)
    all_videos_data.extend(get_video_metadata(video_ids, API_KEY))

    if (idx + 1) % 150 == 0:
        df = pd.DataFrame(all_videos_data)
        df.to_csv(f"youtube_videos_metadata_batch{batch_number}.csv", index=False)
        print(f"Saved batch {batch_number}: {len(all_videos_data)} videos")
        all_videos_data = []  # Clear the list
        batch_number += 1

if all_videos_data:
    df = pd.DataFrame(all_videos_data)
    df.to_csv(f"youtube_videos_metadata_batch{batch_number}.csv", index=False)
    print(f"Saved final batch {batch_number}: {len(all_videos_data)} videos")
