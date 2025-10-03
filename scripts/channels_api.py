
import requests
import pandas as pd
import time

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
if __name__ == "__main__":
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

    # 2. Filter by country - keep only EE or null/empty
    df = df[
        (df['country'].str.upper() == 'EE') |
        (df['country'].isna()) |
        (df['country'] == '')
        ]
    print(f"After country filter (EE or null): {len(df)}")

    df.to_csv("estonian_youtubers.csv", index=False, encoding="utf-8")
    print("Saved to eesti_youtuberid.csv")
    print(df.head())


