from datetime import datetime
import pandas as pd
from googleapiclient.discovery import build
import os
import isodate
from sqlalchemy import text
import database  # your own database.py which holds the SQLAlchemy engine

# ✅ Load API Key
with open("key.txt", "r") as f:
    api_key = f.read().strip()

# ✅ YouTube API client
youtube = build("youtube", "v3", developerKey=api_key)

# ✅ Your target channel IDs (add more if needed)
channel_ids = [
    "UCX6OQ3DkcsbYNE6H8uQQuVA",  # MrBeast for example
    "UC-lHJZR3Gqxm24_Vd_AJ5Yw",   # Another channel (e.g. Daily Dose of Internet)
    "UCq-Fj5jknLsUf-MWSy4_brA"
]

# ✅ Get channel metadata
def get_channel_data(youtube, channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    item = response['items'][0]
    return {
        "Channel_Name": item['snippet']['title'],
        "Suscribers": item['statistics']['subscriberCount'],
        "View": item['statistics']['viewCount'],
        "TotalVideo": item['statistics']['videoCount'],
        "PlaylistId": item['contentDetails']['relatedPlaylists']['uploads']
    }

# ✅ Get all video IDs from playlist
def get_videos(youtube, playlist_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="contentDetails",
        playlistId=playlist_id,
        maxResults=50
    )
    response = request.execute()

    while True:
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        if 'nextPageToken' in response:
            request = youtube.playlistItems().list(
                part="contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=response['nextPageToken']
            )
            response = request.execute()
        else:
            break
    return video_ids

# ✅ Get detailed video metadata (batched using .join)
def get_video_data(video_ids):
    all_video_data = []
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]
        video_response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch_ids)
        ).execute()
        all_video_data.extend(video_response.get("items", []))
    return all_video_data

# ✅ Create table if it doesn't exist (SQL Server)
create_table_sql = text("""
IF NOT EXISTS (
    SELECT * FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME = 'YoutubeData'
)
BEGIN
    CREATE TABLE YoutubeData (
        video_id NVARCHAR(50) PRIMARY KEY,
        channel_title NVARCHAR(MAX),
        title NVARCHAR(MAX),
        description NVARCHAR(MAX),
        like_Count BIGINT,
        favourite_Count BIGINT,
        duration TIME,
        caption NVARCHAR(10),
        defination NVARCHAR(10),
        view_count BIGINT,
        Published_date DATE,
        Published_time TIME,
        commentCount BIGINT,
        video_link NVARCHAR(MAX)
    )
END
""")

with database.engine.begin() as conn:
    conn.execute(create_table_sql)

# ✅ Loop through all channels
for channel_id in channel_ids:
    channel_data = get_channel_data(youtube, channel_id)
    playlist_id = channel_data['PlaylistId']
    all_video_ids = get_videos(youtube, playlist_id)

    with database.engine.connect() as conn:
        try:
            result = conn.execute(text("SELECT video_id FROM YoutubeData"))
            existing_ids = set(row[0] for row in result.fetchall())
        except Exception as e:
            existing_ids = set()
            print("Could not fetch existing videos:", e)

    # ✅ Only process new videos
    new_video_ids = [vid for vid in all_video_ids if vid not in existing_ids]
    video_data = get_video_data(new_video_ids)

    # ✅ Create dataframe
    my_video_data = {
        'video_id': [],
        'channel_title': [],
        'title': [],
        'description': [],
        'published_at': [],
        'view_count': [],
        'like_Count': [],
        'favourite_Count': [],
        'duration': [],
        'caption': [],
        'defination': [],
        'commentCount': []
    }

    for item in video_data:
        my_video_data['video_id'].append(item.get('id', '0'))
        my_video_data['channel_title'].append(item['snippet'].get('channelTitle', '0'))
        my_video_data['title'].append(item['snippet'].get('title', '0'))
        my_video_data['description'].append(item['snippet'].get('description', '0'))
        my_video_data['like_Count'].append(item['statistics'].get('likeCount', '0'))
        my_video_data['favourite_Count'].append(item['statistics'].get('favoriteCount', '0'))
        my_video_data['duration'].append(item['contentDetails'].get('duration', '0'))
        my_video_data['caption'].append(item['contentDetails'].get('caption', '0'))
        my_video_data['defination'].append(item['contentDetails'].get('definition', '0'))
        my_video_data['view_count'].append(item['statistics'].get('viewCount', '0'))
        my_video_data['published_at'].append(item['snippet'].get('publishedAt', '0'))
        my_video_data['commentCount'].append(item['statistics'].get('commentCount', '0'))

    df = pd.DataFrame(my_video_data)
    df['like_Count'] = pd.to_numeric(df['like_Count'], errors='coerce')
    df['favourite_Count'] = pd.to_numeric(df['favourite_Count'], errors='coerce')
    df['commentCount'] = pd.to_numeric(df['commentCount'], errors='coerce')
    df['view_count'] = pd.to_numeric(df['view_count'], errors='coerce')
    df['video_link'] = df['video_id'].apply(lambda x: f"https://www.youtube.com/watch?v={x}")
    df['Published_date'] = pd.to_datetime(df['published_at']).dt.date
    df['Published_time'] = pd.to_datetime(df['published_at']).dt.time
    df['duration'] = df['duration'].apply(lambda x: isodate.parse_duration(x) if isinstance(x, str) else None)
    df['duration'] = df['duration'].apply(lambda x: (datetime.min + x).time() if pd.notnull(x) else None)
    df.drop(columns='published_at', inplace=True)

    # ✅ Merge (upsert) SQL Server
    merge_sql = text("""
    MERGE INTO YoutubeData AS target
    USING (SELECT 
        :video_id AS video_id,
        :channel_title AS channel_title,
        :title AS title,
        :description AS description,
        :like_Count AS like_Count,
        :favourite_Count AS favourite_Count,
        :duration AS duration,
        :caption AS caption,
        :defination AS defination,
        :view_count AS view_count,
        :Published_date AS Published_date,
        :Published_time AS Published_time,
        :commentCount AS commentCount,
        :video_link AS video_link
    ) AS source
    ON target.video_id = source.video_id

    WHEN MATCHED THEN 
        UPDATE SET 
            channel_title = source.channel_title,
            title = source.title,
            description = source.description,
            like_Count = source.like_Count,
            favourite_Count = source.favourite_Count,
            duration = source.duration,
            caption = source.caption,
            defination = source.defination,
            view_count = source.view_count,
            Published_date = source.Published_date,
            Published_time = source.Published_time,
            commentCount = source.commentCount,
            video_link = source.video_link

    WHEN NOT MATCHED THEN 
        INSERT (
            video_id, channel_title, title, description, like_Count,
            favourite_Count, duration, caption, defination, view_count,
            Published_date, Published_time, commentCount, video_link
        ) VALUES (
            source.video_id, source.channel_title, source.title, source.description, source.like_Count,
            source.favourite_Count, source.duration, source.caption, source.defination, source.view_count,
            source.Published_date, source.Published_time, source.commentCount, source.video_link
        );
    """)

    with database.engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(merge_sql, row.to_dict())
