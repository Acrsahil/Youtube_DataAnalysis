from datetime import datetime
import pandas as pd
from googleapiclient.discovery import build 
import os
import pandas as pd
from IPython.display import JSON
import isodate
from datetime import datetime, timedelta
from sqlalchemy import text
import database



api_key = ""
with open("key.txt","r") as f:
    api_key = f.read()


    # Get credentials and create an API client

api_service_name = "youtube"
api_version = "v3"
youtube = build (
    api_service_name, api_version, developerKey=api_key )
channel_id = "UCX6OQ3DkcsbYNE6H8uQQuVA"




def get_channels(youtube,channel_id):
    total_data = []
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    

    # loop through items
    for item in response['items']:
        data = {
                "Channel_Name" : item['snippet']['title'],
                "Suscribers" : item['statistics']['subscriberCount'],
                "View" : item['statistics']['viewCount'],
                "TotalVideo" : item['statistics']['videoCount'],
                "PlaylistId" : item['contentDetails']['relatedPlaylists']['uploads']
               }

    total_data.append(data)
    return pd.DataFrame(total_data)

def get_videos(youtube, video_id):
    video_ids = []
    request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=video_id,
        maxResults = 50
    )
    response = request.execute()
    
    for item in response['items']:
        video_ids.append(item['contentDetails']['videoId'])
    next_page_token = response.get('nextPageToken')

    while next_page_token is not None:
        request = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=video_id,
            maxResults = 50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        # print(len(video_ids))
        
    return video_ids




def get_video_data(video_ids):
    # Get video data for the given video IDs in chunks of 50
    all_video_data = []
    
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]
        
        # Request video details in chunks
        video_response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch_ids)
        ).execute()
        
        # Extend the result into all_video_data
        all_video_data.extend(video_response.get("items", []))
        print(all_video_data)
    
    return all_video_data


channel_data = get_channels(youtube, channel_id)
video_id = str(channel_data['PlaylistId'][0]).strip()
all_video_ids = get_videos(youtube, video_id)







with database.engine.connect() as conn:
    try:
        result = conn.execute(text("SELECT video_id FROM YoutubeData"))
        existing_ids = set(row[0] for row in result.fetchall())
    except Exception as e:
        existing_ids = set()
        print("Table might not exist yet:", e)

# ✅ Filter new video IDs
new_video_ids = [vid for vid in all_video_ids if vid not in existing_ids]

if not new_video_ids:
    print("No new videos found.")
    exit()

# ✅ Fetch data for only new videos
response = get_video_data(new_video_ids)

# ... proceed to process `response` and build DataFrame ...

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

for item in response:
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

# Type conversions
df['like_Count'] = pd.to_numeric(df['like_Count'])
df['favourite_Count'] = pd.to_numeric(df['favourite_Count'])
df['commentCount'] = pd.to_numeric(df['commentCount'])
df['view_count'] = pd.to_numeric(df['view_count'])

# Create additional fields
df['video_link'] = df['video_id'].apply(lambda x: f"https://www.youtube.com/watch?v={x}")
df['Published_date'] = df['published_at'].str.split('T').str[0]
df['Published_time'] = df['published_at'].str.split('T').str[1].str.replace('Z', '')

# Convert to datetime.date and datetime.time
df['Published_date'] = pd.to_datetime(df['Published_date']).dt.date
df['Published_time'] = pd.to_datetime(df['Published_time']).dt.time

# Duration conversion
df['duration'] = df['duration'].apply(
    lambda x: isodate.parse_duration(x) if isinstance(x, str) else None
)
df['duration'] = df['duration'].apply(lambda x: (datetime.min + x).time())

df.drop(columns='published_at', inplace=True)

# ✅ Append only new rows to existing table
df.to_sql("YoutubeData", con=database.engine, if_exists="append", index=False)
print(f"{len(df)} new videos inserted into the database.")




