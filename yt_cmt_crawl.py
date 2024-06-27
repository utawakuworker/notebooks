!pip install google-api-python-client -q

import time
import warnings  # Warning suppression
warnings.filterwarnings('ignore')
from tqdm.auto import tqdm
import pandas as pd
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def retrieve_comments(api_obj, video_id, max_results=100):
    comments = []

    try:
        # Initial request to retrieve comments for the specified video
        response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id, maxResults=max_results).execute()

        # Process each page of comments until there are no more pages
        while response:
            for item in response['items']:
                # Extract information from the top-level comment
                comment = item['snippet']['topLevelComment']['snippet']
                comments.append([comment['textDisplay'], comment['authorDisplayName'], comment['publishedAt'], comment['likeCount']])

                # If there are replies, extract information from each reply
                if item['snippet']['totalReplyCount'] > 0:
                    for reply_item in item['replies']['comments']:
                        reply = reply_item['snippet']
                        comments.append([reply['textDisplay'], reply['authorDisplayName'], reply['publishedAt'], reply['likeCount']])

            # Check if there is a next page and retrieve it
            if 'nextPageToken' in response:
                response = api_obj.commentThreads().list(part='snippet,replies', videoId=video_id, pageToken=response['nextPageToken'], maxResults=max_results).execute()
            else:
                break  # No more pages, exit the loop

        return comments

    except HttpError as e:
        #if e.resp.status == 404:
        #    print(f"Video with ID {video_id} not found.")
        return e
        #else:
        #    print(e)
        #    raise  # Re-raise the exception if it's not a 404 error

def process_comments(comments):
    # Create a DataFrame from the comments list
    df = pd.DataFrame(comments, columns=['comment', 'author', 'date', 'num_likes'])
    df = df[df['comment'].str.contains(':')].reset_index(drop=True)
    # Find the most liked comment and get its text
    if not df.empty:
        most_liked_comment = df.iloc[df['num_likes'].idxmax()]['comment']
        # Find the index of the comment with the maximum number of ':' occurrences
        max_colon_count_idx = df['comment'].str.count(':').idxmax()

        # Get the comment with the maximum number of ':' occurrences
        comment_with_max_colons = df.loc[max_colon_count_idx, 'comment']

        if most_liked_comment != comment_with_max_colons:
            # Compare the lengths of the comments based on colons
            if len(most_liked_comment.split(':')) > len(comment_with_max_colons.split(':')):
                target = most_liked_comment
            else:
                target = comment_with_max_colons
        else:
            target = most_liked_comment

        target = target.replace('#', '')
        # Extract text from each <a> tag
        soup = BeautifulSoup(target, 'html.parser')
        data = [{'timestamp': a.get_text(strip=True), 
                 'title': a.find_next_sibling(text=True)} for a in soup.find_all('a')]
        parsed_df = pd.DataFrame(data)
        
        try:
            parsed_df['title'] = parsed_df['title'].str.rstrip('\r')
            parsed_df = parsed_df[parsed_df['title'].str.strip() != '']
            parsed_df = parsed_df[parsed_df['timestamp'].apply(lambda x: str(x).strip() != '')]
        except:
            pass
        return parsed_df.reset_index(drop=True)

def extract_first_substring(text):
    substrings = text.split()
    for substring in substrings:
        if substring.count(':') >= 1:
            return substring
    return None

def convert_timestamp(timestamp):
    """
    Convert single-colon timestamp to hh:mm:ss format with leading zeros.
    """
    if timestamp is not None:
        # Splitting the timestamp and formatting it with double colon
        return ':'.join(timestamp.split(':')).zfill(8)
    else:
        return None

def remove_unrelated_timestamps(df, timestamp_column='timestamp'):
    
    df = df[df[timestamp_column].apply(lambda x: isinstance(x, str) and 'https:' not in x)]
    
    df[timestamp_column] = df[timestamp_column].apply(
        lambda x: convert_timestamp(extract_first_substring(x))
    )
        
    df['total_seconds'] = df[timestamp_column].apply(
        lambda x: sum(
            int(part) * (60 ** i) for i, part in 
            enumerate(reversed(x.split(':'))))
    )

    # Create a boolean mask for rows where the next row has a smaller timestamp
    mask = df['total_seconds'] <= df['total_seconds'].shift(-1)

    # Apply the mask to filter the DataFrame
    filtered_df = df[mask]

    # Reset index if needed
    filtered_df = filtered_df.reset_index(drop=True)

    return filtered_df.drop(['total_seconds'], axis=1)

def get_video_details(video_ids):
    youtube = build('youtube', 'v3', developerKey=api_key)

    videos_details = []

    # Split video_ids list into chunks of 50, as the API allows max 50 video IDs per request
    for i in range(0, len(video_ids), 50):
        video_ids_chunk = video_ids[i:i + 50]

        videos_response = youtube.videos().list(id=','.join(video_ids_chunk),
                                                part='snippet,statistics').execute()

        for item in videos_response['items']:
            video_id = item['id']
            title = item['snippet']['title']
            description = item['snippet']['description']
            view_count = item['statistics'].get('viewCount', 0)
            like_count = item['statistics'].get('likeCount', 0)
            dislike_count = item['statistics'].get('dislikeCount', 0)

            videos_details.append({
                'video_id': video_id,
                'title': title,
                'description': description,
                'view_count': view_count,
                'like_count': like_count,
                'dislike_count': dislike_count
            })

    return videos_details


api_key = 'SSSS'
api_obj = build('youtube', 'v3', developerKey=api_key)


channel_id = 'UChAOCCFuF2hto05Z68xp56A'


channels_response = api_obj.channels().list(
    id=channel_id,
    part='contentDetails'
).execute()

for channel in channels_response['items']:
    uploads_playlist_id = channel['contentDetails']['relatedPlaylists']['uploads']
    
videos = []
next_page_token = None
while True:
    playlist_items_response = api_obj.playlistItems().list(playlistId=uploads_playlist_id,
                                                            part='contentDetails',
                                                            maxResults=50,
                                                            pageToken=next_page_token).execute()
    videos.extend(playlist_items_response['items'])
    next_page_token = playlist_items_response.get('nextPageToken')

    if not next_page_token:
        break

# Extract video IDs
video_ids = [video['contentDetails']['videoId'] for video in videos]
videos_details = get_video_details(video_ids)
utawakus = [x['video_id'] for x in videos_details if '歌' in x['title']]

comments = {}
for video_id in tqdm(utawakus):
    comments[video_id] = retrieve_comments(api_obj, video_id, max_results=100)
    time.sleep(0.5)

filtered_comments = {k: v for k, v in comments.items() if type(v)!=HttpError}


no_comments = {}
for k, v in filtered_comments.items():
    df = pd.DataFrame(v, columns=['comment', 'author', 'date', 'num_likes'])
    df = df[df['comment'].str.contains(':')].reset_index(drop=True)
    if df.empty:
        print(k)#rR7cMmpjbMU


filterd_timestmaps = {}
for k, v in filtered_comments.items():
    #try:
    filterd_timestmaps[k] = process_comments(v)
    #except:
    #    print(k)#, process_comments(v))
    #    raise


result = {}
for k, v in filterd_timestmaps.items():
    if type(v) != type(None) :
        if not v.empty:
            try:
                result[k] = remove_unrelated_timestamps(v)#.to_csv(
                #    encoding='utf-8-sig', index=False
                #)
            except:
                print(v)
                raise
            #    print(v)

temp_list = []
for k, v in result.items():
    v['id'] = k
    temp_list.append(v)

result_df = pd.concat(temp_list).sort_values('title')

import html
result_df['title'] = result_df['title'].apply(html.unescape)

result_df.to_csv('df.csv', encoding='utf-8-sig', index=False)
