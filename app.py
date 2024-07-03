# Standard library imports
import ast
import functools
import io
import itertools
import os
import random
import re
import subprocess
import sys
import time
import concurrent.futures
from contextlib import redirect_stdout
from typing import List, Optional, Union
from struct import pack
from socket import inet_ntoa
from uuid import uuid4
import json
import logging
#import inspect #getsource

# Third-party imports
import asyncio
import aiohttp
from bson import ObjectId
import feedparser
import pandas as pd
import requests
import schedule
from tqdm.auto import tqdm
from bs4 import BeautifulSoup
from pymongo import (
    MongoClient,
    ReplaceOne,
    UpdateOne,
    InsertOne,
    UpdateMany,
    DeleteMany
)
from aiohttp import ClientSession, ClientResponse
from fake_useragent import UserAgent

class ScheduledVideo:
    def __init__(self, channel_name: str, video_id: str, title: str, 
                 scheduled_start_time, 
                 #thumbnail_url: str,
                 publishedAt:str, duration: str, 
                 actual_start_time, actual_end_time,
                 concurrent_viewers: int,
                 hide: str = 'FALSE', is_streaming: str = 'NULL'):
        
        self.channel_name = channel_name
        self.video_id = video_id
        self.title = title
        self.scheduled_start_time = scheduled_start_time or publishedAt
        #self.thumbnail_url = thumbnail_url
        #self.publishedAt = publishedAt
        self.hide = hide
        self.is_streaming = is_streaming
        self.duration = duration
        self.actual_start_time = actual_start_time
        self.actual_end_time = actual_end_time
        self.concurrent_viewers = concurrent_viewers
        
        if not self.is_scheduled_within_threshold(12) and not pd.isna(self.actual_start_time):
            self.scheduled_start_time = self.actual_start_time

    def __str__(self):
        return (
            f"Title: {self.title}\n"
            #f"URL: https://www.youtube.com/watch?v={self.video_id}\n"
            f"VideoId: {self.video_id}\n"
            f"ChannelName: {self.channel_name}\n"
            f"ScheduledTime: {self.scheduled_start_time:%Y-%m-%d %H:%M:%S}\n"
            #f"ThumbnailURL: {self.thumbnail_url}\n"
            f"Hide: {self.hide}\n"
            f"broadcastStatus: {self.is_streaming}\n"
            f"isVideo: {self.duration}\n"
            f"actualStarTime: {self.actual_start_time}\n"
            f"actualEndTime: {self.actual_end_time}\n"
            f"concurrentViewers: {self.concurrent_viewers}\n"
        )

    def is_scheduled_within_threshold(self, threshold_hours: Optional[float] = None) -> bool:
        if threshold_hours is None:
            return True
        current_time = get_current_time_with_tz()
        #current_time = pd.Timestamp.now().floor('s') + pd.Timedelta(hours=9)
        threshold_delta = pd.Timedelta(hours=threshold_hours)
        return current_time - threshold_delta <= self.scheduled_start_time <= current_time + threshold_delta

def classify_videos(list_of_scheduled_videos):
    
    current_time = get_current_time_with_tz()
    all_video_ids = {video.video_id for video in list_of_scheduled_videos}
    
    live_streams = [video for video in list_of_scheduled_videos
                    if not pd.isna(video.actual_start_time)
                    and video.actual_start_time > current_time - pd.Timedelta(hours=2)
                    and pd.isna(video.actual_end_time)]

    scheduled_soon = [video for video in list_of_scheduled_videos
                      if pd.isna(video.actual_start_time)
                      and current_time - pd.Timedelta(hours=2) < video.scheduled_start_time < current_time + pd.Timedelta(hours=24)
                      and pd.isna(video.actual_end_time)]

    scheduled_later = [video.video_id for video in list_of_scheduled_videos
                       if pd.isna(video.actual_start_time)
                       and video.scheduled_start_time >= current_time + pd.Timedelta(hours=24)]
       
    return live_streams + scheduled_soon, scheduled_later


class TitleChecker:
    def __init__(self, exclude_list, never_allowed, include_list, combination_conditions, conditionally_denied):
        self.exclude_list = exclude_list
        self.never_allowed = never_allowed
        self.include_list = include_list
        self.combination_conditions = combination_conditions
        self.conditionally_denied = conditionally_denied
    
    def validate(self, title):
        title = title.lower()

        # Handle conditionally denied substrings
        if any(substring in title for substring in self.combination_conditions) and any(substring in title for substring in self.conditionally_denied):
            for substring in self.conditionally_denied:
                title = title.replace(substring, '')

        # Special case handling for '歌ってみた' and '雑談'
        if any(substring in title for substring in ('歌ってみた', '歌みた')) and '雑談' in title:
            title = title.replace('歌ってみた', '').replace('歌みた', '')

        # Check for never allowed substrings
        if any(never in title for never in self.never_allowed):
            return False

        # Replace excluded substrings with empty string
        replaced_title = re.sub('|'.join(self.exclude_list), '', title)

        # Parse title
        parsed_title = '㉱'.join([substring for substring in re.split('\W+', replaced_title) if (not substring.endswith('sing') or substring == 'sing')])

        # Check for included substrings using regex
        include_regex = '|'.join(self.include_list)
        return any(re.search(include_regex, substring) for substring in re.split('㉱', parsed_title))


def initialize_title_checker():
    (
        exclude_list, 
        never_allowed, 
        temporarily_excluded,  # Assuming this is not used in the logic
        include_list,
        banned_list,       # Assuming this is not used in the logic
        combination_conditions,
        conditionally_denied,
    ) = get_title_filters(get_collection_data(client, MANAGEMENT_DB, FILTER_COLLECTION))

    return TitleChecker(
        exclude_list=exclude_list,
        never_allowed=never_allowed,
        include_list=include_list,
        combination_conditions=combination_conditions,
        conditionally_denied=conditionally_denied
    ), temporarily_excluded


def is_duration_valid(duration):
    if duration is None:
        return True
    return pd.Timedelta(minutes=1) <= duration #<= pd.Timedelta(minutes=11)


## minor utils
def convert_identifier_to_name(target, id_name_dict):
    return id_name_dict.get(target, '미등록 채널')


def time_localizer(yt_time):
    timestamp = pd.Timestamp(yt_time)
    if timestamp.tz:
        if timestamp.tz == 'Asia/Seoul':
            pass
    else:
        timestamp = timestamp.tz_localize('Asia/Seoul')
    return timestamp
    

def get_current_time_with_tz():
    return (pd.Timestamp.now().floor('s') + pd.Timedelta(hours=9)).tz_localize('Asia/Seoul')


def fetch_youtube_rss(channel_id):

    # Initialize index attribute if it doesn't exist
    if not hasattr(fetch_youtube_rss, 'index'):
        fetch_youtube_rss.index = 0
    
    # Define Cloudflare Worker proxy URLs
    worker_urls = [
        'https://divine-darkness-7d0f.wgo9xgxvy.workers.dev',
        'https://proxy.utawakuworker.workers.dev', 
        'https://proxy.momosenina.workers.dev', 
    ]
    worker_url = worker_urls[fetch_youtube_rss.index]
        
    # Update index for the next call
    fetch_youtube_rss.index = (fetch_youtube_rss.index + 1) % len(worker_urls)

    # Construct the RSS URL with the selected Cloudflare Worker proxy URL
    rss_url = f'{worker_url}/https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}'
    
    # Define request headers
    headers = {
        'Accept': 'text/html',
        'Authorization': CLOUDFLARE_TOKEN,
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US',
        'Referer': 'https://youtube.com',
        'Connection': 'keep-alive',
        'X-Forwarded-Header': inet_ntoa(pack('>I', random.randint(1, 0xffffffff))),
        'User-Agent': UserAgent().random
    }
    
    try:
        # Fetch the RSS feed with a timeout
        response = requests.get(rss_url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors
        
        # Parse the feed content using feedparser
        feed = feedparser.parse(response.content)
        return feed.entries
    
    except requests.exceptions.Timeout:
        pass
        #print(f"Request timed out for: {channel_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


# Function to retrieve all documents from the collection
def retrieve_all_documents(collection):
    all_video_ids = itertools.chain.from_iterable(
        doc.get("video_ids", []) 
        for doc in collection.find()
    )
    return set(all_video_ids)


# Function to append a bulk update operation
def create_cache_bulk_updates(updated_channel, interesting_new_videos):
    cache_bulk_updates = [
        UpdateOne(
            {"channel_id": updated_channel},
            {"$set": {"video_ids": list(interesting_new_videos)}},
            upsert=True
        )
    ]
    return cache_bulk_updates

def create_meta_bulk_updates(chan_meta):
    meta_bulk_updates = [
        UpdateOne(
            {'_id': channel_id},
            {'$set': {'author': author}},
            upsert=True
        )
        for channel_id, author in chan_meta.items()
    ]
    return meta_bulk_updates

def modify_bulk_update(bulk_updates, exclude_vids):
    return [
        UpdateOne(
            {"channel_id": update._filter['channel_id']},
            {"$set": {"video_ids": list(set(update._doc['$set']['video_ids']) - set(exclude_vids))}},
            upsert=True
        )
        for update in bulk_updates
    ]


def perform_bulk_update(collection, bulk_updates):
    if bulk_updates:
        try:
            result = collection.bulk_write(bulk_updates)
            #print(f"Bulk update completed. Modified {result.modified_count} documents.")
        except Exception as e:
            print(f"Error performing bulk update: {e}")


# Function to check updates for a single channel
def check_channel_updates(channel_id, documents, title_checker):
    try:
        feed_entries = fetch_youtube_rss(channel_id)

        interesting_new_videos = {
            entry.yt_videoid
            for entry in feed_entries
            if (entry.yt_videoid not in documents
                and title_checker.validate(entry.title))
        }

        chan_meta = {entry.yt_channelid:entry.author for entry in feed_entries}

        if interesting_new_videos:
            return channel_id, interesting_new_videos, chan_meta

    except Exception as e:
        pass


def check_all_channel_updates(channel_ids, documents, title_checker):
    new_videos_per_channel = set()
    cache_bulk_updates = []
    meta_bulk_updates = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks for each channel asynchronously
        future_to_channel = {
            executor.submit(check_channel_updates, channel_id, documents, title_checker): channel_id 
            for channel_id in channel_ids
        }
        
        # Create a tqdm progress bar with the total number of channels
        progress_bar = tqdm(total=len(channel_ids), desc="Checking updates", unit="channel", leave=False)
        
        for future in concurrent.futures.as_completed(future_to_channel):
            result = future.result()
            if result is not None:
                updated_channel, interesting_new_videos, chan_meta = result
                new_videos_per_channel.update(interesting_new_videos)
                
                # Create cache bulk updates
                cache_updates = create_cache_bulk_updates(updated_channel, interesting_new_videos)
                cache_bulk_updates.extend(cache_updates)
                
                # Create meta bulk updates
                meta_updates = create_meta_bulk_updates(chan_meta)
                meta_bulk_updates.extend(meta_updates)
                
            # Update the progress bar for each completed task
            progress_bar.update(1)
            time.sleep(0.08) 
        
        # Close the progress bar once all tasks are completed
        progress_bar.close()

    # Extract all new interesting video IDs
    all_new_videos_interesting = list(new_videos_per_channel)

    return all_new_videos_interesting, cache_bulk_updates, meta_bulk_updates


def get_video_data_at_once_v4(API_KEY: str, video_ids: list):
    # Split video_ids into chunks of 50 (max limit for videos.list())
    video_id_chunks = [video_ids[i:i+50] for i in range(0, len(video_ids), 50)]
    results = []
    
    USER_AGENTS = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.1264.71 Safari/537.36 Edg/103.0.1264.71'
    headers = {'User-Agent': USER_AGENTS}
    
    for upto_fif_ids in video_id_chunks:
        params = {
            "key": API_KEY,
            "part": "snippet,liveStreamingDetails,contentDetails",
            "id": ",".join(upto_fif_ids)
        }
        response = requests.get("https://www.googleapis.com/youtube/v3/videos", params=params)
        response.raise_for_status()
        data = json.loads(response.text)
        for item in data["items"]:
            video_id = item.get("id")
            snippet = item.get("snippet", {})
            content_details = item.get('contentDetails', {})
            live_details = item.get('liveStreamingDetails', {})
            
            #is_stream = True if item.get('contentDetails').get('duration')=='P0D' else False
            
            title = snippet.get('title')
            channel_id = snippet.get("channelId")

            ##snippet
            published_at = snippet.get("publishedAt")
            #thumbnail_url = next(
            #    (snippet.get("thumbnails", {}).get(resolution, {}).get("url") 
            #     for resolution in ["maxres", "high", "standard","medium", "default"]
            #     if snippet['thumbnails'].get(resolution)
            #    ),
            #    "failed to get"
            #)
            
            ##live_details
            scheduled_start_time = time_localizer(live_details.get("scheduledStartTime") or published_at or pd.Timestamp.now()) 
            actual_start_time = time_localizer(live_details.get('actualStartTime'))
            actual_end_time = time_localizer(live_details.get('actualEndTime'))
            concurrent_viewers = live_details.get('concurrentViewers', 0) if 'concurrentViewers' in live_details else -1

            # Fetch video duration
            video_duration = pd.to_timedelta(content_details.get('duration', None))

            scheduled_video = ScheduledVideo(
                channel_id, video_id, title, 
                scheduled_start_time, 
                #thumbnail_url, 
                published_at, video_duration,
                actual_start_time, actual_end_time,
                concurrent_viewers
                #hide, isStreaming is not explicitly assigned here
            )

            results.append(scheduled_video)

    return results

def convert_to_named_entity_dict(videos_list, id_name_dict):
    data = []
    for video in videos_list:
        try:
            video_data = {
                'Title': video.title,
                'VideoId': video.video_id,
                'ChannelId': video.channel_name,
                'ChannelName': convert_identifier_to_name(
                    video.channel_name, id_name_dict
                ),
                'ScheduledTime': f"{video.scheduled_start_time:%Y-%m-%d %H:%M:%S}",
                #'ThumbnailURL': video.thumbnail_url,
                'Hide': 'FALSE', # Default value for hide, manually mod 
                'broadcastStatus': 'NULL', # Default value for broadcastStatus, will be used later to check if stream is done or not
                'isVideo': "TRUE" if is_duration_valid(video.duration) else "FALSE",
                'concurrentViewers' : video.concurrent_viewers
            }
            data.append(video_data)
        except Exception as e:
            print(f"Error processing video data: {e}")
    return data


def get_new_schedules_v2(chan_id_list):

    id_name_dict = fetch_raw_names_and_merge(client)

    title_checker, temporarily_excluded = initialize_title_checker() 
    schedule_df = get_collection_data(client, SCHEDULE_DB, UPCOMING_COLLECTION, adjust_tz=True)
    
    already_scheduled, streaming_nows = get_current_schedules_and_streaming_ids(
        schedule_df, id_name_dict
    )
    
    need_not_to_check_vids = set(already_scheduled) | set(temporarily_excluded)
    #chan_id_list = [x for x in chan_id_list if x not in streaming_nows]
    chan_id_list = list(set(chan_id_list) - set(streaming_nows))
    
    remote_cache = client[MANAGEMENT_DB][CACHE_COLLECTION]
    cached_vids = retrieve_all_documents(remote_cache)
    collected_vids, cache_bulk_updates, meta_bulk_updates = check_all_channel_updates(
        chan_id_list, cached_vids, title_checker
    )
    
    num_check = len(collected_vids)

    #need_to_be_checked = [x for x in collected_vids if x not in need_not_to_check_vids]
    need_to_be_checked = list(set(collected_vids) - need_not_to_check_vids)
    list_of_scheduled_videos = get_video_data_at_once_v4(API_KEY, need_to_be_checked)

    vids_to_update, not_yet = classify_videos(list_of_scheduled_videos)
    data = convert_to_named_entity_dict(vids_to_update, id_name_dict)
    #_temp_list = [x.get('VideoId') for x in data] if data else []
    #specimens = [x for x in list_of_scheduled_videos if x.video_id not in _temp_list]
    #sample = convert_to_named_entity_dict(specimens, id_name_dict)
    #synchronize_mongodb(client, sample, None, None, SCHEDULE_DB, 'specimens')

    if data:
        write_to_db(client, data)
        update_live_status_v5(client, API_KEY)
    else:
        print(f"No data : {num_check}")
        
    bulk_updates_fedback = modify_bulk_update(cache_bulk_updates, not_yet)
    perform_bulk_update(remote_cache, bulk_updates_fedback)
    perform_bulk_update(client[MANAGEMENT_DB][RAW_NAME_COLLECTION], meta_bulk_updates)


def write_to_db(client, new_data):

    video_id_column = 'VideoId'
    schedule_df = get_collection_data(client, SCHEDULE_DB, UPCOMING_COLLECTION, adjust_tz=True)
        
    existing_vids = []

    if not schedule_df.empty:
        #existing_vids = schedule_df['URL'].apply(lambda x: x.split('=')[-1]).tolist()
        existing_vids = schedule_df[video_id_column].tolist()
        schedule_df, outdated = identify_outdated_data(schedule_df)
    else:
        schedule_df, outdated = None, None

    outdated = outdated.to_dict(orient='records') if outdated is not None else None
    new_docs = [x for x in new_data 
                if x[video_id_column] not in existing_vids] if new_data else None
    
    synchronize_mongodb(client, new_docs, outdated, schedule_df, SCHEDULE_DB, UPCOMING_COLLECTION)
    synchronize_mongodb(client, outdated, None, None, SCHEDULE_DB, PAST_COLLECTION)
    
    print(f"New Streams: {len(new_docs) if new_docs else 0} | Outdated Streams: {len(outdated) if outdated is not None else 0}")


def update_schedule_df(at_least_public_videos, schedule_df, broadcast_status_column='broadcastStatus', scheduled_time_column='ScheduledTime'):
    for scheduled_video in at_least_public_videos:
        video_id = scheduled_video.video_id
        
        try:
            idx = schedule_df[schedule_df['VideoId']==video_id].index[0]
            broadcast_status = (
                'FALSE' if not pd.isna(scheduled_video.actual_end_time) else
                ('TRUE' if not pd.isna(scheduled_video.actual_start_time) else 'NULL')
            )          
            scheduled_time = scheduled_video.scheduled_start_time
            actually_started_at = scheduled_video.actual_start_time
            schedule_df[scheduled_time_column] = schedule_df[scheduled_time_column].astype(object)
            
            #re-title
            if scheduled_video.title:
                schedule_df.at[idx, 'Title'] = scheduled_video.title
            
            #update broadcast status
            schedule_df.at[idx, broadcast_status_column] = broadcast_status
            
            #check if rescheduled (assume delayed)
            if schedule_df.at[idx, broadcast_status_column] == "NULL":
                schedule_df.at[idx, scheduled_time_column] = scheduled_time
                
            if not pd.isna(actually_started_at) and not pd.isna(scheduled_time):
                if abs(scheduled_time - actually_started_at) > pd.Timedelta(minutes=10):
                    schedule_df.at[idx, scheduled_time_column] = actually_started_at
            
            #check if rescheduled (assume forward-pushed) #temporally force update
            #if scheduled_time > actually_started_at:
            #    schedule_df.at[idx, scheduled_time_column] = actually_started_at
                
            #if scheduled_video.thumbnail_url:
            #    schedule_df.at[idx, 'ThumbnailURL'] = scheduled_video.thumbnail_url    
                
            if scheduled_video.concurrent_viewers != -1 and scheduled_video.duration != 0:
                schedule_df.at[idx, 'concurrentViewers'] = scheduled_video.concurrent_viewers  
                
            #schedule_df[scheduled_time_column] = pd.to_datetime(schedule_df[scheduled_time_column])

        except (IndexError, KeyError):
            # Skip over streams that don't have liveStreamingDetails or don't contain actualStartTime key
            continue
        except Exception as exc:
            print(f'Failed while updating schedule df : {exc}')

## db relateds

def connect_to_mongodb(client, db_name, collection_name):
    db = client[db_name]
    collection = db[collection_name]
    return collection


def get_collection_data(client, database_name, collection_name, adjust_tz=False):
    collection = connect_to_mongodb(client, database_name, collection_name)
    all_documents = list(collection.find())
    
    #dtype=object to avoid estimate data type esp. for timestamps
    df = pd.DataFrame(all_documents, dtype=object)  
    
    if adjust_tz:
        df['ScheduledTime'] = pd.to_datetime(df['ScheduledTime'])
        df['ScheduledTime'] = df['ScheduledTime'].dt.tz_localize(
            'UTC'
        ).dt.tz_convert('Asia/Seoul')
        df = df.sort_values("ScheduledTime").reset_index(drop=True)
        
    return df


def synchronize_mongodb(client, new_docs, outdated, updated, db_name, collection_name):

    try:
        collection = connect_to_mongodb(client, db_name, collection_name)
        bulk_operations = []

        if new_docs is not None:
            if isinstance(new_docs, pd.DataFrame):
                new_docs = new_docs.to_dict('records')
            if isinstance(new_docs, list):
                bulk_operations.extend([
                    UpdateOne(
                        {'_id': doc.get('_id', ObjectId())}, 
                        {'$setOnInsert': doc}, upsert=True
                    ) 
                    for doc in new_docs
                ])
            else:
                raise ValueError("Unsupported type for new_docs. It should be either a dictionary or a pandas DataFrame.")

        removed_ids = []
        
        if isinstance(outdated, list):
            removed_ids = [doc['_id'] for doc in outdated]
        if isinstance(outdated, pd.DataFrame) and not outdated.empty:
            removed_ids = outdated['_id'].tolist()          

        if removed_ids:
            #removed_ids = [ObjectId(doc['_id']) for doc in outdated]
            bulk_operations.append(DeleteMany({'_id': {'$in': removed_ids}}))

        if isinstance(updated, pd.DataFrame) and not updated.empty:
            current_time = pd.Timestamp.now()
            bulk_operations.extend([
                UpdateOne(
                    {'_id': row['_id']}, 
                    {'$set': {**row, 'last_modified': current_time}},
                     upsert=True
                )
                for row in updated.to_dict('records')
            ])

        if bulk_operations:
            collection.bulk_write(bulk_operations)
            #print("Synchronization completed successfully.")

    except Exception as e:
        print(f"Error occurred during synchronization: {e}")


def get_title_filters(df):
    # Extracting values from DataFrame
    (
        bad_names, 
        never_allowed, 
        temporarily_excluded, 
        exclude_unique,
        include_list,
        banned_list,
        combination_conditions,
        combination_denied
    )= (
        df[column][0] for column in [
            'bad_names', 'never_allowed', 'temporal_exclude', 'exclude_list',
            'include_list', 'banned_list', 'combination_conditions', 'conditionally_denied'
        ]
    )

    # Cleaning and processing lists
    cleaned_lists = lambda lst: [x.lower().strip() for x in lst if x.strip()]
    exclude_list = cleaned_lists(exclude_unique + bad_names)
    banned_list = cleaned_lists(banned_list)
    include_list = cleaned_lists(include_list)
    never_allowed = cleaned_lists(never_allowed)
    combination_conditions = cleaned_lists(combination_conditions)
    combination_denied = cleaned_lists(combination_denied)

    # Sorting lists
    exclude_list.sort(key=len, reverse=True)
    combination_conditions.sort(key=len, reverse=True)

    return (
        exclude_list, 
        never_allowed, 
        temporarily_excluded, 
        include_list, 
        banned_list, 
        combination_conditions, 
        combination_denied
    )



def identify_outdated_data(df):
    # Calculate current time in Asia/Tokyo timezone
    # Update 'hide' attribute for old videos
    current_time = get_current_time_with_tz()
    #current_time = pd.Timestamp.now().floor('s') + pd.Timedelta(hours=9)
    #df['ScheduledTime'] = pd.to_datetime(df['ScheduledTime'])    
    df.loc[df['ScheduledTime'].between(
        current_time - pd.Timedelta(days=3), current_time
    ), 'Hide'] = 'TRUE'

    outdated = df[
        df['ScheduledTime'] < current_time - pd.Timedelta(days=3)
    ].drop_duplicates('VideoId').sort_values('ScheduledTime')
    
    df = (
        df.drop_duplicates('VideoId')
          .drop(outdated.index)
          .sort_values('ScheduledTime')
          .dropna(how='all')
          .reset_index(drop=True)
    )
    
    return df, outdated

def update_live_status_v5(client, API_KEY):
    # Define the column names as variables for a slightly better maintainability
    broadcast_status_column = 'broadcastStatus'
    scheduled_time_column = 'ScheduledTime'
    hide_column = 'Hide' 
    url_column = 'VideoId'
    num_viewers_column = 'concurrentViewers'
    # hide column is somewhat ambiguous as it is not used strictly for hiding items, 
    # but also used to display scheduled time is passed 

    id_name_dict = fetch_raw_names_and_merge(client)
    title_checker, temporarily_excluded = initialize_title_checker() 
    
    schedule_df = get_collection_data(
        client, SCHEDULE_DB, UPCOMING_COLLECTION, adjust_tz=True
    )
    
    # rename unknow channels if viable
    mask = schedule_df['ChannelName'] == '미등록 채널'
    schedule_df.loc[mask, 'ChannelName'] = schedule_df.loc[mask].apply(
        lambda row: convert_identifier_to_name(row['ChannelId'], id_name_dict), axis=1
    )

    # Parse the scheduledtime
    schedule_df[scheduled_time_column] = pd.to_datetime(schedule_df[scheduled_time_column])
    current_time = get_current_time_with_tz()

    # Set the streams that can be regarded as ended to FALSE
    schedule_df.loc[
        (schedule_df[scheduled_time_column] <= current_time - pd.Timedelta(hours=12)) & 
        (schedule_df[broadcast_status_column]!= "NULL"), ##better to specify "TRUE"
        broadcast_status_column
    ] = "FALSE"

    # Get the IDs of the videos that are currently streaming or about to start streaming
    not_explicitly_ended_vids = schedule_df.loc[
        (schedule_df[scheduled_time_column] > (current_time - pd.Timedelta(hours=12))) &
        (schedule_df[broadcast_status_column] != "FALSE"), 
        url_column
    ].tolist() #str.extract(r'=(.+)$', expand=False)

    if not_explicitly_ended_vids:
        at_least_public_videos = get_video_data_at_once_v4(
            API_KEY, not_explicitly_ended_vids
        )

        update_schedule_df(
            at_least_public_videos, schedule_df, 
            broadcast_status_column,
            scheduled_time_column
        )

    # Check which video IDs are currently streaming
    at_least_public_video_ids = {x.video_id for x in at_least_public_videos}
    not_explicitly_public = list(
        set(not_explicitly_ended_vids) - at_least_public_video_ids
    )

    # drop hidden or cancelled before stream starts
    potentially_cancelled_df = pd.DataFrame()
    if not_explicitly_public:

        #find items that were live till the last update but cant be accessed now
        potentially_ended_idx = schedule_df[
            (schedule_df[url_column].str.contains('|'.join(not_explicitly_public))) &
            (schedule_df[broadcast_status_column] == 'TRUE')
        ].index

        if not potentially_ended_idx.empty:
            schedule_df.loc[
                list(potentially_ended_idx), broadcast_status_column
            ] = 'FALSE'

        #find items that were not started till the last update and cant be accessed now
        potentially_cancelled_idx = schedule_df[
            (schedule_df[url_column].str.contains('|'.join(not_explicitly_public))) & 
            (schedule_df[hide_column] != 'TRUE')
        ].index

        if not potentially_cancelled_idx.empty:
            potentially_cancelled_df = schedule_df.loc[potentially_cancelled_idx]

    # find items delayed but not rescheduled, will be dropped
    non_rescheduled_delayed = schedule_df.loc[
        (schedule_df[hide_column]=='TRUE') & 
        (schedule_df[num_viewers_column]==-1) &
        (schedule_df[broadcast_status_column]=='NULL')
    ]

    # find items of which title changed, or temporally excluded
    invalid_items = schedule_df[
        ~schedule_df['Title'].apply(lambda x: title_checker.validate(x))|
        schedule_df[url_column].str.contains('|'.join(temporarily_excluded))
    ]

    df_to_drop = pd.concat(
        [potentially_cancelled_df, invalid_items, non_rescheduled_delayed]
    )

    synchronize_mongodb(client, None, df_to_drop, None, SCHEDULE_DB, UPCOMING_COLLECTION)

    # Filter the main DataFrame by excluding items to be dropped
    schedule_df = schedule_df[~schedule_df.index.isin(df_to_drop.index)]

    # formatting before db writing
    # Hide is set to string 'TRUE' for schedules older than the past 3 days
    schedule_df.loc[
        schedule_df[scheduled_time_column].between(
            current_time - pd.Timedelta(days=3), current_time
        ), hide_column
    ] = 'TRUE'

    ## Hide is set to str "FALSE" for future schedules
    schedule_df.loc[
        schedule_df[scheduled_time_column] > current_time, 
        hide_column
    ] = 'FALSE'

    ## also set to "TRUE" for current live streams...
    schedule_df.loc[
        (schedule_df[broadcast_status_column] == 'TRUE') & 
        (schedule_df[hide_column] != 'TRUE'), 
        hide_column
    ] = 'TRUE'

    ## again, we limit schedules only in next 25 hours
    schedule_df = schedule_df[
        schedule_df[scheduled_time_column] < current_time + pd.Timedelta(hours=25)
    ]

    schedule_df = schedule_df.sort_values(
        scheduled_time_column).dropna(how='all').reset_index(drop=True)

    synchronize_mongodb(client, None, None, schedule_df, SCHEDULE_DB, UPCOMING_COLLECTION)
    

def get_current_schedules_and_streaming_ids(df, id_name_dict):
    already_scheduled = df[
        (df['Hide']=='FALSE') & (df['broadcastStatus']=='NULL')
    ]['VideoId'].tolist()
    
    streaming_now = df[df['broadcastStatus'] == 'TRUE']['ChannelId'].tolist()
    return already_scheduled, streaming_now


def get_recent_unique_channel_names(client, database_name, threshold=30):
    db = client[database_name]
    unique_channel_names = set()

    # Calculate the date 30 days ago
    thirty_days_ago = pd.Timestamp.now() - pd.Timedelta(days=threshold)

    # Iterate over collections in the database
    for collection_name in [UPCOMING_COLLECTION, PAST_COLLECTION]:
        collection = db[collection_name]

        # MongoDB aggregation pipeline
        pipeline = [
            {"$match": {"ScheduledTime": {"$gte": thirty_days_ago}}},
            {"$group": {"_id": "$ChannelName"}},
            {"$project": {"_id": 0, "ChannelName": "$_id"}}
        ]

        # Execute the aggregation pipeline
        result = list(collection.aggregate(pipeline))

        # Extract and add unique ChannelNames to the set
        for doc in result:
            unique_channel_names.add(doc["ChannelName"])

    return list(unique_channel_names)


def categorize_channels_v2(threshold_pro, threshold_less_pro, days_recently_added):
    # Retrieve channel information
    chan_info_df = get_collection_data(client, MANAGEMENT_DB, CHANNEL_COLLECTION)
    retired_channels = get_collection_data(client, MANAGEMENT_DB, INFREQUENT_COLLECTION)['channel_id'].tolist()

    # Retrieve active channels and create a dictionary mapping channel IDs to names
    #active_channels = chan_info_df[~chan_info_df['waiting'] & ~chan_info_df['channel_id'].isin(retired_channels)]
    active_channels = chan_info_df[
        (~chan_info_df['waiting']) & 
        (~chan_info_df['channel_id'].isin(retired_channels)) & 
        (chan_info_df['alive'] != False)
    ]

    id_name_dict = dict(zip(
        active_channels['channel_id'].tolist(), 
        active_channels['name_kor'].tolist()
    ))

    # Retrieve lists of channels based on activity levels
    proliferates = get_recent_unique_channel_names(client, SCHEDULE_DB, threshold=threshold_pro)
    less_proliferates = get_recent_unique_channel_names(client, SCHEDULE_DB, threshold=threshold_less_pro)
    recently_added = chan_info_df[chan_info_df['createdAt'] > (pd.Timestamp.now() - pd.Timedelta(days=days_recently_added))]['name_kor'].tolist()

    # Categorize channels based on activity levels
    less_actives_keys = [channel_id for channel_id, channel_name in id_name_dict.items() 
        if channel_name in less_proliferates and channel_name not in proliferates
    ]
    more_actives_keys = [channel_id for channel_id, channel_name in id_name_dict.items() 
        if channel_name in (proliferates + recently_added)
    ]
    inactives = [
        channel_id for channel_id in id_name_dict 
        if channel_id not in (more_actives_keys + less_actives_keys)
    ]

    # Print lengths of each returned list
    print(f"Length of Inactives: {len(inactives)}")
    print(f"Length of Less Actives: {len(less_actives_keys)}")
    print(f"Length of More Actives: {len(more_actives_keys)}")
    print(f"Length of Recently Added: {len(recently_added)}")

    return inactives, less_actives_keys, more_actives_keys, id_name_dict

def fetch_raw_names_and_merge(client):
    
    # Fetch data from MongoDB and create result_dict
    raw_names_cursor = client[MANAGEMENT_DB][RAW_NAME_COLLECTION].find()
    result_dict = {item['_id']: item['author'] for item in raw_names_cursor}
    # Determine uniques (_id values not in id_name_dict)
    uniques = set(result_dict.keys()) - set(id_name_dict.keys())
    
    filtered_result_dict = {key: result_dict[key] for key in uniques}
    merged_dict = {**id_name_dict, **filtered_result_dict}

    return merged_dict


class PayloadSender:
    def __init__(self, max_retries=3, initial_retry_delay=2, max_delay=60, chunk_size=1900):
        self.max_retries = max_retries
        self.retry_delay = initial_retry_delay
        self.max_delay = max_delay
        self.chunk_size = chunk_size

    async def send_payload(self, data, webhook_url):
        async with aiohttp.ClientSession() as session:
            while self.max_retries > 0:
                try:
                    await self._send_chunks(session, webhook_url, data)
                    return  # Successful send
                except aiohttp.ClientError as e:
                    print(f"Failed to send message to {webhook_url}. Error: {e}")

                # Retry logic
                self.max_retries -= 1
                if self.max_retries > 0:
                    await asyncio.sleep(self.retry_delay)
                    self.retry_delay = min(self.max_delay, self.retry_delay * 2)  # Exponential backoff
                else:
                    print("Max retries exceeded. Data cannot be sent.")
                    return

    async def _send_chunks(self, session, webhook_url, data):
        if len(data) <= self.chunk_size:
            await self._send_request(session, webhook_url, data)
        else:
            chunks = [data[i:i+self.chunk_size] for i in range(0, len(data), self.chunk_size)]
            for chunk in chunks:
                await self._send_request(session, webhook_url, chunk)

    async def _send_request(self, session, webhook_url, chunk):
        async with session.post(webhook_url, data=chunk, headers={"Content-Type": "application/json"}) as response:
            if response.status != 204:
                raise aiohttp.ClientError(f"Received unexpected status code {response.status}")


def task_wrapper(task_func):
    @functools.wraps(task_func)
    def wrapped_task():
        start_time = time.time()
        f = io.StringIO()

        with redirect_stdout(f):
            timestamp = pd.Timestamp.now(tz='Asia/Tokyo').strftime('%Y-%m-%d %H:%M:%S JST')
            task_name = task_func.__name__
            print(f"----------------------------------")
            print(f"{timestamp}")
            print(f"Task: {task_name}")
            try:
                task_func()
                task_status = "Completed"
            except Exception as exc:
                task_status = f"Failed: {exc}"
            end_time = time.time()
            elapsed_time = int(end_time - start_time)
            print(f"Status: {task_status} in {elapsed_time} seconds\n")

        f_value = f.getvalue()
        keywords = ['Failed', 'unusual']
        should_include = any(keyword in f_value for keyword in keywords)
        content_value = f"@everyone\n{f_value}" if should_include else f_value
        payload = json.dumps({"content": content_value})

        # Using the PayloadSender class for sending payload
        sender = PayloadSender(max_retries=5, initial_retry_delay=1, max_delay=30)
        asyncio.get_event_loop().run_until_complete(sender.send_payload(payload, WEBHOOK_URL))

    return wrapped_task


def sleep_at_night(start_time_str='03:00', end_time_str='07:00', run_frequency=1):
    start_time = pd.Timestamp(start_time_str).time()
    end_time = pd.Timestamp(end_time_str).time()
    
    def decorator(func):
        # Initialize count attribute if it doesn't exist
        if not hasattr(decorator, 'count'):
            decorator.count = 0
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get the current count attribute value
            count = decorator.count
            current_time = pd.Timestamp.now('Asia/Seoul').time()
            
            if start_time <= current_time < end_time:
                count += 1
                decorator.count = count  # Update the count attribute
                if count % run_frequency == 0:
                    return func(*args, **kwargs)
                print("Let the night embrace you with its peace and serenity.")
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator


@task_wrapper
@sleep_at_night(run_frequency=2)
def update_inactives():
    get_new_schedules_v2(inactives)


@task_wrapper
@sleep_at_night(run_frequency=2)
def update_less_actives():
    get_new_schedules_v2(less_actives)


@task_wrapper
@sleep_at_night(run_frequency=4)
def update_more_actives():
    get_new_schedules_v2(more_actives)
    

@task_wrapper
def check_schedule_changes_v2():
    update_live_status_v5(client, API_KEY)

#@task_wrapper
def check_noti():
    liveuta_push_url = 'https://liveuta.vercel.app/api/push'

    df = get_collection_data(client, SCHEDULE_DB, NOTI_COLLECTION)
    df['timestamp'] = df['timestamp'].astype(int)
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms') #unixtime
    current_datetime = pd.Timestamp.now()
    df['mask'] = df['datetime'].between(current_datetime, current_datetime + pd.Timedelta(minutes=5))

    columns_to_exclude = ["datetime", "mask", '_id']
    columns_to_include = [col for col in df.columns if col not in columns_to_exclude]
    expired = df[df['mask']==True]
    payload = expired[columns_to_include].to_dict(orient="records")

    if payload:
        response = requests.post(liveuta_push_url, data=json.dumps(payload))

        if response.status_code == 201:
            print('JSON data sent successfully.')
        else:
            print(f'Error sending JSON data. Status code: {response.status_code}, Response text: {response.text}')

    synchronize_mongodb(client, None, expired, None, SCHEDULE_DB, NOTI_COLLECTION)





client = MongoClient(ATLAS_CONNECTION_STRING)

( inactives, 
  less_actives,
  more_actives, 
  id_name_dict
) = categorize_channels_v2(
    threshold_pro=14, threshold_less_pro=30, days_recently_added=7)


schedule.clear()
check_schedule_changes_v2()
check_noti()


schedule.every(5).minutes.do(check_noti)

for minute in range(2, 60, 5):
    schedule.every().hour.at(f':{minute:02d}').do(check_schedule_changes_v2)

for minute in range(2, 60, 15):
    schedule.every().hour.at(f':{minute:02d}').do(update_more_actives)

schedule.every().hour.at(':01').do(update_inactives)
schedule.every().hour.at(':29').do(update_less_actives)
schedule.every().hour.at(':59').do(update_less_actives)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

while True:
    schedule.run_pending()
    logging.info('Waiting....')
    time.sleep(1)

time.sleep(5)
client.close()
