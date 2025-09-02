from time import sleep
from datetime import datetime, timedelta, timezone
from datetime import date as dt
from collections import defaultdict
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from app.automations.log.state import log

import requests
import json
import re
import time
import pickle
import os

class TwitterController:
    """Controller for Twitter API interactions."""
    def __init__(self, TWITTER_BASE_API_URL:str, key:str):
        self.base_url = TWITTER_BASE_API_URL
        print("Twitter initialized...")
        self.headers = {
            "x-rapidapi-key": key,
            "x-rapidapi-host": "twitter-api45.p.rapidapi.com"
        }
    
    # Fetch Channel Insights Public data on twitter
    def fetch_channel_insights(self, job_id, username):
        """Fetch channel insights for a given date range channel level."""
        params = {"screenname": username}
        response = requests.get(self.base_url+"screenname.php", headers=self.headers, params=params)
        # user_data = response.json()
        try:
            user_data = response.json()
        except ValueError:
            log(job_id, f"[ERROR] Failed to decode JSON for user: {username}")
            log(job_id, f"[DEBUG] Status Code: {response.status_code}")
            log(job_id, f"[DEBUG] Response Text: {response.text[:200]}")  # print first 200 chars
            return None
        # print(user_data)
        if response.status_code == 200 and "status" in user_data:
            
            user_info = {
                "id": user_data['id'],
                "rest_id": user_data['rest_id'],
                "name": user_data['name'],
                "followers_count": user_data['sub_count'],
                "friends_count": user_data['friends'],
                "media_count": user_data['media_count'],
            }
            return user_info
        else:
            log(job_id, "Error fetching user data:", user_data.get("message", "Unknown error"))
            return None
        
    def process_media_response(self,job_id, raw_data):
        """Processes a single page of media response"""
        media_metrics = []
        
        try:
            # Safely get timeline list
            timelines = raw_data.get('timeline', []) if isinstance(raw_data, dict) else []
            
            for timeline in timelines:
                if not isinstance(timeline, dict):
                    continue

                if timeline.get('tweet_id'):
                    # Parse date
                    try:
                        dt = datetime.strptime(timeline.get('created_at', ''), '%a %b %d %H:%M:%S %z %Y')
                    except (ValueError, TypeError):
                        continue
                    
                    # Initialize media variables
                    media_id = ''
                    media_url = ''
                    media = timeline.get('media', {})

                    # Handle video media
                    if 'video' in media and isinstance(media['video'], dict):
                        video_data = media['video']
                        media_id = video_data.get('id', '')
                        media_url = video_data.get('media_url_https', '')
                    # Handle photo media (which is a list of photos)
                    elif 'photo' in media and isinstance(media['photo'], list) and len(media['photo']) > 0:
                        photo_data = media['photo'][0]
                        media_id = photo_data.get('id', '')
                        media_url = photo_data.get('media_url_https', '')

                    # Convert views to int (it comes as string in the data)
                    views_str = str(timeline.get('views', '0')).strip()
                    views = int(views_str) if views_str.isdigit() else 0

                    media_metrics.append({
                        'tweet_id': timeline.get('tweet_id', ''),
                        'media_id': media_id,
                        'views': views,
                        'engagements': {
                            'replies': timeline.get('replies', 0),
                            'retweets': timeline.get('retweets', 0),
                            'likes': timeline.get('favorites', 0),
                            'bookmarks': timeline.get('bookmarks', 0),
                            'quotes': timeline.get('quotes', 0)
                        },
                        'title': timeline.get('text', 'N/A'),
                        'media_url': media_url,
                        'created_at': dt.strftime('%Y/%m/%d'),
                        'year': dt.year,
                        'month': dt.month,
                        'day': dt.day
                    })
        except Exception as e:
            log(job_id, f"\nError processing response: {str(e)}")
            
        return media_metrics
    
    def extract_cursor(self, job_id, response_data):
        """Extracts the bottom cursor from API response"""
        try:
            timeline = response_data.get('next_cursor', "")
            if timeline:
                return timeline
            else:
                log(job_id, "No cursor found in response.")
            
            return None
        except Exception as e:
            log(job_id, f"Error extracting cursor: {str(e)}")
            return None

    # added 09/07/2025
    def get_current_month_media(self, job_id, username, rest_id):
        """
        Fetches all media posts within the last 30 days.
        Adds 'post_age' and saves the result to JSON before returning.
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        yesterday = datetime.now() - timedelta(days=1)

        rolling_window_media = []
        cursor = None
        page_count = 0
        posts_processed = 0

        log(job_id, f"\nFetching media posts from {start_date.date()} to {end_date.date()}...")

        while True:
            params = {"screenname": username, "rest_id": rest_id}
            if cursor:
                params["cursor"] = cursor

            try:
                response = requests.get(self.base_url + "timeline.php", headers=self.headers, params=params, timeout=10)
                response.raise_for_status()
                raw_data = response.json()

                page_media = self.process_media_response(job_id, raw_data)
                posts_processed += len(page_media)

                stop_pagination = False

                for media in page_media:
                    try:
                        post_time = datetime.strptime(media['created_at'], "%Y/%m/%d")
                    except Exception as e:
                        log(job_id, f"\nSkipping post due to date parse error: {e}")
                        continue

                    # Stop pagination if older than the 30-day window
                    if post_time < start_date:
                        stop_pagination = True
                        break

                    if start_date <= post_time <= end_date:
                        # Add indicator and post age in days
                        media["indicator"] = "month"
                        media["post_age"] = (end_date - post_time).days
                        rolling_window_media.append(media)
                
                # print(f"\rPages: {page_count + 1} | 30-day Posts: {len(rolling_window_media)} | Scanned: {posts_processed}", end="", flush=True)
                log(job_id, f"\rPages: {page_count + 1} | 30-day Posts: {len(rolling_window_media)} | Scanned: {posts_processed}")

                cursor = self.extract_cursor(job_id, raw_data)
                if not cursor or stop_pagination:
                    break

                page_count += 1
                sleep(2)

            except Exception as e:
                print(f"\nError: {str(e)}")
                break
        
        return rolling_window_media

    # run this when the account is new to get the total
    def get_current_year_media(self, job_id, username, rest_id):
        """
        Fetches all media posts within the last 30 days
        Maintains same output format as yearly version
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        yesterday = datetime.now() - timedelta(days=1)

        current_year = yesterday.year
        current_month = yesterday.month

        rolling_window_media = []
        cursor = None
        page_count = 0
        posts_processed = 0

        log(job_id, f"\nFetching media posts from {start_date.date()} to {end_date.date()}...")

        while True:
            params = {"screenname": username, "rest_id": rest_id}
            if cursor:
                params["cursor"] = cursor

            try:
                response = requests.get(self.base_url + "timeline.php", headers=self.headers, params=params, timeout=10)
                response.raise_for_status()
                raw_data = response.json()

                page_media = self.process_media_response(job_id, raw_data)
                posts_processed += len(page_media)

                stop_pagination = False
                post_time = None
                for media in page_media:
                    # print(media)
                    try:
                        # post_time = datetime.strptime(media['created_at'], "%a %b %d %H:%M:%S %z %Y")
                        # Parse it
                        post_time = datetime.strptime(media['created_at'], "%Y/%m/%d")
                    except Exception as e:
                        # print(start_date, post_time, end_date)
                        log(job_id, f"\nSkipping post due to date parse error: {e}")
                        continue

                    # Convert to naive datetime (UTC) for comparison
                    # post_time_naive = post_time.replace(tzinfo=None)
                    # print("Timezone:", start_date, post_time, end_date)

                    if media['year'] == current_year:

                        # adding labels
                        if start_date <= post_time <= end_date:
                            media["indicator"] = "month"
                        else:
                            media["indicator"] = "year"

                        # print("==============================================")
                        # print(media)
                        rolling_window_media.append(media)
                    
                    elif (media['year'] < current_year):
                        stop_pagination = True

                    # if start_date <= post_time <= end_date:
                    #     rolling_window_media.append(media)
                    # elif post_time < start_date:
                    #     stop_pagination = True  # We've passed the target window
                # print(f"\rPages: {page_count + 1} | 30-day Posts: {len(rolling_window_media)} | Scanned: {posts_processed}", end="", flush=True)
                log(job_id, f"\rPages: {page_count + 1} | 30-day Posts: {len(rolling_window_media)} | Scanned: {posts_processed}")

                cursor = self.extract_cursor(job_id, raw_data)
                if not cursor or stop_pagination:
                    break

                page_count += 1
                sleep(2)

            except Exception as e:
                log(job_id, f"\nError: {str(e)}")
                break
        
        log(job_id, f"\nFound {len(rolling_window_media)} posts from the last 30 days.")
        
        return rolling_window_media
    
    def analyze_current_year_metrics(self, media_data):
        """Analyzes metrics specifically for current year data"""
        if not media_data:
            return {}

        yesterday = datetime.now().date() - timedelta(days=1)
        current_year = yesterday.year
        current_month = yesterday.month
        current_month_key = f"{current_year}/{current_month:02d}"

        results = {
            'monthly': {
                'posts': 0,
                'views': 0,
                'engagements': 0
            },  # stores all months e.g., '2025/06': {...}
            'total': {
                'posts': 0,
                'views': 0,
                'engagements': 0
            },
            'current_month': {
                'posts': 0,
                'views': 0,
                'engagements': 0
            },
            'yesterday': {
                'posts': 0,
                'views': 0,
                'engagements': 0
            }
        }

        for media in media_data:
            # Parse views + engagement
            views = int(media.get('views', 0) or 0)
            engagements = sum(media['engagements'].values())

            # Totals
            results['total']['posts'] += 1
            results['total']['views'] += views
            results['total']['engagements'] += engagements

            # Monthly key
            # month_key = f"{media['year']}/{media['month']:02d}"
            # if month_key not in results['monthly']:
            #     results['monthly'][month_key] = {
            #         'posts': 0,
            #         'views': 0,
            #         'engagements': 0
            #     }

            if media["indicator"] == "month":
                results['monthly']['posts'] += 1
                results['monthly']['views'] += views
                results['monthly']['engagements'] += engagements

            # Daily check
            try:
                media_date = datetime.strptime(media.get("created_at", ""), "%Y/%m/%d").date()
                if media_date == yesterday:
                    results['yesterday']['posts'] += 1
                    results['yesterday']['views'] += views
                    results['yesterday']['engagements'] += engagements
            except Exception:
                continue
        
        # Extract current month's values from the monthly map
        results['current_month'] = results['monthly']

        # Averages
        if results['total']['posts'] > 0:
            results['total']['avg_views'] = results['total']['views'] / results['total']['posts']
            results['total']['avg_engagements'] = results['total']['engagements'] / results['total']['posts']
        else:
            results['total']['avg_views'] = 0
            results['total']['avg_engagements'] = 0

        return results

