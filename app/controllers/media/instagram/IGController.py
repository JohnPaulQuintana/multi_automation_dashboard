from datetime import datetime, timedelta, timezone
from datetime import date as dt
from collections import defaultdict
from app.automations.log.state import log

import calendar
import requests
import json
import re
import time

# Use UTC instead of local time
end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

class IGController:
    """Controller for IG API interactions."""
    def __init__(self, FACEBOOK_BASE_API_URL:str):
        self.base_url = FACEBOOK_BASE_API_URL
        print("Instagram/Facebook Controller initialized...")

    def _extract_insight_metrics(self,job_id, insights_data, label=''):
        metrics = {
            'engagements': 0,
            'views': 0,
            'reach': 0
        }

        insights = insights_data.get('data', [])
        if insights:
            log(job_id, f"[INFO] {label} insights fetched successfully.")
            for entry in insights:
                name = entry.get('name')
                value = entry.get('total_value', {}).get('value', 0)
                if name in ['total_interactions', 'views', 'reach']:
                    if name == 'total_interactions':
                        metrics['engagements'] = value
                    elif name == 'views':
                        metrics['impressions'] = value
                    elif name == 'reach':
                        metrics['reach'] = value
                    else:
                        metrics[name] = value
        else:
            log(job_id, f"[WARNING] No {label.lower()} insights found.")

        return metrics

    # get yearly insights for IG page
    def get_yearly_metrics(self, job_id, object_id, access_token):
        today = datetime.now(timezone.utc).date() - timedelta(days=1)  # yesterday's date in UTC
        start_of_year = today.replace(month=1, day=1)
        current_start = start_of_year

        yearly_totals = {
            'total_interactions': 0,
            'views': 0,
            'reach': 0,
        }

        # Because Graph API limits date range for insights, fetch monthly chunks
        while current_start <= today:
            # Calculate end of current month chunk
            next_month = (current_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            current_end = min(next_month - timedelta(days=1), today)

            # Print the date range being requested
            log(job_id, f"Requesting data from {current_start} to {current_end}")

            insights_params = {
                'access_token': access_token,
                'metric': 'total_interactions,views,reach',
                'since': current_start.strftime('%Y-%m-%d'),
                'until': current_end.strftime('%Y-%m-%d'),
                'period': 'day',  # get daily breakdown to sum
                'metric_type': 'total_value',
            }
            
            url = f"{self.base_url}{object_id}/insights"
            response = requests.get(url, params=insights_params)
            if response.status_code != 200:
                log(job_id, f"Error fetching insights: {response.status_code} - {response.text}")
                return None

            data = response.json().get('data', [])
            
            # Sum the daily values for each metric
            for entry in data:
                name = entry['name']
                total = entry.get('total_value', {}).get('value', 0)

                if name in yearly_totals:
                    yearly_totals[name] += total
                else:
                    yearly_totals[name] = total  # in case it's not already in the dict

            current_start = current_end + timedelta(days=1)

        return yearly_totals

    def fetch_insights_for_period(self, object_id, access_token, since, until):
        """
        Fetch 'total_interactions' using daily granularity over a specified date range.
        """
        url = f"{self.base_url}{object_id}/insights"
        params = {
            "metric": "total_interactions,views,reach",
            "metric_type": "total_value",
            "period": "day",  # Always 'day', date range defines daily/monthly/yearly
            "since": since,
            "until": until,
            "access_token": access_token
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"[{since} → {until}] Request failed:", e)
            return {"error": str(e)}
        
    # Get monthly insights for IG page (correctly up to yesterday)
    def fetch_monthly_insights(self, object_id, access_token):
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        first_day_of_month = yesterday.replace(day=1)

        since = first_day_of_month.strftime('%Y-%m-%d')
        until = yesterday.strftime('%Y-%m-%d')

        return self.fetch_insights_for_period(object_id, access_token, since, until)

    # get daily insights for IG page
    def fetch_daily_insights(self, object_id, access_token):
        today = datetime.now(timezone.utc).date()
        yesterday = today - timedelta(days=1)
        since = yesterday.strftime('%Y-%m-%d')
        until = today.strftime('%Y-%m-%d')
        return self.fetch_insights_for_period(object_id, access_token, since, until)

    # get IG page insights (Followers, Engagements, Impressions and Reach)
    def get_ig_page_metrics(self, job_id, page_id, ig_id, page_tokens):
        log(job_id, f"[INFO] Processing IG metrics for Page ID: {page_id}, IG ID: {ig_id}")

        if not ig_id:
            log(job_id, "[WARNING] Skipping: No IG link associated with this page.")
            return None
        
        try:
            log(job_id, "[INFO] Fetching basic IG profile data...")
            params = {
                'fields': 'followers_count,name,media_count,follows_count',
                'access_token': page_tokens
            }

            response = requests.get(f"{self.base_url}{ig_id}", params=params)
            response.raise_for_status()
            data = response.json()

            # Fetch and attach detailed insights
            data['daily_insights'] = self._extract_insight_metrics(job_id, 
                self.fetch_daily_insights(ig_id, page_tokens), label='Daily'
            )
            data['monthly_insights'] = self._extract_insight_metrics(job_id,
                self.fetch_monthly_insights(ig_id, page_tokens), label='Monthly'
            )
            yearly_data = self.get_yearly_metrics(job_id, ig_id, page_tokens)

            data['yearly_insights'] = {
                'engagements': yearly_data.get('total_interactions', 0),
                'impressions': yearly_data.get('views', 0),
                'reach': yearly_data.get('reach', 0),
            }
            # data['engagements_yearly'] = yearly_data.get('total_interactions', 0)
            # data['views_yearly'] = yearly_data.get('views', 0)

            log(job_id, "[SUCCESS] IG profile metrics collected successfully.")
            return [data]
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Failed to fetch IG metrics: {e}")
            return {"error": str(e)}
        
    def _get_posts_for_ig(self,job_id, ig_id, page_token, since):
        """Fetch posts for a single page with pagination handling"""
        log(job_id, f"Requesting: {ig_id}")
        all_posts = []
        url = f"{self.base_url}/{ig_id}/media"

        # params = {
        #     'access_token': page_token,
        #     'fields': 'id,message,created_time',
        #     'since': since,# Format: "2025-01-01"
        #     'until': until, # today
        #     'limit': 100  # Maximum per request
        # }

        # for debugging

        params = {
            'access_token': page_token,
            'fields': 'id,caption,media_url,timestamp',
            'since': f"{since}",# Format: "2025-01-01"
            'until': f"{end_date}", # today
            'limit': 100  # Maximum per request
        }

        while url:
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                all_posts.extend(data.get('data', []))
                
                # Paginate WITHOUT resetting params
                url = data.get('paging', {}).get('next')

            except requests.exceptions.RequestException as e:
                log(job_id, f"Error fetching posts for page {ig_id}: {str(e)}")
                break
            except json.JSONDecodeError:
                log(job_id, f"Invalid JSON response from page {ig_id}")
                break

        return all_posts

    def fetch_all_ig_posts(self, job_id, page_tokens, since):
        log(job_id, "Page Tokens")
        log(job_id, f"{page_tokens}")
        all_posts = []

        for page_id, page_token, ig_id in page_tokens:
            if ig_id is None:
                log(job_id, f"⚠️ Skipping page {page_id} because IG ID is None")
                continue  # Skip this entry if IG ID is missing
            try:
                posts = self._get_posts_for_ig(job_id, ig_id, page_token, since)
                for post in posts:
                    all_posts.append({
                        'source_page_id': page_id,
                        'source_ig_id': ig_id,
                        'source_page_token': page_token,
                        'post_id': post.get('id'),
                        'created_time': post.get('timestamp'),
                        'caption': (post.get('caption') or '')[:200],
                        'media_url': post.get('media_url'),
                    })
            except Exception as e:
                log(job_id, f"❌ Error processing page {page_id}: {str(e)}")

        return all_posts
    
    def get_insights_batch(self, job_id, post_ids, page_token):
        """Get insights for a batch of posts with robust error handling"""
        if not post_ids:
            return []
            
        batch_requests = []
        base_params = f"metric=reach,views,total_interactions&access_token={page_token}"
        
        for post_id in post_ids:
            batch_requests.append({
                "method": "GET",
                "relative_url": f"{post_id}/insights?{base_params}"
            })

        try:
            response = requests.post(
                f"{self.base_url}",
                data={
                    'access_token': page_token,
                    'batch': json.dumps(batch_requests)
                },
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            log(job_id, f"Batch request failed: {str(e)}")
            # Return empty insights for all requested posts
            return [{'data': []} for _ in post_ids]
    
    def _create_empty_insights(self):
        """Return default empty insights structure"""
        return {
            'impressions': 0,
            'reach': 0,
            'reactions': 0,
        }

    def _parse_insights(self,job_id, insights_list):
        """Safely parse insights data with comprehensive checks"""
        metrics = self._create_empty_insights()
        # print("Parsing insights data...")
        # print(insights_list)
        if not isinstance(insights_list, list):
            return metrics
        
        for metric in insights_list:
            # print("Parsing metric:", metric)
            try:
                name = metric.get('name')
                values = metric.get('values', [{}])
                value = values[0].get('value', 0) if values else 0
                
                if name == 'reach':
                    metrics['reach'] = int(value) if value else 0
                elif name == 'total_interactions':
                    metrics['reactions'] = int(value) if value else 0
                elif name == 'views':
                    metrics['impressions'] = int(value) if value else 0

            except Exception as e:
                log(job_id, f"Error parsing metric {name}: {str(e)}")
                continue
                
        return metrics
    
    def process_all_post_insights(self, job_id, posts_data):
        """Process insights for all posts while maintaining page associations"""
        # Group posts by their source page
        page_groups = defaultdict(list)
        for post in posts_data:
            key = (post['source_ig_id'], post['source_page_token'])
            page_groups[key].append(post)

        # Process each page's posts
        all_insights = []
        for (ig_id, page_token), posts in page_groups.items():
            log(job_id, f"Processing {len(posts)} posts for page {ig_id}")

            # Process in batches of 50
            for i in range(0, len(posts), 50):
                batch = posts[i:i+50]
                post_ids = [p['post_id'] for p in batch]

                try:
                    insights_batch = self.get_insights_batch(job_id, post_ids, page_token)
                    log(job_id, f"Fetched insights for batch {i//50 + 1}")

                    # print(insights_batch)
                    for post, insights in zip(batch, insights_batch):
                        # print(f"Processing insights for post {post['post_id']}")
                        # print(insights)
                        body = json.loads(insights.get('body', '{}'))
                        parsed_metrics = self._parse_insights(job_id, body.get('data', []))
                        # post['insights'] = self._parse_insights(insights.get('data', []))
                        # parsed = self._parse_insights(insights.get('data', []))
                        post['insights'] = parsed_metrics
                        # post['post_link'] = f"https://www.facebook.com/{post['source_page_id']}/posts/{post['post_id']}?view=insights"
                        all_insights.append(post)
                        # log(job_id, all_insights)

                except Exception as e:
                    log(job_id, f"Error processing batch: {str(e)}")

                    for post in batch:
                        post['insights'] = self._create_empty_insights()
                        all_insights.append(post)
        return all_insights
    