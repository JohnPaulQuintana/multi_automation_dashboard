from datetime import datetime, timedelta, timezone
from datetime import date as dt, timedelta
from collections import defaultdict
from app.automations.log.state import log

import requests
import json
import re
import time

# Calculate date range (Jan 1st to today)
# Use UTC instead of local time
end_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

class FacebookController:

    """Controller for Facebook API interactions."""
    def __init__(self, FACEBOOK_BASE_API_URL:str, account:list):
        self.account = account
        self.base_url = FACEBOOK_BASE_API_URL
        print("FacebookController initialized...")

    def get_facebook_pages_with_instagram(self, job_id):
        try:
            log(job_id, "Fetching pages and link ig from Facebook...")
            params = {
                'fields': 'id,name,access_token,instagram_business_account',
                'access_token': self.account[4]
            }
            response = requests.get(self.base_url+self.account[5]+"/accounts", params=params)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.json()      # Returns a dict
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def get_yearly_metrics(self, job_id, page_id, page_access_token):
        today = datetime.now(timezone.utc).date() - timedelta(days=1)  # yesterday's date in UTC
        start_of_year = today.replace(month=1, day=1)
        current_start = start_of_year

        yearly_totals = {
            'page_views_total': 0,
            'page_post_engagements': 0,
            'page_impressions': 0,
            'page_impressions_unique': 0,
        }

        # Because Graph API limits date range for insights, fetch monthly chunks
        while current_start <= today:
            # Calculate end of current month chunk
            next_month = (current_start.replace(day=28) + timedelta(days=4)).replace(day=1)
            current_end = min(next_month - timedelta(days=1), today)

            # Print the date range being requested
            log(job_id, f"Requesting data from {current_start} to {current_end}")

            insights_params = {
                'access_token': page_access_token,
                'metric': 'page_post_engagements,page_impressions,page_impressions_unique,page_views_total',
                'since': current_start.strftime('%Y-%m-%d'),
                'until': current_end.strftime('%Y-%m-%d'),
                'period': 'day'  # get daily breakdown to sum
            }
            
            url = f"{self.base_url}{page_id}/insights"
            response = requests.get(url, params=insights_params)
            if response.status_code != 200:
                log(job_id, f"Error fetching insights: {response.status_code} - {response.text}")
                return None

            data = response.json().get('data', [])
            
            # Sum the daily values for each metric
            for entry in data:
                name = entry['name']
                values = entry.get('values', [])
                total = sum(item.get('value', 0) for item in values)

                if name in yearly_totals:
                    yearly_totals[name] += total

            current_start = current_end + timedelta(days=1)

        return yearly_totals

    def get_facebook_page_metrics(self, job_id, page_id, page_access_token, date_str):
        """
        Fetches Facebook Page metrics including:
        - Followers count (lifetime total)
        - Daily post engagements, impressions, reach, page views, and new likes
        """
        from datetime import datetime, date, timedelta

        try:
            log(job_id, "Fetching Facebook Page followers and daily insights...")

            # Step 1: Get followers count
            followers_params = {
                'access_token': page_access_token,
                'fields': 'followers_count'
            }
            followers_response = requests.get(
                f"{self.base_url}{page_id}",
                params=followers_params
            )
            followers_response.raise_for_status()
            followers_data = followers_response.json()

            # Step 2: Get daily insights (for current month up to yesterday)
            today = date.today()
            yesterday = today - timedelta(days=1)
            todaym = yesterday  # Set monthly cutoff to yesterday
            since = yesterday.replace(day=1).isoformat()
            until = yesterday.isoformat()
            log(job_id, f"📅 Date range for insights: {since}, {until}")

            insights_params = {
                'access_token': page_access_token,
                'metric': 'page_post_engagements,page_impressions,'
                        'page_impressions_unique,page_views_total,page_fan_adds,page_fans,page_daily_follows',
                'period': 'day',
                'since': since,
                'until': until
            }
            insights_response = requests.get(
                f"{self.base_url}{page_id}/insights",
                params=insights_params
            )
            insights_response.raise_for_status()
            insights_data = insights_response.json().get('data', [])

            log(job_id, "📊 Raw insights data:")
            log(job_id, f"{insights_data}")

            # Step 3: Parse and aggregate insights
            metrics = {}
            first_of_month = todaym.replace(day=1)

            for entry in insights_data:
                metric_name = entry['name']
                values = entry.get('values', [])

                monthly_sum = 0
                today_value = 0

                for val_entry in values:
                    date_str = val_entry.get('end_time', '')[:10]
                    try:
                        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                    except ValueError:
                        continue  # Skip malformed dates

                    if first_of_month <= date_obj <= todaym:
                        val = val_entry.get('value', 0)
                        if isinstance(val, int):
                            monthly_sum += val
                            if date_obj == todaym:
                                today_value = val

                metrics[f'{metric_name}_day'] = today_value
                if 'Lifetime' not in entry.get('title', ''):
                    metrics[f'{metric_name}_month'] = monthly_sum

            # Step 4: Call your yearly aggregation function
            yearly_totals = self.get_yearly_metrics(job_id, page_id, page_access_token)
            log(job_id, "📈 Yearly totals:")
            log(job_id, f"{yearly_totals}")

            # Step 5: Combine all data
            return {
                'date': date_str,
                'id': followers_data.get('id', 0),
                'followers_count': followers_data.get('followers_count', 0),

                # Daily
                'page_views_total_day': metrics.get('page_views_total_day', 0),
                'page_post_engagements_day': metrics.get('page_post_engagements_day', 0),
                'page_impressions_day': metrics.get('page_impressions_day', 0),
                'page_impressions_unique_day': metrics.get('page_impressions_unique_day', 0),
                'total_likes_today': metrics.get('page_fans_day', 0),
                'new_likes_today': metrics.get('page_fan_adds_day', 0),
                'page_daily_follows_day': metrics.get("page_daily_follows_day", 0),
                
                # Monthly
                'page_views_total_monthly': metrics.get('page_views_total_month', 0),
                'page_post_engagements_monthly': metrics.get('page_post_engagements_month', 0),
                'page_impressions_monthly': metrics.get('page_impressions_month', 0),
                'page_impressions_unique_monthly': metrics.get('page_impressions_unique_month', 0),

                # Yearly
                'yearly_page_views_total': yearly_totals.get('page_views_total', 0) if yearly_totals else 0,
                'yearly_page_post_engagements': yearly_totals.get('page_post_engagements', 0) if yearly_totals else 0,
                'yearly_page_impressions': yearly_totals.get('page_impressions', 0) if yearly_totals else 0,
                'yearly_page_impressions_unique': yearly_totals.get('page_impressions_unique', 0) if yearly_totals else 0,
            }

        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
        

    def _get_posts_for_page(self, job_id, page_id, page_token, since, until):
        """Fetch posts for a single page with pagination handling"""
        log(job_id, f"Requesting: {page_id}")
        all_posts = []
        url = f"{self.base_url}/{page_id}/posts"
        
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
            'fields': 'id,message,created_time',
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
                log(job_id, f"Error fetching posts for page {page_id}: {str(e)}")
                break
            except json.JSONDecodeError:
                log(job_id, f"Invalid JSON response from page {page_id}")
                break
        
        return all_posts

    def fetch_all_posts_for_pages(self, job_id, page_tokens, since, until):
        log(job_id, "Page Tokens")
        log(job_id, f"{page_tokens}")
        all_posts = []
        for page_id, page_token, ig_id in page_tokens:
            try:
                posts = self._get_posts_for_page(job_id, page_id, page_token, since, until)
                for post in posts:
                    all_posts.append({
                        'source_page_id': page_id,  # Track origin page
                        'source_page_token': page_token,  # Keep token for later use
                        'post_id': post.get('id'),
                        'created_time': post.get('created_time'),
                        'message': (post.get('message') or '')[:200]
                    })
            except Exception as e:
                log(job_id, f"Error processing page {page_id}: {str(e)}")
        return all_posts
    
    def get_insights_batch(self, job_id, post_ids, page_token):
        """Get insights for a batch of posts with robust error handling"""
        if not post_ids:
            return []
            
        batch_requests = []
        base_params = f"metric=post_impressions,post_impressions_unique,post_reactions_by_type_total,post_clicks&access_token={page_token}"
        
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
            'clicks': 0
        }
    
    def _parse_insights(self, job_id, insights_list):
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
                
                if name == 'post_impressions':
                    metrics['impressions'] = int(value) if value else 0
                elif name == 'post_impressions_unique':
                    metrics['reach'] = int(value) if value else 0
                elif name == 'post_reactions_by_type_total':
                    if isinstance(value, dict):
                        metrics['reactions'] = sum(int(v) for v in value.values())
                    else:
                        metrics['reactions'] = int(value) if value else 0
                elif name == 'post_clicks':
                    metrics['clicks'] = int(value) if value else 0

            except Exception as e:
                log(job_id, f"Error parsing metric {name}: {str(e)}")
                continue
                
        return metrics

    def process_all_pages_insights(self, job_id, posts_data):
        """Process insights for all posts while maintaining page associations"""
        page_groups = defaultdict(list)
        for post in posts_data:
            key = (post['source_page_id'], post['source_page_token'])
            page_groups[key].append(post)
        
        all_insights = []
        for (page_id, page_token), posts in page_groups.items():
            log(job_id, f"Processing {len(posts)} posts for page {page_id}")

            # Process in batches of 50
            for i in range(0, len(posts), 50):
                batch = posts[i:i+50]
                post_ids = [p['post_id'] for p in batch]

                try:
                    insights_batch = self.get_insights_batch(job_id, post_ids, page_token)
                    print(f"Fetched insights for batch {i//50 + 1}")
                    # print(insights_batch)
                    for post, insights in zip(batch, insights_batch):
                        # print(f"Processing insights for post {post['post_id']}")
                        # print(insights)
                        body = json.loads(insights.get('body', '{}'))
                        parsed_metrics = self._parse_insights(job_id, body.get('data', []))
                        # post['insights'] = self._parse_insights(insights.get('data', []))
                        # parsed = self._parse_insights(insights.get('data', []))
                        post['insights'] = parsed_metrics
                        post['post_link'] = f"https://www.facebook.com/{post['source_page_id']}/posts/{post['post_id']}?view=insights"
                        all_insights.append(post)
                        # print(all_insights)
                except Exception as e:
                    log(job_id, f"Error processing batch: {str(e)}")
                    for post in batch:
                        post['insights'] = self._create_empty_insights()
                        all_insights.append(post)

        return all_insights