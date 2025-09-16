from app.config.loader import TYPE,PROJECT_ID,PRIVATE_KEY_ID,PRIVATE_KEY,CLIENT_EMAIL,CLIENT_ID,AUTH_URI,TOKEN_URI,AUTH_PROVIDER_X509_CERT_URL,CLIENT_X509_CERT_URL,UNIVERSE_DOMAIN
from app.automations.log.state import log
# from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
# from config.config import Config
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests
import os
import time
import re
import random


class SpreadSheetController:
    def __init__(self, spreadsheet, range=None):
        self.spreadsheet = spreadsheet
        self.range = range if range else "ACCOUNTS!A3:I"

        self.config_dict = {
            "type": TYPE,
            "project_id": PROJECT_ID,
            "private_key_id": PRIVATE_KEY_ID,
            "private_key": PRIVATE_KEY,
            "client_email": CLIENT_EMAIL,
            "client_id": CLIENT_ID,
            "auth_uri": AUTH_URI,
            "token_uri": TOKEN_URI,
            "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
            "client_x509_cert_url": CLIENT_X509_CERT_URL,
            "universe_domain": UNIVERSE_DOMAIN,
        }

    def get_facebook_accounts(self, job_id):
        log(job_id, "Fetching accounts from spreadsheet...")
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(self.config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range=self.range).execute()
            values = result.get('values', [])
            if not values:
                log(job_id, "No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            log(job_id, f"An error occurred: {err}")

    def get_facebook_pages(self, job_id):
        log(job_id, "Fetching pages from spreadsheet...")
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(self.config_dict, scopes=scope)
        try:
            service = build('sheets', 'v4', credentials=creds)
            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.spreadsheet, range="PAGES!A2:K").execute()
            values = result.get('values', [])
            if not values:
                log(job_id, "No data found.")
            else:
                # for row in values:
                #     print(row)  # Process each row as needed
                return values
        except HttpError as err:
            log(job_id, f"An error occurred: {err}")
    

    def _initialize_google_sheets_service(self, job_id):
        """Initialize and return the Google Sheets service."""
        log(job_id, "Initializing Google Sheets service...")
        # config_dict = Config.as_dict()
        scope = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(self.config_dict, scopes=scope)
        return build('sheets', 'v4', credentials=creds)

    def _get_sheet_id(self, service, spreadsheet_id: str, tab_name: str) -> int:
        """Helper method to get sheetId from tab name"""
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields='sheets.properties'
        ).execute()
        
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == tab_name:
                return sheet['properties']['sheetId']
        
        raise ValueError(f"Sheet '{tab_name}' not found in spreadsheet")


    def _handle_date_column(self, job_id, service, spreadsheet_id, sheet_id, tab_name, today_str):
        """Check if date column exists or create it, returning the column index."""
        log(job_id, f"Handling date column for {today_str}...")
        sheet = service.spreadsheets()
        
        # Check if date exists in headers
        header_result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!1:1"
        ).execute()
        headers = header_result.get('values', [[]])[0]

        if today_str in headers:
            log(job_id, f"Column for {today_str} already exists.")
            return headers.index(today_str)
        
        # Create new date column
        log(job_id, f"Creating new column for {today_str}...")
        date_col_index = 3  # Column D
        requests = [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": date_col_index,
                        "endIndex": date_col_index + 1
                    },
                    "inheritFromBefore": True
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": date_col_index,
                        "endColumnIndex": date_col_index + 1
                    },
                    "cell": {
                        "userEnteredValue": {"stringValue": today_str},
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0, "green": 0, "blue": 0},
                            "horizontalAlignment": "CENTER",
                            "textFormat": {
                                "foregroundColor": {"red": 1, "green": 1, "blue": 1}
                            }
                        }
                    },
                    "fields": "userEnteredValue,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "startColumnIndex": date_col_index,
                        "endColumnIndex": date_col_index + 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment)"
                }
            }
        ]
        body = {"requests": requests}
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        return date_col_index

    def _get_sheet_values(self, job_id, service, spreadsheet_id, tab_name):
        """Get all values from the specified sheet tab."""
        log(job_id, "Getting sheet values...")
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, 
            range=f"{tab_name}!A1:E"
        ).execute()
        values = result.get('values', [])
        if not values:
            log(job_id, "No data found.")
        return values

    def _find_currency_row(self, job_id, values, currency, page_type):
        """Find the row index for the specified currency."""
        log(job_id, f"Finding row for currency '{currency}'...")
        for i, row in enumerate(values):
            currency_with_type = currency + "-" + page_type if page_type == "NEW" else currency
            if row and row[0] == currency_with_type:
                return i + 1  # sheet rows are 1-indexed
        log(job_id, f"Currency '{currency}' not found.")
        return None

    def _get_value_from_column_e(self, job_id, values, currency_row_index, currency):
        """Get the value from column E for difference calculation."""
        log(job_id, "Getting value from column E...")
        if len(values[currency_row_index - 1]) > 4:
            return float(values[currency_row_index - 1][4].replace(',', ''))
        log(job_id, f"No value found in column E for the currency '{currency}'.")
        return None

    def safe_execute_update(self, job_id, func, retries=5, delay=2, backoff=2):
        for attempt in range(retries):
            try:
                return func()  # get the request object
                # return request.execute()  # execute here
            except HttpError as e:
                if e.resp.status == 429 and attempt < retries - 1:
                    wait_time = delay * (backoff ** attempt)
                    log(job_id, f"[429] Rate limit hit. Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    raise

    def _update_sheet_values(self, job_id ,service, spreadsheet_id, sheet_id, tab_name, 
                            today_str, currency_row_index, insights, total_followers, value_in_column_e):
        log(job_id, "Updating sheet values...")
        sheet = service.spreadsheets()

        # Values
        #default is 0 if negative
        # difference = max(0, total_followers - value_in_column_e)
        difference = total_followers - value_in_column_e

        values_only = [
            insights.get('page_daily_follows_day', 0),
            insights.get('page_post_engagements_day', 0),
            insights.get('page_post_engagements_monthly', 0),
            insights.get('yearly_page_post_engagements', 0),
            insights.get('page_impressions_day', 0),
            insights.get('page_impressions_monthly', 0),
            insights.get('yearly_page_impressions', 0),
            insights.get('page_impressions_unique_day', 0),
            insights.get('page_impressions_unique_monthly', 0),
            insights.get('yearly_page_impressions_unique', 0),
            insights.get('page_views_total_day', 0),
            insights.get('page_views_total_monthly', 0),
            insights.get('yearly_page_views_total', 0),
            insights.get('total_likes_today', 0),
            insights.get('new_likes_today', 0),
        ]

        all_values = [[today_str], [total_followers]] + [[v] for v in values_only]

        # Batch update all values from D{currency_row_index-1} to D{currency_row_index+15}
        data = [
            {
                "range": f"{tab_name}!D{currency_row_index-1}:D{currency_row_index+15}",
                "values": all_values
            }
        ]
        self.safe_execute_update(job_id, lambda: sheet.values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": data}
        ).execute())

    def get_spreadsheet_column(self, job_id, spreadsheet_id: str, tab_name: str, currency: str, insights:list, total_followers: int = 0 ,page_type: str = "page"):
        try:
            # Initialize service
            service = self._initialize_google_sheets_service(job_id)
            
            # Get sheet metadata and ID
            sheet_id = self._get_sheet_id(service, spreadsheet_id, tab_name)
            if sheet_id is None:
                return None

            #ON DEVELOPMENT
            # Get today's date string
            # today_str = datetime.now().strftime('%d/%m/%Y') # Current date

            #ON DEPLOYED
            today = datetime.now(timezone.utc).date()
            yesterday = today - timedelta(days=1)
            today_str = yesterday.strftime('%d/%m/%Y') #Yesterday date
            # today_str = today.strftime('%d/%m/%Y') #Yesterday date

            # Check or create date column
            date_col_index = self._handle_date_column(
                job_id, service, spreadsheet_id, sheet_id, tab_name, today_str
            )
            
            # Get all values from sheet
            values = self._get_sheet_values(job_id, service, spreadsheet_id, tab_name)
            if not values:
                return None

            # Find currency row
            currency_row_index = self._find_currency_row(job_id, values, currency, page_type)
            if currency_row_index is None:
                return None

            # Get value from column E for difference calculation
            value_in_column_e = self._get_value_from_column_e(job_id, values, currency_row_index, currency)
            if value_in_column_e is None:
                return None

            # Update cells with today's date, total followers, and difference
            self._update_sheet_values(
                job_id, service, spreadsheet_id, sheet_id, tab_name,
                today_str, currency_row_index, insights,total_followers, value_in_column_e
            )
            
            return values

        except HttpError as err:
            log(job_id, f"An error occurred: {err}")
            return None
        


    # This is for insight process but not beeing used for now

    def extract_facebook_post_id(self,url: str) -> Optional[str]:
        """
        Extracts the post ID from a Facebook post URL.
        
        Supports formats like:
        - https://www.facebook.com/{page_id}/posts/{page_id}_{post_id}
        - https://www.facebook.com/{page_id}/posts/{post_id}
        - https://www.facebook.com/story.php?story_fbid={post_id}&id={page_id}
        
        Returns:
            post_id (str) if found, else None
        """
        # Pattern: /posts/{page_id}_{post_id}
        match = re.search(r'/posts/\d+_(\d+)', url)
        if match:
            return match.group(1)

        # Pattern: /posts/{post_id}
        match = re.search(r'/posts/(\d+)', url)
        if match:
            return match.group(1)

        # Pattern: story.php?story_fbid={post_id}
        match = re.search(r'story_fbid=(\d+)', url)
        if match:
            return match.group(1)

        return None

    def calculate_day_deltas(self, job_id, post_age, insights, existing_row):
        """
        Calculates reach, impressions, and reactions deltas for 3, 7, and 30 days.
        """
        log(job_id, f"Calculating deltas for post age: {post_age} days")

        def safe_int(value, fallback=0):
            try:
                return int(value)
            except:
                return fallback
        
        reach_3 = safe_int(existing_row[3]) if len(existing_row) > 3 else 0
        reach_7 = safe_int(existing_row[4]) if len(existing_row) > 4 else 0
        imp_3 = safe_int(existing_row[7]) if len(existing_row) > 7 else 0
        imp_7 = safe_int(existing_row[8]) if len(existing_row) > 8 else 0
        react_3 = safe_int(existing_row[11]) if len(existing_row) > 11 else 0
        react_7 = safe_int(existing_row[12]) if len(existing_row) > 12 else 0

        reach_now = insights.get('reach', 0)
        imp_now = insights.get('impressions', 0)
        react_now = insights.get('reactions', 0)

        result = {
            "reach_3": '',
            "reach_7": '',
            "reach_30": '',
            "imp_3": '',
            "imp_7": '',
            "imp_30": '',
            "react_3": '',
            "react_7": '',
            "react_30": ''
        }

        # 3-day snapshot (if old enough)
        if post_age >= 3:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)

        # 7-day delta (keep 3-day too)
        if post_age >= 7:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)

            result["reach_7"] = str(max(reach_now - reach_3, 0)) if reach_3 else ''
            result["imp_7"] = str(max(imp_now - imp_3, 0)) if imp_3 else ''
            result["react_7"] = str(max(react_now - react_3, 0)) if react_3 else ''

        # 30-day delta (keep 3-day and 7-day too)
        if post_age >= 30:
            result["reach_3"] = str(reach_3) if reach_3 > 0 else str(reach_now)
            result["reach_7"] = str(reach_7) if reach_7 > 0 else str(max(reach_now - reach_3, 0)) if reach_3 else ''
            result["reach_30"] = str(max(reach_now - (reach_3 + reach_7), 0)) if (reach_3 or reach_7) else ''

            result["imp_3"] = str(imp_3) if imp_3 > 0 else str(imp_now)
            result["imp_7"] = str(imp_7) if imp_7 > 0 else str(max(imp_now - imp_3, 0)) if imp_3 else ''
            result["imp_30"] = str(max(imp_now - (imp_3 + imp_7), 0)) if (imp_3 or imp_7) else ''

            result["react_3"] = str(react_3) if react_3 > 0 else str(react_now)
            result["react_7"] = str(react_7) if react_7 > 0 else str(max(react_now - react_3, 0)) if react_3 else ''
            result["react_30"] = str(max(react_now - (react_3 + react_7), 0)) if (react_3 or react_7) else ''


        return result

    def trim_sheet_rows(self, job_id, spreadsheet_id: str, tab_name: str, buffer: int = 10):
        try:
            service = self._initialize_google_sheets_service(job_id)
            sheet = service.spreadsheets()

            metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_info = next(
                s for s in metadata['sheets'] if s['properties']['title'] == tab_name
            )
            sheet_id = sheet_info['properties']['sheetId']

            # Check how many rows are used based on column A
            result = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A:A",
                majorDimension="COLUMNS"
            ).execute()
            used_rows = len(result.get('values', [[]])[0]) or 1
            new_row_count = used_rows + buffer

            total_cells = new_row_count * 20  # assuming 10 columns
            log(job_id, f"📊 Estimated cell usage after trim: {total_cells} cells")

            request = {
                'updateSheetProperties': {
                    'properties': {
                        'sheetId': sheet_id,
                        'gridProperties': {
                            'rowCount': new_row_count,
                            'columnCount': 20  # fixed
                        }
                    },
                    'fields': 'gridProperties(rowCount,columnCount)'
                }
            }

            sheet.batchUpdate(spreadsheetId=spreadsheet_id, body={'requests': [request]}).execute()
            log(job_id, f"🧹 Trimmed sheet '{tab_name}' to {new_row_count} rows")
            return True

        except Exception as e:
            print(f"🔴 Failed to trim: {str(e)}")
            return False

    def safe_execute(self, job_id, request, retries=3, delay=5):
        for attempt in range(retries):
            try:
                return request.execute()
            except (ConnectionResetError, HttpError) as e:
                wait = delay * (2 ** attempt)  # exponential backoff
                log(job_id, f"[Retry {attempt + 1}] Error: {e}. Waiting {wait}s before retry...")
                time.sleep(wait)
        raise Exception("Google Sheets API call failed after multiple retries")

    def transfer_insight_data(self, job_id, spreadsheet_id: str, tab_name: str, insights_data: list, followers: dict, date: str = None):
        try:
            print(f"\n📅 DAILY INSIGHTS DUMP FOR {date or 'today'}")
            service = self._initialize_google_sheets_service(job_id)
            sheet = service.spreadsheets()

            # 1. Set up dates (YYYY-MM-DD)
            today = datetime.now(timezone.utc).date()
            processing_date = date or today.strftime('%Y-%m-%d')
            yesterday = (today - timedelta(days=1))
            compare_date = (today - timedelta(days=2)).strftime('%Y-%m-%d')  # ⬅️ string now
            if not insights_data:
                print("⚠️ No posts processed today")
                return False
            
            # 2. Check if processing_date already exists in Column H (row 2)
            existing_data = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!Q3",
                majorDimension="ROWS"
            ).execute().get('values', [[]])

            if existing_data and existing_data[0] and existing_data[0][0] == yesterday:
                log(job_id, f"⏭️ Data for {yesterday} already exists - skipping")
                return True
            
            # i need to fetched all the data on last update 
            # then performed the checking based on post_id located on each row column G3 and below if matched i need to get that row print it for now
            # on G3 its a post url thats why i have this function extract_facebook_post_id its returning post_id
            # 2.5 Fetch all existing post URLs (Column G from G3 downward) and rows
            existing_rows = sheet.values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A3:R",
                majorDimension="ROWS"
            ).execute().get('values', [])

            # 3. Prepare today's data
            new_rows = []
            for post in insights_data:
                insights = post.get('insights', {})
                date_str = post.get('created_time', yesterday) # fallback to yesterday if missing
                created_date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
                only_date = created_date.date().isoformat()  # 'YY-MM-DD'

                # Convert only_date back to date object
                posted_date = datetime.strptime(only_date, "%Y-%m-%d").date()
                # Calculate post age
                post_age = (yesterday - posted_date).days

                # Convert insights post links into a set of post_ids
                incoming_post_id = self.extract_facebook_post_id(post.get("post_link", ""))
                # Match rows where:
                deltas = {
                    "reach_3": '',
                    "reach_7": '',
                    "reach_30": '',
                    "imp_3": '',
                    "imp_7": '',
                    "imp_30": '',
                    "react_3": '',
                    "react_7": '',
                    "react_30": ''
                }
                for row_index, row in enumerate(existing_rows):
                    # Ensure row has at least 15 columns (to access P and Q)
                    if len(row) >= 15:
                        post_url = row[15]
                        last_update_date = row[16]
                        
                        existing_post_id = self.extract_facebook_post_id(post_url)
                        if existing_post_id == incoming_post_id and last_update_date == compare_date:
                            print(f"🔁 Matched Row {row_index + 3} (Updated on {last_update_date}): {row}")
                            deltas = self.calculate_day_deltas(job_id, post_age, insights, row)
                            # break
                        # else:
                            # print(f"❌ No match for Row {row_index + 3}") 
                    # break

                # print(deltas)
                if post_age == 3 and deltas['reach_3'] == '':
                    deltas['reach_3'] = str(insights.get('reach', ''))
                    deltas['imp_3'] = str(insights.get('impressions', ''))
                    deltas['react_3'] = str(insights.get('reactions', ''))

                if post_age == 7 and deltas['reach_7'] == '':
                    deltas['reach_7'] = str(insights.get('reach', ''))
                    deltas['imp_7'] = str(insights.get('impressions', ''))
                    deltas['react_7'] = str(insights.get('reactions', ''))
                
                new_rows.append([
                    str(followers['followers_count']),
                    only_date,
                    post.get('message', '')[:500],
                    deltas["reach_3"],
                    deltas["reach_7"],
                    deltas["reach_30"],
                    str(insights.get('reach', 0)),
                    deltas["imp_3"],
                    deltas["imp_7"],
                    deltas["imp_30"],
                    str(insights.get('impressions', 0)),
                    deltas["react_3"],
                    deltas["react_7"],
                    deltas["react_30"],
                    str(insights.get('reactions', 0)),
                    post.get('post_link', ''),
                    yesterday.strftime('%Y-%m-%d'),
                    post_age
                ])

            # Normalize row lengths (pad to equal length)
            num_columns = max(len(row) for row in new_rows)
            for row in new_rows:
                while len(row) < num_columns:
                    row.append("")

            # 4. Get sheet ID
            sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
            sheet_id = next(s['properties']['sheetId'] 
                    for s in sheet_metadata['sheets'] 
                    if s['properties']['title'] == tab_name)
            
            # 🔧 Expand the sheet's column count if needed
            sheet_properties = next(s for s in sheet_metadata['sheets'] if s['properties']['title'] == tab_name)
            current_columns = sheet_properties['properties']['gridProperties'].get('columnCount', 0)

            if num_columns > current_columns:
                print(f"📐 Expanding columns from {current_columns} → {num_columns}")
                resize_request = {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "columnCount": num_columns
                            }
                        },
                        "fields": "gridProperties.columnCount"
                    }
                }
            else:
                resize_request = None

            # 5. Create centered cell format
            centered_format = {
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE"
            }
            # Trim the sheet to avoid breaching 10M cell limit
            self.trim_sheet_rows(job_id, spreadsheet_id, tab_name)
            # 6. Batch requests (insert rows + format all cells)
            requests = []

            # ⬅️ Make sure to insert column resize request FIRST if needed
            if resize_request:
                requests.append(resize_request)
            
            requests.extend([
                # Insert blank rows
                {
                    'insertDimension': {
                        'range': {
                            'sheetId': sheet_id,
                            'dimension': 'ROWS',
                            'startIndex': 2,# Row 3 (0-based index 2)
                            'endIndex': 2 + len(new_rows)
                        },
                        'inheritFromBefore': False
                    }
                },
                # Apply center alignment to headers (row 1)
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 0,
                            'endRowIndex': 1,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'cell': {
                            'userEnteredFormat': centered_format
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                    }
                },
                # Apply center alignment to new data
                {
                    'repeatCell': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 2 + len(new_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'cell': {
                            'userEnteredFormat': centered_format
                        },
                        'fields': 'userEnteredFormat(horizontalAlignment,verticalAlignment)'
                    }
                },
                # Insert data values
                {
                    'updateCells': {
                        'range': {
                            'sheetId': sheet_id,
                            'startRowIndex': 2,
                            'endRowIndex': 2 + len(new_rows),
                            'startColumnIndex': 0,
                            'endColumnIndex': num_columns
                        },
                        'rows': [{'values': [
                            {'userEnteredValue': {'stringValue': str(value)}}
                            for value in row
                        ]} for row in new_rows],
                        'fields': 'userEnteredValue'
                    }
                }
            ])

            # 7. Execute batch update
            request = sheet.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'requests': requests}
            )
            self.safe_execute(job_id, request)

            log(job_id, f"✅ Added {len(new_rows)} centered records for {yesterday}")
            return True
        
        except HttpError as e:
            print(f"🔴 Sheets API Error: {str(e)}")
            if e.resp.status == 429:
                log(job_id, "⏳ Rate limited - waiting 60 seconds...")
                time.sleep(60)
                return self.transfer_insight_data(job_id, spreadsheet_id, tab_name, insights_data, followers, date)
            return False
        except Exception as e:
            print(f"🔴 Critical Failure: {str(e)}")
            return False
    
    def hide_old_rows(self, job_id, spreadsheet_id: str, tab_name: str):
        service = self._initialize_google_sheets_service(job_id)
        sheet = service.spreadsheets()
        
        # Get all dates in Column Q (index 16, so range Q3 down)
        result = sheet.values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!Q3:Q",
            majorDimension="COLUMNS"
        ).execute()

        values = result.get("values", [[]])[0]
        old_yesterday = (datetime.now().date() - timedelta(days=2))

        rows_to_hide = []
        for i, val in enumerate(values):
            try:
                row_date = datetime.strptime(val.strip(), "%Y-%m-%d").date()
                if row_date <= old_yesterday:
                    rows_to_hide.append(i + 2)  # index starts at 0, row 3 = index 0 + 2
            except Exception:
                continue

        if not rows_to_hide:
            log(job_id, "ℹ️ No rows to hide.")
            return
        
        # Get the sheet ID
        sheet_metadata = sheet.get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = next(
            s["properties"]["sheetId"]
            for s in sheet_metadata["sheets"]
            if s["properties"]["title"] == tab_name
        )

        requests = [{
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row,
                    "endIndex": row + 1,
                },
                "properties": {"hiddenByUser": True},
                "fields": "hiddenByUser",
            }
        } for row in rows_to_hide]

        body = {"requests": requests}
        sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

        log(job_id, f"✅ Hidden {len(rows_to_hide)} rows with date <= yesterday.")