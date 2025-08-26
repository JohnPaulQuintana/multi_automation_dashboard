from app.config.loader import ACCOUNT_SHEET_ID, FB_GAINED_SHEET_ID, IG_GAINED_SHEET_ID, YT_GAINED_SHEET_ID, TW_GAINED_SHEET_ID, CLIENT_SHEET_ID, FACEBOOK_BASE_API_URL, YOUTUBE_BASE_API_URL, TWITTER_BASE_API_URL, SPREADSHEET_RANGE, RAJI_ACCOUNT
# from app.constant.tracker import TRACKER_RANGE

# from app.controllers.tracker.SpreadSheetController import SpreadsheetController
from app.controllers.media.facebook.SpreadSheetController import SpreadSheetController
from app.controllers.media.instagram.IGSpreadSheetController import IGSpreadsheetController
from app.controllers.media.twitter.TwitterSpreadSheetController import TwitterSpreadsheetController
from app.controllers.media.youtube.YoutubeSpreadsheetController import YoutubeSpreadsheetController
from app.controllers.media.client.ClientSheetController import ClientSheetController
from app.controllers.media.facebook.FacebookController import FacebookController
from app.controllers.media.instagram.IGController import IGController
from app.controllers.media.twitter.TwitterController import TwitterController
from app.controllers.media.youtube.YoutubeController import YoutubeController
from app.helpers.media.Client_Helper import ClientHelper
from app.helpers.media.IG_Helper import IGHELPER
from app.helpers.media.Facebook_Helper import FacebookHelper

from app.automations.log.state import log  # ✅ import from new file
from app.debug.line import debug_line, debug_title

from datetime import datetime, timedelta, timezone
from collections import defaultdict
import json
import os
import re


def get_currency(job_id, currency, brand):
    log(job_id, "-------------------------")
    log(job_id, currency, brand)
    log(job_id, "-------------------------")

    curr = None
    if currency == "PKR" and brand=='BAJI':
        curr = "bajilive.casino"
    elif currency == "NPR" and brand=='BAJI':
        curr = "baji.sports"
    elif currency == "BDT" and brand=='JEETBUZZ':
        curr="jeetbuzzcasino"
    elif currency=="INR" and brand=="JEETBUZZ":
        curr="jeetbuzzsports"
    elif currency=="PKR" and brand=="SIX6S":
        curr="six6s.sport"
    elif currency=="INR" and brand=="SIX6S":
        curr="six6s.casino"
    elif currency=="BDT" and brand=="BADSHA":
        curr="BDT"
        
    return curr

def run(job_id):
    log(job_id, "🚀 Running Social Media Automation...")
    debug_title("Running Social Media Automation...")
    debug_line()

    log(job_id, "Begin the automation for followers gain....")

    # ON DEVELOPMENT
    # Get today's date string
    # today_str = datetime.now().strftime('%d/%m/%Y') # Current date

    # ON DEPLOYED
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    today_str = yesterday.strftime('%Y-%m-%d') #Yesterday date
    
    # read the spreadsheet data
    spreadsheet = SpreadSheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    ig_spreadsheet = IGSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    yt_spreadsheet = YoutubeSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    tw_spreadsheet = TwitterSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)

    # Initialize client
    client_sheet = ClientSheetController()

    accounts = spreadsheet.get_facebook_accounts(job_id)
    pages_sp = spreadsheet.get_facebook_pages(job_id)

    for account in accounts:

        # Verify if the account is active and token is valid
        # print(f"Processing account: {account[0]} with name: {account[3]}")
        # token_validator = FacebookTokenValidator(FACEBOOK_BASE_API_URL,account[6], account[7])
        # token_info = token_validator.check_token_validity(account[4])
        # print(f"Token info: {token_info}")
        #end of token validation

        facebookController = FacebookController(FACEBOOK_BASE_API_URL ,account)
        ig_Controller = IGController(FACEBOOK_BASE_API_URL)
        youtube_Controller = YoutubeController(YOUTUBE_BASE_API_URL)
        twitter_Controller = TwitterController(TWITTER_BASE_API_URL, account[4])
        client_helper = ClientHelper()

        if account[0].startswith("FB"):
            pages = facebookController.get_facebook_pages_with_instagram(job_id)

            pages_info = []  # Array of page info objects

            # get only badsha pages for this account ragi:
            if account[5] == RAJI_ACCOUNT:
                log(job_id, f"Processing account: {account[0]} with name: {account[3]} (RAJI ACCOUNT)")
                pages['data'] = [page for page in pages.get('data', []) if page.get('name') == 'Badsha']

            for page in pages.get('date', []):
                page_id = page.get('id', 0)
                page_access_token = page.get('access_token', 'xxxxxxxxxxxxx')
                ig = page.get('instagram_business_account', False)
                ig_id = ig.get('id', False) if ig else False

                # Match page_id to index 3
                matched_info = next((item for item in pages_sp if item[3] == page_id), None)

                if matched_info:
                    currency = matched_info[1]
                    brand = matched_info[2]
                    PAGE_TYPE = matched_info[4]
                    SPREAD_SHEET = matched_info[5]#facebook sheet
                    IG_SHEET = matched_info[6]

                    # Extract spreadsheet ID 
                    followers = facebookController.get_facebook_page_metrics(job_id, page_id, page_access_token, today_str)
                    log(job_id, f"Page ID: {page_id}, Followers: {followers}, Currency: {currency}, Brand: {brand}, Page Type: {PAGE_TYPE}")

                    ig_page_insights = ig_Controller.get_ig_page_metrics(job_id, page_id,ig_id,page_access_token)
                    if ig_page_insights:
                        log(job_id, "------------------------------------------------------------------------------")
                        log(job_id, ig_page_insights)
                        log(job_id, "------------------------------------------------------------------------------")

                        #processing ig page insights
                        ig_spreadsheet.get_ig_spreadsheet_column(job_id, IG_GAINED_SHEET_ID,brand,get_currency(currency,brand),ig_page_insights,ig_page_insights[0].get('followers_count', 0), PAGE_TYPE)        

                        #update client sheet
                        # Access monthly insights safely
                        monthly = ig_page_insights[0].get('monthly_insights', {})
                        monthly_impressions = monthly.get('impressions', 0)
                        monthly_engagements = monthly.get('engagements', 0)
                        # TARGET = "INSTAGRAM"
                        # if brand == 'BAJI' and matched_info[10] == 'BDT':
                        #     print("Target")

                        client_helper._process_data(
                                job_id, f"{matched_info[2]} {matched_info[10]}", CLIENT_SHEET_ID, "INSTAGRAM", client_sheet, 
                                [ig_page_insights[0].get('followers_count', 0), monthly_impressions, monthly_engagements]
                            )
                        
                    # get the target column and brand name
                    target_column = spreadsheet.get_spreadsheet_column(FB_GAINED_SHEET_ID,brand,currency,followers,followers['followers_count'], PAGE_TYPE)

                    #update client sheet
                    client_helper._process_data(
                            job_id, f"{matched_info[2]} {matched_info[10]}", CLIENT_SHEET_ID, matched_info[9], client_sheet, 
                            [followers['followers_count'], followers['page_impressions_monthly'], followers['page_post_engagements_monthly']]
                        )
                        
                    # print(target_column)
                    # Build the page info object
                    page_info = {
                        "page_id": page_id,
                        "instagram_id": ig_id,
                        "access_token": page_access_token,
                        "currency": currency,
                        "brand": brand,
                        "page_type": PAGE_TYPE,
                        "followers": followers,
                        "ig_followers": ig_page_insights[0].get('followers_count', 0) if ig_page_insights else 0,
                        "target_column": target_column,
                        "spreadsheet": SPREAD_SHEET,
                        "ig_spreadsheet": IG_SHEET
                    }

                    pages_info.append(page_info)
                else:
                    log(job_id, f"Page ID: {page_id} not found in page_info_list.")
                
                # transfer it to designated spreadsheet

            # 1. Get all pages and their tokens
            # page_tokens = [(page['id'], page['access_token']) for page in pages.get('data', [])]
            page_tokens = [
                (
                    page['id'],
                    page.get('access_token'),
                    page.get('instagram_business_account', {}).get('id')  # This could be None
                )
                for page in pages.get('data', [])
            ]

            # 2. Fetch all posts (now with page tracking)
            # # Get the current year and today’s date
            # current_year = datetime.now().year
            # today_date = datetime.now().strftime('%Y-%m-%d')
            # # Set the starting date to January 1st of the current year
            # start_date = f"{current_year}-01-01"
            #new updates
            # Get today’s date
            today = datetime.now()
            today_date = today.strftime('%Y-%m-%d')

            # Set the start date to 30 days before today
            start_date = (today - timedelta(days=31)).strftime('%Y-%m-%d')  # 29 to include today as the 30th day

            # # INSTAGRAM
            ig_posts_data = ig_Controller.fetch_all_ig_posts(job_id, page_tokens, start_date)
            log(job_id, "This is IG POST...")

            all_ig_insights = ig_Controller.process_all_post_insights(job_id, ig_posts_data)
            # Send to ig helper to process insights
            ig_helper = IGHELPER(all_ig_insights)
            log(job_id, "This is IG HELPER...")
            sorted_data = ig_helper.get_sorted_posts(True)
            ig_helper.process_ig_insights_by_ig_id(job_id, sorted_data, pages_info, ig_spreadsheet)


            # # FACEBBOOK
            posts_data = facebookController.fetch_all_posts_for_pages(job_id, page_tokens, start_date, today_date)
            all_facebook_insights = facebookController.process_all_pages_insights(job_id, posts_data)
            #Send to facebook helper to process insights
            log(job_id, "This is FACEBOOK HELPER...")
            facebook_helper = FacebookHelper(all_facebook_insights)
            sorted_data = facebook_helper.get_sorted_posts(True)
            facebook_helper.process_facebook_insights_by_page_id(job_id, sorted_data, pages_info, spreadsheet)
            # print(sorted_data)
    log(job_id, "✅ Job complete")