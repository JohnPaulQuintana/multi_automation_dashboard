from app.config.loader import ACCOUNT_SHEET_ID, FB_GAINED_SHEET_ID, IG_GAINED_SHEET_ID, YT_GAINED_SHEET_ID, TW_GAINED_SHEET_ID, CLIENT_SHEET_ID, FACEBOOK_BASE_API_URL, YOUTUBE_BASE_API_URL, TWITTER_BASE_API_URL, SPREADSHEET_RANGE, RAJI_ACCOUNT
from app.constant.tracker import TRACKER_RANGE
from app.debug.line import debug_line, debug_title
from app.controllers.tracker.SpreadSheetController import SpreadsheetController
from app.controllers.media.facebook.SpreadSheetController import SpreadsheetController
from app.controllers.tracker.AffiliateController import AffiliateController
from app.helpers.tracker.TrackerSpreadsheet import spreadsheet
from app.automations.log.state import log  # ✅ import from new file

from datetime import datetime, timedelta, timezone
from collections import defaultdict
import json
import os
import re


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
    spreadsheet = SpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    # ig_spreadsheet = IGSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    # yt_spreadsheet = YoutubeSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)
    # tw_spreadsheet = TwitterSpreadsheetController(ACCOUNT_SHEET_ID, SPREADSHEET_RANGE)

    # Initialize client
    # client_sheet = ClientSheetController()
    log(job_id, "✅ Job complete")