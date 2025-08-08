
from app.config.loader import NSU_FTD_TRACKER_SHEET
from app.constant.tracker import START_DATE, END_DATE, TRACKER_RANGE
from app.debug.line import debug_line, debug_title
# from app.helpers.conversion.conversion import build_social_row,build_affiliate_row,build_affiliate_row_socmed
from app.controllers.tracker.SpreadSheetController import SpreadsheetController
from app.controllers.tracker.AffiliateController import AffiliateController
# from app.helpers.conversion.spreadsheet import SpreadsheetController as Sheet
from app.helpers.tracker.TrackerSpreadsheet import spreadsheet
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta
import threading


def process_sheet(job_id, sheet_id, range, date):
    try:
        log(job_id, "Processing USER SHEETS")
        # Fetch and sort accounts
        gs = SpreadsheetController(sheet_id, range).get_account()
        gs_sorted = sorted(gs, key=lambda row: row[0])
        log(job_id, "Returned running accounts:")

        # Process each row
        brand_urls = {
            "BAJI": "https://bajipartners.xyz/page/affiliate/login.jsp",
            "JB": "https://jeetbuzzpartners.com/page/affiliate/login.jsp",
            "6S": "https://6saffiliates.com/page/affiliate/login.jsp"
        }
        target_brands = brand_urls.keys()
        all_data = []
        for row in gs_sorted:
            brand = row[0].strip()

            if brand not in target_brands:
                log(job_id, f"Brand not matched: {brand}")
                continue

            log(job_id, f"Processing Brand: {brand}")
            username = row[1].strip()
            password = row[2].strip()
            currency = row[3].strip()
            platform = row[5].strip()
            login_url = brand_urls[brand]

            data = AffiliateController(
                login_url,
                brand,
                username,
                password,
                currency,
                platform,
                date,
            )
            result = data.run(job_id)
            all_data.extend(
                result
            )

        return all_data

    except Exception as e:
            log(job_id, f"❌ ERROR in : {e}")
            print(f" ERROR: {e}")


def run(job_id):
    log(job_id, "🚀 Running Conversion Automation...")
    debug_title("Running NSU/FTD Tracker Automation...")
    # print(USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID)
    debug_line()

    log(job_id, "🚀 Running Tracker...")
    debug_title("Running Tracker...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()
    today = datetime.today()

    if today.weekday() == 0: #monday=0, sunday=6
        # Last Friday, Saturday, Sunday
        dates_to_process = [
            today - timedelta(days=3),  # Friday
            today - timedelta(days=2),  # Saturday
            today - timedelta(days=1)   # Sunday
        ]
    else:
        # Just yesterday
        dates_to_process = [today - timedelta(days=1)]
    
    for date in dates_to_process:
        date_str = date.strftime("%m/%d/%Y")

        all_data = process_sheet(
            job_id,
            NSU_FTD_TRACKER_SHEET, TRACKER_RANGE["USER"], date_str,
        )

        for row in all_data:
            log(job_id, row)
        
        log(job_id, "Scraping Process Finished")    

        #COMMENTED FOR NOW THIS IS FOR TRANSFERRING TO SHEET
        log(job_id, f" Writing rows to spreadsheet…")
        gs = spreadsheet (
            all_data,
            NSU_FTD_TRACKER_SHEET, TRACKER_RANGE
        ).transfer(job_id)

    log(job_id, "✅ Job complete")