
from app.config.loader import NSU_FTD_TRACKER_SHEET
from app.constant.tracker import TRACKER_RANGE
from app.debug.line import debug_line, debug_title
from app.controllers.tracker.SpreadSheetController import SpreadsheetController
from app.controllers.tracker.AffiliateController import AffiliateController
from app.helpers.tracker.TrackerSpreadsheet import spreadsheet
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta
import threading


def process_brand(job_id, brand, accounts, brand_urls, date, failed_accounts):
    log(job_id, f"🚀 Starting brand: {brand}")
    all_data = []

    for row in accounts:
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
        if result:
            all_data.extend(result)
        else:
            log(job_id, f"⚠️ No data returned for {username}, skipping.")
            failed_accounts.append(username)

    log(job_id, f"✅ Finished brand: {brand} ({len(all_data)} records)")
    return all_data

def process_thread(job_id, sheet_id, range, date, failed_accounts):
    log(job_id, "Processing USER SHEETS")
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

    # Group accounts by brand
    accounts_by_brand = {brand: [] for brand in target_brands}
    for row in gs_sorted:
        brand = row[0].strip()
        if brand in target_brands:
            accounts_by_brand[brand].append(row)
        else:
            log(job_id, f"Brand not matched: {brand}")
    
    threads = []
    results = []
    def thread_runner(b):
        data = process_brand(job_id, b, accounts_by_brand[b], brand_urls, date, failed_accounts)
        results.extend(data)

    for brand in target_brands:
        if accounts_by_brand[brand]:
            t = threading.Thread(target=thread_runner, args=(brand,))
            threads.append(t)
            t.start()

    for t in threads:
        t.join()

    return results

def run(job_id):
    log(job_id, "🚀 Running NSU/FTD Tracker Automation...")
    debug_title("Running NSU/FTD Tracker Automation...")
    # print(USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID)
    debug_line()

    log(job_id, "🚀 Running Tracker...")
    debug_title("Running Tracker...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()
    today = datetime.today()
    failed_accounts = []
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

        all_data = process_thread(
            job_id,
            NSU_FTD_TRACKER_SHEET, TRACKER_RANGE["USER"], date_str, failed_accounts,  
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

    if failed_accounts:
        log(job_id, "⚠️ The following accounts returned no data:")
        for username in failed_accounts:
            log(job_id, f" - {username}")
    else:
        log(job_id, "✅ All accounts returned data.")
    log(job_id, "✅ Job complete")