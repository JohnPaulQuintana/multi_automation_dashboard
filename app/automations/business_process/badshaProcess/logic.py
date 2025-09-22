
from app.config.loader import (
    BP_USERNAME, BP_PASSWORD, BP_DRIVE, BP_DAILY, BP_WEEKLY, BP_MONTHLY, BP_BADSHA_SHEET
)
from app.debug.line import debug_line, debug_title
from app.controllers.business_process.badsha_process.badshaProcessController import badshaProcessController
from app.helpers.business_process.badsha_process.badshaSpreadsheet import badshaSpreadsheet
from app.helpers.business_process.badsha_process.badshaTransferData import badshaTransferData
from app.api.business_process.googledrive import googledrive
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta


def badsha_process_data(job_id, username, password, startDate, endDate, brand, time_grain, drive_url, sheet_url):
    log(job_id, f"🚀 Starting BO Basha: {username}")
    url = "https://ag.badsha.live/index.jsp"
    data = badshaProcessController(
        username,
        password,
        url,
        startDate,
        endDate,
        time_grain
    )
    result = data.run(job_id)

    if not (result and isinstance(result, dict) and "status" in result and result["status"] == 200):
        raise ValueError(f"Scraping failed: 'No error message provided'")
    
    log(job_id, "Scraping Process Successfuly Completed")

    debug_title("Getting GoogleSheet URL")
    debug_line()
    gdrive = googledrive (
        drive_url,
        BP_DAILY,
        BP_WEEKLY,
        BP_MONTHLY,
        startDate,
        endDate,
        time_grain,
        brand
    )
    url = gdrive.process(job_id)

    if not (url and isinstance(url, dict) and "status" in url and url["status"] == 200):
        raise ValueError(f"Google Sheet Not Found: 'Invalid URL'")
    log(job_id, "Google Sheet Located Successfuly")

    debug_title("Fetching in GoogleSheet")
    debug_line()
    log(job_id, "Preparing to Fetch Data in Spreadsheet....")

    fetch_data = badshaSpreadsheet(
        result,
        sheet_url,
        url
        # startDate

    ).transfer(job_id)

    if not (fetch_data and isinstance(fetch_data, dict) and "status" in fetch_data and fetch_data["status"] == 200):
        return {
            "status": "Failed",
            "message": "Failed to Transfer or Copy data into Spreadsheet"
        }
    
    transfer_data = badshaTransferData(
        fetch_data,
        sheet_url,
        url,
        startDate,
        endDate,
        time_grain
    ).badsha_transfer_data(job_id)

    log(job_id, "Scraping Process is success Data has been Fetch")

def run(job_id, brand, currency, start_date, end_date, time_grain):
    log(job_id, "🚀 Running Badsha Process Automation...")
    
    
    debug_title("Running Badsha Process Automation...")
    debug_line()

    log(job_id, "🚀 Badsha Process...")
    debug_title("Badsha Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Start date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{BP_PASSWORD}, and {BP_USERNAME}")

    all_data = badsha_process_data(
        job_id,
        BP_USERNAME, BP_PASSWORD, startDate, endDate, brand, time_grain, BP_DRIVE, BP_BADSHA_SHEET
    )

    log(job_id, "Win BDT is Done Processing....")

    
    
    log(job_id, "✅ Job complete")