
from app.config.loader import (
    WB_USERNAME, WB_PASSWORD, WB_DRIVE, WB_DAILY, WB_WEEKLY, WB_MONTHLY, WB_WINBDT_SHEET
)
# from app.constant.badsha import DAILY_BO_BADSHA_RANGE, TODAY, TODAY_DATE, YESTERDAY, TIME
# from app.api.winbdt_process.googledrive import googledrive
from app.debug.line import debug_line, debug_title
from app.controllers.business_process.winbdt.winbdtController import winbdtController
from app.helpers.business_process.winbdt.winBdtSpreadsheet import winBdtSpreadsheet
from app.helpers.business_process.winbdt.transferData import transferData
from app.api.business_process.googledrive import googledrive
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta


def process_data(job_id, username, password, startDate, endDate, brand, time_grain, drive_url, sheet_url):
    log(job_id, f"🚀 Starting BO Basha: {username}")
    url = "https://ag.winbdt.co/index.jsp"
    data = winbdtController(
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
        WB_DAILY,
        WB_WEEKLY,
        WB_MONTHLY,
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

    fetch_data = winBdtSpreadsheet(
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
    
    transfer_data = transferData(
        fetch_data,
        sheet_url,
        url,
        startDate,
        endDate,
        time_grain
    ).transfer_data(job_id)

    log(job_id, "Scraping Process is success Data has been Fetch")

def run(job_id, brand, currency, start_date, end_date, time_grain):
    log(job_id, "🚀 Running WINBDT Process Automation...")
    
    
    debug_title("Running WINBDT Process Automation...")
    debug_line()

    log(job_id, "🚀 WINBDT Process...")
    debug_title("WINBDT Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Start date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{brand}, {currency} {time_grain}")
    log(job_id, f"{WB_PASSWORD}, and {WB_USERNAME}")
    
    all_data = process_data(
        job_id,
        WB_USERNAME, WB_PASSWORD, startDate, endDate, brand, time_grain, WB_DRIVE, WB_WINBDT_SHEET
    )

    log(job_id, "Win BDT is Done Processing....")

    log(job_id, "✅ Job complete")