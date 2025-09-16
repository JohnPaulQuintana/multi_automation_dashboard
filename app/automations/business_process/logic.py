from app.config.loader import (
    SS_USERNAME, SS_PASSWORD, 
    WB_USERNAME, WB_PASSWORD, WB_DRIVE, WB_DAILY, WB_WEEKLY, WB_MONTHLY, WB_WINBDT_SHEET, 
    BP_USERNAME, BP_PASSWORD, BP_DRIVE, BP_DAILY, BP_WEEKLY, BP_MONTHLY, BP_BADSHA_SHEET
)
from app.debug.line import debug_line, debug_title
from app.controllers.business_process.supersetScraping import supersetScraping
from app.helpers.business_process.businessSpreadsheet import Spreadsheet
from app.controllers.business_process.winbdt.winbdtController import winbdtController
from app.helpers.business_process.winbdt.winBdtSpreadsheet import winBdtSpreadsheet
from app.helpers.business_process.winbdt.transferData import transferData

from app.controllers.business_process.badsha_process.badshaProcessController import badshaProcessController
from app.helpers.business_process.badsha_process.badshaSpreadsheet import badshaSpreadsheet
from app.helpers.business_process.badsha_process.badshaTransferData import badshaTransferData

from app.api.business_process.googledrive import googledrive
from .schema import BusinessAutomationInput
from datetime import datetime
from app.automations.log.state import log  # ✅ import from new file
import json
import os

async def run(job_id, data: BusinessAutomationInput):
    log(job_id, "🚀 Running Business Process Automation...")
    debug_title("Running Business Process Automation...")

    log(job_id, f"✅ Started automation for brand: {data.brand}, currency: {data.currency}, timegrain: {data.timeGrain}, date: {data.startDate} to {data.endDate}")
    debug_line()

    if not all([data.brand, data.currency, data.timeGrain, data.startDate, data.endDate]):
        log(job_id, "❌ Missing required input fields.")
        raise RuntimeError("Missing brand, currency, timeGrain, startDate, or endDate.")
    
    try:
        data = supersetScraping(
            SS_USERNAME,
            SS_PASSWORD,
            data.brand,
            data.currency,
            data.timeGrain,
            data.startDate,
            data.endDate
        )
        result = await data.scraping(job_id)

        if not (result and isinstance(result, dict) and "status" in result and result["status"] == 200):
                raise ValueError(f"Scraping failed: {result['text', 'No error message provided']}")
        log(job_id, "Scraping Completed Successfully")
        log(job_id, "✅ Scraping and data saving completed successfully.")

        log(job_id, "Fetching data for spreadsheet processing.")
        # log(job_id, f"Merged Results: {json.dumps(result.get('data'), indent=2)}") Checking Result in Terminal or Log

        spreadsheet = Spreadsheet(
            data.brand,
            data.currency,
            data.timeGrain,
            data.startDate,
            data.endDate,
            data=result.get('data')
        )
        gs = spreadsheet.transfer(job_id)
        if not (gs and isinstance(gs, dict) and "status" in gs and gs["status"] == 200):
                raise ValueError(f"Scraping failed: {gs['text', 'No error message provided']}")
        log(job_id, "Fetching Completed Successfully")
    except Exception as e:
        # Handle errors that occurred during the process
        print(f"❌ Error: {e}")
        log(job_id, f"❌ Error: {e}")
            
    log(job_id, "✅ Job complete")



# ============= WINBDT ================
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

def winbdt(job_id, data: BusinessAutomationInput):
    log(job_id, "🚀 Running WinBdt Process Automation...")


    debug_title("Running WinBdt Process Automation...")
    debug_line()

    log(job_id, "🚀 WinBDT Process...")
    debug_title("WinBDT Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(data.startDate, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(data.endDate, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Yesterday date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{WB_PASSWORD}, and {WB_USERNAME}")
    
    all_data = process_data(
        job_id,
        WB_USERNAME, WB_PASSWORD, startDate, endDate, data.brand, data.timeGrain, WB_DRIVE, WB_WINBDT_SHEET
    )

    log(job_id, "Win BDT is Done Processing....")

    log(job_id, "✅ Job complete")

# ============= BADSHA ================
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

def badsha(job_id, data: BusinessAutomationInput):
    log(job_id, "🚀 Running Badsha Process Automation...")

    debug_title("Running Badsha Process Automation...")
    debug_line()

    log(job_id, "🚀 Badsha Process...")
    debug_title("Badsha Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(data.startDate, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(data.endDate, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Start date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{BP_PASSWORD}, and {BP_USERNAME}")
    
    all_data = badsha_process_data(
        job_id,
        BP_USERNAME, BP_PASSWORD, startDate, endDate, data.brand, data.timeGrain, BP_DRIVE, BP_BADSHA_SHEET
    )

    log(job_id, "Win BDT is Done Processing....")

    log(job_id, "✅ Job complete")