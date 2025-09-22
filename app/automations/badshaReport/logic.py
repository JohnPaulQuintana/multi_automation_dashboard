
from app.config.loader import BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA
from app.constant.badsha import DAILY_BO_BADSHA_RANGE, TODAY, TODAY_DATE, YESTERDAY, TIME
from app.debug.line import debug_line, debug_title
from app.helpers.conversion.conversion import build_social_row,build_affiliate_row,build_affiliate_row_socmed
from app.controllers.conversion.AcquisitionController import AcquisitionController
from app.controllers.conversion.SpreadSheetController import SpreadsheetController
from app.controllers.badsha.badshaController import BadshaController
from app.helpers.badsha.BadshaSpreadsheet import spreadsheet
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta


def process_data(job_id, username, password, sheet_url, startDate, endDate, time, sheet_range):
    log(job_id, f"🚀 Starting BO Basha: {username}")
    url = "https://ag.badsha.live/index.jsp"
    data = BadshaController(
        username,
        password,
        url,
        startDate,
        endDate,
        time
    )
    result = data.run(job_id)

    if not (result and isinstance(result, dict) and "status" in result and result["status"] == 200):
        raise ValueError(f"Scraping failed: 'No error message provided'")
    
    log(job_id, "Scraping Process Successfuly Completed")

    log(job_id, "Preparing to Fetch Data in Spreadsheet....")

    fetch_data = spreadsheet(
        result,
        sheet_url,
        sheet_range,
        startDate

    ).transfer(job_id)

    log(job_id, "Scraping Process is success Data has been Fetch")

def run(job_id, date):
    log(job_id, "🚀 Running BO BADSHA Automation...")
    
    
    debug_title("Running BO BADSHA Automation...")
    debug_line()

    log(job_id, "🚀 BO BADSHA...")
    debug_title("BO BADSHA...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_obj = datetime.strptime(date, "%Y-%m-%d")
    startDate = date_obj.strftime("%d-%m-%Y")
    log(job_id, f"Yesterday date: {startDate}")
    
    all_data = process_data(
        job_id,
        BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA, startDate, TODAY_DATE, TIME, DAILY_BO_BADSHA_RANGE,
    )

    # for row in all_data:
    #     log(job_id, row)
        
    # log(job_id, "Scraping and Fetching is Successfully Completed")    

        # COMMENTED FOR NOW THIS IS FOR TRANSFERRING TO SHEET
        # log(job_id, f" Writing rows to spreadsheet…")
        # gs = spreadsheet (
        #     all_data,
        #     NSU_FTD_TRACKER_SHEET, TRACKER_RANGE
        # ).transfer(job_id)
    log(job_id, "✅ Job complete")