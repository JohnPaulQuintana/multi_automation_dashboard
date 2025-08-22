
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

def run(job_id, startDate):
    log(job_id, "🚀 Running BO BADSHA Automation...")
    log(job_id, f"This is the date: {startDate}")
    date_obj = datetime.strptime(startDate, "%Y-%m-%d")
    debug_title("Running BO BADSHA Automation...")
    
    formatted_date = date_obj.strftime("%d-%m-%Y")
    log(job_id, f"🚀 Automation started with startDate={formatted_date}")
    # print(USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID)
    debug_line()

    log(job_id, "🚀 BO BADSHA...")
    debug_title("BO BADSHA...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()
    # today = datetime.today()
    
    # log(job_id, "Scraping Process Finished")    
    # if TODAY.weekday() == 0: #monday=0, sunday=6
    #     # Last Friday, Saturday, Sunday
    #     dates_to_process = [
    #         TODAY - timedelta(days=3),  # Friday
    #         TODAY - timedelta(days=2),  # Saturday
    #         TODAY - timedelta(days=1)   # Sunday
    #     ]
    # else:
    #     # Just yesterday
    #     dates_to_process = [YESTERDAY]

    # for date in dates_to_process:

    #     startDate = date.strftime("%d-%m-%Y") # Yesterday Date
    #     endDate   = (date + timedelta(days=1)).strftime("%d-%m-%Y") # Today Date
    #     all_data = process_data(
    #         job_id,
    #         BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA, startDate, endDate, TIME, DAILY_BO_BADSHA_RANGE,
    #     )

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