
from app.config.loader import BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA
from app.constant.badsha import DAILY_BO_BADSHA_RANGE, TODAY_DATE, YESTERDAY_DATE, TIME
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
    
    fetch_data = spreadsheet(
        result,
        sheet_url,
        sheet_range,
        endDate

    ).transfer(job_id)

    log(job_id, "Scraping Process is success Data has been Fetch")

def run(job_id):
    log(job_id, "🚀 Running BO BADSHA Automation...")
    debug_title("Running BO BADSHA Automation...")
    # print(USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID)
    debug_line()

    log(job_id, "🚀 BO BADSHA...")
    debug_title("BO BADSHA...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()
    today = datetime.today()
    all_data = process_data(
            job_id,
            BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA, TODAY_DATE, YESTERDAY_DATE, TIME, DAILY_BO_BADSHA_RANGE,
        )
    log(job_id, "Scraping Process Finished")    
    # if today.weekday() == 0: #monday=0, sunday=6
    #     # Last Friday, Saturday, Sunday
    #     dates_to_process = [
    #         today - timedelta(days=3),  # Friday
    #         today - timedelta(days=2),  # Saturday
    #         today - timedelta(days=1)   # Sunday
    #     ]
    # else:
    #     # Just yesterday
    #     dates_to_process = [today - timedelta(days=1)]

    # for date in dates_to_process:

    #     date_str = date.strftime("%m/%d/%Y")
        # all_data = process_data(
        #     job_id,
        #     BR_USERNAME, BR_PASSWORD, DAILY_BO_BADSHA, date_str,
        # )

        # for row in all_data:
        #     log(job_id, row)
        
        # log(job_id, "Scraping Process Finished")    

        #COMMENTED FOR NOW THIS IS FOR TRANSFERRING TO SHEET
        # log(job_id, f" Writing rows to spreadsheet…")
        # gs = spreadsheet (
        #     all_data,
        #     NSU_FTD_TRACKER_SHEET, TRACKER_RANGE
        # ).transfer(job_id)
    log(job_id, "✅ Job complete")