
from app.config.loader import WB_USERNAME, WB_PASSWORD, WB_DRIVE
# from app.constant.badsha import DAILY_BO_BADSHA_RANGE, TODAY, TODAY_DATE, YESTERDAY, TIME
from app.api.winbdt_process.googledrive import googledrive
from app.debug.line import debug_line, debug_title
from app.controllers.badsha.badshaController import BadshaController
from app.helpers.badsha.BadshaSpreadsheet import spreadsheet
from app.controllers.winbdt_process.winbdtController import winbdtController
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta


def process_data(job_id, username, password, startDate, endDate, drive_url):
    log(job_id, f"🚀 Starting BO Basha: {username}")
    url = "https://ag.winbdt.co/index.jsp"
    # data = winbdtController(
    #     username,
    #     password,
    #     url,
    #     startDate,
    #     endDate
    # )
    # result = data.run(job_id)

    # if not (result and isinstance(result, dict) and "status" in result and result["status"] == 200):
    #     raise ValueError(f"Scraping failed: 'No error message provided'")
    
    log(job_id, "Scraping Process Successfuly Completed")

    gdrive = googledrive (
        drive_url,
        startDate,
        endDate
    ).process(job_id)

    # log(job_id, "Preparing to Fetch Data in Spreadsheet....")

    # fetch_data = spreadsheet(
    #     result,
    #     sheet_url,
    #     sheet_range,
    #     startDate

    # ).transfer(job_id)

    log(job_id, "Scraping Process is success Data has been Fetch")

def run(job_id, start_date, end_date):
    log(job_id, "🚀 Running WinBdt Process Automation...")
    
    
    debug_title("Running WinBdt Process Automation...")
    debug_line()

    log(job_id, "🚀 WinBDT Process...")
    debug_title("WinBDT Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Yesterday date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{WB_PASSWORD}, and {WB_USERNAME}")
    
    all_data = process_data(
        job_id,
        WB_USERNAME, WB_PASSWORD, startDate, endDate, WB_DRIVE
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