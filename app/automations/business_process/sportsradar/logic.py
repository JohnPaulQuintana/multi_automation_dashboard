
from app.config.loader import WB_USERNAME, WB_PASSWORD, WB_DRIVE, WB_WINBDT_SHEET
# from app.constant.badsha import DAILY_BO_BADSHA_RANGE, TODAY, TODAY_DATE, YESTERDAY, TIME
# from app.api.winbdt_process.googledrive import googledrive
from app.debug.line import debug_line, debug_title
# from app.controllers.winbdt_process.winbdtController import winbdtController
# from app.helpers.winbdt_process.winBdtSpreadsheet import spreadsheet
# from app.helpers.winbdt_process.transferData import transferData
from app.automations.log.state import log  # ✅ import from new file
from datetime import datetime, timedelta




def run(job_id, brand, currency, start_date, end_date, time_grain):
    log(job_id, "🚀 Running Sportsradar Process Automation...")
    
    
    debug_title("Running sportradar Process Automation...")
    debug_line()

    log(job_id, "🚀 SportsRadar Process...")
    debug_title("SportsRadar Process...")
    # print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()

    date_start = datetime.strptime(start_date, "%Y-%m-%d")
    startDate = date_start.strftime("%d-%m-%Y")

    date_end = datetime.strptime(end_date, "%Y-%m-%d")
    endDate = date_end.strftime("%d-%m-%Y")


    log(job_id, f"Yesterday date: {startDate}")
    log(job_id, f"Yesterday date: {endDate}")
    log(job_id, f"{brand}, {currency} {time_grain}")
    # log(job_id, f"{WB_PASSWORD}, and {WB_USERNAME}")
    
    # all_data = process_data(
    #     job_id,
    #     WB_USERNAME, WB_PASSWORD, startDate, endDate, time_grain, WB_DRIVE, WB_WINBDT_SHEET
    # )

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