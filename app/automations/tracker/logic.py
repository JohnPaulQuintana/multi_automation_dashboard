
from app.config.loader import NSU_FTD_TRACKER_SHEET
from app.constant.tracker import START_DATE, END_DATE, TRACKER_RANGE
from app.debug.line import debug_line, debug_title
# from app.helpers.conversion.conversion import build_social_row,build_affiliate_row,build_affiliate_row_socmed
from app.controllers.tracker.SpreadSheetController import SpreadsheetController
# from app.helpers.conversion.spreadsheet import SpreadsheetController as Sheet
from app.automations.log.state import log  # ✅ import from new file

def process_sheet(job_id, sheet_id, range, startDate, endDate):
        try:
            log(job_id, f"Processing USER SHEETS")
            print(f"Processing USER SHEETS")


            gs = SpreadsheetController(sheet_id, range).get_account()
            gs_sorted = sorted(gs, key=lambda row: row[0])
            log(job_id, "Returned running accounts:")
            for row in gs_sorted:
                log(job_id, row)



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
    print("Hello World")
    log(job_id, "Hello World")
    
    # if not all([USERNAME, PASSWORD, SOCIAL_SHEET_ID, AFFILIATE_SHEET_ID]):
    #     log(job_id, "🚀 Missing BO_USERNAME / BO_PASSWORD / SOCIALMEDIA_SHEET / AFFILIATE_SHEET...")
    #     raise RuntimeError("Missing BO_USERNAME / BO_PASSWORD / SOCIALMEDIA_SHEET / AFFILIATE_SHEET")


    process_sheet(
        job_id,
        NSU_FTD_TRACKER_SHEET, TRACKER_RANGE["USER"], START_DATE, END_DATE
    )
    # # 1️⃣  SocialMedia ➜ fixed tab "*Daily_Data (Player)"
    # process_sheet(
    #     job_id,
    #     SOCIAL_SHEET_ID, SOCIAL_RANGES, build_social_row, build_affiliate_row_socmed, "SocialMedia",
    #     fixed_tab="*Daily_Data (Player)")

    # # 2️⃣  Affiliates ➜ tab derived from range ("Affiliates")
    # #     i.e. "Affiliates!A1:A" → "Affiliates"
    # process_sheet(
    #     job_id,
    #     AFFILIATE_SHEET_ID, AFFILIATE_RANGES, build_affiliate_row, build_affiliate_row_socmed,  # fixed_tab=None (default behavior, no fixed tab)
    #     type="Affiliates"  # type is not used in this context, but kept for consistency
    # )

    log(job_id, "✅ Job complete")