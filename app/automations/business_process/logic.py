from app.config.loader import SS_USERNAME, SS_PASSWORD
from app.constant.conversion import YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES
from app.debug.line import debug_line, debug_title
from app.controllers.business_process.supersetScraping import supersetScraping
from app.controllers.business_process.businessSpreadsheet import Spreadsheet
from app.helpers.conversion.spreadsheet import SpreadsheetController as Sheet
from .schema import BusinessAutomationInput
from app.automations.log.state import log  # ✅ import from new file
import json
import os


# def delete_json(job_id, filepath="json/result.json"):
#     try:
#         if os.path.exists(filepath):
#             os.remove(filepath)
#             print(f"✅ Deleted {filepath} after successful automation.")
#             log(job_id, f"✅ Deleted {filepath} after successful automation.")
#             debug_line()
#     except Exception as e:
#         log(job_id, f"❌ Error deleting {filepath}: {e}")
#         print(f"❌ Error deleting {filepath}: {e}")


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