
from app.config.loader import USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID
from app.constant.conversion import YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES
from app.debug.line import debug_line, debug_title
from app.helpers.conversion.conversion import build_social_row,build_affiliate_row,build_affiliate_row_socmed
from app.controllers.conversion.AcquisitionController import AcquisitionController
from app.controllers.conversion.SpreadSheetController import SpreadsheetController
from app.helpers.conversion.spreadsheet import SpreadsheetController as Sheet
from app.automations.log.state import log  # ✅ import from new file

def fetch_dual(job_id, type, ac: AcquisitionController, kw, target_date, batch=3):
    """Return both Affiliates and SocialMedia data in one dict."""
    log(job_id, f"Fetching {type} data…")
    if type == "Affiliates":
        aff_rows = ac.fetch_bo_batched(job_id,"Affiliates",  kw, target_date, batch)["data"]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
        return {"data": aff_rows, "socmed_data": []}
    else:
        aff_rows = ac.fetch_bo_batched(job_id,"SocialMedia",  kw, target_date, batch)["data"]
        soc_rows = ac.fetch_bo_batched(job_id, "SocialMedia", kw, target_date, batch)["data_socmed"]
        return {"data": aff_rows, "socmed_data": soc_rows}

def process_sheet(job_id, sheet_id, ranges, row_builder, row_builder_socmed, type, fixed_tab=None):
    for brand, rng in ranges.items():
        try:
            log(job_id, f"Processing {type} - {brand}")
            print(f"Processing {type}: {brand}")

            kw = SpreadsheetController(sheet_id, rng).get_keywords()
            if not kw:
                log(job_id, f"⚠️ {brand}: No keywords")
                print(f"{brand}: no keywords")
                continue

            dest_sheet = kw[1] if len(kw) > 1 else sheet_id
            tab_name = fixed_tab or brand
            print(f"fixed_tab: {fixed_tab}, dest_sheet: {dest_sheet}, TabName: {tab_name}")

            data = AcquisitionController(
                email=USERNAME, password=PASSWORD,
                currency="all", currency_type=-1,
                brand=brand, targetdate=TARGET_DATE
            )
            out = fetch_dual(job_id, type, data, kw, TARGET_DATE)
            data_aff = out["data"]
            data_soc = out["socmed_data"]



            #COMMENTED FOR NOW THIS IS FOR TRANSFERRING TO SHEET
            print("Writing rows to spreadsheet…")
            # log(job_id, f" Writing rows to spreadsheet…")
            # rows = [row_builder(r, SHEET_DATE) for r in data_aff]
            # sheet = Sheet(spreadsheet=dest_sheet, tab=tab_name, type=type)
            # sheet.append_rows_return_last(rows, debug=True)
            # log(job_id, f"✅ {brand}: {len(rows)} rows → {tab_name}")
            # print("Done writing rows to spreadsheet.")

            # if not data_soc:
            #     log(job_id, f"{brand}: No SocialMedia rows")
            #     print("No Social‑Media rows found")
            # else:
            #     log(job_id, f" {len(data_soc)} Social‑Media rows")
            #     print(f"{len(data_soc)} Social‑Media rows")
            #     rows2 = [row_builder_socmed(r, SHEET_DATE) for r in data_soc]
            #     sheet2 = Sheet(spreadsheet=dest_sheet, tab="*Daily_Data (Aff)", type=type)
            #     sheet2.append_rows_return_last(rows2, debug=True)
            #     log(job_id, f"✅ {brand}: {len(rows2)} SocialMedia rows")

            # print(f"{brand}: {len(rows)} rows → {tab_name}")
            # log(job_id, f"✅ Processing {type} - {brand} completed...")
        except Exception as e:
            log(job_id, f"❌ ERROR in {brand}: {e}")
            print(f"{brand} ERROR: {e}")


def run(job_id):
    log(job_id, "🚀 Running Conversion Automation...")
    debug_title("Running Coversion Automation...")
    print(USERNAME,PASSWORD,SOCIAL_SHEET_ID,AFFILIATE_SHEET_ID)
    debug_line()

    log(job_id, "🚀 Running Contants...")
    debug_title("Running Contants...")
    print(YESTERDAY,TARGET_DATE,SHEET_DATE,SOCIAL_RANGES,AFFILIATE_RANGES)
    debug_line()
    
    if not all([USERNAME, PASSWORD, SOCIAL_SHEET_ID, AFFILIATE_SHEET_ID]):
        log(job_id, "🚀 Missing BO_USERNAME / BO_PASSWORD / SOCIALMEDIA_SHEET / AFFILIATE_SHEET...")
        raise RuntimeError("Missing BO_USERNAME / BO_PASSWORD / SOCIALMEDIA_SHEET / AFFILIATE_SHEET")

    # 1️⃣  SocialMedia ➜ fixed tab "*Daily_Data (Player)"
    process_sheet(
        job_id,
        SOCIAL_SHEET_ID, SOCIAL_RANGES, build_social_row, build_affiliate_row_socmed, "SocialMedia",
        fixed_tab="*Daily_Data (Player)")

    # 2️⃣  Affiliates ➜ tab derived from range ("Affiliates")
    #     i.e. "Affiliates!A1:A" → "Affiliates"
    process_sheet(
        job_id,
        AFFILIATE_SHEET_ID, AFFILIATE_RANGES, build_affiliate_row, build_affiliate_row_socmed,  # fixed_tab=None (default behavior, no fixed tab)
        type="Affiliates"  # type is not used in this context, but kept for consistency
    )

    log(job_id, "✅ Job complete")