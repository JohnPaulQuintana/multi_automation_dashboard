from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
YESTERDAY        = (datetime.now() - timedelta(days=1)).date()
YESTERDAY_DATE = YESTERDAY.strftime("%d-%m-%Y")   # 2025/07/14
TODAY_DATE  = datetime.now().strftime("%d-%m-%Y")   # 14/07/2025
TIME = "12:00:00"


DAILY_BO_BADSHA_RANGE = {
    "NSU": "NSU DATA!D4:R",
    "FTD": "FTD DATA!D4:P",
    "DEPOSIT": "DEPOSIT!D4:P",
    "WITHDRAWAL": "WITHDRAWAL!D4:P",
    "VT/APL/TPL": "VT/APL/TPL!D4:R"
}
