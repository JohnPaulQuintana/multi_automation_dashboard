from datetime import datetime, timedelta

# ────────────────────── CONSTANTS ────────────────────────
YESTERDAY = (datetime.now() - timedelta(days=1)).date()
START_DATE = YESTERDAY.strftime("%m/%d/%Y")   # 08/04/2025
END_DATE = datetime.now().strftime("%m/%d/%Y")


TRACKER_RANGE = {
    "USER": "USER!A2:H",
    "BAJI": "BAJI",
    "6S": "6S",
    "JB": "JB",
}